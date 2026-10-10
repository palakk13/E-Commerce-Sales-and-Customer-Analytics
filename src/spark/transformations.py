"""
PySpark Transformation & Business Analytics Engine.

Performs:
1. Multi-way distributed DataFrame joins (items + orders + products + customers).
2. Financial metric calculations (Gross Revenue, Discounts, Net Revenue, COGS, Profit, Margin).
3. Window function analytical aggregations (customer purchase sequence, cumulative spend, running monthly revenue, MoM growth).
4. RFM analytical feature engineering.
5. Exporting curated analytical datasets as Parquet and CSV.
"""

from pathlib import Path
from typing import Dict, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from config.logging_config import setup_logger
from config.pipeline_config import config

logger = setup_logger("spark_transformations")


def load_clean_datasets(spark: SparkSession, processed_dir: Optional[Path] = None) -> Dict[str, DataFrame]:
    """
    Reads cleaned CSV files from data/processed/ into PySpark DataFrames with explicit typing.

    Args:
        spark: Active SparkSession.
        processed_dir: Directory containing cleaned datasets.

    Returns:
        Dictionary of PySpark DataFrames for customers, products, orders, and order_items.
    """
    p_dir = Path(processed_dir) if processed_dir else config.paths.processed_dir
    logger.info("Loading cleaned datasets from: %s", p_dir.resolve())

    # 1. Customers
    cust_path = p_dir / "customers_cleaned.csv"
    customers_df = (
        spark.read.option("header", "true")
        .option("inferSchema", "false")
        .csv(str(cust_path))
        .select(
            F.col("customer_id").cast("string"),
            F.col("customer_name").cast("string"),
            F.col("email").cast("string"),
            F.col("phone").cast("string"),
            F.col("city").cast("string"),
            F.col("state").cast("string"),
            F.col("country").cast("string"),
            F.col("postal_code").cast("string"),
            F.to_date(F.col("signup_date"), "yyyy-MM-dd").alias("signup_date"),
        )
    )

    # 2. Products
    prod_path = p_dir / "products_cleaned.csv"
    products_df = (
        spark.read.option("header", "true")
        .option("inferSchema", "false")
        .csv(str(prod_path))
        .select(
            F.col("product_id").cast("string"),
            F.col("product_name").cast("string"),
            F.col("category").cast("string"),
            F.col("sub_category").cast("string"),
            F.col("unit_cost").cast("double"),
            F.col("list_price").cast("double"),
        )
    )

    # 3. Orders
    orders_path = p_dir / "orders_cleaned.csv"
    orders_df = (
        spark.read.option("header", "true")
        .option("inferSchema", "false")
        .csv(str(orders_path))
        .select(
            F.col("order_id").cast("string"),
            F.col("customer_id").cast("string"),
            F.to_date(F.col("order_date"), "yyyy-MM-dd").alias("order_date"),
            F.col("order_status").cast("string"),
            F.col("payment_method").cast("string"),
            F.col("shipping_cost").cast("double"),
        )
    )

    # 4. Order Items
    items_path = p_dir / "order_items_cleaned.csv"
    items_df = (
        spark.read.option("header", "true")
        .option("inferSchema", "false")
        .csv(str(items_path))
        .select(
            F.col("order_item_id").cast("string"),
            F.col("order_id").cast("string"),
            F.col("product_id").cast("string"),
            F.col("quantity").cast("integer"),
            F.col("unit_price").cast("double"),
            F.col("discount_percent").cast("double"),
        )
    )

    logger.info(
        "Clean datasets loaded: customers=%d, products=%d, orders=%d, items=%d",
        customers_df.count(),
        products_df.count(),
        orders_df.count(),
        items_df.count(),
    )

    return {
        "customers": customers_df,
        "products": products_df,
        "orders": orders_df,
        "order_items": items_df,
    }


def join_ecommerce_datasets(
    items_df: DataFrame,
    orders_df: DataFrame,
    products_df: DataFrame,
    customers_df: DataFrame,
) -> DataFrame:
    """
    Executes multi-way relational joins to produce an enriched sales event DataFrame.
    Validates join keys to guarantee no row duplication or orphan record loss.
    """
    initial_items_count = items_df.count()
    logger.info("Executing multi-way joins on line items (initial count = %d)...", initial_items_count)

    # Step 1: Join Line Items with Orders on order_id
    items_orders = items_df.join(orders_df, on="order_id", how="inner")

    # Step 2: Join with Products on product_id
    items_orders_products = items_orders.join(products_df, on="product_id", how="inner")

    # Step 3: Join with Customers on customer_id
    enriched_df = items_orders_products.join(customers_df, on="customer_id", how="inner")

    final_count = enriched_df.count()
    logger.info("Joined DataFrame produced: %d rows (matches initial items: %s)", final_count, final_count == initial_items_count)

    if final_count != initial_items_count:
        raise ValueError(
            f"Join integrity violation! Initial items ({initial_items_count}) != Joined rows ({final_count})."
        )

    return enriched_df


def compute_financial_measures(df: DataFrame) -> DataFrame:
    """
    Computes all standard business financial metrics and order fulfillment indicators.

    Separation of Gross Demand vs. Recognized Revenue:
    - Gross Demand (all valid orders):
        gross_revenue = quantity * unit_price
        discount_amount = gross_revenue * (discount_percent / 100.0)
        net_revenue = gross_revenue - discount_amount
        total_cost (COGS) = quantity * unit_cost
        profit = net_revenue - total_cost
        profit_margin_pct = (profit / net_revenue) * 100.0

    - Strict Delivery-Based Revenue Recognition (FOB Destination Rule):
        Delivered: Revenue and profit are recognized (control transferred).
        Shipped: Inventory in transit, tracked as in_transit_revenue.
        Processing: Order confirmed pending dispatch, tracked as pipeline_revenue.
        Cancelled: Pre-dispatch void, tracked as cancelled_revenue_loss.
        Returned: Post-delivery customer return, tracked as returned_revenue_loss.
    """
    logger.info("Computing business measures and financial metrics...")

    # Financial line-item calculations
    gross_rev = F.round(F.col("quantity") * F.col("unit_price"), 2)
    disc_amt = F.round(gross_rev * (F.col("discount_percent") / 100.0), 2)
    net_rev = F.round(gross_rev - disc_amt, 2)
    cogs = F.round(F.col("quantity") * F.col("unit_cost"), 2)
    profit = F.round(net_rev - cogs, 2)

    profit_margin = F.when(
        net_rev > 0,
        F.round((profit / net_rev) * 100.0, 2)
    ).otherwise(0.0)

    # Operational status indicators (mutually exclusive)
    is_delivered = F.when(F.col("order_status") == "Delivered", 1).otherwise(0)
    is_shipped = F.when(F.col("order_status") == "Shipped", 1).otherwise(0)
    is_processing = F.when(F.col("order_status") == "Processing", 1).otherwise(0)
    is_returned = F.when(F.col("order_status") == "Returned", 1).otherwise(0)
    is_cancelled = F.when(F.col("order_status") == "Cancelled", 1).otherwise(0)

    # Strict Revenue Recognition: Delivered sales only
    rec_revenue = F.when(is_delivered == 1, net_rev).otherwise(0.0)
    rec_profit = F.when(is_delivered == 1, profit).otherwise(0.0)

    # Pipeline and attrition breakdowns
    in_transit_rev = F.when(is_shipped == 1, net_rev).otherwise(0.0)
    pipeline_rev = F.when(is_processing == 1, net_rev).otherwise(0.0)
    returned_loss = F.when(is_returned == 1, net_rev).otherwise(0.0)
    cancelled_loss = F.when(is_cancelled == 1, net_rev).otherwise(0.0)

    with_metrics = (
        df.withColumn("gross_revenue", gross_rev)
        .withColumn("discount_amount", disc_amt)
        .withColumn("net_revenue", net_rev)
        .withColumn("total_cost", cogs)
        .withColumn("profit", profit)
        .withColumn("profit_margin_pct", profit_margin)
        .withColumn("is_delivered", is_delivered)
        .withColumn("is_shipped", is_shipped)
        .withColumn("is_processing", is_processing)
        .withColumn("is_returned", is_returned)
        .withColumn("is_cancelled", is_cancelled)
        .withColumn("recognized_revenue", rec_revenue)
        .withColumn("recognized_profit", rec_profit)
        .withColumn("in_transit_revenue", in_transit_rev)
        .withColumn("pipeline_revenue", pipeline_rev)
        .withColumn("returned_revenue_loss", returned_loss)
        .withColumn("cancelled_revenue_loss", cancelled_loss)
    )

    return with_metrics


def apply_window_metrics(df: DataFrame) -> DataFrame:
    """
    Applies distributed PySpark window functions:
    1. Customer purchase sequence rank: Identifies order sequence (1st, 2nd, 3rd, ...).
    2. Customer cumulative recognized spend: Running total of recognized revenue per customer.
    """
    logger.info("Applying PySpark window functions for customer sequence and cumulative spend...")

    # Window 1: Customer purchase sequence (order level sequence partitioned by customer)
    cust_order_window = Window.partitionBy("customer_id").orderBy(F.col("order_date").asc(), F.col("order_id").asc())

    # Window 2: Customer cumulative spend running total (unbounded preceding to current row)
    cust_running_window = (
        Window.partitionBy("customer_id")
        .orderBy(F.col("order_date").asc(), F.col("order_id").asc())
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    with_windows = (
        df.withColumn("customer_order_rank", F.dense_rank().over(cust_order_window))
        .withColumn(
            "customer_cumulative_spend",
            F.round(F.sum("recognized_revenue").over(cust_running_window), 2),
        )
    )

    return with_windows


def generate_rfm_features(
    sales_df: DataFrame,
    customers_df: DataFrame,
    snapshot_date: Optional[str] = None,
) -> DataFrame:
    """
    Constructs the customer-level analytical dataset for RFM customer segmentation.

    Metrics (Strictly Delivered Revenue):
    - Recency (R): Days since the customer's most recent Delivered order relative to snapshot date.
    - Frequency (F): Total count of distinct Delivered orders placed by customer.
    - Monetary (M): Total recognized net revenue spent by customer across Delivered orders.
    - Diagnostic features: Total orders placed, shipped count, returned count, cancelled count, AOV.
    """
    logger.info("Generating customer RFM feature matrix (strict Delivered order accounting)...")

    # Determine snapshot date: default to max(order_date)
    if snapshot_date is None:
        max_dt = sales_df.select(F.max("order_date")).collect()[0][0]
        ref_date = max_dt.strftime("%Y-%m-%d")
    else:
        ref_date = snapshot_date

    logger.info("Using RFM snapshot reference date: %s", ref_date)

    # Aggregate customer order performance: only Delivered orders count toward R, F, M
    cust_agg = sales_df.groupBy("customer_id").agg(
        # Recency inputs: latest Delivered order
        F.min(F.when(F.col("is_delivered") == 1, F.col("order_date"))).alias("first_delivered_order_date"),
        F.max(F.when(F.col("is_delivered") == 1, F.col("order_date"))).alias("last_delivered_order_date"),
        # Frequency: distinct Delivered orders
        F.countDistinct(F.when(F.col("is_delivered") == 1, F.col("order_id"))).alias("frequency"),
        # Monetary: total recognized revenue from Delivered orders
        F.round(F.sum("recognized_revenue"), 2).alias("monetary"),
        # Delivered units & profit
        F.sum(F.when(F.col("is_delivered") == 1, F.col("quantity"))).alias("delivered_units"),
        F.round(F.sum("recognized_profit"), 2).alias("delivered_profit"),
        # Operational order status breakdown
        F.countDistinct(F.when(F.col("is_delivered") == 1, F.col("order_id"))).alias("delivered_orders_count"),
        F.countDistinct(F.when(F.col("is_shipped") == 1, F.col("order_id"))).alias("shipped_orders_count"),
        F.countDistinct(F.when(F.col("is_processing") == 1, F.col("order_id"))).alias("processing_orders_count"),
        F.countDistinct(F.when(F.col("is_returned") == 1, F.col("order_id"))).alias("returned_orders_count"),
        F.countDistinct(F.when(F.col("is_cancelled") == 1, F.col("order_id"))).alias("cancelled_orders_count"),
        F.countDistinct("order_id").alias("total_orders_placed"),
        # Operational spend breakdown
        F.round(F.sum("in_transit_revenue"), 2).alias("in_transit_spend"),
        F.round(F.sum("returned_revenue_loss"), 2).alias("returned_spend"),
        F.round(F.sum("cancelled_revenue_loss"), 2).alias("cancelled_spend"),
    )

    # Join with full customer profile (preserves all customers)
    rfm_df = customers_df.join(cust_agg, on="customer_id", how="left")

    # Compute derived RFM metrics
    rfm_final = (
        rfm_df.withColumn(
            "tenure_days",
            F.datediff(F.lit(ref_date), F.col("signup_date")),
        )
        .withColumn(
            "recency_days",
            F.when(
                F.col("last_delivered_order_date").isNotNull(),
                F.datediff(F.lit(ref_date), F.col("last_delivered_order_date")),
            ).otherwise(F.col("tenure_days")),  # Fallback to customer tenure if zero delivered orders
        )
        .withColumn("frequency", F.coalesce(F.col("frequency"), F.lit(0)))
        .withColumn("monetary", F.coalesce(F.col("monetary"), F.lit(0.0)))
        .withColumn("delivered_units", F.coalesce(F.col("delivered_units"), F.lit(0)))
        .withColumn("delivered_profit", F.coalesce(F.col("delivered_profit"), F.lit(0.0)))
        .withColumn("delivered_orders_count", F.coalesce(F.col("delivered_orders_count"), F.lit(0)))
        .withColumn("shipped_orders_count", F.coalesce(F.col("shipped_orders_count"), F.lit(0)))
        .withColumn("processing_orders_count", F.coalesce(F.col("processing_orders_count"), F.lit(0)))
        .withColumn("returned_orders_count", F.coalesce(F.col("returned_orders_count"), F.lit(0)))
        .withColumn("cancelled_orders_count", F.coalesce(F.col("cancelled_orders_count"), F.lit(0)))
        .withColumn("total_orders_placed", F.coalesce(F.col("total_orders_placed"), F.lit(0)))
        .withColumn("in_transit_spend", F.coalesce(F.col("in_transit_spend"), F.lit(0.0)))
        .withColumn("returned_spend", F.coalesce(F.col("returned_spend"), F.lit(0.0)))
        .withColumn("cancelled_spend", F.coalesce(F.col("cancelled_spend"), F.lit(0.0)))
        .withColumn(
            "avg_order_value",
            F.when(
                F.col("frequency") > 0,
                F.round(F.col("monetary") / F.col("frequency"), 2),
            ).otherwise(0.0),
        )
        .withColumn(
            "return_rate_pct",
            F.when(
                F.col("total_orders_placed") > 0,
                F.round((F.col("returned_orders_count") / F.col("total_orders_placed")) * 100.0, 2),
            ).otherwise(0.0),
        )
    )

    logger.info("RFM feature matrix built: %d customers processed", rfm_final.count())
    return rfm_final


def generate_monthly_metrics(sales_df: DataFrame) -> DataFrame:
    """
    Computes monthly time-series analytics:
    - Gross Demand vs. Delivered Recognized Revenue.
    - In-transit, pipeline, returned, and cancelled breakdowns.
    - Running Total Recognized Revenue (window function).
    - Month-over-Month (MoM) recognized revenue growth % (window function LAG).
    """
    logger.info("Generating monthly sales performance metrics and running totals...")

    monthly_base = (
        sales_df.withColumn("year_month", F.date_format(F.col("order_date"), "yyyy-MM"))
        .withColumn("year", F.year(F.col("order_date")))
        .withColumn("month", F.month(F.col("order_date")))
        .groupBy("year_month", "year", "month")
        .agg(
            F.round(F.sum("gross_revenue"), 2).alias("gross_sales"),
            F.round(F.sum("discount_amount"), 2).alias("discount_amount"),
            F.round(F.sum("net_revenue"), 2).alias("net_sales"),
            F.round(F.sum("recognized_revenue"), 2).alias("delivered_revenue"),
            F.round(F.sum("recognized_profit"), 2).alias("delivered_profit"),
            F.round(F.sum("in_transit_revenue"), 2).alias("in_transit_revenue"),
            F.round(F.sum("pipeline_revenue"), 2).alias("pipeline_revenue"),
            F.round(F.sum("returned_revenue_loss"), 2).alias("returned_loss"),
            F.round(F.sum("cancelled_revenue_loss"), 2).alias("cancelled_loss"),
            F.countDistinct("order_id").alias("total_orders"),
            F.countDistinct(F.when(F.col("is_delivered") == 1, F.col("order_id"))).alias("delivered_orders"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.sum("quantity").alias("units_ordered"),
            F.sum(F.when(F.col("is_delivered") == 1, F.col("quantity"))).alias("units_delivered"),
        )
        .orderBy("year_month")
    )

    # Window 1: Cumulative running total of recognized revenue
    monthly_run_window = Window.orderBy("year_month").rowsBetween(Window.unboundedPreceding, Window.currentRow)

    # Window 2: Prior month revenue for MoM calculation
    prior_month_window = Window.orderBy("year_month")

    monthly_metrics = (
        monthly_base.withColumn(
            "running_total_recognized_revenue",
            F.round(F.sum("delivered_revenue").over(monthly_run_window), 2),
        )
        .withColumn(
            "prev_month_delivered_revenue",
            F.lag("delivered_revenue", 1).over(prior_month_window),
        )
        .withColumn(
            "mom_recognized_growth_pct",
            F.when(
                F.col("prev_month_delivered_revenue").isNotNull() & (F.col("prev_month_delivered_revenue") > 0),
                F.round(
                    ((F.col("delivered_revenue") - F.col("prev_month_delivered_revenue")) / F.col("prev_month_delivered_revenue")) * 100.0,
                    2,
                ),
            ).otherwise(0.0),
        )
    )

    return monthly_metrics


def generate_category_metrics(sales_df: DataFrame) -> DataFrame:
    """
    Computes merchandise category and sub-category performance:
    - Total Gross Sales vs. Delivered Recognized Revenue.
    - Delivered COGS, Delivered Profit, and Delivered Margin %.
    - Category Rank by Profit (window function).
    """
    logger.info("Generating category merchandising metrics...")

    cat_base = (
        sales_df.groupBy("category", "sub_category")
        .agg(
            F.sum("quantity").alias("total_units_ordered"),
            F.sum(F.when(F.col("is_delivered") == 1, F.col("quantity"))).alias("units_delivered"),
            F.round(F.sum("gross_revenue"), 2).alias("gross_sales"),
            F.round(F.sum("net_revenue"), 2).alias("net_sales"),
            F.round(F.sum("recognized_revenue"), 2).alias("delivered_revenue"),
            F.round(F.sum(F.when(F.col("is_delivered") == 1, F.col("total_cost"))), 2).alias("delivered_cogs"),
            F.round(F.sum("recognized_profit"), 2).alias("delivered_profit"),
            F.countDistinct("order_id").alias("total_orders"),
            F.countDistinct(F.when(F.col("is_delivered") == 1, F.col("order_id"))).alias("delivered_orders"),
        )
        .withColumn(
            "delivered_margin_pct",
            F.when(
                F.col("delivered_revenue") > 0,
                F.round((F.col("delivered_profit") / F.col("delivered_revenue")) * 100.0, 2),
            ).otherwise(0.0),
        )
    )

    cat_window = Window.orderBy(F.col("delivered_profit").desc())

    cat_metrics = cat_base.withColumn("profit_rank", F.dense_rank().over(cat_window))
    return cat_metrics



def export_curated_datasets(
    datasets: Dict[str, DataFrame],
    analytics_dir: Optional[Path] = None,
) -> None:
    """
    Saves analytical DataFrames into data/processed/analytics/ in both
    Parquet format (high-performance columnar) and CSV (inspection & BI compatibility).
    """
    out_dir = Path(analytics_dir) if analytics_dir else config.paths.analytics_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    logger.info("Exporting curated analytical datasets to: %s...", out_dir.resolve())

    import shutil

    for name, df in datasets.items():
        pdf = df.toPandas()

        # 1. Export Parquet using PyArrow (cross-platform, single-file columnar format)
        parquet_path = out_dir / f"{name}.parquet"
        if parquet_path.exists():
            if parquet_path.is_dir():
                shutil.rmtree(parquet_path, ignore_errors=True)
            else:
                parquet_path.unlink()
        pdf.to_parquet(parquet_path, engine="pyarrow", index=False)
        logger.info("Persisted Parquet dataset: %s (%d rows)", parquet_path.resolve(), len(pdf))

        # 2. Export single-file CSV for direct Power BI / inspection compatibility
        csv_path = out_dir / f"{name}.csv"
        if csv_path.exists():
            if csv_path.is_dir():
                shutil.rmtree(csv_path, ignore_errors=True)
            else:
                csv_path.unlink()
        pdf.to_csv(csv_path, index=False, encoding="utf-8")
        logger.info("Persisted clean CSV dataset: %s (%d rows)", csv_path.resolve(), len(pdf))
