"""
Unit Tests for PySpark Transformations & Analytics Engine.

Tests:
1. Financial measure calculations (Gross/Net Revenue, COGS, Profit, Margin).
2. Strict revenue recognition by order status (Delivered vs Shipped vs Processing vs Cancelled vs Returned).
3. Product unit cost and discount impact on profit.
4. Relational joins and line-item grain preservation (preventing row multiplication).
5. PySpark Window functions (Order sequence rank, cumulative spend, running monthly revenue).
6. Customer RFM feature calculation with strict exclusion of cancelled and returned orders.
"""

import sys
import unittest
from pathlib import Path

# Add project root to sys.path
BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from pyspark.sql import functions as F
from src.spark.spark_session import get_spark_session
from src.spark.transformations import (
    apply_window_metrics,
    compute_financial_measures,
    generate_category_metrics,
    generate_monthly_metrics,
    generate_rfm_features,
    join_ecommerce_datasets,
)


class TestSparkTransformations(unittest.TestCase):
    """Test suite for PySpark transformations and financial formulas."""

    @classmethod
    def setUpClass(cls):
        cls.spark = get_spark_session("SparkTransformationTest")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_financial_measures_calculation(self):
        """Validates exact math for gross revenue, discount amount, net revenue, COGS, and profit."""
        # 2 units @ $100.00 with 10% discount and $60.00 unit cost
        data = [(
            "ITEM_001", "ORD_001", "PROD_001", 2, 100.00, 10.00,
            "CUST_001", "2025-05-01", "Delivered", "Credit Card", 5.00,
            "PROD_001", "Item", "Category", "Sub", 60.00, 100.00,
            "CUST_001", "Alice", "alice@example.com", "+91-9999999999", "Delhi", "Delhi", "India", "110001", "2024-01-01"
        )]
        cols = [
            "order_item_id", "order_id", "product_id", "quantity", "unit_price", "discount_percent",
            "customer_id", "order_date", "order_status", "payment_method", "shipping_cost",
            "prod_pk", "product_name", "category", "sub_category", "unit_cost", "list_price",
            "cust_pk", "customer_name", "email", "phone", "city", "state", "country", "postal_code", "signup_date"
        ]
        df = self.spark.createDataFrame(data, cols)
        res = compute_financial_measures(df).collect()[0]

        # Expected:
        # gross_revenue = 2 * 100 = 200.00
        # discount_amount = 200 * 0.10 = 20.00
        # net_revenue = 200 - 20 = 180.00
        # total_cost = 2 * 60 = 120.00
        # profit = 180 - 120 = 60.00
        # profit_margin_pct = (60 / 180) * 100 = 33.33%
        self.assertEqual(res["gross_revenue"], 200.00)
        self.assertEqual(res["discount_amount"], 20.00)
        self.assertEqual(res["net_revenue"], 180.00)
        self.assertEqual(res["total_cost"], 120.00)
        self.assertEqual(res["profit"], 60.00)
        self.assertAlmostEqual(res["profit_margin_pct"], 33.33, places=2)
        self.assertEqual(res["recognized_revenue"], 180.00)
        self.assertEqual(res["recognized_profit"], 60.00)

    def test_strict_revenue_recognition_by_status(self):
        """
        Validates strict revenue recognition rule (FOB Destination):
        - Only Delivered orders recognize revenue and profit.
        - Shipped orders are tracked as in-transit revenue (0 recognized).
        - Processing orders are tracked as pipeline revenue (0 recognized).
        - Cancelled orders are tracked as cancelled revenue loss (0 recognized).
        - Returned orders are tracked as returned revenue loss (0 recognized).
        """
        data = [
            ("Delivered", 2, 100.0, 10.0, 60.0),   # Net: 180.0, Profit: 60.0
            ("Shipped", 1, 100.0, 0.0, 50.0),     # Net: 100.0, Profit: 50.0
            ("Processing", 1, 80.0, 0.0, 40.0),   # Net: 80.0, Profit: 40.0
            ("Cancelled", 1, 150.0, 0.0, 75.0),   # Net: 150.0, Profit: 75.0
            ("Returned", 1, 200.0, 0.0, 100.0),   # Net: 200.0, Profit: 100.0
        ]
        cols = ["order_status", "quantity", "unit_price", "discount_percent", "unit_cost"]
        df = self.spark.createDataFrame(data, cols)
        res = compute_financial_measures(df).collect()

        status_map = {row["order_status"]: row for row in res}

        # Delivered: recognized
        deliv = status_map["Delivered"]
        self.assertEqual(deliv["recognized_revenue"], 180.0)
        self.assertEqual(deliv["recognized_profit"], 60.0)
        self.assertEqual(deliv["is_delivered"], 1)

        # Shipped: in transit only
        ship = status_map["Shipped"]
        self.assertEqual(ship["recognized_revenue"], 0.0)
        self.assertEqual(ship["recognized_profit"], 0.0)
        self.assertEqual(ship["in_transit_revenue"], 100.0)
        self.assertEqual(ship["is_shipped"], 1)

        # Processing: pipeline only
        proc = status_map["Processing"]
        self.assertEqual(proc["recognized_revenue"], 0.0)
        self.assertEqual(proc["recognized_profit"], 0.0)
        self.assertEqual(proc["pipeline_revenue"], 80.0)
        self.assertEqual(proc["is_processing"], 1)

        # Cancelled: loss tracked, 0 recognized
        canc = status_map["Cancelled"]
        self.assertEqual(canc["recognized_revenue"], 0.0)
        self.assertEqual(canc["recognized_profit"], 0.0)
        self.assertEqual(canc["cancelled_revenue_loss"], 150.0)
        self.assertEqual(canc["is_cancelled"], 1)

        # Returned: loss tracked, 0 recognized
        ret = status_map["Returned"]
        self.assertEqual(ret["recognized_revenue"], 0.0)
        self.assertEqual(ret["recognized_profit"], 0.0)
        self.assertEqual(ret["returned_revenue_loss"], 200.0)
        self.assertEqual(ret["is_returned"], 1)

    def test_join_preserves_row_count(self):
        """Joining items with orders, products, and customers preserves line-item grain and count."""
        items = self.spark.createDataFrame([
            ("ITEM_1", "ORD_1", "PROD_1", 1, 50.0, 0.0),
            ("ITEM_2", "ORD_1", "PROD_2", 2, 30.0, 0.0),
        ], ["order_item_id", "order_id", "product_id", "quantity", "unit_price", "discount_percent"])

        orders = self.spark.createDataFrame([
            ("ORD_1", "CUST_1", "2025-01-01", "Delivered", "UPI", 5.0)
        ], ["order_id", "customer_id", "order_date", "order_status", "payment_method", "shipping_cost"])

        products = self.spark.createDataFrame([
            ("PROD_1", "Product 1", "Cat", "Sub", 20.0, 50.0),
            ("PROD_2", "Product 2", "Cat", "Sub", 15.0, 30.0),
        ], ["product_id", "product_name", "category", "sub_category", "unit_cost", "list_price"])

        customers = self.spark.createDataFrame([
            ("CUST_1", "Customer 1", "c1@test.com", "NOT_PROVIDED", "City", "State", "Country", "000000", "2024-01-01")
        ], ["customer_id", "customer_name", "email", "phone", "city", "state", "country", "postal_code", "signup_date"])

        joined = join_ecommerce_datasets(items, orders, products, customers)
        self.assertEqual(joined.count(), 2)

    def test_window_functions_customer_sequence_and_running_spend(self):
        """Verifies customer order sequence ranking and cumulative spend running totals."""
        data = [
            ("CUST_A", "ORD_101", "2025-01-01", "Delivered", 100.0, 50.0),
            ("CUST_A", "ORD_102", "2025-03-01", "Delivered", 200.0, 100.0),
            ("CUST_A", "ORD_103", "2025-06-01", "Delivered", 50.0, 25.0),
        ]
        cols = ["customer_id", "order_id", "order_date", "order_status", "net_revenue", "profit"]
        df = self.spark.createDataFrame(data, cols).withColumn("recognized_revenue", F.col("net_revenue"))

        with_windows = apply_window_metrics(df).orderBy("order_date").collect()

        self.assertEqual(with_windows[0]["customer_order_rank"], 1)
        self.assertEqual(with_windows[0]["customer_cumulative_spend"], 100.0)

        self.assertEqual(with_windows[1]["customer_order_rank"], 2)
        self.assertEqual(with_windows[1]["customer_cumulative_spend"], 300.0)

        self.assertEqual(with_windows[2]["customer_order_rank"], 3)
        self.assertEqual(with_windows[2]["customer_cumulative_spend"], 350.0)

    def test_rfm_metrics_calculation(self):
        """Verifies Recency, Frequency, and Monetary calculation with strict exclusion of non-delivered orders."""
        raw_sales_data = [
            # CUST_1: 2 Delivered orders (Jan 1 and Jun 1, 2025) totalling $300 recognized spend, plus 1 Returned order
            ("ITEM_1", "ORD_01", "PROD_1", 1, 100.0, 0.0, "CUST_1", "2025-01-01", "Delivered", "UPI", 0.0, "PROD_1", "P1", "Cat", "Sub", 50.0, 100.0, "CUST_1", "U1", "u1@test.com", "NOT_PROVIDED", "City", "State", "India", "001", "2024-01-01"),
            ("ITEM_2", "ORD_02", "PROD_1", 2, 100.0, 0.0, "CUST_1", "2025-06-01", "Delivered", "UPI", 0.0, "PROD_1", "P1", "Cat", "Sub", 50.0, 100.0, "CUST_1", "U1", "u1@test.com", "NOT_PROVIDED", "City", "State", "India", "001", "2024-01-01"),
            ("ITEM_3", "ORD_03", "PROD_1", 1, 80.0, 0.0, "CUST_1", "2025-06-15", "Returned", "UPI", 0.0, "PROD_1", "P1", "Cat", "Sub", 50.0, 80.0, "CUST_1", "U1", "u1@test.com", "NOT_PROVIDED", "City", "State", "India", "001", "2024-01-01"),
            # CUST_2: 1 Cancelled order -> 0 recognized frequency and 0 monetary
            ("ITEM_4", "ORD_04", "PROD_2", 1, 50.0, 0.0, "CUST_2", "2025-04-01", "Cancelled", "COD", 0.0, "PROD_2", "P2", "Cat", "Sub", 25.0, 50.0, "CUST_2", "U2", "u2@test.com", "NOT_PROVIDED", "City", "State", "India", "002", "2024-01-01"),
        ]
        cols = [
            "order_item_id", "order_id", "product_id", "quantity", "unit_price", "discount_percent",
            "customer_id", "order_date", "order_status", "payment_method", "shipping_cost",
            "prod_pk", "product_name", "category", "sub_category", "unit_cost", "list_price",
            "cust_pk", "customer_name", "email", "phone", "city", "state", "country", "postal_code", "signup_date"
        ]
        sales_df = (
            self.spark.createDataFrame(raw_sales_data, cols)
            .withColumn("order_date", F.to_date("order_date"))
            .withColumn("signup_date", F.to_date("signup_date"))
        )
        sales_with_metrics = compute_financial_measures(sales_df)

        cust_data = [
            ("CUST_1", "User One", "u1@test.com", "NOT_PROVIDED", "City", "State", "India", "001", "2024-01-01"),
            ("CUST_2", "User Two", "u2@test.com", "NOT_PROVIDED", "City", "State", "India", "002", "2024-01-01"),
        ]
        c_cols = ["customer_id", "customer_name", "email", "phone", "city", "state", "country", "postal_code", "signup_date"]
        cust_df = self.spark.createDataFrame(cust_data, c_cols).withColumn("signup_date", F.to_date("signup_date"))

        rfm = generate_rfm_features(sales_with_metrics, cust_df, snapshot_date="2025-07-01").collect()
        rfm_map = {row["customer_id"]: row for row in rfm}

        # CUST_1:
        # Last delivered order was 2025-06-01. Snapshot is 2025-07-01 -> Recency = 30 days.
        # Frequency = 2 (ORD_01, ORD_02). ORD_03 (Returned) is excluded from frequency.
        # Monetary = 300.0 (100.0 + 200.0). ORD_03 is excluded from monetary.
        # Returned orders count = 1. Returned spend = 80.0. Total orders placed = 3.
        c1 = rfm_map["CUST_1"]
        self.assertEqual(c1["recency_days"], 30)
        self.assertEqual(c1["frequency"], 2)
        self.assertEqual(c1["monetary"], 300.0)
        self.assertEqual(c1["avg_order_value"], 150.0)
        self.assertEqual(c1["returned_orders_count"], 1)
        self.assertEqual(c1["returned_spend"], 80.0)
        self.assertEqual(c1["total_orders_placed"], 3)

        # CUST_2:
        # 0 delivered orders -> Frequency = 0, Monetary = 0.0.
        # Cancelled orders count = 1. Total orders placed = 1.
        c2 = rfm_map["CUST_2"]
        self.assertEqual(c2["frequency"], 0)
        self.assertEqual(c2["monetary"], 0.0)
        self.assertEqual(c2["cancelled_orders_count"], 1)
        self.assertEqual(c2["cancelled_spend"], 50.0)
        self.assertEqual(c2["total_orders_placed"], 1)

    def test_monthly_metrics_aggregation_and_windows(self):
        """Verifies monthly aggregation, running totals, and MoM growth rate."""
        data = [
            # Jan 2025: 1 Delivered order ($100), 1 Cancelled ($50)
            ("ORD_01", "CUST_1", "2025-01-15", "Delivered", 1, 100.0, 10.0, 90.0, 90.0, 30.0, 0.0, 0.0, 0.0, 0.0, 1, 0, 0, 0, 0),
            ("ORD_02", "CUST_2", "2025-01-20", "Cancelled", 1, 50.0, 0.0, 50.0, 0.0, 0.0, 0.0, 0.0, 0.0, 50.0, 0, 0, 0, 0, 1),
            # Feb 2025: 1 Delivered order ($180)
            ("ORD_03", "CUST_1", "2025-02-10", "Delivered", 2, 100.0, 20.0, 180.0, 180.0, 60.0, 0.0, 0.0, 0.0, 0.0, 1, 0, 0, 0, 0),
        ]
        cols = [
            "order_id", "customer_id", "order_date", "order_status", "quantity",
            "gross_revenue", "discount_amount", "net_revenue", "recognized_revenue", "recognized_profit",
            "in_transit_revenue", "pipeline_revenue", "returned_revenue_loss", "cancelled_revenue_loss",
            "is_delivered", "is_shipped", "is_processing", "is_returned", "is_cancelled"
        ]
        df = self.spark.createDataFrame(data, cols).withColumn("order_date", F.to_date("order_date"))
        monthly = generate_monthly_metrics(df).collect()

        # Jan 2025
        jan = monthly[0]
        self.assertEqual(jan["year_month"], "2025-01")
        self.assertEqual(jan["gross_sales"], 150.0)
        self.assertEqual(jan["delivered_revenue"], 90.0)
        self.assertEqual(jan["delivered_profit"], 30.0)
        self.assertEqual(jan["cancelled_loss"], 50.0)
        self.assertEqual(jan["running_total_recognized_revenue"], 90.0)
        self.assertIsNone(jan["prev_month_delivered_revenue"])
        self.assertEqual(jan["mom_recognized_growth_pct"], 0.0)

        # Feb 2025
        feb = monthly[1]
        self.assertEqual(feb["year_month"], "2025-02")
        self.assertEqual(feb["delivered_revenue"], 180.0)
        self.assertEqual(feb["running_total_recognized_revenue"], 270.0)
        self.assertEqual(feb["prev_month_delivered_revenue"], 90.0)
        # ((180 - 90) / 90) * 100 = 100.0%
        self.assertEqual(feb["mom_recognized_growth_pct"], 100.0)

    def test_category_performance_metrics(self):
        """Verifies category profit ranking and delivered margin calculations."""
        data = [
            # Electronics: $500 delivered rev, $200 cost -> $300 profit
            ("Electronics", "Phones", 5, 500.0, 500.0, 500.0, 200.0, 300.0, 1, "ORD_1"),
            # Books: $200 delivered rev, $150 cost -> $50 profit
            ("Books", "Fiction", 10, 200.0, 200.0, 200.0, 150.0, 50.0, 1, "ORD_2"),
        ]
        cols = [
            "category", "sub_category", "quantity", "gross_revenue", "net_revenue",
            "recognized_revenue", "total_cost", "recognized_profit", "is_delivered", "order_id"
        ]
        df = self.spark.createDataFrame(data, cols)
        cats = generate_category_metrics(df).collect()

        # Electronics rank 1, Books rank 2
        self.assertEqual(cats[0]["category"], "Electronics")
        self.assertEqual(cats[0]["profit_rank"], 1)
        self.assertEqual(cats[0]["delivered_profit"], 300.0)
        self.assertAlmostEqual(cats[0]["delivered_margin_pct"], 60.0, places=1)

        self.assertEqual(cats[1]["category"], "Books")
        self.assertEqual(cats[1]["profit_rank"], 2)
        self.assertEqual(cats[1]["delivered_profit"], 50.0)
        self.assertAlmostEqual(cats[1]["delivered_margin_pct"], 25.0, places=1)


if __name__ == "__main__":
    unittest.main()

