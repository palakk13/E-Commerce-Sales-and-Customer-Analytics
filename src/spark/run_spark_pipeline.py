"""
Master PySpark Pipeline Runner.

Orchestrates Phase 3 distributed data processing:
1. Ingests cleaned datasets into Spark DataFrames.
2. Joins operational entities with key integrity verification.
3. Computes business measures & order fulfillment indicators.
4. Calculates distributed window functions (customer sequence, running spend, monthly running totals, MoM growth).
5. Constructs customer-level RFM analytical matrices.
6. Exports curated analytical datasets to data/processed/analytics/ in Parquet and CSV.
"""

import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

# Ensure project root is in sys.path
BASE_DIR = Path(__file__).resolve().parent.parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from config.logging_config import setup_logger
from config.pipeline_config import config
from src.spark.spark_session import get_spark_session
from src.spark.transformations import (
    apply_window_metrics,
    compute_financial_measures,
    export_curated_datasets,
    generate_category_metrics,
    generate_monthly_metrics,
    generate_rfm_features,
    join_ecommerce_datasets,
    load_clean_datasets,
)

logger = setup_logger("run_spark_pipeline")


def run_spark_pipeline() -> Dict[str, Any]:
    """
    Executes the end-to-end PySpark processing pipeline.
    Returns key execution metrics and analytical KPI summaries.
    """
    start_time = time.perf_counter()
    logger.info("================================================================================")
    logger.info("STARTING PHASE 3: PYSPARK PROCESSING & ANALYTICS PIPELINE")
    logger.info("================================================================================")

    spark = get_spark_session("EcommerceSparkPlatform")

    try:
        # Step 1: Load cleaned datasets
        dfs = load_clean_datasets(spark)

        # Step 2: Multi-way Relational Joins
        joined_df = join_ecommerce_datasets(
            items_df=dfs["order_items"],
            orders_df=dfs["orders"],
            products_df=dfs["products"],
            customers_df=dfs["customers"],
        )

        # Step 3: Compute Financial Business Measures
        financial_df = compute_financial_measures(joined_df)

        # Step 4: Apply Distributed Window Metrics
        enriched_sales_fact = apply_window_metrics(financial_df)

        # Cache enriched sales fact for downstream analytical aggregations
        enriched_sales_fact.cache()

        # Step 5: RFM Feature Engineering
        rfm_features_df = generate_rfm_features(
            sales_df=enriched_sales_fact,
            customers_df=dfs["customers"],
        )

        # Step 6: Monthly Performance & Running Totals
        monthly_metrics_df = generate_monthly_metrics(enriched_sales_fact)

        # Step 7: Category & Sub-Category Performance
        category_metrics_df = generate_category_metrics(enriched_sales_fact)

        # Step 8: Export Curated Datasets to data/processed/analytics/
        curated_datasets = {
            "curated_sales_fact": enriched_sales_fact,
            "customer_rfm_features": rfm_features_df,
            "monthly_sales_metrics": monthly_metrics_df,
            "category_performance_metrics": category_metrics_df,
        }
        export_curated_datasets(curated_datasets)

        # Gather summary metrics for reporting
        total_items = enriched_sales_fact.count()
        total_customers = rfm_features_df.count()
        total_months = monthly_metrics_df.count()
        total_categories = category_metrics_df.count()

        # Aggregated KPI numbers
        kpi_row = enriched_sales_fact.agg(
            {"gross_revenue": "sum", "net_revenue": "sum", "recognized_revenue": "sum", "recognized_profit": "sum"}
        ).collect()[0]

        total_gross_rev = round(kpi_row["sum(gross_revenue)"], 2)
        total_net_rev = round(kpi_row["sum(net_revenue)"], 2)
        total_rec_rev = round(kpi_row["sum(recognized_revenue)"], 2)
        total_rec_profit = round(kpi_row["sum(recognized_profit)"], 2)
        gross_margin_pct = round((total_rec_profit / total_rec_rev) * 100.0, 2) if total_rec_rev > 0 else 0.0

        duration_sec = round(time.perf_counter() - start_time, 3)

        summary = {
            "status": "SUCCESS",
            "execution_time_seconds": duration_sec,
            "executed_at": datetime.now(timezone.utc).isoformat(),
            "datasets": {
                "curated_sales_fact_rows": total_items,
                "customer_rfm_features_rows": total_customers,
                "monthly_sales_metrics_rows": total_months,
                "category_performance_metrics_rows": total_categories,
            },
            "financial_kpis": {
                "total_gross_revenue": total_gross_rev,
                "total_net_revenue": total_net_rev,
                "total_recognized_revenue": total_rec_rev,
                "total_recognized_profit": total_rec_profit,
                "gross_margin_pct": gross_margin_pct,
            },
        }

        logger.info("================================================================================")
        logger.info("PHASE 3 PYSPARK PIPELINE COMPLETED IN %.3f SECONDS", duration_sec)
        logger.info("Curated Sales Fact Rows: %d", total_items)
        logger.info("Total Recognized Revenue: $%s", f"{total_rec_rev:,.2f}")
        logger.info("Total Recognized Profit:  $%s (Margin: %.2f%%)", f"{total_rec_profit:,.2f}", gross_margin_pct)
        logger.info("================================================================================")

        return summary

    finally:
        logger.info("Stopping SparkSession...")
        spark.stop()


if __name__ == "__main__":
    result = run_spark_pipeline()
    print("\n" + "=" * 80)
    print("PHASE 3: PYSPARK ENGINE EXECUTION SUMMARY")
    print("=" * 80)
    print(f"Status:               {result['status']}")
    print(f"Duration:             {result['execution_time_seconds']} seconds")
    print(f"Curated Sales Fact:   {result['datasets']['curated_sales_fact_rows']} rows")
    print(f"Customer RFM Matrix:  {result['datasets']['customer_rfm_features_rows']} rows")
    print(f"Monthly Metrics:      {result['datasets']['monthly_sales_metrics_rows']} months")
    print(f"Category Metrics:     {result['datasets']['category_performance_metrics_rows']} categories")
    print("-" * 80)
    print("KEY FINANCIAL METRICS:")
    print(f"  Total Gross Revenue:       ${result['financial_kpis']['total_gross_revenue']:,.2f}")
    print(f"  Total Net Revenue:         ${result['financial_kpis']['total_net_revenue']:,.2f}")
    print(f"  Total Recognized Revenue:  ${result['financial_kpis']['total_recognized_revenue']:,.2f}")
    print(f"  Total Recognized Profit:   ${result['financial_kpis']['total_recognized_profit']:,.2f}")
    print(f"  Overall Gross Margin:      {result['financial_kpis']['gross_margin_pct']:.2f}%")
    print("=" * 80)
