"""
Pipeline Execution Orchestrator for Phase 2.

Coordinates:
1. Raw Data Ingestion & Hash Auditing
2. Data Quality Gatekeeper & Quarantine Routing
3. Data Cleaning & Canonicalization
4. Conservation of Records Reconciliation
"""

import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

# Ensure project root is in sys.path
BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from config.logging_config import setup_logger
from config.pipeline_config import config
from src.ingestion.ingestion_engine import DataIngestionEngine
from src.transformation.cleaner import DataCleaner
from src.validation.data_validator import DataValidator

logger = setup_logger("phase2_runner")


def run_phase2_pipeline() -> Dict[str, Any]:
    """
    Executes the entire Phase 2 Data Engineering pipeline.
    Returns comprehensive execution summary and audit metrics.
    """
    start_time = time.perf_counter()
    logger.info("================================================================================")
    logger.info("STARTING PHASE 2: INGESTION, VALIDATION & CLEANING PIPELINE")
    logger.info("================================================================================")

    # --------------------------------------------------------------------------
    # Step 1: Ingestion Engine
    # --------------------------------------------------------------------------
    ingestion_engine = DataIngestionEngine()
    ingested_data = ingestion_engine.ingest_all()

    raw_dfs = {k: v[0] for k, v in ingested_data.items()}
    audit_records = {k: v[1] for k, v in ingested_data.items()}

    # --------------------------------------------------------------------------
    # Step 2: Data Quality Gatekeeper & Validation
    # --------------------------------------------------------------------------
    validator = DataValidator()
    valid_dfs, quarantine_df, val_metrics = validator.validate_all(raw_dfs)

    # --------------------------------------------------------------------------
    # Step 3: Data Cleaning & Normalization
    # --------------------------------------------------------------------------
    cleaner = DataCleaner()
    cleaned_dfs = cleaner.clean_all(valid_dfs)

    total_duration_sec = time.perf_counter() - start_time

    # --------------------------------------------------------------------------
    # Step 4: Conservation of Records & Reconciliation Verification
    # --------------------------------------------------------------------------
    reconciliation_report = {}
    for entity in ["customers", "products", "orders", "order_items"]:
        raw_cnt = len(raw_dfs[entity])
        val_cnt = len(valid_dfs[entity])
        quar_cnt = len(quarantine_df[quarantine_df["table_name"] == entity])
        clean_cnt = len(cleaned_dfs[entity])

        # Verify Conservation: raw == valid + quarantined
        is_conserved = (raw_cnt == val_cnt + quar_cnt)
        reconciliation_report[entity] = {
            "raw_count": raw_cnt,
            "valid_count": val_cnt,
            "quarantined_count": quar_cnt,
            "cleaned_count": clean_cnt,
            "is_conserved": is_conserved,
        }

    # Summary dictionary
    summary = {
        "status": "SUCCESS",
        "executed_at": datetime.now(timezone.utc).isoformat(),
        "total_duration_seconds": round(total_duration_sec, 3),
        "total_raw_records": val_metrics.total_raw_records,
        "total_valid_records": val_metrics.total_valid_records,
        "total_quarantined_records": val_metrics.total_quarantined_records,
        "total_defect_occurrences": val_metrics.total_defect_occurrences,
        "reconciliation": reconciliation_report,
        "defect_breakdown": val_metrics.defect_counts_by_reason,
        "hashes": {k: v.file_hash_sha256 for k, v in audit_records.items()},
    }

    logger.info("================================================================================")
    logger.info("PHASE 2 COMPLETED SUCCESSFULLY IN %.3f SECONDS", total_duration_sec)
    logger.info("Total Raw Records:        %d", summary["total_raw_records"])
    logger.info("Total Valid Records:      %d", summary["total_valid_records"])
    logger.info("Total Quarantined Records: %d", summary["total_quarantined_records"])
    logger.info("Total Defect Occurrences: %d", summary["total_defect_occurrences"])
    logger.info("================================================================================")

    return summary


def run_phase3_pipeline() -> Dict[str, Any]:
    """Invokes Phase 3 PySpark Processing Engine."""
    from src.spark.run_spark_pipeline import run_spark_pipeline
    return run_spark_pipeline()


def run_phase4_pipeline(use_sqlite: bool = True, reset: bool = True) -> Dict[str, Any]:
    """Invokes Phase 4 Relational Data Warehouse Loader."""
    from src.warehouse.warehouse_loader import WarehouseLoader
    db_url = None
    if use_sqlite:
        sqlite_dir = BASE_DIR / "data" / "warehouse"
        sqlite_dir.mkdir(parents=True, exist_ok=True)
        sqlite_path = sqlite_dir / "ecommerce_dwh.db"
        db_url = f"sqlite:///{sqlite_path.resolve()}"

    loader = WarehouseLoader(db_url=db_url)
    return loader.load_all(drop_first=reset)


def run_phase5_pipeline() -> Dict[str, Any]:
    """
    Executes Phase 5 Apache Airflow Orchestrated Workflow.
    Executes the task chain verified by the Airflow DAG definition.
    """
    from airflow.dags.ecommerce_analytics_dag import (
        task_validate_raw_data_integrity,
        task_data_quality_and_cleaning,
        task_pyspark_transformations,
        task_load_mysql_warehouse,
        task_verify_warehouse_reconciliation,
        task_generate_pipeline_summary,
    )
    logger.info("Executing Phase 5 Airflow orchestrated tasks sequentially...")
    s1 = task_validate_raw_data_integrity()
    s2 = task_data_quality_and_cleaning()
    s3 = task_pyspark_transformations()
    s4 = task_load_mysql_warehouse()
    s5 = task_verify_warehouse_reconciliation()
    s6 = task_generate_pipeline_summary()
    return {
        "status": "SUCCESS",
        "dag_id": "ecommerce_analytics_pipeline",
        "stages": [
            "validate_raw_data_integrity",
            "data_quality_and_cleaning",
            "pyspark_transformations",
            "load_mysql_warehouse",
            "verify_warehouse_reconciliation",
            "generate_pipeline_summary",
        ],
        "summary": s6,
    }


def run_phase6_pipeline() -> Dict[str, Any]:
    """Invokes Phase 6 Customer Segmentation with RFM and K-Means."""
    from src.analytics.rfm_segmentation import RFMSegmentationEngine
    engine = RFMSegmentationEngine()
    return engine.run_pipeline(save_outputs=True, update_warehouse=True)


def run_phase7_pipeline() -> Dict[str, Any]:
    """Invokes Phase 7 Power BI Semantic Model & Dashboard Asset Generator."""
    from src.analytics.generate_powerbi_assets import generate_powerbi_assets
    return generate_powerbi_assets()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="E-Commerce Analytics Master Pipeline Runner")
    parser.add_argument(
        "--phase",
        type=str,
        default="all",
        choices=["2", "3", "4", "5", "6", "7", "all"],
        help="Specify which pipeline phase to execute (2, 3, 4, 5, 6, 7, or all).",
    )
    parser.add_argument(
        "--mysql",
        action="store_true",
        help="Target MySQL instead of SQLite for Phase 4 warehouse loading.",
    )
    args = parser.parse_args()

    phases_to_run = [2, 3, 4, 5, 6, 7] if args.phase == "all" else [int(args.phase)]

    print("\n" + "=" * 80)
    print("MASTER PIPELINE EXECUTION ORCHESTRATOR")
    print(f"Executing Phases: {phases_to_run}")
    print("=" * 80)

    # Phase 2
    if 2 in phases_to_run:
        p2_res = run_phase2_pipeline()
        print(f"\n[PHASE 2 SUCCESS] Ingested {p2_res['total_raw_records']:,} raw records -> {p2_res['total_valid_records']:,} valid, {p2_res['total_quarantined_records']} quarantined.")

    # Phase 3
    if 3 in phases_to_run:
        p3_res = run_phase3_pipeline()
        print(f"\n[PHASE 3 SUCCESS] Processed {p3_res['datasets']['curated_sales_fact_rows']:,} line items via PySpark.")
        print(f"Recognized Revenue: ${p3_res['financial_kpis']['total_recognized_revenue']:,.2f}")

    # Phase 4
    if 4 in phases_to_run:
        use_sqlite = not args.mysql
        p4_res = run_phase4_pipeline(use_sqlite=use_sqlite, reset=True)
        print(f"\n[PHASE 4 SUCCESS] Loaded {p4_res['table_counts']['fact_sales']:,} fact rows into warehouse ({p4_res['database_url']}).")
        print(f"Warehouse Recognized Revenue: ${p4_res['financial_reconciliation']['total_recognized_revenue']:,.2f}")

    # Phase 5
    if 5 in phases_to_run:
        p5_res = run_phase5_pipeline()
        print(f"\n[PHASE 5 SUCCESS] Airflow DAG '{p5_res['dag_id']}' orchestrated tasks successfully.")
        print(f"Pipeline Execution Summary saved to logs/pipeline_execution_summary.json")

    # Phase 6
    if 6 in phases_to_run:
        p6_res = run_phase6_pipeline()
        print(f"\n[PHASE 6 SUCCESS] Segmented {p6_res['customer_count']:,} customers into {p6_res['selected_k']} clusters (Silhouette: {p6_res['silhouette_score']:.4f}).")
        print(f"Total Reconciled Revenue: ${p6_res['total_recognized_revenue']:,.2f}")

    # Phase 7
    if 7 in phases_to_run:
        p7_res = run_phase7_pipeline()
        print(f"\n[PHASE 7 SUCCESS] Exported {p7_res['star_schema_tables']['fact_sales']:,} sales facts and 5 dimension tables for Power BI.")
        print(f"Net Demand: ${p7_res['measures']['net_demand_revenue']:,.2f} | Recognized Revenue: ${p7_res['measures']['recognized_revenue']:,.2f}")

    print("\n" + "=" * 80)
    print("ALL REQUESTED PIPELINE PHASES COMPLETED SUCCESSFULLY")
    print("=" * 80)



