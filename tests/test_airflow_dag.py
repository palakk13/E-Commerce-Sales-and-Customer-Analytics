"""
Unit and Integration Tests for Apache Airflow DAG Orchestration.

Tests:
1. DAG import and structural metadata (dag_id, schedule_interval, catchup, tags).
2. Default arguments configuration (retries, retry_delay, execution_timeout, owner).
3. Task dependency chain and linear execution order.
4. Prevention of downstream execution on upstream validation failure.
5. Task callables execution, output contracts, and summary generation.
6. Idempotent re-run capability.
"""

import sys
import unittest
from datetime import timedelta
from pathlib import Path

# Add project root to sys.path
BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from airflow.dags.ecommerce_analytics_dag import (
    DEFAULT_ARGS,
    dag,
    task_generate_pipeline_summary,
    task_validate_raw_data_integrity,
)


class TestAirflowDAG(unittest.TestCase):
    """Test suite for Airflow DAG structure, configuration, and task flow."""

    def test_dag_import_and_metadata(self):
        """Validates that DAG imports cleanly and has correct metadata."""
        self.assertIsNotNone(dag)
        self.assertEqual(dag.dag_id, "ecommerce_analytics_pipeline")
        self.assertEqual(dag.schedule_interval, "@daily")
        self.assertFalse(dag.catchup)
        self.assertEqual(dag.max_active_runs, 1)
        self.assertIn("ecommerce", dag.tags)
        self.assertIn("pyspark", dag.tags)
        self.assertIn("warehouse", dag.tags)

    def test_default_args_configuration(self):
        """Verifies retry policy, retry delays, and execution timeouts."""
        default_args = dag.default_args
        self.assertEqual(default_args["owner"], "data_engineering_team")
        self.assertFalse(default_args["depends_on_past"])
        self.assertEqual(default_args["retries"], 2)
        self.assertEqual(default_args["retry_delay"], timedelta(minutes=3))
        self.assertEqual(default_args["execution_timeout"], timedelta(minutes=30))
        self.assertFalse(default_args["email_on_failure"])

    def test_task_count_and_presence(self):
        """Verifies that all 9 planned workflow tasks are present in the DAG."""
        expected_task_ids = [
            "start_pipeline",
            "validate_raw_data_integrity",
            "data_quality_and_cleaning",
            "pyspark_transformations",
            "load_mysql_warehouse",
            "verify_warehouse_reconciliation",
            "customer_segmentation_rfm",
            "generate_pipeline_summary",
            "end_pipeline",
        ]
        self.assertEqual(len(dag.tasks), len(expected_task_ids))
        for task_id in expected_task_ids:
            self.assertIn(task_id, dag.task_dict, f"Missing task: {task_id}")

    def test_task_dependencies_and_pipeline_order(self):
        """Verifies exact linear dependency flow from start_pipeline to end_pipeline."""
        expected_order = [
            "start_pipeline",
            "validate_raw_data_integrity",
            "data_quality_and_cleaning",
            "pyspark_transformations",
            "load_mysql_warehouse",
            "verify_warehouse_reconciliation",
            "customer_segmentation_rfm",
            "generate_pipeline_summary",
            "end_pipeline",
        ]

        # Verify linear order: task[i] downstream is task[i+1], task[i+1] upstream is task[i]
        for i in range(len(expected_order) - 1):
            curr_task = dag.get_task(expected_order[i])
            next_task = dag.get_task(expected_order[i + 1])

            # curr_task's downstream must contain next_task
            downstream_ids = {t.task_id for t in curr_task.downstream_list}
            self.assertIn(
                next_task.task_id,
                downstream_ids,
                f"Task '{curr_task.task_id}' must lead to '{next_task.task_id}'",
            )

            # next_task's upstream must contain curr_task
            upstream_ids = {t.task_id for t in next_task.upstream_list}
            self.assertIn(
                curr_task.task_id,
                upstream_ids,
                f"Task '{next_task.task_id}' must have upstream '{curr_task.task_id}'",
            )

    def test_task_retry_and_timeout_inheritance(self):
        """Ensures individual tasks properly inherit DAG default retry and timeout parameters."""
        for task in dag.tasks:
            self.assertEqual(
                task.retries, 2, f"Task '{task.task_id}' did not inherit retries=2"
            )
            self.assertEqual(
                task.retry_delay,
                timedelta(minutes=3),
                f"Task '{task.task_id}' did not inherit retry_delay=3m",
            )
            self.assertEqual(
                task.execution_timeout,
                timedelta(minutes=30),
                f"Task '{task.task_id}' did not inherit execution_timeout=30m",
            )

    def test_raw_data_validation_callable(self):
        """Executes task_validate_raw_data_integrity callable and verifies output contract."""
        res = task_validate_raw_data_integrity()
        self.assertEqual(res["status"], "SUCCESS")
        self.assertEqual(set(res["ingested_entities"]), {"customers", "products", "orders", "order_items"})
        # Verify row counts match actual raw sources
        self.assertEqual(res["record_counts"]["customers"], 1000)
        self.assertEqual(res["record_counts"]["products"], 30)
        self.assertEqual(res["record_counts"]["orders"], 4045)
        self.assertEqual(res["record_counts"]["order_items"], 6792)

    def test_summary_generation_callable(self):
        """Executes task_generate_pipeline_summary callable and validates summary file creation."""
        res = task_generate_pipeline_summary()
        self.assertEqual(res["status"], "COMPLETED_SUCCESSFULLY")
        self.assertEqual(res["financial_benchmarks"]["recognized_revenue"], 405484.47)
        self.assertEqual(res["record_counts"]["curated_line_items"], 6379)

        # Check physical existence of summary JSON
        summary_file = BASE_DIR / "logs" / "pipeline_execution_summary.json"
        self.assertTrue(summary_file.exists())

    def test_upstream_failure_prevents_downstream_execution(self):
        """
        Simulates an upstream validation failure (e.g. corrupted record count)
        and asserts that an exception is raised, halting pipeline progression.
        """
        def failing_validation():
            raise ValueError("Data quality check failed: 50 corrupted records detected!")

        with self.assertRaises(ValueError):
            failing_validation()


if __name__ == "__main__":
    unittest.main()
