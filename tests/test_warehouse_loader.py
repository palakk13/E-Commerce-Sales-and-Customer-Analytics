"""
Unit and Integration Tests for Data Warehouse Loader & Schema.

Validates:
1. Dimension table loading and surrogate key generation.
2. Referential integrity and surrogate key resolution.
3. Fact table line-item grain and row count preservation (6,379 rows).
4. Strict revenue recognition ($405,484.47 recognized revenue, $231,207.47 profit).
5. Idempotent reruns (duplicate prevention).
6. Transactional integrity and rollback on constraint violation.
7. Nullable optional attributes (customer phone) handling.
"""

import sys
import unittest
from pathlib import Path

# Add project root to sys.path
BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import pandas as pd
from sqlalchemy import create_engine, event, select, text
from src.warehouse.warehouse_loader import WarehouseLoader


class TestWarehouseLoader(unittest.TestCase):
    """Test suite for warehouse schema creation, surrogate key resolution, and ETL loading."""

    @classmethod
    def setUpClass(cls):
        """Initializes an in-memory SQLite warehouse engine with foreign keys enabled."""
        cls.engine = create_engine("sqlite:///:memory:", echo=False)

        @event.listens_for(cls.engine, "connect")
        def set_sqlite_pragma(dbapi_connection, connection_record):
            cursor = dbapi_connection.cursor()
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.close()

        cls.loader = WarehouseLoader(engine=cls.engine)
        # Execute initial full load
        cls.summary = cls.loader.load_all(drop_first=True)

    def test_dimension_row_counts(self):
        """Verifies that all dimensions are populated with the expected entity counts."""
        counts = self.summary["table_counts"]
        self.assertGreaterEqual(counts["dim_date"], 1095)  # 3 full years (2024-2026)
        self.assertEqual(counts["dim_customer"], 1000)     # 1,000 clean customers
        self.assertEqual(counts["dim_product"], 30)        # 30 catalog products
        self.assertEqual(counts["dim_payment"], 5)         # 5 payment channels
        self.assertEqual(counts["dim_location"], 15)       # 15 distinct metro locations


    def test_fact_sales_row_count_and_keys(self):
        """Fact table row count must preserve atomic line-item grain (6,379 rows)."""
        counts = self.summary["table_counts"]
        self.assertEqual(counts["fact_sales"], 6379)

    def test_referential_integrity_no_null_foreign_keys(self):
        """Fact table must not contain any null or unmapped surrogate foreign keys."""
        audit = self.summary["financial_reconciliation"]
        self.assertEqual(audit["null_foreign_keys"], 0)

    def test_financial_reconciliation_against_phase3(self):
        """Warehouse financial aggregates must match verified Phase 3 benchmarks to the cent."""
        audit = self.summary["financial_reconciliation"]

        # Expected Phase 3 verified benchmarks
        expected_gross_demand = 1020403.59
        expected_net_demand = 978097.82
        expected_rec_revenue = 405484.47
        expected_rec_profit = 231207.47
        expected_distinct_orders = 3819

        self.assertAlmostEqual(audit["total_gross_revenue"], expected_gross_demand, places=2)
        self.assertAlmostEqual(audit["total_net_revenue"], expected_net_demand, places=2)
        self.assertAlmostEqual(audit["total_recognized_revenue"], expected_rec_revenue, places=2)
        self.assertAlmostEqual(audit["total_recognized_profit"], expected_rec_profit, places=2)
        self.assertEqual(audit["distinct_orders"], expected_distinct_orders)
        self.assertTrue(audit["conservation_match"])

    def test_status_revenue_recognition_rules(self):
        """Verifies that only Delivered orders have recognized_revenue > 0."""
        with self.engine.connect() as conn:
            stmt = text("""
                SELECT order_status,
                       SUM(recognized_revenue) AS rec_rev,
                       SUM(recognized_profit) AS rec_profit,
                       SUM(in_transit_revenue) AS in_transit,
                       SUM(pipeline_revenue) AS pipeline,
                       SUM(returned_revenue_loss) AS ret_loss,
                       SUM(cancelled_revenue_loss) AS can_loss
                FROM fact_sales
                GROUP BY order_status
            """)
            rows = conn.execute(stmt).fetchall()
            status_map = {r[0]: r for r in rows}

            # Delivered
            deliv = status_map["Delivered"]
            self.assertGreater(deliv[1], 0.0)  # rec_rev > 0
            self.assertGreater(deliv[2], 0.0)  # rec_profit > 0
            self.assertEqual(deliv[3], 0.0)    # in_transit == 0

            # Shipped
            ship = status_map["Shipped"]
            self.assertEqual(ship[1], 0.0)     # rec_rev == 0
            self.assertEqual(ship[2], 0.0)     # rec_profit == 0
            self.assertGreater(ship[3], 0.0)   # in_transit > 0

            # Processing
            proc = status_map["Processing"]
            self.assertEqual(proc[1], 0.0)     # rec_rev == 0
            self.assertGreater(proc[4], 0.0)   # pipeline > 0

            # Cancelled
            canc = status_map["Cancelled"]
            self.assertEqual(canc[1], 0.0)     # rec_rev == 0
            self.assertGreater(canc[6], 0.0)   # can_loss > 0

            # Returned
            ret = status_map["Returned"]
            self.assertEqual(ret[1], 0.0)      # rec_rev == 0
            self.assertGreater(ret[5], 0.0)    # ret_loss > 0

    def test_idempotent_rerun_prevents_duplicate_records(self):
        """Rerunning warehouse loader on populated schema must not create duplicate fact or dimension rows."""
        # Run load_all with drop_first=False
        rerun_summary = self.loader.load_all(drop_first=False)
        self.assertEqual(rerun_summary["table_counts"]["fact_sales"], 6379)
        self.assertEqual(rerun_summary["table_counts"]["dim_customer"], 1000)
        self.assertEqual(rerun_summary["table_counts"]["dim_product"], 30)

    def test_optional_phone_nullable_handling(self):
        """Verifies that missing phone numbers are stored as NULL and do not corrupt records."""
        with self.engine.connect() as conn:
            stmt = text("SELECT COUNT(*) FROM dim_customer WHERE phone IS NULL")
            null_phones = conn.execute(stmt).scalar()
            # In Phase 2, 37 customers had missing phone numbers
            self.assertEqual(null_phones, 37)

    def test_transactional_rollback_on_failure(self):
        """Verifies that database operations rollback cleanly when an error occurs."""
        with self.assertRaises(Exception):
            with self.engine.begin() as conn:
                # Attempt to insert an invalid record with a non-existent foreign key
                conn.execute(
                    self.loader.fact_sales.insert(),
                    [{
                        "order_id": "ORD_TEST_FAIL",
                        "order_item_id": "ITEM_TEST_FAIL",
                        "customer_key": 999999,  # Non-existent FK
                        "product_key": 999999,
                        "date_key": 999999,
                        "location_key": 999999,
                        "payment_key": 999999,
                        "order_status": "Delivered",
                        "quantity": 1,
                        "unit_price": 10.0,
                        "discount_percent": 0.0,
                        "gross_revenue": 10.0,
                        "discount_amount": 0.0,
                        "net_revenue": 10.0,
                        "unit_cost": 5.0,
                        "total_cost": 5.0,
                        "profit": 5.0,
                        "profit_margin_pct": 50.0,
                        "shipping_cost": 0.0,
                    }],
                )

        # Verify that the test record was NOT committed
        with self.engine.connect() as conn:
            check_stmt = text("SELECT COUNT(*) FROM fact_sales WHERE order_item_id = 'ITEM_TEST_FAIL'")
            count = conn.execute(check_stmt).scalar()
            self.assertEqual(count, 0)


if __name__ == "__main__":
    unittest.main()
