"""
Unit and Integration Tests for Analytical SQL Queries.

Executes all 7 analytical SQL queries in sql/analytics/ against the populated
warehouse star schema and validates that:
1. All queries parse and execute without SQL errors.
2. Monthly revenues and profits match verified Phase 3 benchmarks ($405,484.47 rec rev).
3. Window functions (LAG, Running Totals, DENSE_RANK, NTILE) produce valid outputs.
4. No double-counting occurs when aggregating across line items.
"""

import sys
import unittest
from datetime import datetime
from pathlib import Path

# Add project root to sys.path
BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import pandas as pd
from sqlalchemy import create_engine, event, text
from src.warehouse.warehouse_loader import WarehouseLoader


class TestAnalyticalSQL(unittest.TestCase):
    """Test suite executing the 7 analytical SQL scripts against a populated warehouse."""

    @classmethod
    def setUpClass(cls):
        """Initializes warehouse with SQLite user-defined functions matching MySQL dialect."""
        cls.engine = create_engine("sqlite:///:memory:", echo=False)

        @event.listens_for(cls.engine, "connect")
        def register_sqlite_functions(dbapi_connection, connection_record):
            # Foreign keys
            cursor = dbapi_connection.cursor()
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.close()

            # MySQL CONCAT emulation
            dbapi_connection.create_function(
                "CONCAT", -1, lambda *args: "".join(str(a) if a is not None else "" for a in args)
            )

            # MySQL LPAD emulation
            dbapi_connection.create_function(
                "LPAD", 3, lambda val, length, pad: str(val).rjust(length, pad[:1])
            )


            # MySQL DATEDIFF emulation
            def sqlite_datediff(d1, d2):
                if d1 is None or d2 is None:
                    return None
                dt1 = pd.to_datetime(str(d1)[:10])
                dt2 = pd.to_datetime(str(d2)[:10])
                return (dt1 - dt2).days

            dbapi_connection.create_function("DATEDIFF", 2, sqlite_datediff)

        cls.loader = WarehouseLoader(engine=cls.engine)
        cls.loader.load_all(drop_first=True)
        cls.sql_dir = BASE_DIR / "sql" / "analytics"

    def _execute_sql_file(self, filename: str) -> pd.DataFrame:
        """Helper to read and execute a SQL file, stripping dialect-specific USE statements."""
        sql_path = self.sql_dir / filename
        self.assertTrue(sql_path.exists(), f"SQL file not found: {sql_path}")

        raw_sql = sql_path.read_text(encoding="utf-8")
        # Split statements by semicolon and filter out comments / USE statements
        statements = []
        for stmt in raw_sql.split(";"):
            clean_stmt = stmt.strip()
            # Remove comment lines
            lines = [l for l in clean_stmt.splitlines() if not l.strip().startswith("--")]
            clean_code = "\n".join(lines).strip()
            if clean_code and not clean_code.upper().startswith("USE "):
                statements.append(clean_code)

        results = []
        with self.engine.connect() as conn:
            for s in statements:
                df = pd.read_sql_query(text(s), conn)
                results.append(df)

        return results[0] if len(results) == 1 else results

    def test_query_01_monthly_revenue_and_profit(self):
        """Validates monthly financial query and exact total recognized revenue match."""
        df = self._execute_sql_file("01_monthly_revenue_and_profit.sql")
        self.assertFalse(df.empty)
        self.assertEqual(len(df), 33)  # 33 months (2024-01 through 2026-09)

        # Check total recognized revenue matches benchmark exactly
        total_rec_rev = round(df["recognized_revenue"].sum(), 2)
        total_rec_profit = round(df["recognized_profit"].sum(), 2)
        self.assertEqual(total_rec_rev, 405484.47)
        self.assertEqual(total_rec_profit, 231207.47)

        # Margin check: profit margin is positive and between 0 and 100
        for margin in df["recognized_margin_pct"]:
            self.assertGreaterEqual(margin, 0.0)
            self.assertLessEqual(margin, 100.0)

    def test_query_02_mom_revenue_growth(self):
        """Validates running totals and Month-over-Month LAG calculations."""
        df = self._execute_sql_file("02_mom_revenue_growth.sql")
        self.assertFalse(df.empty)
        self.assertEqual(len(df), 33)

        # The final running total must equal the total recognized revenue
        final_running_total = df["running_total_recognized_revenue"].iloc[-1]
        self.assertEqual(final_running_total, 405484.47)

        # First month prev_month_recognized_revenue is None and growth is 0.0
        self.assertTrue(pd.isna(df["prev_month_recognized_revenue"].iloc[0]))
        self.assertEqual(df["mom_growth_pct"].iloc[0], 0.0)

    def test_query_03_product_category_rankings(self):
        """Validates product and category merchandising profit rankings."""
        results = self._execute_sql_file("03_product_category_rankings.sql")
        cat_df = results[0]
        prod_df = results[1]

        # 30 category/sub-category combinations
        self.assertEqual(len(cat_df), 30)
        self.assertEqual(cat_df["category_profit_rank"].min(), 1)
        self.assertEqual(cat_df["category_profit_rank"].max(), 30)

        # Top 10 products
        self.assertEqual(len(prod_df), 10)
        self.assertEqual(prod_df["product_profit_rank"].min(), 1)

    def test_query_04_customer_ltv_repeat_purchases(self):
        """Validates repeat purchase cohort distributions."""
        df = self._execute_sql_file("04_customer_ltv_repeat_purchases.sql")
        self.assertFalse(df.empty)

        # Sum of customer share % should equal 100%
        total_share = round(df["customer_share_pct"].sum(), 1)
        self.assertAlmostEqual(total_share, 100.0, places=1)

        # Sum of recognized revenue across cohorts must match exactly
        total_rev = round(df["total_recognized_revenue"].sum(), 2)
        self.assertEqual(total_rev, 405484.47)

    def test_query_05_rfm_segmentation_analysis(self):
        """Validates SQL RFM calculation and customer segmentation segments."""
        df = self._execute_sql_file("05_rfm_segmentation_analysis.sql")
        self.assertFalse(df.empty)

        # Total customers segmented must sum to 1,000
        total_customers = df["customer_count"].sum()
        self.assertEqual(total_customers, 1000)

        # Total revenue across segments must sum to $405,484.47
        total_seg_rev = round(df["total_segment_revenue"].sum(), 2)
        self.assertEqual(total_seg_rev, 405484.47)

    def test_query_06_regional_performance(self):
        """Validates regional state and city aggregation."""
        results = self._execute_sql_file("06_regional_performance.sql")
        state_df = results[0]
        city_df = results[1]

        self.assertFalse(state_df.empty)
        total_rev = round(state_df["recognized_revenue"].sum(), 2)
        self.assertEqual(total_rev, 405484.47)

        self.assertFalse(city_df.empty)
        self.assertLessEqual(len(city_df), 15)

    def test_query_07_payment_and_order_status(self):
        """Validates payment methods and order status funnel metrics."""
        results = self._execute_sql_file("07_payment_and_order_status.sql")
        pay_df = results[0]
        status_df = results[1]

        # 5 payment methods
        self.assertEqual(len(pay_df), 5)
        total_orders = pay_df["total_orders_placed"].sum()
        self.assertEqual(total_orders, 3819)

        # 5 fulfillment statuses
        self.assertEqual(len(status_df), 5)
        total_items = status_df["line_items_count"].sum()
        self.assertEqual(total_items, 6379)


if __name__ == "__main__":
    unittest.main()
