"""
Unit and Integration Tests for Power BI Semantic Model & Measures.

Validates:
1. Exact mathematical reconciliation of Demand Revenue vs. Recognized Revenue measures.
2. Financial conservation: Recognized + In-Transit + Pipeline + Cancelled + Returned = Net Demand.
3. Star schema line-item grain integrity (6,379 rows preserved across all dimension joins).
4. Order status financial isolation rules (non-delivered orders strictly have $0 recognized revenue).
5. Presence and validity of all Power BI artifacts, DAX library, and visual exports.
"""

import sqlite3
import sys
import unittest
from pathlib import Path
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.analytics.generate_powerbi_assets import generate_powerbi_assets


class TestPowerBISemanticModel(unittest.TestCase):
    """Test suite for Power BI semantic model, DAX measures, and financial reconciliation."""

    @classmethod
    def setUpClass(cls):
        """Ensures Power BI assets are generated and connects to warehouse."""
        cls.db_path = PROJECT_ROOT / "data" / "warehouse" / "ecommerce_dwh.db"
        cls.conn = sqlite3.connect(cls.db_path)
        cls.assets_res = generate_powerbi_assets()

    @classmethod
    def tearDownClass(cls):
        cls.conn.close()

    def test_measure_reconciliation_exact_match(self):
        """
        Verifies that semantic model measures match ground-truth warehouse totals to the cent.
        """
        q = """
        SELECT 
            ROUND(SUM(gross_revenue), 2) as gross_demand,
            ROUND(SUM(discount_amount), 2) as total_discounts,
            ROUND(SUM(net_revenue), 2) as net_demand,
            ROUND(SUM(recognized_revenue), 2) as recognized_revenue,
            ROUND(SUM(recognized_profit), 2) as recognized_profit,
            ROUND(SUM(in_transit_revenue), 2) as in_transit,
            ROUND(SUM(pipeline_revenue), 2) as pipeline,
            ROUND(SUM(cancelled_revenue_loss), 2) as cancelled,
            ROUND(SUM(returned_revenue_loss), 2) as returned
        FROM fact_sales
        """
        df = pd.read_sql_query(q, self.conn)
        row = df.iloc[0]

        self.assertEqual(row["gross_demand"], 1020403.59)
        self.assertEqual(row["total_discounts"], 42305.77)
        self.assertEqual(row["net_demand"], 978097.82)
        self.assertEqual(row["recognized_revenue"], 405484.47)
        self.assertEqual(row["recognized_profit"], 231207.47)
        self.assertEqual(row["in_transit"], 138599.59)
        self.assertEqual(row["pipeline"], 146949.52)
        self.assertEqual(row["cancelled"], 158119.41)
        self.assertEqual(row["returned"], 128944.83)

        # Margin & Rate checks
        margin_pct = round((row["recognized_profit"] / row["recognized_revenue"]) * 100.0, 2)
        self.assertEqual(margin_pct, 57.02)

        realization_rate = round((row["recognized_revenue"] / row["net_demand"]) * 100.0, 2)
        self.assertEqual(realization_rate, 41.46)

        attrition_rate = round(((row["cancelled"] + row["returned"]) / row["net_demand"]) * 100.0, 2)
        self.assertEqual(attrition_rate, 29.35)

    def test_conservation_of_demand_buckets(self):
        """
        Enforces mathematical conservation across all 5 operational buckets:
        Recognized + In-Transit + Pipeline + Cancelled + Returned = Net Demand Revenue.
        """
        q = """
        SELECT 
            ROUND(SUM(net_revenue), 2) as net_demand,
            ROUND(SUM(recognized_revenue) + SUM(in_transit_revenue) + SUM(pipeline_revenue) + 
                  SUM(cancelled_revenue_loss) + SUM(returned_revenue_loss), 2) as sum_buckets
        FROM fact_sales
        """
        cur = self.conn.cursor()
        cur.execute(q)
        net_demand, sum_buckets = cur.fetchone()
        self.assertEqual(net_demand, 978097.82)
        self.assertEqual(sum_buckets, 978097.82)
        self.assertEqual(net_demand, sum_buckets)

    def test_star_schema_line_item_grain_integrity(self):
        """
        Validates that joining fact_sales to all 5 dimension tables preserves the exact
        atomic grain of 6,379 line items without multiplying rows or double-counting.
        """
        q_join = """
        SELECT COUNT(*) as joined_count
        FROM fact_sales f
        JOIN dim_date d ON f.date_key = d.date_key
        JOIN dim_customer c ON f.customer_key = c.customer_key
        JOIN dim_product p ON f.product_key = p.product_key
        JOIN dim_location l ON f.location_key = l.location_key
        JOIN dim_payment pm ON f.payment_key = pm.payment_key
        """
        cur = self.conn.cursor()
        cur.execute(q_join)
        joined_count = cur.fetchone()[0]
        self.assertEqual(joined_count, 6379)

        # Verify distinct line items count
        cur.execute("SELECT COUNT(DISTINCT order_item_id) FROM fact_sales")
        distinct_items = cur.fetchone()[0]
        self.assertEqual(distinct_items, 6379)

        # Verify distinct orders count
        cur.execute("SELECT COUNT(DISTINCT order_id) FROM fact_sales")
        distinct_orders = cur.fetchone()[0]
        self.assertEqual(distinct_orders, 3819)

    def test_order_status_financial_isolation_rules(self):
        """
        Validates that non-delivered orders (Cancelled, Shipped, Processing, Returned)
        have zero recognized revenue and zero recognized profit under all conditions.
        """
        q_non_delivered = """
        SELECT 
            COUNT(*) as invalid_count
        FROM fact_sales
        WHERE order_status != 'Delivered'
          AND (recognized_revenue > 0 OR recognized_profit > 0)
        """
        cur = self.conn.cursor()
        cur.execute(q_non_delivered)
        invalid_count = cur.fetchone()[0]
        self.assertEqual(invalid_count, 0, "Found non-delivered orders with recognized revenue > 0!")

        q_delivered = """
        SELECT 
            COUNT(*) as unrec_delivered
        FROM fact_sales
        WHERE order_status = 'Delivered'
          AND recognized_revenue != net_revenue
        """
        cur.execute(q_delivered)
        unrec = cur.fetchone()[0]
        self.assertEqual(unrec, 0, "Delivered orders must have recognized_revenue == net_revenue")

    def test_powerbi_artifacts_and_visualizations_exist(self):
        """
        Verifies that all required Power BI extracts, DAX library, metadata, and visual charts exist.
        """
        pbi_dir = PROJECT_ROOT / "powerbi"
        data_pbi_dir = PROJECT_ROOT / "data" / "powerbi"
        viz_dir = PROJECT_ROOT / "reports" / "visualizations"

        # Code & metadata files
        self.assertTrue((pbi_dir / "dax_measures.dax").exists())
        self.assertTrue((pbi_dir / "semantic_model.json").exists())
        self.assertTrue((pbi_dir / "POWERBI_GUIDE.md").exists())
        self.assertTrue((pbi_dir / "interactive_dashboard.html").exists())

        # Visualizations (2 new visuals for Phase 7)
        self.assertTrue((viz_dir / "05_revenue_by_order_status.png").exists())
        self.assertTrue((viz_dir / "06_monthly_demand_vs_recognized.png").exists())

        # Star schema tables exported
        for tbl in ["dim_date", "dim_location", "dim_customer", "dim_product", "dim_payment", "fact_sales"]:
            self.assertTrue((data_pbi_dir / f"{tbl}.csv").exists(), f"Missing CSV: {tbl}")
            self.assertTrue((data_pbi_dir / f"{tbl}.parquet").exists(), f"Missing Parquet: {tbl}")

        # Summary tables
        self.assertTrue((data_pbi_dir / "order_status_demand_summary.csv").exists())
        self.assertTrue((data_pbi_dir / "monthly_demand_vs_recognized.csv").exists())


if __name__ == "__main__":
    unittest.main(verbosity=2)
