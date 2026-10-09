"""
Unit and Integration Tests for Phase 6 Customer Segmentation (RFM + K-Means).

Validates:
1. RFM calculation compliance with FOB Destination revenue recognition.
2. Feature scaling, log1p transformation, and null handling.
3. Deterministic and reproducible clustering across runs.
4. 100% coverage of customer profiles with valid segment assignments.
5. Financial and record conservation reconciliation.
6. Behavioral business logic of cluster profiles.
7. Output artifact generation and raw data immutability.
"""

import hashlib
import json
import sys
import unittest
from pathlib import Path

import numpy as np
import pandas as pd

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.analytics.rfm_segmentation import RFMSegmentationEngine


class TestRFMSegmentation(unittest.TestCase):
    """Test suite for RFM Feature Engineering and K-Means Segmentation Engine."""

    @classmethod
    def setUpClass(cls):
        """Initializes engine and runs segmentation pipeline once for test suite."""
        cls.engine = RFMSegmentationEngine(random_state=42, default_clusters=4)
        cls.pipeline_results = cls.engine.run_pipeline(save_outputs=True, update_warehouse=True)
        cls.rfm_df = cls.engine.load_rfm_data()
        cls.segmented_df = pd.read_csv(cls.engine.analytics_dir / "customer_segments.csv")
        cls.summary_df = pd.read_csv(cls.engine.analytics_dir / "customer_segment_summary.csv")

    def test_rfm_calculations_and_purchase_eligibility(self):
        """
        Verifies that customer RFM metrics strictly reflect FOB Destination revenue recognition:
        - Only Delivered orders contribute to recognized monetary spend.
        - Cancelled and Returned transactions are excluded from realized spend.
        - Total monetary sum across 1,000 customers equals $405,484.47.
        """
        self.assertEqual(len(self.rfm_df), 1000)
        total_monetary = round(float(self.rfm_df["monetary"].sum()), 2)
        self.assertEqual(total_monetary, 405484.47)

        # Non-purchasing customers must have 0 frequency and 0 monetary spend
        zero_freq = self.rfm_df[self.rfm_df["frequency"] == 0]
        self.assertEqual(len(zero_freq), 214)
        self.assertEqual(float(zero_freq["monetary"].sum()), 0.0)

        # Active purchasing customers must have frequency >= 1 and monetary > 0
        active = self.rfm_df[self.rfm_df["frequency"] > 0]
        self.assertEqual(len(active), 786)
        self.assertTrue((active["monetary"] > 0).all())

    def test_feature_scaling_and_missing_value_handling(self):
        """
        Validates feature preprocessing:
        - log1p transform eliminates extreme right-skew.
        - Scaled features have zero mean and unit variance.
        - No NaN or Inf values are produced.
        """
        X_scaled, scaler, work_df = self.engine.preprocess_features(self.rfm_df)
        self.assertEqual(X_scaled.shape, (1000, 3))
        self.assertFalse(np.isnan(X_scaled).any())
        self.assertFalse(np.isinf(X_scaled).any())

        # Check mean is ~0 and std is ~1.0
        np.testing.assert_allclose(X_scaled.mean(axis=0), 0.0, atol=1e-5)
        np.testing.assert_allclose(X_scaled.std(axis=0), 1.0, atol=1e-5)

        # Check work_df contains required transformed columns
        for col in ["log_recency", "log_frequency", "log_monetary", "scaled_recency", "scaled_frequency", "scaled_monetary"]:
            self.assertIn(col, work_df.columns)

    def test_reproducible_clustering(self):
        """
        Verifies that running K-Means clustering multiple times with random_state=42
        yields strictly identical cluster assignments and inertia.
        """
        X_scaled, _, _ = self.engine.preprocess_features(self.rfm_df)

        m1, labels1 = self.engine.fit_model(X_scaled, n_clusters=4)
        m2, labels2 = self.engine.fit_model(X_scaled, n_clusters=4)

        self.assertAlmostEqual(m1.inertia_, m2.inertia_, places=4)
        np.testing.assert_array_equal(labels1, labels2)

    def test_valid_cluster_assignments_for_every_customer(self):
        """
        Validates that every one of the 1,000 customers receives a valid, non-null
        cluster ID and business segment label.
        """
        self.assertEqual(len(self.segmented_df), 1000)
        self.assertFalse(self.segmented_df["cluster_id"].isnull().any())
        self.assertFalse(self.segmented_df["segment_name"].isnull().any())

        expected_segments = {
            "Champions",
            "Recent Active Shoppers",
            "At-Risk / Lapsed Buyers",
            "Unconverted / Inactive",
        }
        actual_segments = set(self.segmented_df["segment_name"].unique())
        self.assertEqual(actual_segments, expected_segments)
        self.assertEqual(set(self.segmented_df["cluster_id"].unique()), {0, 1, 2, 3})

    def test_cluster_summary_reconciliation(self):
        """
        Verifies that the aggregated cluster summary reconciles 100% with verified benchmarks:
        - Customer counts sum to 1,000 (100.0%).
        - Recognized revenue sums to $405,484.47 (100.0%).
        - Realized profit sums to $231,207.47.
        """
        self.assertEqual(len(self.summary_df), 4)

        total_customers = int(self.summary_df["customer_count"].sum())
        self.assertEqual(total_customers, 1000)
        total_customer_pct = round(float(self.summary_df["customer_pct"].sum()), 1)
        self.assertEqual(total_customer_pct, 100.0)

        total_rev = round(float(self.summary_df["total_recognized_revenue"].sum()), 2)
        self.assertEqual(total_rev, 405484.47)
        total_rev_pct = round(float(self.summary_df["revenue_contribution_pct"].sum()), 1)
        self.assertEqual(total_rev_pct, 100.0)

        total_profit = round(float(self.summary_df["profit_contribution_total"].sum()), 2)
        self.assertEqual(total_profit, 231207.47)

    def test_cluster_behavioral_logic(self):
        """
        Validates the behavioral distinctiveness of each customer segment:
        - Champions must have the highest monetary average (> $700) and highest frequency (> 2.5).
        - Recent Active Shoppers must have the lowest recency (< 50 days).
        - At-Risk / Lapsed Buyers must have high recency (> 500 days) and moderate spend.
        - Unconverted / Inactive must have exactly 0 frequency and $0 spend.
        """
        summary_map = self.summary_df.set_index("segment_name").to_dict(orient="index")

        # Champions
        champs = summary_map["Champions"]
        self.assertGreater(champs["monetary_mean"], 700.0)
        self.assertGreater(champs["frequency_mean"], 2.5)
        self.assertGreater(champs["revenue_contribution_pct"], 50.0)  # Over 50% of revenue

        # Recent Active Shoppers
        recent = summary_map["Recent Active Shoppers"]
        self.assertLess(recent["recency_mean_days"], 50.0)
        self.assertGreater(recent["frequency_mean"], 2.0)

        # At-Risk / Lapsed Buyers
        lapsed = summary_map["At-Risk / Lapsed Buyers"]
        self.assertGreater(lapsed["recency_mean_days"], 500.0)
        self.assertLess(lapsed["frequency_mean"], 2.0)

        # Unconverted / Inactive
        inactive = summary_map["Unconverted / Inactive"]
        self.assertEqual(inactive["frequency_mean"], 0.0)
        self.assertEqual(inactive["monetary_mean"], 0.0)
        self.assertEqual(inactive["total_recognized_revenue"], 0.0)

    def test_output_artifacts_and_eval_report_existence(self):
        """Verifies that all required Parquet, CSV, Markdown, and PNG output files exist."""
        # Datasets
        self.assertTrue((self.engine.analytics_dir / "customer_segments.parquet").exists())
        self.assertTrue((self.engine.analytics_dir / "customer_segments.csv").exists())
        self.assertTrue((self.engine.analytics_dir / "customer_segment_summary.parquet").exists())
        self.assertTrue((self.engine.analytics_dir / "customer_segment_summary.csv").exists())

        # Reports
        self.assertTrue((self.engine.reports_dir / "model_evaluation_report.md").exists())
        self.assertTrue((self.engine.reports_dir / "model_evaluation_report.json").exists())

        # Visualizations (4 plots)
        for p_name in [
            "01_elbow_silhouette_evaluation.png",
            "02_rfm_scatter_distribution.png",
            "03_segment_revenue_vs_customers.png",
            "04_segment_rfm_profiles.png",
        ]:
            self.assertTrue((self.engine.viz_dir / p_name).exists(), f"Missing visualization: {p_name}")

    def test_raw_dataset_hash_immutability(self):
        """Validates that Phase 6 processing did not modify any raw operational source files."""
        expected_hashes = {
            "customers.csv": "55e2d362da504e00165e3704e2b8c202a2bd9954f77f891f110f13a6ea057407",
            "products.csv": "2b9063d1d85f0fa0a7ff6c3d3fed3fbf19f9d752930d74bb5ca705e5b8a5ca8e",
            "orders.csv": "c005cfff7325545804f8b9e957427f3af9f18ef14e839efa2c7be8940002763b",
            "order_items.csv": "860a32cd1fc5245007daee54146ed83a9879b3406168ad69c56806f2a0ee0f6f",
        }
        raw_dir = PROJECT_ROOT / "data" / "raw"
        for fname, exp_hash in expected_hashes.items():
            actual_hash = hashlib.sha256((raw_dir / fname).read_bytes()).hexdigest()
            self.assertEqual(
                actual_hash,
                exp_hash,
                f"Raw dataset {fname} was modified! Hash mismatch.",
            )


if __name__ == "__main__":
    unittest.main(verbosity=2)
