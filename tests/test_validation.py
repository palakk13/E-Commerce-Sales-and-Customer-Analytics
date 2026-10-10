"""
Unit Tests for Data Quality Validation Gatekeeper.

Tests constraint validations, edge cases, multi-defect overlaps,
and quarantine routing logic.
"""

import shutil
import unittest
from pathlib import Path

import pandas as pd

from src.validation.data_validator import DataValidator


class TestDataValidator(unittest.TestCase):
    """Test suite for DataValidator class."""

    def setUp(self):
        self.test_dir = Path("logs/test_rejected")
        self.validator = DataValidator(rejected_dir=self.test_dir)

    def tearDown(self):
        if self.test_dir.exists():
            shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_customer_validation_valid(self):
        """Valid customer record passes validation."""
        df = pd.DataFrame([{
            "customer_id": "CUST_99001",
            "customer_name": "Test User",
            "email": "test@example.com",
            "phone": "+91-9999999999",
            "city": "Bengaluru",
            "state": "Karnataka",
            "country": "India",
            "postal_code": "560001",
            "signup_date": "2024-01-15",
        }])
        valid_df, q_df = self.validator.validate_customers(df)
        self.assertEqual(len(valid_df), 1)
        self.assertTrue(q_df.empty)

    def test_customer_optional_phone_allowed_null(self):
        """Missing phone number is legitimate and must NOT trigger rejection."""
        df = pd.DataFrame([{
            "customer_id": "CUST_99002",
            "customer_name": "No Phone User",
            "email": "nophone@example.com",
            "phone": "",  # Empty optional field
            "city": "Mumbai",
            "state": "Maharashtra",
            "country": "India",
            "postal_code": "400001",
            "signup_date": "2024-02-01",
        }])
        valid_df, q_df = self.validator.validate_customers(df)
        self.assertEqual(len(valid_df), 1, "Customer with missing phone must not be rejected")
        self.assertTrue(q_df.empty, "Quarantine must be empty for optional phone missing")

    def test_customer_duplicate_id_quarantined(self):
        """Duplicate customer_id triggers DUPLICATE_PRIMARY_KEY quarantine."""
        df = pd.DataFrame([
            {
                "customer_id": "CUST_DUP01",
                "customer_name": "First Instance",
                "email": "first@example.com",
                "phone": "",
                "city": "Delhi",
                "state": "Delhi",
                "country": "India",
                "postal_code": "110001",
                "signup_date": "2024-01-01",
            },
            {
                "customer_id": "CUST_DUP01",  # Duplicate PK
                "customer_name": "Second Instance",
                "email": "second@example.com",
                "phone": "",
                "city": "Delhi",
                "state": "Delhi",
                "country": "India",
                "postal_code": "110001",
                "signup_date": "2024-01-01",
            },
        ])
        valid_df, q_df = self.validator.validate_customers(df)
        self.assertEqual(len(valid_df), 1)
        self.assertEqual(len(q_df), 1)
        self.assertIn("DUPLICATE_PRIMARY_KEY", q_df.iloc[0]["reject_reason"])

    def test_customer_null_id_quarantined(self):
        """Missing customer_id triggers NULL_CUSTOMER_KEY quarantine."""
        df = pd.DataFrame([{
            "customer_id": "",
            "customer_name": "Nameless ID",
            "email": "test@example.com",
            "phone": "",
            "city": "Pune",
            "state": "Maharashtra",
            "country": "India",
            "postal_code": "411001",
            "signup_date": "2024-03-01",
        }])
        valid_df, q_df = self.validator.validate_customers(df)
        self.assertEqual(len(valid_df), 0)
        self.assertEqual(len(q_df), 1)
        self.assertIn("NULL_CUSTOMER_KEY", q_df.iloc[0]["reject_reason"])

    def test_product_negative_cost_and_price(self):
        """Non-positive cost and price are quarantined."""
        df = pd.DataFrame([
            {
                "product_id": "PROD_BAD1",
                "product_name": "Bad Cost Item",
                "category": "Electronics",
                "sub_category": "Gadgets",
                "unit_cost": "-10.00",
                "list_price": "20.00",
            },
            {
                "product_id": "PROD_BAD2",
                "product_name": "Zero Price Item",
                "category": "Books",
                "sub_category": "Paperbacks",
                "unit_cost": "5.00",
                "list_price": "0.00",
            },
        ])
        valid_df, q_df = self.validator.validate_products(df)
        self.assertEqual(len(valid_df), 0)
        self.assertEqual(len(q_df), 2)
        self.assertIn("INVALID_UNIT_COST", q_df.iloc[0]["reject_reason"])
        self.assertIn("INVALID_LIST_PRICE", q_df.iloc[1]["reject_reason"])

    def test_order_invalid_date_format(self):
        """Unparseable order date triggers INVALID_DATE_FORMAT."""
        df = pd.DataFrame([{
            "order_id": "ORD_9901",
            "customer_id": "CUST_001",
            "order_date": "2026-99-99",  # Invalid calendar date
            "order_status": "Delivered",
            "payment_method": "UPI",
            "shipping_cost": "5.00",
        }])
        valid_df, q_df = self.validator.validate_orders(df, valid_customer_ids={"CUST_001"})
        self.assertEqual(len(valid_df), 0)
        self.assertEqual(len(q_df), 1)
        self.assertIn("INVALID_DATE_FORMAT", q_df.iloc[0]["reject_reason"])

    def test_order_dual_defect_overlap(self):
        """Record violating both customer ID and date format captures both reasons."""
        df = pd.DataFrame([{
            "order_id": "ORD_DUAL01",
            "customer_id": "",  # Missing customer key
            "order_date": "INVALID_DATE",  # Corrupt date
            "order_status": "Shipped",
            "payment_method": "Credit Card",
            "shipping_cost": "10.00",
        }])
        valid_df, q_df = self.validator.validate_orders(df, valid_customer_ids={"CUST_001"})
        self.assertEqual(len(valid_df), 0)
        self.assertEqual(len(q_df), 1)
        reasons = q_df.iloc[0]["reject_reason"]
        self.assertIn("NULL_CUSTOMER_KEY", reasons)
        self.assertIn("INVALID_DATE_FORMAT", reasons)

    def test_order_items_negative_price_and_quantity(self):
        """Non-positive unit prices and quantities are properly quarantined."""
        df = pd.DataFrame([
            {
                "order_item_id": "ITEM_NEG01",
                "order_id": "ORD_001",
                "product_id": "PROD_001",
                "quantity": "-2",  # Negative qty
                "unit_price": "50.00",
                "discount_percent": "0.0",
            },
            {
                "order_item_id": "ITEM_NEG02",
                "order_id": "ORD_001",
                "product_id": "PROD_001",
                "quantity": "1",
                "unit_price": "-25.00",  # Negative price
                "discount_percent": "0.0",
            },
        ])
        valid_df, q_df = self.validator.validate_order_items(
            df, valid_order_ids={"ORD_001"}, valid_product_ids={"PROD_001"}
        )
        self.assertEqual(len(valid_df), 0)
        self.assertEqual(len(q_df), 2)
        self.assertIn("INVALID_QUANTITY", q_df.iloc[0]["reject_reason"])
        self.assertIn("NEGATIVE_UNIT_PRICE", q_df.iloc[1]["reject_reason"])

    def test_order_items_referential_integrity(self):
        """Invalid product FK and dangling order FK are quarantined."""
        df = pd.DataFrame([{
            "order_item_id": "ITEM_FK01",
            "order_id": "ORD_001",
            "product_id": "PROD_DOES_NOT_EXIST",  # Dangling product FK
            "quantity": "2",
            "unit_price": "30.00",
            "discount_percent": "5.0",
        }])
        valid_df, q_df = self.validator.validate_order_items(
            df, valid_order_ids={"ORD_001"}, valid_product_ids={"PROD_001"}
        )
        self.assertEqual(len(valid_df), 0)
        self.assertEqual(len(q_df), 1)
        self.assertIn("INVALID_PRODUCT_FK", q_df.iloc[0]["reject_reason"])

    def test_order_items_excess_discount(self):
        """Discount exceeding 100% triggers INVALID_DISCOUNT_PERCENT."""
        df = pd.DataFrame([{
            "order_item_id": "ITEM_DISC01",
            "order_id": "ORD_001",
            "product_id": "PROD_001",
            "quantity": "1",
            "unit_price": "100.00",
            "discount_percent": "125.0",  # Excess discount > 100%
        }])
        valid_df, q_df = self.validator.validate_order_items(
            df, valid_order_ids={"ORD_001"}, valid_product_ids={"PROD_001"}
        )
        self.assertEqual(len(valid_df), 0)
        self.assertEqual(len(q_df), 1)
        self.assertIn("INVALID_DISCOUNT_PERCENT", q_df.iloc[0]["reject_reason"])


if __name__ == "__main__":
    unittest.main()
