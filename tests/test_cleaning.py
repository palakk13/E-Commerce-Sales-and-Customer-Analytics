"""
Unit Tests for Data Cleaning and Canonicalization Module.

Tests string stripping, casing normalization, numeric type casting,
and optional missing-value handling.
"""

import unittest
import pandas as pd

from src.transformation.cleaner import DataCleaner


class TestDataCleaner(unittest.TestCase):
    """Test suite for DataCleaner class."""

    def setUp(self):
        self.cleaner = DataCleaner()

    def test_clean_customers_whitespace_and_email(self):
        """Whitespace is trimmed and email is converted to lowercase."""
        df = pd.DataFrame([{
            "customer_id": "CUST_001",
            "customer_name": "   Jane Doe   ",
            "email": "JANE.DOE@EXAMPLE.COM",
            "phone": "+91-9876543210",
            "city": " Mumbai ",
            "state": "Maharashtra",
            "country": "India",
            "postal_code": "400001",
            "signup_date": "2024-05-10",
        }])
        cleaned = self.cleaner.clean_customers(df)
        self.assertEqual(cleaned.iloc[0]["customer_name"], "Jane Doe")
        self.assertEqual(cleaned.iloc[0]["email"], "jane.doe@example.com")
        self.assertEqual(cleaned.iloc[0]["city"], "Mumbai")

    def test_clean_customers_optional_phone_handling(self):
        """Missing phone numbers are standardized to 'NOT_PROVIDED' without dropping the row."""
        df = pd.DataFrame([
            {
                "customer_id": "CUST_002",
                "customer_name": "Bob Smith",
                "email": "bob@example.com",
                "phone": "",  # Empty string
                "city": "Bengaluru",
                "state": "Karnataka",
                "country": "India",
                "postal_code": "560001",
                "signup_date": "2024-06-15",
            },
            {
                "customer_id": "CUST_003",
                "customer_name": "Alice Wong",
                "email": "alice@example.com",
                "phone": None,  # None / NaN
                "city": "London",
                "state": "Greater London",
                "country": "UK",
                "postal_code": "EC1A 1BB",
                "signup_date": "2024-07-20",
            },
        ])
        cleaned = self.cleaner.clean_customers(df)
        self.assertEqual(len(cleaned), 2, "Both customers with missing phone must be preserved")
        self.assertEqual(cleaned.iloc[0]["phone"], "NOT_PROVIDED")
        self.assertEqual(cleaned.iloc[1]["phone"], "NOT_PROVIDED")

    def test_clean_products_casing_and_numeric_rounding(self):
        """Categories are canonicalized to Title Case and prices rounded to 2 decimals."""
        df = pd.DataFrame([{
            "product_id": "PROD_001",
            "product_name": "  noise-cancelling headphones  ",
            "category": "electronics",  # Lowercase
            "sub_category": "audio gadgets",
            "unit_cost": "45.0001",
            "list_price": "99.994",
        }])
        cleaned = self.cleaner.clean_products(df)
        self.assertEqual(cleaned.iloc[0]["category"], "Electronics")
        self.assertEqual(cleaned.iloc[0]["sub_category"], "Audio Gadgets")
        self.assertEqual(cleaned.iloc[0]["product_name"], "noise-cancelling headphones")
        self.assertEqual(cleaned.iloc[0]["unit_cost"], 45.00)
        self.assertEqual(cleaned.iloc[0]["list_price"], 99.99)

    def test_clean_orders_and_dates(self):
        """Dates are formatted as YYYY-MM-DD and strings title-cased."""
        df = pd.DataFrame([{
            "order_id": "ORD_001",
            "customer_id": "CUST_001",
            "order_date": "2025-04-18",
            "order_status": "delivered",
            "payment_method": "credit card",
            "shipping_cost": "7.5",
        }])
        cleaned = self.cleaner.clean_orders(df)
        self.assertEqual(cleaned.iloc[0]["order_status"], "Delivered")
        self.assertEqual(cleaned.iloc[0]["payment_method"], "Credit Card")
        self.assertEqual(cleaned.iloc[0]["order_date"], "2025-04-18")
        self.assertEqual(cleaned.iloc[0]["shipping_cost"], 7.50)

    def test_clean_order_items_types(self):
        """Quantity is cast to int and prices cast to float."""
        df = pd.DataFrame([{
            "order_item_id": "ITEM_001",
            "order_id": "ORD_001",
            "product_id": "PROD_001",
            "quantity": "3",
            "unit_price": "49.99",
            "discount_percent": "10.0",
        }])
        cleaned = self.cleaner.clean_order_items(df)
        self.assertEqual(cleaned.iloc[0]["quantity"], 3)
        self.assertEqual(cleaned.iloc[0]["unit_price"], 49.99)
        self.assertEqual(cleaned.iloc[0]["discount_percent"], 10.0)


if __name__ == "__main__":
    unittest.main()
