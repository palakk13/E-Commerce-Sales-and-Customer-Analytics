"""
Data Cleaning & Canonicalization Module.

Standardizes, cleans, and normalizes validated datasets:
- Strips leading/trailing whitespace across all text columns.
- Canonicalizes casing (e.g. category 'electronics' -> 'Electronics').
- Formats dates to strict ISO YYYY-MM-DD.
- Safely casts numeric fields to standardized float/int datatypes with 2-decimal precision.
- Imputes legitimate optional missing values (e.g. customer phone -> 'NOT_PROVIDED')
  without dropping valid profile records.
- Emits curated datasets into data/processed/.
"""

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd

from config.logging_config import setup_logger
from config.pipeline_config import config

logger = setup_logger("data_cleaner")


class DataCleaner:
    """
    Cleans, normalizes, and canonicalizes validated e-commerce datasets.
    """

    def __init__(self, processed_dir: Optional[Path] = None):
        self.processed_dir = Path(processed_dir) if processed_dir else config.paths.processed_dir
        self.processed_dir.mkdir(parents=True, exist_ok=True)

    # --------------------------------------------------------------------------
    # Individual Entity Cleaning Functions
    # --------------------------------------------------------------------------

    def clean_customers(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Cleans customer records.
        - Strips whitespace from customer_name, email, city, state, country, postal_code.
        - Normalizes email to lowercase.
        - Handles optional phone numbers: fills empty/null with 'NOT_PROVIDED'.
        - Validates signup_date as ISO string YYYY-MM-DD.
        """
        logger.info("Cleaning customer master records (%d rows)...", len(df))
        cleaned = df.copy()

        # 1. String trimming
        str_cols = ["customer_id", "customer_name", "email", "phone", "city", "state", "country", "postal_code"]
        for col in str_cols:
            if col in cleaned.columns:
                cleaned[col] = cleaned[col].astype(str).str.strip()

        # 2. Email normalization
        cleaned["email"] = cleaned["email"].str.lower()

        # 3. Handle legitimate optional missing phone numbers
        # If empty or 'nan', standardize to 'NOT_PROVIDED'
        
        cleaned["phone"] = cleaned["phone"].replace(r"^\s*$", pd.NA, regex=True)
        cleaned["phone"] = cleaned["phone"].fillna("NOT_PROVIDED")


        # 4. Standardize signup_date to ISO YYYY-MM-DD
        cleaned["signup_date"] = pd.to_datetime(cleaned["signup_date"]).dt.strftime("%Y-%m-%d")

        # 5. Remove any duplicate customer_id defensively
        cleaned = cleaned.drop_duplicates(subset=["customer_id"]).reset_index(drop=True)

        logger.info("Customer cleaning complete (%d cleaned rows).", len(cleaned))
        return cleaned

    def clean_products(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Cleans product catalog records.
        - Strips whitespace from product_name, category, sub_category.
        - Canonicalizes category casing to Title Case (e.g., 'electronics' -> 'Electronics').
        - Casts unit_cost and list_price to float rounded to 2 decimals.
        """
        logger.info("Cleaning product catalog (%d rows)...", len(df))
        cleaned = df.copy()

        # 1. String trimming
        for col in ["product_id", "product_name", "category", "sub_category"]:
            if col in cleaned.columns:
                cleaned[col] = cleaned[col].astype(str).str.strip()

        # 2. Canonicalize category casing
        # e.g., 'electronics' -> 'Electronics', 'home & kitchen' -> 'Home & Kitchen'
        cleaned["category"] = cleaned["category"].str.title()
        cleaned["sub_category"] = cleaned["sub_category"].str.title()

        # 3. Numeric casting
        cleaned["unit_cost"] = cleaned["unit_cost"].astype(float).round(2)
        cleaned["list_price"] = cleaned["list_price"].astype(float).round(2)

        # 4. Deduplicate product_id
        cleaned = cleaned.drop_duplicates(subset=["product_id"]).reset_index(drop=True)

        logger.info("Product cleaning complete (%d cleaned rows).", len(cleaned))
        return cleaned

    def clean_orders(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Cleans order headers.
        - Strips whitespace.
        - Standardizes order_status and payment_method title case.
        - Parses order_date to strict ISO YYYY-MM-DD string.
        - Casts shipping_cost to float with 2 decimal precision.
        """
        logger.info("Cleaning orders (%d rows)...", len(df))
        cleaned = df.copy()

        # 1. String trimming
        for col in ["order_id", "customer_id", "order_status", "payment_method"]:
            if col in cleaned.columns:
                cleaned[col] = cleaned[col].astype(str).str.strip()

        # 2. Canonicalize status and payment
        cleaned["order_status"] = cleaned["order_status"].str.title()
        cleaned["payment_method"] = cleaned["payment_method"].str.title()

        # 3. Parse and standardize order_date
        cleaned["order_date"] = pd.to_datetime(cleaned["order_date"]).dt.strftime("%Y-%m-%d")

        # 4. Numeric casting
        cleaned["shipping_cost"] = cleaned["shipping_cost"].astype(float).round(2)

        # 5. Deduplicate order_id
        cleaned = cleaned.drop_duplicates(subset=["order_id"]).reset_index(drop=True)

        logger.info("Order cleaning complete (%d cleaned rows).", len(cleaned))
        return cleaned

    def clean_order_items(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Cleans order line items.
        - Strips whitespace.
        - Casts quantity to int64.
        - Casts unit_price and discount_percent to float rounded to 2 decimals.
        """
        logger.info("Cleaning order items (%d rows)...", len(df))
        cleaned = df.copy()

        # 1. String trimming
        for col in ["order_item_id", "order_id", "product_id"]:
            if col in cleaned.columns:
                cleaned[col] = cleaned[col].astype(str).str.strip()

        # 2. Numeric casting
        cleaned["quantity"] = cleaned["quantity"].astype(int)
        cleaned["unit_price"] = cleaned["unit_price"].astype(float).round(2)
        cleaned["discount_percent"] = cleaned["discount_percent"].astype(float).round(2)

        # 3. Deduplicate order_item_id
        cleaned = cleaned.drop_duplicates(subset=["order_item_id"]).reset_index(drop=True)

        logger.info("Order items cleaning complete (%d cleaned rows).", len(cleaned))
        return cleaned

    # --------------------------------------------------------------------------
    # Master Cleaning Pipeline Execution
    # --------------------------------------------------------------------------

    def clean_all(
        self,
        valid_datasets: Dict[str, pd.DataFrame],
    ) -> Dict[str, pd.DataFrame]:
        """
        Executes cleaning across all validated datasets and persists them to data/processed/.

        Args:
            valid_datasets: Dictionary containing validated DataFrames.

        Returns:
            Dictionary mapping entity names to cleaned DataFrames.
        """
        logger.info("=== Starting Data Cleaning & Canonicalization Pipeline ===")

        cleaned_customers = self.clean_customers(valid_datasets["customers"])
        cleaned_products = self.clean_products(valid_datasets["products"])
        cleaned_orders = self.clean_orders(valid_datasets["orders"])
        cleaned_order_items = self.clean_order_items(valid_datasets["order_items"])

        cleaned_datasets = {
            "customers": cleaned_customers,
            "products": cleaned_products,
            "orders": cleaned_orders,
            "order_items": cleaned_order_items,
        }

        # Save to data/processed/
        self.save_processed_datasets(cleaned_datasets)

        logger.info("=== Cleaning Pipeline Successfully Completed ===")
        return cleaned_datasets

    def save_processed_datasets(self, datasets: Dict[str, pd.DataFrame]) -> None:
        """Saves cleaned DataFrames into data/processed/ as standardized CSVs."""
        for entity_name, df in datasets.items():
            out_file = self.processed_dir / f"{entity_name}_cleaned.csv"
            df.to_csv(out_file, index=False, encoding="utf-8")
            logger.info("Persisted cleaned dataset: %s (%d rows)", out_file.resolve(), len(df))
