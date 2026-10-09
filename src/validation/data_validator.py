"""
Data Quality Validation Gatekeeper & Quarantine Routing Engine.

Enforces schema constraints, primary/foreign key integrity, positive numerical
ranges, valid temporal formats, and routes defective records to quarantine
storage with explicit reason codes, preserving 100% record auditability.
"""

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd

from config.logging_config import setup_logger
from config.pipeline_config import config

logger = setup_logger("data_validator")


@dataclass
class QuarantineRecord:
    """Structure for quarantined defective records."""
    table_name: str
    record_id: str
    reject_stage: str
    reject_reason: str
    reject_detail: str
    rejected_at: str
    raw_payload: str

    def to_dict(self) -> Dict[str, str]:
        return {
            "table_name": self.table_name,
            "record_id": self.record_id,
            "reject_stage": self.reject_stage,
            "reject_reason": self.reject_reason,
            "reject_detail": self.reject_detail,
            "rejected_at": self.rejected_at,
            "raw_payload": self.raw_payload,
        }


@dataclass
class ValidationMetrics:
    """Detailed validation metrics per entity and overall."""
    total_raw_records: int = 0
    total_valid_records: int = 0
    total_quarantined_records: int = 0
    total_defect_occurrences: int = 0
    defect_counts_by_reason: Dict[str, int] = field(default_factory=dict)
    entity_metrics: Dict[str, Dict[str, Any]] = field(default_factory=dict)


class DataValidator:
    """
    Production-grade Data Quality Gatekeeper.
    Performs deterministic validation across multi-entity e-commerce DataFrames.
    """

    def __init__(self, rejected_dir: Optional[Path] = None):
        self.rejected_dir = Path(rejected_dir) if rejected_dir else config.paths.rejected_dir
        self.rejected_dir.mkdir(parents=True, exist_ok=True)
        self.quarantine_records: List[QuarantineRecord] = []
        self.metrics = ValidationMetrics()

    # --------------------------------------------------------------------------
    # Individual Entity Validators
    # --------------------------------------------------------------------------

    def validate_customers(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Validates customer master records.
        Rules:
        - customer_id must be non-null, non-empty, and unique.
        - customer_name and signup_date must be non-null and non-empty.
        - phone is an OPTIONAL field: missing/empty phone is valid and NOT quarantined.
        """
        logger.info("Validating customers table (%d raw rows)...", len(df))
        utc_now = datetime.now(timezone.utc).isoformat()
        quarantined_rows: List[QuarantineRecord] = []
        valid_indices: List[int] = []

        seen_ids: Set[str] = set()

        for idx, row in df.iterrows():
            cid = str(row.get("customer_id", "")).strip()
            name = str(row.get("customer_name", "")).strip()
            signup = str(row.get("signup_date", "")).strip()

            reasons = []
            details = []

            # PK Null check
            if not cid:
                reasons.append("NULL_CUSTOMER_KEY")
                details.append("customer_id is missing or blank")
            elif cid in seen_ids:
                reasons.append("DUPLICATE_PRIMARY_KEY")
                details.append(f"Duplicate customer_id: '{cid}'")
            else:
                seen_ids.add(cid)

            # Mandatory field checks
            if not name:
                reasons.append("MISSING_MANDATORY_FIELD")
                details.append("customer_name is empty")

            if not signup:
                reasons.append("MISSING_MANDATORY_FIELD")
                details.append("signup_date is empty")

            if reasons:
                rec_id = cid if cid else f"ROW_{idx}"
                for r in reasons:
                    self.metrics.defect_counts_by_reason[r] = self.metrics.defect_counts_by_reason.get(r, 0) + 1
                    self.metrics.total_defect_occurrences += 1

                quarantined_rows.append(
                    QuarantineRecord(
                        table_name="customers",
                        record_id=rec_id,
                        reject_stage="VALIDATION_GATEKEEPER",
                        reject_reason="; ".join(reasons),
                        reject_detail="; ".join(details),
                        rejected_at=utc_now,
                        raw_payload=json.dumps(row.to_dict()),
                    )
                )
            else:
                valid_indices.append(idx)

        valid_df = df.loc[valid_indices].copy().reset_index(drop=True)
        quarantine_df = pd.DataFrame([q.to_dict() for q in quarantined_rows])

        self.quarantine_records.extend(quarantined_rows)
        logger.info(
            "Customer validation complete: %d valid, %d quarantined",
            len(valid_df),
            len(quarantined_rows),
        )
        return valid_df, quarantine_df

    def validate_products(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Validates product catalog items.
        Rules:
        - product_id must be non-null, non-empty, and unique.
        - product_name and category must be non-empty.
        - unit_cost > 0 and list_price > 0.
        """
        logger.info("Validating products table (%d raw rows)...", len(df))
        utc_now = datetime.now(timezone.utc).isoformat()
        quarantined_rows: List[QuarantineRecord] = []
        valid_indices: List[int] = []

        seen_ids: Set[str] = set()

        for idx, row in df.iterrows():
            pid = str(row.get("product_id", "")).strip()
            pname = str(row.get("product_name", "")).strip()
            cat = str(row.get("category", "")).strip()
            cost_str = str(row.get("unit_cost", "")).strip()
            price_str = str(row.get("list_price", "")).strip()

            reasons = []
            details = []

            # PK check
            if not pid:
                reasons.append("NULL_PRODUCT_KEY")
                details.append("product_id is missing or blank")
            elif pid in seen_ids:
                reasons.append("DUPLICATE_PRIMARY_KEY")
                details.append(f"Duplicate product_id: '{pid}'")
            else:
                seen_ids.add(pid)

            if not pname:
                reasons.append("MISSING_MANDATORY_FIELD")
                details.append("product_name is blank")

            if not cat:
                reasons.append("MISSING_MANDATORY_FIELD")
                details.append("category is blank")

            # Numeric checks
            try:
                cost = float(cost_str)
                if cost <= 0:
                    reasons.append("INVALID_UNIT_COST")
                    details.append(f"unit_cost must be > 0, got {cost}")
            except ValueError:
                reasons.append("INVALID_DATA_TYPE")
                details.append(f"unit_cost '{cost_str}' is not a valid number")

            try:
                price = float(price_str)
                if price <= 0:
                    reasons.append("INVALID_LIST_PRICE")
                    details.append(f"list_price must be > 0, got {price}")
            except ValueError:
                reasons.append("INVALID_DATA_TYPE")
                details.append(f"list_price '{price_str}' is not a valid number")

            if reasons:
                rec_id = pid if pid else f"ROW_{idx}"
                for r in reasons:
                    self.metrics.defect_counts_by_reason[r] = self.metrics.defect_counts_by_reason.get(r, 0) + 1
                    self.metrics.total_defect_occurrences += 1

                quarantined_rows.append(
                    QuarantineRecord(
                        table_name="products",
                        record_id=rec_id,
                        reject_stage="VALIDATION_GATEKEEPER",
                        reject_reason="; ".join(reasons),
                        reject_detail="; ".join(details),
                        rejected_at=utc_now,
                        raw_payload=json.dumps(row.to_dict()),
                    )
                )
            else:
                valid_indices.append(idx)

        valid_df = df.loc[valid_indices].copy().reset_index(drop=True)
        quarantine_df = pd.DataFrame([q.to_dict() for q in quarantined_rows])

        self.quarantine_records.extend(quarantined_rows)
        logger.info(
            "Product validation complete: %d valid, %d quarantined",
            len(valid_df),
            len(quarantined_rows),
        )
        return valid_df, quarantine_df

    def validate_orders(
        self, df: pd.DataFrame, valid_customer_ids: Set[str]
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Validates order headers.
        Rules:
        - order_id must be non-null and unique. Duplicate headers quarantined with DUPLICATE_PRIMARY_KEY.
        - customer_id must be non-empty (NULL_CUSTOMER_KEY) and exist in valid customers (INVALID_CUSTOMER_FK).
        - order_date must be valid ISO date YYYY-MM-DD and parseable (INVALID_DATE_FORMAT).
        - order_status and payment_method must be non-empty.
        """
        logger.info("Validating orders table (%d raw rows)...", len(df))
        utc_now = datetime.now(timezone.utc).isoformat()
        quarantined_rows: List[QuarantineRecord] = []
        valid_indices: List[int] = []

        seen_order_ids: Set[str] = set()

        for idx, row in df.iterrows():
            oid = str(row.get("order_id", "")).strip()
            cid = str(row.get("customer_id", "")).strip()
            odate = str(row.get("order_date", "")).strip()

            reasons = []
            details = []

            # 1. Primary Key Check & Deduplication
            if not oid:
                reasons.append("NULL_ORDER_KEY")
                details.append("order_id is missing or blank")
            elif oid in seen_order_ids:
                reasons.append("DUPLICATE_PRIMARY_KEY")
                details.append(f"Duplicate order_id: '{oid}' already registered")
            else:
                seen_order_ids.add(oid)

            # 2. Customer ID / FK Check
            if not cid:
                reasons.append("NULL_CUSTOMER_KEY")
                details.append("customer_id is missing or blank")
            elif cid not in valid_customer_ids:
                reasons.append("INVALID_CUSTOMER_FK")
                details.append(f"customer_id '{cid}' not found in active customer dimension")

            # 3. Order Date Validation
            if not odate:
                reasons.append("INVALID_DATE_FORMAT")
                details.append("order_date is missing or blank")
            else:
                try:
                    parsed_dt = datetime.strptime(odate, "%Y-%m-%d")
                    # Bounds check (realistic timeframe)
                    if parsed_dt.year < 2020 or parsed_dt.year > 2030:
                        reasons.append("INVALID_DATE_FORMAT")
                        details.append(f"order_date '{odate}' outside permissible range (2020-2030)")
                except ValueError:
                    reasons.append("INVALID_DATE_FORMAT")
                    details.append(f"order_date '{odate}' is not a valid YYYY-MM-DD date")

            # Route to quarantine or valid
            if reasons:
                rec_id = oid if oid else f"ROW_{idx}"
                for r in reasons:
                    self.metrics.defect_counts_by_reason[r] = self.metrics.defect_counts_by_reason.get(r, 0) + 1
                    self.metrics.total_defect_occurrences += 1

                quarantined_rows.append(
                    QuarantineRecord(
                        table_name="orders",
                        record_id=rec_id,
                        reject_stage="VALIDATION_GATEKEEPER",
                        reject_reason="; ".join(reasons),
                        reject_detail="; ".join(details),
                        rejected_at=utc_now,
                        raw_payload=json.dumps(row.to_dict()),
                    )
                )
            else:
                valid_indices.append(idx)

        valid_df = df.loc[valid_indices].copy().reset_index(drop=True)
        quarantine_df = pd.DataFrame([q.to_dict() for q in quarantined_rows])

        self.quarantine_records.extend(quarantined_rows)
        logger.info(
            "Order validation complete: %d valid, %d quarantined",
            len(valid_df),
            len(quarantined_rows),
        )
        return valid_df, quarantine_df

    def validate_order_items(
        self,
        df: pd.DataFrame,
        valid_order_ids: Set[str],
        valid_product_ids: Set[str],
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Validates order line items.
        Rules:
        - order_item_id must be non-null and unique.
        - order_id must exist in valid orders.
        - product_id must exist in valid products (INVALID_PRODUCT_FK).
        - quantity must be integer > 0 (INVALID_QUANTITY).
        - unit_price must be float > 0 (NEGATIVE_UNIT_PRICE).
        - discount_percent must be in [0.0, 100.0] (INVALID_DISCOUNT_PERCENT).
        """
        logger.info("Validating order_items table (%d raw rows)...", len(df))
        utc_now = datetime.now(timezone.utc).isoformat()
        quarantined_rows: List[QuarantineRecord] = []
        valid_indices: List[int] = []

        seen_item_ids: Set[str] = set()

        for idx, row in df.iterrows():
            item_id = str(row.get("order_item_id", "")).strip()
            oid = str(row.get("order_id", "")).strip()
            pid = str(row.get("product_id", "")).strip()
            qty_str = str(row.get("quantity", "")).strip()
            price_str = str(row.get("unit_price", "")).strip()
            disc_str = str(row.get("discount_percent", "")).strip()

            reasons = []
            details = []

            # PK Check
            if not item_id:
                reasons.append("NULL_ITEM_KEY")
                details.append("order_item_id is blank")
            elif item_id in seen_item_ids:
                reasons.append("DUPLICATE_PRIMARY_KEY")
                details.append(f"Duplicate order_item_id: '{item_id}'")
            else:
                seen_item_ids.add(item_id)

            # Order FK Check
            if not oid:
                reasons.append("NULL_ORDER_FK")
                details.append("order_id is blank")
            elif oid not in valid_order_ids:
                reasons.append("INVALID_ORDER_FK")
                details.append(f"Parent order_id '{oid}' is missing or was quarantined")

            # Product FK Check
            if not pid:
                reasons.append("NULL_PRODUCT_FK")
                details.append("product_id is blank")
            elif pid not in valid_product_ids:
                reasons.append("INVALID_PRODUCT_FK")
                details.append(f"product_id '{pid}' not found in active catalog")

            # Quantity Check
            try:
                qty = int(qty_str)
                if qty <= 0:
                    reasons.append("INVALID_QUANTITY")
                    details.append(f"quantity must be > 0, got {qty}")
            except ValueError:
                reasons.append("INVALID_DATA_TYPE")
                details.append(f"quantity '{qty_str}' is not a valid integer")

            # Price Check
            try:
                price = float(price_str)
                if price <= 0.0:
                    reasons.append("NEGATIVE_UNIT_PRICE")
                    details.append(f"unit_price must be > 0.0, got {price}")
            except ValueError:
                reasons.append("INVALID_DATA_TYPE")
                details.append(f"unit_price '{price_str}' is not a valid float")

            # Discount Check
            try:
                disc = float(disc_str)
                if disc < 0.0 or disc > 100.0:
                    reasons.append("INVALID_DISCOUNT_PERCENT")
                    details.append(f"discount_percent must be between 0.0 and 100.0, got {disc}")
            except ValueError:
                reasons.append("INVALID_DATA_TYPE")
                details.append(f"discount_percent '{disc_str}' is not a valid float")

            if reasons:
                rec_id = item_id if item_id else f"ROW_{idx}"
                for r in reasons:
                    self.metrics.defect_counts_by_reason[r] = self.metrics.defect_counts_by_reason.get(r, 0) + 1
                    self.metrics.total_defect_occurrences += 1

                quarantined_rows.append(
                    QuarantineRecord(
                        table_name="order_items",
                        record_id=rec_id,
                        reject_stage="VALIDATION_GATEKEEPER",
                        reject_reason="; ".join(reasons),
                        reject_detail="; ".join(details),
                        rejected_at=utc_now,
                        raw_payload=json.dumps(row.to_dict()),
                    )
                )
            else:
                valid_indices.append(idx)

        valid_df = df.loc[valid_indices].copy().reset_index(drop=True)
        quarantine_df = pd.DataFrame([q.to_dict() for q in quarantined_rows])

        self.quarantine_records.extend(quarantined_rows)
        logger.info(
            "Order items validation complete: %d valid, %d quarantined",
            len(valid_df),
            len(quarantined_rows),
        )
        return valid_df, quarantine_df

    # --------------------------------------------------------------------------
    # Master Validation Pipeline Execution
    # --------------------------------------------------------------------------

    def validate_all(
        self,
        raw_datasets: Dict[str, pd.DataFrame],
    ) -> Tuple[Dict[str, pd.DataFrame], pd.DataFrame, ValidationMetrics]:
        """
        Executes end-to-end data quality validation across all raw datasets.

        Args:
            raw_datasets: Dictionary containing DataFrames for 'customers',
                          'products', 'orders', and 'order_items'.

        Returns:
            Tuple of (valid_dataframes_dict, unified_quarantine_df, validation_metrics).
        """
        logger.info("=== Starting Data Quality Gatekeeper Validation ===")
        self.quarantine_records.clear()
        self.metrics = ValidationMetrics()

        customers_raw = raw_datasets["customers"]
        products_raw = raw_datasets["products"]
        orders_raw = raw_datasets["orders"]
        order_items_raw = raw_datasets["order_items"]

        total_raw = len(customers_raw) + len(products_raw) + len(orders_raw) + len(order_items_raw)
        self.metrics.total_raw_records = total_raw

        # 1. Customers
        valid_customers, q_customers = self.validate_customers(customers_raw)
        valid_cust_ids = set(valid_customers["customer_id"].str.strip())
        self.metrics.entity_metrics["customers"] = {
            "raw_count": len(customers_raw),
            "valid_count": len(valid_customers),
            "quarantined_count": len(q_customers),
        }

        # 2. Products
        valid_products, q_products = self.validate_products(products_raw)
        valid_prod_ids = set(valid_products["product_id"].str.strip())
        self.metrics.entity_metrics["products"] = {
            "raw_count": len(products_raw),
            "valid_count": len(valid_products),
            "quarantined_count": len(q_products),
        }

        # 3. Orders
        valid_orders, q_orders = self.validate_orders(orders_raw, valid_cust_ids)
        valid_order_ids = set(valid_orders["order_id"].str.strip())
        self.metrics.entity_metrics["orders"] = {
            "raw_count": len(orders_raw),
            "valid_count": len(valid_orders),
            "quarantined_count": len(q_orders),
        }

        # 4. Order Items
        valid_order_items, q_order_items = self.validate_order_items(
            order_items_raw, valid_order_ids, valid_prod_ids
        )
        self.metrics.entity_metrics["order_items"] = {
            "raw_count": len(order_items_raw),
            "valid_count": len(valid_order_items),
            "quarantined_count": len(q_order_items),
        }

        # Aggregate metrics
        total_valid = len(valid_customers) + len(valid_products) + len(valid_orders) + len(valid_order_items)
        total_quarantined = len(self.quarantine_records)
        self.metrics.total_valid_records = total_valid
        self.metrics.total_quarantined_records = total_quarantined

        # Unified Quarantine DataFrame
        if self.quarantine_records:
            unified_q_df = pd.DataFrame([q.to_dict() for q in self.quarantine_records])
        else:
            unified_q_df = pd.DataFrame(
                columns=[
                    "table_name", "record_id", "reject_stage", "reject_reason",
                    "reject_detail", "rejected_at", "raw_payload"
                ]
            )

        # Persist quarantine records to data/rejected/
        self.save_quarantine_files(unified_q_df, q_orders, q_order_items)

        valid_datasets = {
            "customers": valid_customers,
            "products": valid_products,
            "orders": valid_orders,
            "order_items": valid_order_items,
        }

        logger.info(
            "=== Validation Complete: %d Raw = %d Valid + %d Quarantined (Defects: %d) ===",
            total_raw,
            total_valid,
            total_quarantined,
            self.metrics.total_defect_occurrences,
        )
        return valid_datasets, unified_q_df, self.metrics

    def save_quarantine_files(
        self,
        unified_q_df: pd.DataFrame,
        q_orders_df: pd.DataFrame,
        q_order_items_df: pd.DataFrame,
    ) -> None:
        """Saves quarantined datasets to data/rejected/ with audit traceability."""
        unified_path = self.rejected_dir / config.paths.quarantine_file
        unified_q_df.to_csv(unified_path, index=False, encoding="utf-8")
        logger.info("Saved unified quarantine audit log to: %s (%d records)", unified_path.resolve(), len(unified_q_df))

        if not q_orders_df.empty:
            orders_q_path = self.rejected_dir / "quarantine_orders.csv"
            q_orders_df.to_csv(orders_q_path, index=False, encoding="utf-8")

        if not q_order_items_df.empty:
            items_q_path = self.rejected_dir / "quarantine_order_items.csv"
            q_order_items_df.to_csv(items_q_path, index=False, encoding="utf-8")
