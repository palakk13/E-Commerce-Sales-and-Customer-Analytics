"""
MySQL Data Warehouse ETL Loader.

Extracts cleaned and curated analytical datasets from Phase 2 and Phase 3,
populates the dimensional star schema (dimensions first, then fact table),
resolves surrogate foreign keys, verifies referential integrity, and provides
idempotent load capabilities with transactional guarantees.
"""

import math
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from sqlalchemy import (
    BigInteger,
    Column,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    Numeric,
    SmallInteger,
    String,
    Table,
    UniqueConstraint,
    create_engine,
    func,
    select,
    text,
)
from sqlalchemy.engine import Engine

# Ensure project root in sys.path
BASE_DIR = Path(__file__).resolve().parent.parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from config.logging_config import setup_logger
from config.pipeline_config import config

logger = setup_logger("warehouse_loader")


class WarehouseLoader:
    """
    Orchestrates extraction, surrogate key resolution, and loading of
    star schema tables in the relational Data Warehouse.
    """

    def __init__(
        self,
        db_url: Optional[str] = None,
        engine: Optional[Engine] = None,
        processed_dir: Optional[Path] = None,
        analytics_dir: Optional[Path] = None,
    ):
        """
        Initializes the WarehouseLoader with database connection and data directory paths.
        """
        self.processed_dir = Path(processed_dir) if processed_dir else config.paths.processed_dir
        self.analytics_dir = Path(analytics_dir) if analytics_dir else config.paths.analytics_dir

        if engine is not None:
            self.engine = engine
            self.db_url = str(engine.url)
        elif db_url:
            self.db_url = db_url
            self.engine = create_engine(db_url, pool_pre_ping=True)
        else:
            self.db_url = config.database.connection_url
            self.engine = create_engine(self.db_url, pool_pre_ping=True)

        self.metadata = MetaData()
        self._init_table_definitions()

        # Cache for resolved surrogate keys
        self.customer_key_map: Dict[str, int] = {}
        self.product_key_map: Dict[str, int] = {}
        self.location_key_map: Dict[Tuple[str, str, str, str], int] = {}
        self.payment_key_map: Dict[str, int] = {}

    def _init_table_definitions(self) -> None:
        """Declares SQLAlchemy Table metadata for schema management across MySQL and SQLite."""
        self.dim_date = Table(
            "dim_date",
            self.metadata,
            Column("date_key", Integer, primary_key=True),
            Column("full_date", Date, nullable=False, unique=True),
            Column("day_of_week", SmallInteger, nullable=False),
            Column("day_name", String(15), nullable=False),
            Column("day_of_month", SmallInteger, nullable=False),
            Column("month_num", SmallInteger, nullable=False),
            Column("month_name", String(15), nullable=False),
            Column("quarter", SmallInteger, nullable=False),
            Column("year", SmallInteger, nullable=False),
            Column("is_weekend", SmallInteger, nullable=False, default=0),
        )

        self.dim_location = Table(
            "dim_location",
            self.metadata,
            Column("location_key", Integer, primary_key=True, autoincrement=True),
            Column("city", String(100), nullable=False),
            Column("state", String(100), nullable=False),
            Column("country", String(100), nullable=False),
            Column("postal_code", String(20), nullable=False),
            Column("created_at", DateTime, default=lambda: datetime.now(timezone.utc)),
            UniqueConstraint("city", "state", "country", "postal_code", name="uq_dim_location_geo"),
        )

        self.dim_customer = Table(
            "dim_customer",
            self.metadata,
            Column("customer_key", Integer, primary_key=True, autoincrement=True),
            Column("customer_id", String(50), nullable=False, unique=True),
            Column("customer_name", String(150), nullable=False),
            Column("email", String(150), nullable=False),
            Column("phone", String(50), nullable=True),
            Column("city", String(100), nullable=False),
            Column("state", String(100), nullable=False),
            Column("country", String(100), nullable=False),
            Column("postal_code", String(20), nullable=False),
            Column("signup_date", Date, nullable=False),
            Column("rfm_segment", String(50), default="Unassigned"),
            Column("created_at", DateTime, default=lambda: datetime.now(timezone.utc)),
            Column("updated_at", DateTime, default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc)),
        )

        self.dim_product = Table(
            "dim_product",
            self.metadata,
            Column("product_key", Integer, primary_key=True, autoincrement=True),
            Column("product_id", String(50), nullable=False, unique=True),
            Column("product_name", String(150), nullable=False),
            Column("category", String(100), nullable=False),
            Column("sub_category", String(100), nullable=False),
            Column("unit_cost", Numeric(10, 2), nullable=False),
            Column("list_price", Numeric(10, 2), nullable=False),
            Column("created_at", DateTime, default=lambda: datetime.now(timezone.utc)),
        )

        self.dim_payment = Table(
            "dim_payment",
            self.metadata,
            Column("payment_key", Integer, primary_key=True, autoincrement=True),
            Column("payment_method", String(50), nullable=False, unique=True),
            Column("created_at", DateTime, default=lambda: datetime.now(timezone.utc)),
        )

        self.fact_sales = Table(
            "fact_sales",
            self.metadata,
            Column("sales_key", BigInteger().with_variant(Integer, "sqlite"), primary_key=True, autoincrement=True),
            Column("order_id", String(50), nullable=False),
            Column("order_item_id", String(50), nullable=False, unique=True),
            Column("customer_key", Integer, ForeignKey("dim_customer.customer_key"), nullable=False),
            Column("product_key", Integer, ForeignKey("dim_product.product_key"), nullable=False),
            Column("date_key", Integer, ForeignKey("dim_date.date_key"), nullable=False),
            Column("location_key", Integer, ForeignKey("dim_location.location_key"), nullable=False),
            Column("payment_key", Integer, ForeignKey("dim_payment.payment_key"), nullable=False),
            Column("order_status", String(30), nullable=False),
            Column("quantity", Integer, nullable=False),
            Column("unit_price", Numeric(10, 2), nullable=False),
            Column("discount_percent", Numeric(5, 2), nullable=False),
            Column("gross_revenue", Numeric(12, 2), nullable=False),
            Column("discount_amount", Numeric(12, 2), nullable=False),
            Column("net_revenue", Numeric(12, 2), nullable=False),
            Column("unit_cost", Numeric(10, 2), nullable=False),
            Column("total_cost", Numeric(12, 2), nullable=False),
            Column("profit", Numeric(12, 2), nullable=False),
            Column("profit_margin_pct", Numeric(5, 2), nullable=False),
            Column("shipping_cost", Numeric(10, 2), nullable=False),
            Column("is_delivered", SmallInteger, nullable=False, default=0),
            Column("is_shipped", SmallInteger, nullable=False, default=0),
            Column("is_processing", SmallInteger, nullable=False, default=0),
            Column("is_returned", SmallInteger, nullable=False, default=0),
            Column("is_cancelled", SmallInteger, nullable=False, default=0),
            Column("recognized_revenue", Numeric(12, 2), nullable=False, default=0.00),
            Column("recognized_profit", Numeric(12, 2), nullable=False, default=0.00),
            Column("in_transit_revenue", Numeric(12, 2), nullable=False, default=0.00),
            Column("pipeline_revenue", Numeric(12, 2), nullable=False, default=0.00),
            Column("returned_revenue_loss", Numeric(12, 2), nullable=False, default=0.00),
            Column("cancelled_revenue_loss", Numeric(12, 2), nullable=False, default=0.00),
            Column("customer_order_rank", Integer, nullable=False, default=1),
            Column("customer_cumulative_spend", Numeric(12, 2), nullable=False, default=0.00),
            Column("loaded_at", DateTime, default=lambda: datetime.now(timezone.utc)),
        )


    def create_schema(self, drop_first: bool = False) -> None:
        """Creates warehouse tables using SQLAlchemy metadata."""
        logger.info("Initializing Data Warehouse schema (drop_first=%s)...", drop_first)
        if drop_first:
            self.metadata.drop_all(self.engine)
        self.metadata.create_all(self.engine)
        logger.info("Warehouse schema successfully created/verified.")

    def load_dim_date(self, start_date_str: str = "2024-01-01", end_date_str: str = "2026-12-31") -> int:
        """
        Populates dim_date for the multi-year transactional range.
        Returns the number of date records inserted.
        """
        logger.info("Generating date dimension records from %s to %s...", start_date_str, end_date_str)
        start_dt = datetime.strptime(start_date_str, "%Y-%m-%d").date()
        end_dt = datetime.strptime(end_date_str, "%Y-%m-%d").date()

        records = []
        cur = start_dt
        while cur <= end_dt:
            date_key = int(cur.strftime("%Y%m%d"))
            # Python weekday: Monday is 0, Sunday is 6 -> convert to 1-7
            day_of_week = cur.weekday() + 1
            is_weekend = 1 if day_of_week in (6, 7) else 0
            records.append(
                {
                    "date_key": date_key,
                    "full_date": cur,
                    "day_of_week": day_of_week,
                    "day_name": cur.strftime("%A"),
                    "day_of_month": cur.day,
                    "month_num": cur.month,
                    "month_name": cur.strftime("%B"),
                    "quarter": (cur.month - 1) // 3 + 1,
                    "year": cur.year,
                    "is_weekend": is_weekend,
                }
            )
            cur += timedelta(days=1)

        with self.engine.begin() as conn:
            # Check existing dates to ensure idempotence
            existing_keys = set(conn.execute(select(self.dim_date.c.date_key)).scalars().all())
            new_records = [r for r in records if r["date_key"] not in existing_keys]
            if new_records:
                conn.execute(self.dim_date.insert(), new_records)
                logger.info("Inserted %d new date records into dim_date (total: %d)", len(new_records), len(records))
            else:
                logger.info("dim_date already populated (%d records exist).", len(existing_keys))

        return len(records)

    def load_dim_location(self, customers_df: pd.DataFrame) -> int:
        """
        Extracts unique geographic combinations from customers and loads dim_location.
        Caches resolved location_key mappings.
        """
        logger.info("Loading geographic locations into dim_location...")
        geo_cols = ["city", "state", "country", "postal_code"]
        unique_locs = (
            customers_df[geo_cols]
            .drop_duplicates()
            .dropna(subset=["city", "country"])
            .sort_values(geo_cols)
            .to_dict(orient="records")
        )

        with self.engine.begin() as conn:
            # Fetch existing locations
            stmt = select(
                self.dim_location.c.location_key,
                self.dim_location.c.city,
                self.dim_location.c.state,
                self.dim_location.c.country,
                self.dim_location.c.postal_code,
            )
            existing = {
                (r.city, r.state, r.country, str(r.postal_code)): r.location_key
                for r in conn.execute(stmt).all()
            }

            to_insert = [
                loc for loc in unique_locs
                if (loc["city"], loc["state"], loc["country"], str(loc["postal_code"])) not in existing
            ]

            if to_insert:
                conn.execute(self.dim_location.insert(), to_insert)
                logger.info("Inserted %d new location records into dim_location", len(to_insert))

            # Refresh cache
            updated = conn.execute(stmt).all()
            self.location_key_map = {
                (r.city, r.state, r.country, str(r.postal_code)): r.location_key
                for r in updated
            }

        logger.info("dim_location populated: %d total locations cached", len(self.location_key_map))
        return len(self.location_key_map)

    def load_dim_customer(
        self,
        customers_df: pd.DataFrame,
        rfm_df: Optional[pd.DataFrame] = None,
    ) -> int:
        """
        Loads customer profile dimension with optional RFM segments and nullable phone handling.
        """
        logger.info("Loading customer profiles into dim_customer...")
        cust_records = customers_df.copy()

        # Handle optional phone values (replace 'NOT_PROVIDED' or NaN with None)
        cust_records["phone"] = cust_records["phone"].apply(
            lambda x: None if pd.isna(x) or str(x).strip().upper() in ("NOT_PROVIDED", "NONE", "NULL", "") else str(x)
        )
        cust_records["signup_date"] = pd.to_datetime(cust_records["signup_date"]).dt.date

        # Merge RFM segment if available
        if rfm_df is not None and "rfm_segment" in rfm_df.columns:
            seg_map = dict(zip(rfm_df["customer_id"], rfm_df["rfm_segment"]))
            cust_records["rfm_segment"] = cust_records["customer_id"].map(seg_map).fillna("Unassigned")
        else:
            cust_records["rfm_segment"] = "Unassigned"

        records = cust_records[
            ["customer_id", "customer_name", "email", "phone", "city", "state", "country", "postal_code", "signup_date", "rfm_segment"]
        ].to_dict(orient="records")

        with self.engine.begin() as conn:
            stmt = select(self.dim_customer.c.customer_key, self.dim_customer.c.customer_id)
            existing = {r.customer_id: r.customer_key for r in conn.execute(stmt).all()}

            to_insert = [r for r in records if r["customer_id"] not in existing]
            if to_insert:
                conn.execute(self.dim_customer.insert(), to_insert)
                logger.info("Inserted %d new customers into dim_customer", len(to_insert))

            updated = conn.execute(stmt).all()
            self.customer_key_map = {r.customer_id: r.customer_key for r in updated}

        logger.info("dim_customer populated: %d total customers cached", len(self.customer_key_map))
        return len(self.customer_key_map)

    def load_dim_product(self, products_df: pd.DataFrame) -> int:
        """
        Loads product catalog into dim_product and caches product_key mappings.
        """
        logger.info("Loading products into dim_product...")
        prod_records = products_df[
            ["product_id", "product_name", "category", "sub_category", "unit_cost", "list_price"]
        ].drop_duplicates("product_id").to_dict(orient="records")

        with self.engine.begin() as conn:
            stmt = select(self.dim_product.c.product_key, self.dim_product.c.product_id)
            existing = {r.product_id: r.product_key for r in conn.execute(stmt).all()}

            to_insert = [r for r in prod_records if r["product_id"] not in existing]
            if to_insert:
                conn.execute(self.dim_product.insert(), to_insert)
                logger.info("Inserted %d new products into dim_product", len(to_insert))

            updated = conn.execute(stmt).all()
            self.product_key_map = {r.product_id: r.product_key for r in updated}

        logger.info("dim_product populated: %d total products cached", len(self.product_key_map))
        return len(self.product_key_map)

    def load_dim_payment(self, orders_df: pd.DataFrame) -> int:
        """
        Extracts distinct payment methods and loads dim_payment.
        """
        logger.info("Loading payment methods into dim_payment...")
        methods = orders_df["payment_method"].dropna().unique().tolist()
        records = [{"payment_method": str(m).strip()} for m in sorted(methods)]

        with self.engine.begin() as conn:
            stmt = select(self.dim_payment.c.payment_key, self.dim_payment.c.payment_method)
            existing = {r.payment_method: r.payment_key for r in conn.execute(stmt).all()}

            to_insert = [r for r in records if r["payment_method"] not in existing]
            if to_insert:
                conn.execute(self.dim_payment.insert(), to_insert)
                logger.info("Inserted %d new payment methods into dim_payment", len(to_insert))

            updated = conn.execute(stmt).all()
            self.payment_key_map = {r.payment_method: r.payment_key for r in updated}

        logger.info("dim_payment populated: %d total payment methods cached", len(self.payment_key_map))
        return len(self.payment_key_map)

    def load_fact_sales(self, curated_sales_df: pd.DataFrame, batch_size: int = 1000) -> int:
        """
        Resolves surrogate keys and loads transactional line items into fact_sales.
        Strictly enforces that no unmapped foreign keys or orphan records occur.
        """
        logger.info("Loading atomic sales transactions into fact_sales (input rows: %d)...", len(curated_sales_df))
        start_time = time.perf_counter()

        # Pre-resolve foreign keys
        records: List[Dict[str, Any]] = []
        unmapped_customers = 0
        unmapped_products = 0
        unmapped_locations = 0
        unmapped_payments = 0

        for _, row in curated_sales_df.iterrows():
            cid = str(row["customer_id"])
            pid = str(row["product_id"])
            pmethod = str(row["payment_method"]).strip()
            loc_tuple = (str(row["city"]), str(row["state"]), str(row["country"]), str(row["postal_code"]))
            
            # 1. customer_key
            ckey = self.customer_key_map.get(cid)
            if ckey is None:
                unmapped_customers += 1

            # 2. product_key
            pkey = self.product_key_map.get(pid)
            if pkey is None:
                unmapped_products += 1

            # 3. location_key
            lkey = self.location_key_map.get(loc_tuple)
            if lkey is None:
                unmapped_locations += 1

            # 4. payment_key
            pay_key = self.payment_key_map.get(pmethod)
            if pay_key is None:
                unmapped_payments += 1

            # 5. date_key (YYYYMMDD integer)
            odt = pd.to_datetime(row["order_date"]).date()
            dkey = int(odt.strftime("%Y%m%d"))

            record = {
                "order_id": str(row["order_id"]),
                "order_item_id": str(row["order_item_id"]),
                "customer_key": ckey,
                "product_key": pkey,
                "date_key": dkey,
                "location_key": lkey,
                "payment_key": pay_key,
                "order_status": str(row["order_status"]),
                "quantity": int(row["quantity"]),
                "unit_price": round(float(row["unit_price"]), 2),
                "discount_percent": round(float(row["discount_percent"]), 2),
                "gross_revenue": round(float(row["gross_revenue"]), 2),
                "discount_amount": round(float(row["discount_amount"]), 2),
                "net_revenue": round(float(row["net_revenue"]), 2),
                "unit_cost": round(float(row["unit_cost"]), 2),
                "total_cost": round(float(row["total_cost"]), 2),
                "profit": round(float(row["profit"]), 2),
                "profit_margin_pct": round(float(row["profit_margin_pct"]), 2),
                "shipping_cost": round(float(row["shipping_cost"]), 2),
                "is_delivered": int(row.get("is_delivered", 1 if row["order_status"] == "Delivered" else 0)),
                "is_shipped": int(row.get("is_shipped", 1 if row["order_status"] == "Shipped" else 0)),
                "is_processing": int(row.get("is_processing", 1 if row["order_status"] == "Processing" else 0)),
                "is_returned": int(row.get("is_returned", 1 if row["order_status"] == "Returned" else 0)),
                "is_cancelled": int(row.get("is_cancelled", 1 if row["order_status"] == "Cancelled" else 0)),
                "recognized_revenue": round(float(row.get("recognized_revenue", 0.0)), 2),
                "recognized_profit": round(float(row.get("recognized_profit", 0.0)), 2),
                "in_transit_revenue": round(float(row.get("in_transit_revenue", 0.0)), 2),
                "pipeline_revenue": round(float(row.get("pipeline_revenue", 0.0)), 2),
                "returned_revenue_loss": round(float(row.get("returned_revenue_loss", 0.0)), 2),
                "cancelled_revenue_loss": round(float(row.get("cancelled_revenue_loss", 0.0)), 2),
                "customer_order_rank": int(row.get("customer_order_rank", 1)),
                "customer_cumulative_spend": round(float(row.get("customer_cumulative_spend", 0.0)), 2),
            }
            records.append(record)

        # Integrity Validation Check: zero unmapped foreign keys allowed
        if unmapped_customers or unmapped_products or unmapped_locations or unmapped_payments:
            err_msg = (
                f"Surrogate key resolution failure! Unmapped: "
                f"customers={unmapped_customers}, products={unmapped_products}, "
                f"locations={unmapped_locations}, payments={unmapped_payments}"
            )
            logger.error(err_msg)
            raise ValueError(err_msg)

        # Transactional Bulk Insert in Batches
        inserted_count = 0
        with self.engine.begin() as conn:
            # Check existing items to guarantee idempotent execution
            existing_items = set(conn.execute(select(self.fact_sales.c.order_item_id)).scalars().all())
            new_records = [r for r in records if r["order_item_id"] not in existing_items]

            if new_records:
                for i in range(0, len(new_records), batch_size):
                    batch = new_records[i : i + batch_size]
                    conn.execute(self.fact_sales.insert(), batch)
                    inserted_count += len(batch)
                logger.info("Inserted %d fact records into fact_sales", inserted_count)
            else:
                logger.info("fact_sales already populated (%d existing rows). Skipping duplicates.", len(existing_items))
                inserted_count = len(existing_items)

        elapsed = round(time.perf_counter() - start_time, 2)
        logger.info("fact_sales load complete in %s seconds (rows: %d)", elapsed, inserted_count)
        return inserted_count

    def load_all(self, drop_first: bool = False) -> Dict[str, Any]:
        """
        Executes end-to-end warehouse loading:
        1. Initializes schema tables.
        2. Ingests cleaned data from data/processed/ and analytics/.
        3. Loads dimensions in dependency order.
        4. Loads fact_sales with key resolution.
        5. Validates row counts and financial totals.
        """
        start_time = time.perf_counter()
        logger.info("================================================================================")
        logger.info("STARTING PHASE 4: DATA WAREHOUSE ETL LOAD")
        logger.info("Database URL: %s", self.db_url)
        logger.info("================================================================================")

        # 1. Initialize Tables
        self.create_schema(drop_first=drop_first)

        # 2. Read Source Datasets
        cust_path = self.processed_dir / "customers_cleaned.csv"
        prod_path = self.processed_dir / "products_cleaned.csv"
        ord_path = self.processed_dir / "orders_cleaned.csv"
        sales_path = self.analytics_dir / "curated_sales_fact.csv"
        rfm_path = self.analytics_dir / "customer_rfm_features.csv"

        logger.info("Reading source files for warehouse staging...")
        customers_df = pd.read_csv(cust_path)
        products_df = pd.read_csv(prod_path)
        orders_df = pd.read_csv(ord_path)
        curated_sales_df = pd.read_csv(sales_path)
        rfm_df = pd.read_csv(rfm_path) if rfm_path.exists() else None

        # 3. Load Dimensions in Order
        date_count = self.load_dim_date()
        loc_count = self.load_dim_location(customers_df)
        cust_count = self.load_dim_customer(customers_df, rfm_df)
        prod_count = self.load_dim_product(products_df)
        pay_count = self.load_dim_payment(orders_df)

        # 4. Load Fact Table
        fact_count = self.load_fact_sales(curated_sales_df)

        # 5. Run Verification & Financial Integrity Audit
        audit = self.verify_integrity()

        elapsed = round(time.perf_counter() - start_time, 2)
        summary = {
            "status": "SUCCESS",
            "duration_seconds": elapsed,
            "database_url": self.db_url,
            "table_counts": {
                "dim_date": date_count,
                "dim_location": loc_count,
                "dim_customer": cust_count,
                "dim_product": prod_count,
                "dim_payment": pay_count,
                "fact_sales": fact_count,
            },
            "financial_reconciliation": audit,
        }

        logger.info("================================================================================")
        logger.info("PHASE 4 DATA WAREHOUSE LOAD COMPLETE IN %s SECONDS", elapsed)
        logger.info("Fact Sales Loaded: %d rows", fact_count)
        logger.info("Total Recognized Revenue: $%s", f"{audit['total_recognized_revenue']:,.2f}")
        logger.info("Total Recognized Profit:  $%s", f"{audit['total_recognized_profit']:,.2f}")
        logger.info("================================================================================")

        return summary

    def verify_integrity(self) -> Dict[str, Any]:
        """
        Queries warehouse tables directly via SQL to verify foreign key resolution,
        row count reconciliation, and financial measure conservation.
        """
        logger.info("Verifying warehouse referential integrity and financial conservation...")
        with self.engine.connect() as conn:
            # 1. Fact Count
            total_fact_rows = conn.execute(select(func.count(self.fact_sales.c.sales_key))).scalar() or 0

            # 2. Null Foreign Key Check
            null_fk_stmt = select(func.count(self.fact_sales.c.sales_key)).where(
                (self.fact_sales.c.customer_key == None)  # noqa: E711
                | (self.fact_sales.c.product_key == None)  # noqa: E711
                | (self.fact_sales.c.date_key == None)  # noqa: E711
                | (self.fact_sales.c.location_key == None)  # noqa: E711
                | (self.fact_sales.c.payment_key == None)  # noqa: E711
            )
            null_fks = conn.execute(null_fk_stmt).scalar() or 0

            # 3. Financial Measure Aggregation
            agg_stmt = select(
                func.round(func.sum(self.fact_sales.c.gross_revenue), 2).label("gross_rev"),
                func.round(func.sum(self.fact_sales.c.discount_amount), 2).label("discount_amt"),
                func.round(func.sum(self.fact_sales.c.net_revenue), 2).label("net_rev"),
                func.round(func.sum(self.fact_sales.c.total_cost), 2).label("cogs"),
                func.round(func.sum(self.fact_sales.c.profit), 2).label("profit"),
                func.round(func.sum(self.fact_sales.c.recognized_revenue), 2).label("rec_rev"),
                func.round(func.sum(self.fact_sales.c.recognized_profit), 2).label("rec_profit"),
                func.round(func.sum(self.fact_sales.c.in_transit_revenue), 2).label("in_transit_rev"),
                func.round(func.sum(self.fact_sales.c.pipeline_revenue), 2).label("pipeline_rev"),
                func.round(func.sum(self.fact_sales.c.returned_revenue_loss), 2).label("returned_loss"),
                func.round(func.sum(self.fact_sales.c.cancelled_revenue_loss), 2).label("cancelled_loss"),
                func.count(func.distinct(self.fact_sales.c.order_id)).label("distinct_orders"),
            )
            agg_row = conn.execute(agg_stmt).one()

            gross_rev = float(agg_row.gross_rev or 0.0)
            net_rev = float(agg_row.net_rev or 0.0)
            rec_rev = float(agg_row.rec_rev or 0.0)
            rec_profit = float(agg_row.rec_profit or 0.0)
            in_transit_rev = float(agg_row.in_transit_rev or 0.0)
            pipeline_rev = float(agg_row.pipeline_rev or 0.0)
            returned_loss = float(agg_row.returned_loss or 0.0)
            cancelled_loss = float(agg_row.cancelled_loss or 0.0)
            distinct_orders = int(agg_row.distinct_orders or 0)

            # Check conservation: Sum of 5 mutually exclusive buckets == Net Demand
            buckets_sum = round(rec_rev + in_transit_rev + pipeline_rev + returned_loss + cancelled_loss, 2)
            conservation_match = math.isclose(net_rev, buckets_sum, rel_tol=1e-5)

            results = {
                "total_fact_rows": total_fact_rows,
                "null_foreign_keys": null_fks,
                "distinct_orders": distinct_orders,
                "total_gross_revenue": gross_rev,
                "total_net_revenue": net_rev,
                "total_recognized_revenue": rec_rev,
                "total_recognized_profit": rec_profit,
                "in_transit_revenue": in_transit_rev,
                "pipeline_revenue": pipeline_rev,
                "returned_revenue_loss": returned_loss,
                "cancelled_revenue_loss": cancelled_loss,
                "conservation_match": conservation_match,
            }

            if null_fks > 0:
                raise ValueError(f"Integrity check failed: {null_fks} fact rows contain NULL foreign keys!")

            return results


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="E-Commerce Data Warehouse ETL Loader")
    parser.add_argument(
        "--sqlite",
        action="store_true",
        help="Use a local persistent SQLite warehouse (data/warehouse/ecommerce_dwh.db) instead of MySQL.",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Drop and recreate all warehouse tables before loading.",
    )
    parser.add_argument(
        "--db-url",
        type=str,
        default=None,
        help="Custom SQLAlchemy database connection URL.",
    )
    args = parser.parse_args()

    db_target_url = args.db_url
    if args.sqlite and not db_target_url:
        sqlite_dir = BASE_DIR / "data" / "warehouse"
        sqlite_dir.mkdir(parents=True, exist_ok=True)
        sqlite_path = sqlite_dir / "ecommerce_dwh.db"
        db_target_url = f"sqlite:///{sqlite_path.resolve()}"
        print(f"[*] Targeting local SQLite warehouse: {sqlite_path.resolve()}")

    try:
        loader = WarehouseLoader(db_url=db_target_url)
        summary = loader.load_all(drop_first=args.reset)
        print("\n" + "=" * 80)
        print("PHASE 4: DATA WAREHOUSE ETL LOAD SUMMARY")
        print("=" * 80)
        print(f"Status:       {summary['status']}")
        print(f"Target DB:    {summary['database_url']}")
        print(f"Duration:     {summary['duration_seconds']} seconds")
        print("-" * 80)
        print("TABLE ROW COUNTS:")
        for k, v in summary["table_counts"].items():
            print(f"  {k:18}: {v:,} rows")
        print("-" * 80)
        print("FINANCIAL RECONCILIATION AUDIT:")
        rec = summary["financial_reconciliation"]
        print(f"  Total Fact Rows:           {rec['total_fact_rows']:,}")
        print(f"  Null Foreign Keys:         {rec['null_foreign_keys']}")
        print(f"  Distinct Orders:           {rec['distinct_orders']:,}")
        print(f"  Total Gross Revenue:       ${rec['total_gross_revenue']:,.2f}")
        print(f"  Total Net Revenue:         ${rec['total_net_revenue']:,.2f}")
        print(f"  Total Recognized Revenue:  ${rec['total_recognized_revenue']:,.2f}")
        print(f"  Total Recognized Profit:   ${rec['total_recognized_profit']:,.2f}")
        print(f"  Conservation Exact Match:  {rec['conservation_match']}")
        print("=" * 80)
    except Exception as e:
        print("\n" + "!" * 80)
        print(f"WAREHOUSE LOAD ERROR: {e}")
        print("!" * 80)
        if "Access denied" in str(e) or "Can't connect to MySQL" in str(e):
            print("\n[DIAGNOSTIC GUIDANCE]")
            print("1. To connect to a live MySQL instance, provide valid credentials in your .env file:")
            print("     MYSQL_HOST=localhost")
            print("     MYSQL_PORT=3306")
            print("     MYSQL_USER=ecommerce_user")
            print("     MYSQL_PASSWORD=ecommerce_pass")
            print("     MYSQL_DATABASE=ecommerce_dwh")
            print("2. For instant offline execution and testing without a live MySQL server, run:")
            print("     python src/warehouse/warehouse_loader.py --sqlite --reset")
        sys.exit(1)

