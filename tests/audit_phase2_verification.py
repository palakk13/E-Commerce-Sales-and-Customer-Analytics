"""
Phase 2 Focused Verification & Idempotency Audit Script.

Verifies:
1. Idempotency: Multiple runs produce identical outputs without duplicate appending.
2. Referential Integrity: Cleaned order items strictly reference cleaned orders and products.
3. Conservation of Records: 11,867 raw = 11,308 processed + 559 quarantined.
4. Entity Preservation: Zero record loss in customers (1,000) and products (30).
5. Optional values & Decimal precision: Phone standardization & 2-decimal floats.
"""

import hashlib
import sys
from pathlib import Path

# Add project root to path
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

import pandas as pd
from src.pipeline_runner import run_phase2_pipeline


def file_sha256(path: Path) -> str:
    """Computes SHA-256 hash of a file."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def run_audit():
    print("================================================================================")
    print("STARTING PHASE 2 FOCUSED VERIFICATION AUDIT")
    print("================================================================================")

    # --------------------------------------------------------------------------
    # Check 1: Idempotency & Repeatability
    # --------------------------------------------------------------------------
    print("\n--- [Audit 1/5] Idempotency & Repeatability Check ---")
    run1 = run_phase2_pipeline()

    # Capture outputs from Run 1
    proc_dir = BASE_DIR / "data" / "processed"
    rej_dir = BASE_DIR / "data" / "rejected"

    run1_q_records_len = len(pd.read_csv(rej_dir / "quarantine_records.csv"))
    run1_q_orders_len = len(pd.read_csv(rej_dir / "quarantine_orders.csv"))
    run1_q_items_len = len(pd.read_csv(rej_dir / "quarantine_order_items.csv"))

    run1_cleaned_hashes = {
        f.name: file_sha256(f)
        for f in proc_dir.glob("*_cleaned.csv")
    }

    # Execute Run 2
    run2 = run_phase2_pipeline()

    run2_q_records_len = len(pd.read_csv(rej_dir / "quarantine_records.csv"))
    run2_q_orders_len = len(pd.read_csv(rej_dir / "quarantine_orders.csv"))
    run2_q_items_len = len(pd.read_csv(rej_dir / "quarantine_order_items.csv"))

    run2_cleaned_hashes = {
        f.name: file_sha256(f)
        for f in proc_dir.glob("*_cleaned.csv")
    }

    assert run1_q_records_len == run2_q_records_len == 559, f"Quarantine count mismatch: Run1={run1_q_records_len}, Run2={run2_q_records_len}"
    assert run1_q_orders_len == run2_q_orders_len == 146, f"Quarantine orders mismatch: Run1={run1_q_orders_len}, Run2={run2_q_orders_len}"
    assert run1_q_items_len == run2_q_items_len == 413, f"Quarantine items mismatch: Run1={run1_q_items_len}, Run2={run2_q_items_len}"
    assert run1_cleaned_hashes == run2_cleaned_hashes, "Cleaned dataset hashes changed between runs!"

    print("[PASS] Pass: Running Phase 2 twice produces 100% identical outputs without appending duplicate quarantine rows.")
    print(f"  Quarantine Records Count: Run 1 = {run1_q_records_len}, Run 2 = {run2_q_records_len}")

    # --------------------------------------------------------------------------
    # Check 2: Referential Integrity in Processed Datasets
    # --------------------------------------------------------------------------
    print("\n--- [Audit 2/5] Referential Integrity in Processed Datasets ---")
    cust_df = pd.read_csv(proc_dir / "customers_cleaned.csv")
    prod_df = pd.read_csv(proc_dir / "products_cleaned.csv")
    orders_df = pd.read_csv(proc_dir / "orders_cleaned.csv")
    items_df = pd.read_csv(proc_dir / "order_items_cleaned.csv")

    valid_cust_ids = set(cust_df["customer_id"])
    valid_prod_ids = set(prod_df["product_id"])
    valid_order_ids = set(orders_df["order_id"])

    # 1. Orders -> Customers FK check
    dangling_order_cust = set(orders_df["customer_id"]) - valid_cust_ids
    assert len(dangling_order_cust) == 0, f"Found dangling customers in orders: {dangling_order_cust}"

    # 2. Order Items -> Orders FK check
    dangling_item_orders = set(items_df["order_id"]) - valid_order_ids
    assert len(dangling_item_orders) == 0, f"Found dangling orders in items: {dangling_item_orders}"

    # 3. Order Items -> Products FK check
    dangling_item_prods = set(items_df["product_id"]) - valid_prod_ids
    assert len(dangling_item_prods) == 0, f"Found dangling products in items: {dangling_item_prods}"

    print("[PASS] Pass: 100% Referential Integrity confirmed:")
    print(f"  - Cleaned orders referencing valid customers: {len(orders_df)} / {len(orders_df)} (0 orphan)")
    print(f"  - Cleaned items referencing valid orders:    {len(items_df)} / {len(items_df)} (0 orphan)")
    print(f"  - Cleaned items referencing valid products:  {len(items_df)} / {len(items_df)} (0 orphan)")

    # --------------------------------------------------------------------------
    # Check 3: Record Reconciliation
    # --------------------------------------------------------------------------
    print("\n--- [Audit 3/5] Conservation of Records Reconciliation ---")
    raw_dir = BASE_DIR / "data" / "raw"
    raw_counts = {
        "customers": len(pd.read_csv(raw_dir / "customers.csv")),
        "products": len(pd.read_csv(raw_dir / "products.csv")),
        "orders": len(pd.read_csv(raw_dir / "orders.csv")),
        "order_items": len(pd.read_csv(raw_dir / "order_items.csv")),
    }
    proc_counts = {
        "customers": len(cust_df),
        "products": len(prod_df),
        "orders": len(orders_df),
        "order_items": len(items_df),
    }
    q_df = pd.read_csv(rej_dir / "quarantine_records.csv")
    q_counts = {
        "customers": len(q_df[q_df["table_name"] == "customers"]),
        "products": len(q_df[q_df["table_name"] == "products"]),
        "orders": len(q_df[q_df["table_name"] == "orders"]),
        "order_items": len(q_df[q_df["table_name"] == "order_items"]),
    }

    total_raw = sum(raw_counts.values())
    total_proc = sum(proc_counts.values())
    total_q = sum(q_counts.values())

    print(f"  Raw Total:        {total_raw}")
    print(f"  Processed Total:  {total_proc}")
    print(f"  Quarantined Total: {total_q}")
    print(f"  Conservation:     {total_proc} + {total_q} = {total_proc + total_q}")

    assert total_raw == 11867, f"Expected 11867 raw records, got {total_raw}"
    assert total_proc == 11308, f"Expected 11308 processed records, got {total_proc}"
    assert total_q == 559, f"Expected 559 quarantined records, got {total_q}"
    assert total_raw == total_proc + total_q, "Conservation violation!"

    for entity in raw_counts:
        assert raw_counts[entity] == proc_counts[entity] + q_counts[entity], f"Conservation mismatch in {entity}"

    print("[PASS] Pass: Exact record reconciliation confirmed (11,867 raw = 11,308 valid + 559 quarantined).")

    # --------------------------------------------------------------------------
    # Check 4: Preservation of Master Entities
    # --------------------------------------------------------------------------
    print("\n--- [Audit 4/5] Entity Preservation in Master Tables ---")
    assert len(cust_df) == 1000, f"Expected 1000 customers, got {len(cust_df)}"
    assert len(prod_df) == 30, f"Expected 30 products, got {len(prod_df)}"
    assert cust_df["customer_id"].nunique() == 1000, "Duplicate customer_id detected in processed table!"
    assert prod_df["product_id"].nunique() == 30, "Duplicate product_id detected in processed table!"
    print(f"[PASS] Pass: Zero records lost in master tables (Customers: 1,000/1,000, Products: 30/30).")

    # --------------------------------------------------------------------------
    # Check 5: Optional Missing Values & Financial Decimal Precision
    # --------------------------------------------------------------------------
    print("\n--- [Audit 5/5] Optional Phone Handling & Decimal Precision ---")
    # Phone checks
    not_provided_count = (cust_df["phone"] == "NOT_PROVIDED").sum()
    null_phone_count = cust_df["phone"].isnull().sum()
    empty_phone_count = (cust_df["phone"] == "").sum()
    valid_phone_count = len(cust_df) - not_provided_count

    print(f"  Customers with phone 'NOT_PROVIDED': {not_provided_count} (matches 37 missing in raw)")
    print(f"  Customers with active phone numbers: {valid_phone_count}")
    print(f"  Customers with null or empty phone:   {null_phone_count + empty_phone_count}")

    assert not_provided_count == 37, f"Expected 37 NOT_PROVIDED phones, got {not_provided_count}"
    assert null_phone_count == 0, f"Found unhandled null phone values: {null_phone_count}"
    assert empty_phone_count == 0, f"Found unhandled empty string phone values: {empty_phone_count}"

    # Precision checks
    # Decimal places check function
    def has_max_2_decimals(series: pd.Series) -> bool:
        return series.apply(lambda x: len(str(x).split(".")[1]) <= 2 if "." in str(x) else True).all()

    assert has_max_2_decimals(prod_df["unit_cost"]), "unit_cost has > 2 decimal places"
    assert has_max_2_decimals(prod_df["list_price"]), "list_price has > 2 decimal places"
    assert has_max_2_decimals(orders_df["shipping_cost"]), "shipping_cost has > 2 decimal places"
    assert has_max_2_decimals(items_df["unit_price"]), "unit_price has > 2 decimal places"
    assert has_max_2_decimals(items_df["discount_percent"]), "discount_percent has > 2 decimal places"

    # Positivity checks
    assert (prod_df["unit_cost"] > 0).all(), "Found non-positive unit_cost"
    assert (prod_df["list_price"] > 0).all(), "Found non-positive list_price"
    assert (items_df["unit_price"] > 0).all(), "Found non-positive unit_price"
    assert (items_df["quantity"] > 0).all(), "Found non-positive quantity"
    assert ((items_df["discount_percent"] >= 0) & (items_df["discount_percent"] <= 100)).all(), "Found invalid discount"

    print("[PASS] Pass: Optional phone values standardized consistently to 'NOT_PROVIDED' (exactly 37).")
    print("[PASS] Pass: All monetary and percentage fields enforce valid positive ranges and <= 2 decimal precision.")

    print("\n================================================================================")
    print("ALL 5 VERIFICATION AUDITS PASSED WITH ZERO FAILURES")
    print("================================================================================")
    return True


if __name__ == "__main__":
    success = run_audit()
    sys.exit(0 if success else 1)
