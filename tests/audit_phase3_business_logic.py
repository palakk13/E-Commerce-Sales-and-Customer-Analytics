"""
Audit Script for Phase 3 PySpark Implementation Review.

Validates:
1. Status breakdown in curated_sales_fact (Delivered, Shipped, Processing, Cancelled, Returned)
2. Revenue recognition & conservation across financial metrics
3. Customer RFM dataset consistency with recognized revenue
4. Monthly time series reconciliation
5. Category merchandising reconciliation
6. Raw dataset SHA-256 hash invariance
"""

import hashlib
from pathlib import Path
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent
data_dir = BASE_DIR / "data"
analytics_dir = data_dir / "processed" / "analytics"

sales_df = pd.read_csv(analytics_dir / "curated_sales_fact.csv")
rfm_df = pd.read_csv(analytics_dir / "customer_rfm_features.csv")
monthly_df = pd.read_csv(analytics_dir / "monthly_sales_metrics.csv")
cat_df = pd.read_csv(analytics_dir / "category_performance_metrics.csv")

print("=" * 80)
print("1. CURATED SALES FACT BREAKDOWN BY ORDER STATUS")
print("=" * 80)
status_grp = sales_df.groupby("order_status").agg(
    order_items_count=("order_item_id", "count"),
    distinct_orders=("order_id", "nunique"),
    gross_revenue=("gross_revenue", "sum"),
    discount_amount=("discount_amount", "sum"),
    net_revenue=("net_revenue", "sum"),
    cogs=("total_cost", "sum"),
    profit=("profit", "sum"),
    recognized_revenue=("recognized_revenue", "sum"),
    recognized_profit=("recognized_profit", "sum"),
    in_transit_revenue=("in_transit_revenue", "sum"),
    pipeline_revenue=("pipeline_revenue", "sum"),
    returned_loss=("returned_revenue_loss", "sum"),
    cancelled_loss=("cancelled_revenue_loss", "sum"),
).round(2)
print(status_grp.to_string())

print("\n" + "=" * 80)
print("TOTALS ACROSS CURATED SALES FACT")
print("=" * 80)
totals = sales_df[[
    "gross_revenue", "discount_amount", "net_revenue", "total_cost", "profit",
    "recognized_revenue", "recognized_profit", "in_transit_revenue", "pipeline_revenue",
    "returned_revenue_loss", "cancelled_revenue_loss"
]].sum().round(2)
print(totals.to_string())

print("\n" + "=" * 80)
print("2. RECONCILIATION & CONSERVATION AUDIT")
print("=" * 80)
net_sum = totals["net_revenue"]
comp_sum = round(
    totals["recognized_revenue"]
    + totals["in_transit_revenue"]
    + totals["pipeline_revenue"]
    + totals["returned_revenue_loss"]
    + totals["cancelled_revenue_loss"],
    2,
)
print(f"Net Revenue (Demand):              ${net_sum:,.2f}")
print(f"Sum of Mutually Exclusive Buckets: ${comp_sum:,.2f}")
print(f"Conservation Exact Match:          {net_sum == comp_sum}")

print("\n" + "=" * 80)
print("3. RFM DATASET RECONCILIATION")
print("=" * 80)
delivered_custs = (rfm_df["frequency"] > 0).sum()
zero_custs = (rfm_df["frequency"] == 0).sum()
rfm_monetary_sum = round(rfm_df["monetary"].sum(), 2)
rfm_profit_sum = round(rfm_df["delivered_profit"].sum(), 2)
rfm_freq_sum = int(rfm_df["frequency"].sum())
rfm_total_orders_sum = int(rfm_df["total_orders_placed"].sum())

print(f"Total Customers Profiled:          {len(rfm_df):,}")
print(f"Customers with >=1 Delivered Order:{delivered_custs:,} ({delivered_custs/len(rfm_df)*100:.1f}%)")
print(f"Customers with 0 Delivered Orders: {zero_custs:,} ({zero_custs/len(rfm_df)*100:.1f}%)")
print(f"Sum of RFM Monetary:               ${rfm_monetary_sum:,.2f} (Matches Rec Rev: {rfm_monetary_sum == totals['recognized_revenue']})")
print(f"Sum of RFM Delivered Profit:       ${rfm_profit_sum:,.2f} (Matches Rec Profit: {rfm_profit_sum == totals['recognized_profit']})")
print(f"Sum of RFM Frequency:              {rfm_freq_sum:,} (Matches Delivered Orders: {rfm_freq_sum == status_grp.loc['Delivered', 'distinct_orders']})")
print(f"Sum of Total Orders Placed:        {rfm_total_orders_sum:,} (Matches Cleaned Orders: {rfm_total_orders_sum == 3899})")

print("\n" + "=" * 80)
print("4. MONTHLY METRICS RECONCILIATION")
print("=" * 80)
monthly_rev_sum = round(monthly_df["delivered_revenue"].sum(), 2)
monthly_profit_sum = round(monthly_df["delivered_profit"].sum(), 2)
last_running_rev = round(monthly_df["running_total_recognized_revenue"].iloc[-1], 2)

print(f"Total Months Aggregated:           {len(monthly_df)}")
print(f"Date Range:                        {monthly_df['year_month'].iloc[0]} to {monthly_df['year_month'].iloc[-1]}")
print(f"Sum of Monthly Delivered Revenue:  ${monthly_rev_sum:,.2f} (Matches Rec Rev: {monthly_rev_sum == totals['recognized_revenue']})")
print(f"Sum of Monthly Delivered Profit:   ${monthly_profit_sum:,.2f} (Matches Rec Profit: {monthly_profit_sum == totals['recognized_profit']})")
print(f"Final Running Total Revenue:       ${last_running_rev:,.2f} (Matches Rec Rev: {last_running_rev == totals['recognized_revenue']})")

print("\n" + "=" * 80)
print("5. CATEGORY METRICS RECONCILIATION")
print("=" * 80)
cat_rev_sum = round(cat_df["delivered_revenue"].sum(), 2)
cat_profit_sum = round(cat_df["delivered_profit"].sum(), 2)
print(f"Total Categories/Sub-categories:   {len(cat_df)}")
print(f"Sum of Category Delivered Revenue: ${cat_rev_sum:,.2f} (Matches Rec Rev: {cat_rev_sum == totals['recognized_revenue']})")
print(f"Sum of Category Delivered Profit:  ${cat_profit_sum:,.2f} (Matches Rec Profit: {cat_profit_sum == totals['recognized_profit']})")

print("\n" + "=" * 80)
print("6. RAW FILE INTEGRITY AUDIT (SHA-256)")
print("=" * 80)
import json
with open(BASE_DIR / "logs" / "ingestion_manifest.json", "r") as f:
    manifest = json.load(f)

for entity, meta in manifest["entities"].items():
    raw_path = Path(meta["file_path"])
    actual_hash = hashlib.sha256(raw_path.read_bytes()).hexdigest()
    exp_hash = meta["file_hash_sha256"]
    match = actual_hash == exp_hash
    print(f"{raw_path.name:22}: {'MATCH' if match else 'MISMATCH'} ({actual_hash[:16]}...)")

print("\n" + "=" * 80)
print("7. INVESTIGATING ORDERS VS LINE ITEMS")
print("=" * 80)
orders_cleaned = pd.read_csv(data_dir / "processed" / "orders_cleaned.csv")
items_cleaned = pd.read_csv(data_dir / "processed" / "order_items_cleaned.csv")
print(f"Total orders in orders_cleaned:             {len(orders_cleaned)}")
print(f"Unique order_ids in orders_cleaned:         {orders_cleaned['order_id'].nunique()}")
print(f"Unique order_ids in order_items_cleaned:    {items_cleaned['order_id'].nunique()}")
diff_orders = set(orders_cleaned["order_id"]) - set(items_cleaned["order_id"])
print(f"Orders in orders_cleaned with no items:     {len(diff_orders)}")

quarantine_df = pd.read_csv(data_dir / "rejected" / "quarantine_records.csv")
raw_items = pd.read_csv(data_dir / "raw" / "order_items.csv")
raw_items_with_diff = raw_items[raw_items["order_id"].isin(diff_orders)]
print(f"Raw items belonging to those 80 orders:     {len(raw_items_with_diff)}")
quarantined_items_diff = quarantine_df[quarantine_df["record_id"].isin(raw_items_with_diff["order_item_id"])]
print(f"Of those, quarantined items:               {len(quarantined_items_diff)}")
print(f"Quarantine reject reasons for those items:\n{quarantined_items_diff['reject_reason'].value_counts()}")



