# Power BI Implementation & Semantic Modeling Guide

## 1. Overview & Business Objectives
This guide outlines the semantic modeling, relationship architecture, and DAX measure configuration for the **E-Commerce Sales and Customer Analytics Platform** in Power BI Desktop.

The central design principle is the **strict separation between Demand Revenue and Recognized Revenue**:
- **Demand Layer**: Evaluates total customer demand, ordering volume, and fulfillment pipeline across all 5 statuses.
- **Financial Recognition Layer**: Reports realized financial metrics strictly when physical delivery occurs (`order_status = 'Delivered'`), ensuring compliance with FOB Destination accounting standards.

---

## 2. Data Sources & File Locations
Power BI can load the dimensional star schema directly from either CSV or Parquet files in `data/powerbi/`:

| Table Name | Source CSV File | Source Parquet File | Grain / Entity | Row Count |
| :--- | :--- | :--- | :--- | :---: |
| `fact_sales` | `data/powerbi/fact_sales.csv` | `fact_sales.parquet` | 1 Order Line Item | 6,379 |
| `dim_date` | `data/powerbi/dim_date.csv` | `dim_date.parquet` | 1 Calendar Day (2024–2026) | 1,096 |
| `dim_customer` | `data/powerbi/dim_customer.csv` | `dim_customer.parquet` | 1 Customer Account (RFM enriched) | 1,000 |
| `dim_product` | `data/powerbi/dim_product.csv` | `dim_product.parquet` | 1 Product SKU | 30 |
| `dim_location` | `data/powerbi/dim_location.csv` | `dim_location.parquet` | 1 Metro Postal Area | 15 |
| `dim_payment` | `data/powerbi/dim_payment.csv` | `dim_payment.parquet` | 1 Checkout Payment Method | 5 |

---

## 3. Semantic Model Relationships (Star Schema)
Configure single-directional `1-to-Many` relationships connecting dimension primary keys to fact table surrogate keys:

```
dim_date[date_key]          (1) ───► (*) fact_sales[date_key]
dim_customer[customer_key]  (1) ───► (*) fact_sales[customer_key]
dim_product[product_key]    (1) ───► (*) fact_sales[product_key]
dim_location[location_key]  (1) ───► (*) fact_sales[location_key]
dim_payment[payment_key]    (1) ───► (*) fact_sales[payment_key]
```
- **Cross-filter direction**: `Single` (filters propagate from dimensions to `fact_sales`).
- **Referential Integrity**: 100% verified (0 null or dangling surrogate keys).

---

## 4. Financial Reconciliation Benchmarks (Verified Warehouse Totals)

Every KPI measure in Power BI must tie out to these verified ground-truth values:

| Measure Name | Ground-Truth Value | DAX Definition |
| :--- | :---: | :--- |
| **Gross Demand Revenue** | **$1,020,403.59** | `SUM(fact_sales[gross_revenue])` |
| **Total Discounts** | **$42,305.77** | `SUM(fact_sales[discount_amount])` |
| **Net Demand Revenue** | **$978,097.82** | `SUM(fact_sales[net_revenue])` |
| **Recognized Revenue** | **$405,484.47** | `CALCULATE(SUM(fact_sales[net_revenue]), fact_sales[order_status] = "Delivered")` |
| **Recognized COGS** | **$174,277.00** | `CALCULATE(SUM(fact_sales[total_cost]), fact_sales[order_status] = "Delivered")` |
| **Recognized Profit** | **$231,207.47** | `[Recognized Revenue] - [Recognized COGS]` |
| **Recognized Profit Margin** | **57.02%** | `DIVIDE([Recognized Profit], [Recognized Revenue], 0)` |
| **In-Transit Revenue** | **$138,599.59** | `CALCULATE(SUM(fact_sales[net_revenue]), fact_sales[order_status] = "Shipped")` |
| **Pipeline Revenue** | **$146,949.52** | `CALCULATE(SUM(fact_sales[net_revenue]), fact_sales[order_status] = "Processing")` |
| **Cancelled Revenue Loss** | **$158,119.41** | `CALCULATE(SUM(fact_sales[net_revenue]), fact_sales[order_status] = "Cancelled")` |
| **Returned Revenue Loss** | **$128,944.83** | `CALCULATE(SUM(fact_sales[net_revenue]), fact_sales[order_status] = "Returned")` |
| **Fulfillment Realization Rate** | **41.46%** | `DIVIDE([Recognized Revenue], [Net Demand Revenue], 0)` |
| **Demand Attrition Rate** | **29.35%** | `DIVIDE([Cancelled Revenue Loss] + [Returned Revenue Loss], [Net Demand Revenue], 0)` |

---

## 5. Dashboard Layout & Visual Specifications

### Page 1: Executive Financial & Demand Overview
1. **Top KPI Cards**:
   - Card 1: `Net Demand Revenue` ($978.10K) [Subtext: Total Customer Demand Placed]
   - Card 2: `Recognized Revenue` ($405.48K) [Subtext: Realized Delivered Sales]
   - Card 3: `Recognized Profit` ($231.21K) [Subtext: Realized Operating Margin: 57.02%]
   - Card 4: `Fulfillment Realization Rate` (41.46%) [Subtext: Demand Realization Efficiency]
2. **Visual 1 (Bar Chart)**: `Revenue by Order Status`
   - Shows Net Revenue broken down by Delivered ($405.5K), Cancelled ($158.1K), Processing ($146.9K), Shipped ($138.6K), and Returned ($128.9K).
   - Highlighting Delivered as Realized vs. others as Unrealized.
3. **Visual 2 (Line & Shaded Area Chart)**: `Monthly Net Demand vs. Recognized Revenue`
   - X-Axis: `dim_date[Year-Month]` (2024-01 to 2026-09)
   - Lines: `Net Demand Revenue` vs. `Recognized Revenue`
   - Gap: Shaded area indicating the unrealized fulfillment pipeline and attrition spread.

### Page 2: Merchandising & Regional Intelligence
1. **Product Category Matrix**: Category, Units, Net Demand, Recognized Revenue, Margin %.
2. **Geographic Map / Treemap**: State/City sales performance.
3. **Payment Channel Breakdown**: UPI, Net Banking, Credit Card, PayPal demand share.

### Page 3: Customer Segmentation & RFM Lifecycle
1. **Cluster Distribution**: Champions (55.0% revenue), Recent Active Shoppers (19.6%), At-Risk Buyers (25.3%), Unconverted (0.0%).
2. **Scatter Plot**: Recency vs. Monetary spend color-coded by segment.
