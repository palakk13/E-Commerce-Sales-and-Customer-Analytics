# Data Dictionary & Schema Specifications

## E-Commerce Sales and Customer Analytics

This document describes the operational source schemas, data formats, constraints, and validation criteria for the e-commerce transaction ecosystem.

---

### 1. Raw Source Datasets

#### 1.1 Customers (`customers.csv`)
Represents registered customer accounts and profile attributes.

| Column Name | Data Type | Nullable | Example Value | Description |
| :--- | :--- | :--- | :--- | :--- |
| `customer_id` | String | No (PK) | `CUST_00001` | Unique identifier for customer account |
| `customer_name` | String | No | `Aarav Sharma` | Customer full name (may contain whitespace noise) |
| `email` | String | No | `aarav.sharma1@example.com` | Primary contact email address |
| `phone` | String | Yes | `+91-9876543210` | Contact phone number (empty for ~4% users) |
| `city` | String | No | `Mumbai` | Residential/shipping city |
| `state` | String | No | `Maharashtra` | State or administrative territory |
| `country` | String | No | `India` | Country name |
| `postal_code` | String | No | `400001` | ZIP or postal code |
| `signup_date` | String (ISO) | No | `2023-05-14` | Customer onboarding registration date (`YYYY-MM-DD`) |

---

#### 1.2 Products (`products.csv`)
Product catalog master table containing merchandising metadata and pricing structures.

| Column Name | Data Type | Nullable | Example Value | Description |
| :--- | :--- | :--- | :--- | :--- |
| `product_id` | String | No (PK) | `PROD_0001` | Unique SKU identifier |
| `product_name` | String | No | `Noise-Cancelling Headphones` | Merchandising catalog title |
| `category` | String | No | `Electronics` | Top-level category (may contain casing noise) |
| `sub_category` | String | No | `Noise-Cancelling` | Granular product grouping |
| `unit_cost` | Decimal(10,2) | No | `45.00` | Cost of goods sold (COGS) incurred per unit |
| `list_price` | Decimal(10,2) | No | `99.99` | Baseline retail price |

---

#### 1.3 Orders (`orders.csv`)
Header-level transactional table capturing order placements and shipment details.

| Column Name | Data Type | Nullable | Example Value | Description |
| :--- | :--- | :--- | :--- | :--- |
| `order_id` | String | No (PK) | `ORD_000001` | Unique transaction header ID |
| `customer_id` | String | No (FK) | `CUST_00123` | Customer reference (injected nulls test validation) |
| `order_date` | String (ISO) | No | `2025-04-18` | Transaction date (injected invalid dates test validation) |
| `order_status` | String | No | `Delivered` | Fulfillment status (`Delivered`, `Shipped`, `Processing`, `Cancelled`, `Returned`) |
| `payment_method` | String | No | `Credit Card` | Payment mode (`Credit Card`, `UPI`, `Debit Card`, `PayPal`, `Net Banking`) |
| `shipping_cost` | Decimal(10,2) | No | `5.50` | Freight shipping fee |

---

#### 1.4 Order Items (`order_items.csv`)
Line-item detail table specifying quantities and selling prices per product ordered.

| Column Name | Data Type | Nullable | Example Value | Description |
| :--- | :--- | :--- | :--- | :--- |
| `order_item_id` | String | No (PK) | `ITEM_0000001` | Unique line item identifier |
| `order_id` | String | No (FK) | `ORD_000001` | Reference to parent order |
| `product_id` | String | No (FK) | `PROD_0001` | Reference to catalog SKU (injected non-existent SKUs) |
| `quantity` | Integer | No | `2` | Number of units ordered (injected negative/zero values) |
| `unit_price` | Decimal(10,2) | No | `94.99` | Selling price at checkout (injected negative values) |
| `discount_percent` | Decimal(5,2) | No | `10.00` | Discount applied (injected > 100% values) |

---

### 2. Injected Anomalies & Validation Expectations

The ingestion gatekeeper implements strict business integrity gates:

| Anomaly Type | Target Entity | Rule Violation | Gatekeeper Action |
| :--- | :--- | :--- | :--- |
| **Duplicate PK** | `orders.csv` | Duplicate `order_id` | Quarantine duplicate instances into `/data/rejected/` |
| **Missing FK** | `orders.csv` | Empty `customer_id` | Quarantine order with reason `NULL_CUSTOMER_KEY` |
| **Corrupt Date** | `orders.csv` | Unparseable date (e.g. `2026-99-99`) | Quarantine with reason `INVALID_DATE_FORMAT` |
| **Negative / Zero Qty** | `order_items.csv` | `quantity <= 0` | Quarantine item with reason `INVALID_QUANTITY` |
| **Negative Unit Price**| `order_items.csv` | `unit_price <= 0` | Quarantine item with reason `NEGATIVE_UNIT_PRICE` |
| **Dangling FK** | `order_items.csv` | `product_id` not in catalog | Quarantine item with reason `FOREIGN_KEY_NOT_FOUND` |
| **Excess Discount** | `order_items.csv` | `discount_percent > 100` | Quarantine item with reason `INVALID_DISCOUNT_PERCENT` |
| **Whitespace / Casing** | `customers`, `products` | Non-canonical text | Cleaned & standardized in Layer 3 |

---

### 2.1 Defect Audit Reconciliation: Defect Occurrences vs. Unique Affected Records

To ensure complete reconciliation during auditing and quarantine routing:

- **Defect Occurrences (435 total across all files)**: The total frequency of rule violations triggered across all tables.
- **Transactional Quarantine Defect Occurrences (398)**: The sum of violation occurrences in `orders.csv` (147) and `order_items.csv` (251).
- **Unique Affected Records (397 transactional, 434 total)**: The actual count of distinct rows containing one or more defects.

| Entity | Defect / Anomaly Name | Defect Occurrences | Overlap With Other Defects | Unique Affected Records |
| :--- | :--- | :---: | :---: | :---: |
| `customers.csv` | Missing optional phone number | 37 | 0 | 37 |
| `orders.csv` | Duplicate `order_id` (redundant rows) | 45 | 0 | 45 |
| `orders.csv` | Missing `customer_id` | 60 | 1 (overlaps with invalid date) | 60 |
| `orders.csv` | Invalid / corrupt `order_date` | 42 | 1 (overlaps with missing customer) | 42 |
| **`orders.csv` Subtotal** | *Transactional header defects* | **147** | **1 row has 2 defects** | **146** |
| `order_items.csv` | Invalid `product_id` (non-existent FK) | 83 | 0 | 83 |
| `order_items.csv` | Invalid `quantity` ($\le 0$) | 80 | 0 | 80 |
| `order_items.csv` | Negative `unit_price` | 61 | 0 | 61 |
| `order_items.csv` | Excess `discount_percent` ($> 100\%$) | 27 | 0 | 27 |
| **`order_items.csv` Subtotal** | *Line-item defects* | **251** | **0** | **251** |
| **Transactional Subtotal** | *Targeted for `/data/rejected/` quarantine* | **398** | **1 overlap** | **397** |
| **Grand Total** | *All files (including profile nulls)* | **435** | **1 overlap** | **434** |

---

### 3. Financial Metrics Data Dictionary: Demand Revenue vs. Recognized Revenue

To eliminate ambiguity across data engineering, analytical SQL, and Power BI reporting, the platform enforces an explicit distinction between **Customer Demand** and **Recognized Financials** under **FOB Destination (IFRS 15 / US GAAP)** accounting principles:

#### 3.1 Core Measure Definitions & Business Logic

| Metric Name | Business Definition & Formula | Data Grain | Power BI / SQL Category | Reconciled Total |
| :--- | :--- | :--- | :--- | :---: |
| **Gross Demand Revenue** | Total top-line order value placed by customers before any discount deductions: $\sum (\text{quantity} \times \text{unit\_price})$. | Line Item (`fact_sales`) | Demand Layer | **$1,020,403.59** |
| **Total Discounts** | Sum of promotional markdown deductions granted at checkout: $\sum \text{gross\_revenue} \times (\text{discount\_percent} / 100.0)$. | Line Item (`fact_sales`) | Demand Layer | **$42,305.77** |
| **Net Demand Revenue** | Total invoiced demand placed by customers across all 5 order statuses: $\text{Gross Demand} - \text{Total Discounts}$. | Line Item (`fact_sales`) | Demand Layer | **$978,097.82** |
| **Recognized Revenue** | Revenue realized exclusively when control of goods transfers upon physical delivery: $\sum \text{net\_revenue} \text{ WHERE } (\text{order\_status} = \text{'Delivered'})$. | Line Item (`fact_sales`) | Realized Financials | **$405,484.47** |
| **Recognized COGS** | Direct catalog acquisition cost incurred on units that were physically delivered: $\sum (\text{quantity} \times \text{unit\_cost}) \text{ WHERE } (\text{order\_status} = \text{'Delivered'})$. | Line Item (`fact_sales`) | Realized Financials | **$174,277.00** |
| **Recognized Profit** | Net operating margin realized on delivered transactions: $\text{Recognized Revenue} - \text{Recognized COGS}$. | Line Item (`fact_sales`) | Realized Financials | **$231,207.47** |
| **Recognized Profit Margin**| Proportion of realized revenue retained as operating profit: $(\text{Recognized Profit} / \text{Recognized Revenue}) \times 100.0$. | Ratio / KPI | Realized Financials | **57.02%** |
| **In-Transit Revenue** | Value of orders shipped via courier but pending delivery: $\sum \text{net\_revenue} \text{ WHERE } (\text{order\_status} = \text{'Shipped'})$. | Line Item (`fact_sales`) | Operational Pipeline | **$138,599.59** (14.17%) |
| **Pipeline Revenue** | Value of orders in warehouse fulfillment queue: $\sum \text{net\_revenue} \text{ WHERE } (\text{order\_status} = \text{'Processing'})$. | Line Item (`fact_sales`) | Operational Pipeline | **$146,949.52** (15.02%) |
| **Cancelled Revenue Loss** | Pre-dispatch cancellations resulting in lost demand: $\sum \text{net\_revenue} \text{ WHERE } (\text{order\_status} = \text{'Cancelled'})$. | Line Item (`fact_sales`) | Operational Attrition | **$158,119.41** (16.17%) |
| **Returned Revenue Loss** | Post-delivery returns resulting in customer refunds: $\sum \text{net\_revenue} \text{ WHERE } (\text{order\_status} = \text{'Returned'})$. | Line Item (`fact_sales`) | Operational Attrition | **$128,944.83** (13.18%) |
| **Fulfillment Realization Rate**| Percentage of customer demand converted to recognized sales: $(\text{Recognized Revenue} / \text{Net Demand Revenue}) \times 100.0$. | Ratio / KPI | Operational Efficiency | **41.46%** |
| **Demand Attrition Rate** | Percentage of customer demand lost to cancellations and returns: $(\text{Cancelled Loss} + \text{Returned Loss}) / \text{Net Demand Revenue} \times 100.0$. | Ratio / KPI | Operational Attrition | **29.35%** |

---

#### 3.2 Fulfillment Status Treatment Matrix

Every transaction in `fact_sales` is classified into exactly one fulfillment status. The table below details how each status affects demand vs. financial reporting:

| Order Status | Line Items | Distinct Orders | Net Demand ($) | Demand Share (%) | Counts in Demand? | Counts in Recognized Revenue? | Financial Rationale |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :--- |
| **`Delivered`** | **2,681** | **1,594** | **$405,484.47** | **41.46%** | Yes | **YES ($405,484.47)** | Performance obligation fulfilled; risk/reward transferred. |
| **`Cancelled`** | 1,039 | 623 | $158,119.41 | 16.17% | Yes | **NO ($0.00)** | Order voided prior to dispatch; zero economic transfer. |
| **`Processing`** | 934 | 560 | $146,949.52 | 15.02% | Yes | **NO ($0.00)** | In picking/packing; recognized only upon final delivery. |
| **`Shipped`** | 875 | 522 | $138,599.59 | 14.17% | Yes | **NO ($0.00)** | In-transit courier custody; title transfers at destination. |
| **`Returned`** | 850 | 520 | $128,944.83 | 13.18% | Yes | **NO ($0.00)** | Goods returned; revenue reversed/refunded to customer. |
| **Grand Total** | **6,379** | **3,819\*** | **$978,097.82** | **100.00%** | **100% Demand** | **$405,484.47 Realized** | **100% Conserved ($\Delta = \$0.00$)** |

\* *Note: Distinct order counts sum to more than 3,819 across statuses because individual line items within a multi-item order inherit order status uniformly, but across the entire dataset distinct orders are mutually exclusive by status.*

---

#### 3.3 Conservation of Financial Measures

$$\text{Recognized Revenue} + \text{In-Transit} + \text{Pipeline} + \text{Returned Loss} + \text{Cancelled Loss} = \text{Net Demand Revenue}$$
$$\$405,484.47 + \$138,599.59 + \$146,949.52 + \$128,944.83 + \$158,119.41 = \mathbf{\$978,097.82}$$

$$\text{Net Demand Revenue} + \text{Total Discounts} = \text{Gross Demand Revenue}$$
$$\$978,097.82 + \$42,305.77 = \mathbf{\$1,020,403.59}$$

#### 3.4 Line-Item Grain & Aggregation Integrity
- `fact_sales` is modeled at the atomic grain of **one row per ordered line item** (`order_item_id`).
- All revenue, cost, discount, and profit measures are fully additive along the product, customer, date, and geographic dimensions without requiring non-additive adjustments.
- For order-level attributes like `shipping_cost`, aggregations must group by `order_id` or calculate distinct order counts (`DISTINCTCOUNT(order_id)`) to prevent inflating shipping fees across multi-item baskets.


