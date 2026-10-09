# Dimensional Warehouse Design: Star Schema Specification

## Data Warehouse Modeling (Kimball Methodology)

This document formalizes the star-schema dimensional model implemented for the **E-Commerce Sales and Customer Analytics Platform** in MySQL (with cross-compatible local execution via SQLite/SQLAlchemy).

---

### 1. Conceptual & Logical Star Schema Model

```
       ┌────────────────────────┐                   ┌────────────────────────┐
       │      dim_customer      │                   │       dim_product      │
       ├────────────────────────┤                   ├────────────────────────┤
       │ PK  customer_key (INT) │                   │ PK  product_key (INT)  │
       │     customer_id        │                   │     product_id         │
       │     customer_name      │                   │     product_name       │
       │     email              │                   │     category           │
       │     phone (nullable)   │                   │     sub_category       │
       │     city, state, etc.  │                   │     unit_cost          │
       │     signup_date        │                   │     list_price         │
       │     rfm_segment        │                   └───────────┬────────────┘
       └───────────┬────────────┘                               │ 1
                   │ 1                                          │
                   │                                            │
                   │              ┌──────────────────┐          │
                   └─────────────►│    fact_sales    │◄─────────┘
                                  ├──────────────────┤
                   ┌─────────────►│(Atomic Line Item)│◄─────────┐
                   │              └────────┬─────────┘          │
                   │ 1                     │                    │ 1
       ┌───────────┴────────────┐          │ 1      ┌───────────┴────────────┐
       │      dim_location      │          │        │       dim_payment      │
       ├────────────────────────┤          ▼        ├────────────────────────┤
       │ PK  location_key (INT) │  ┌──────────────┐ │ PK  payment_key (INT)  │
       │     city               │  │   dim_date   │ │     payment_method     │
       │     state              │  ├──────────────┤ └────────────────────────┘
       │     country            │  │ PK date_key  │
       │     postal_code        │  │    full_date │
       └────────────────────────┘  │    day, etc. │
                                   └──────────────┘
```

---

### 2. Grain & Measure Specifications

#### 2.1 Fact Table Grain
The fact table `fact_sales` has an atomic grain of **one row per individual ordered line item** (`order_item_id`). This permits drill-down capabilities across SKU, category, customer profile, time, geography, and payment channels without data loss.

#### 2.2 Additive Measures & Business Calculations

| Measure Column | Data Type | Formula / Business Definition | Purpose |
| :--- | :--- | :--- | :--- |
| `quantity` | `INT` | Quantity ordered | Additive volume count |
| `unit_price` | `DECIMAL(10,2)` | Catalog selling price | Price per unit |
| `discount_percent`| `DECIMAL(5,2)` | Promotional discount % | Applied markdown |
| `gross_revenue` | `DECIMAL(12,2)` | $\text{quantity} \times \text{unit\_price}$ | Total gross demand before discounts |
| `discount_amount` | `DECIMAL(12,2)` | $\text{gross\_revenue} \times (\text{discount\_percent} / 100.0)$ | Markdown deduction |
| `net_revenue` | `DECIMAL(12,2)` | $\text{gross\_revenue} - \text{discount\_amount}$ | Net invoice demand after discounts |
| `unit_cost` | `DECIMAL(10,2)` | Unit acquisition cost from catalog | Direct product cost |
| `total_cost` | `DECIMAL(12,2)` | $\text{quantity} \times \text{unit\_cost}$ | Cost of Goods Sold (COGS) |
| `profit` | `DECIMAL(12,2)` | $\text{net\_revenue} - \text{total\_cost}$ | Operating profit before status filter |
| `profit_margin_pct`| `DECIMAL(5,2)` | $(\text{profit} / \text{net\_revenue}) \times 100.0$ | Profit margin percentage |
| `shipping_cost` | `DECIMAL(10,2)` | Allocated delivery cost | Freight expense |

#### 2.3 Strict Revenue Recognition Measures (FOB Destination)

To adhere to enterprise revenue recognition standards (IFRS 15 / US GAAP), realized financial metrics are recognized only upon verified delivery:

| Status Indicator / Financial Measure | Formula / Rule | Business Purpose |
| :--- | :--- | :--- |
| `is_delivered` | `1 if status == 'Delivered' else 0` | Delivered indicator |
| `is_shipped` | `1 if status == 'Shipped' else 0` | In-transit indicator |
| `is_processing` | `1 if status == 'Processing' else 0` | Pipeline indicator |
| `is_returned` | `1 if status == 'Returned' else 0` | Customer return indicator |
| `is_cancelled` | `1 if status == 'Cancelled' else 0` | Pre-dispatch cancellation indicator |
| `recognized_revenue` | $\mathbf{net\_revenue} \text{ if } (\text{status} = \text{'Delivered'}) \text{ else } \$0.00$ | **Realized top-line revenue** |
| `recognized_profit` | $\mathbf{profit} \text{ if } (\text{status} = \text{'Delivered'}) \text{ else } \$0.00$ | **Realized operating profit** |
| `in_transit_revenue` | $\mathbf{net\_revenue} \text{ if } (\text{status} = \text{'Shipped'}) \text{ else } \$0.00$ | Courier inventory pipeline |
| `pipeline_revenue` | $\mathbf{net\_revenue} \text{ if } (\text{status} = \text{'Processing'}) \text{ else } \$0.00$ | Warehouse fulfillment queue |
| `returned_revenue_loss` | $\mathbf{net\_revenue} \text{ if } (\text{status} = \text{'Returned'}) \text{ else } \$0.00$ | Post-delivery refund leakage |
| `cancelled_revenue_loss` | $\mathbf{net\_revenue} \text{ if } (\text{status} = \text{'Cancelled'}) \text{ else } \$0.00$ | Pre-dispatch lost revenue |
| `customer_order_rank` | Dense rank of customer purchase sequence | Order frequency sequencing |
| `customer_cumulative_spend` | Running total recognized spend per customer | Customer LTV trajectory |

---

### 3. Star Schema Table Specifications

#### 3.1 `dim_customer`
Stores customer profile records with surrogate primary key.
- `customer_key`: Surrogate integer primary key (`AUTO_INCREMENT`).
- `customer_id`: Operational natural key (`CUST_00001` through `CUST_01000`).
- `customer_name`: Standardized full name.
- `email`: Normalized contact email.
- `phone`: Contact telephone number (nullable for customers with omitted phone).
- `city`, `state`, `country`, `postal_code`: Customer geographic profile.
- `signup_date`: Account registration date.
- `rfm_segment`: Segment assignment derived from RFM analysis.

#### 3.2 `dim_product`
Stores catalog merchandising hierarchy.
- `product_key`: Surrogate integer primary key (`AUTO_INCREMENT`).
- `product_id`: Operational SKU code (`PROD_0001` through `PROD_0030`).
- `product_name`: Merchandised title.
- `category`: Broad merchandise category.
- `sub_category`: Specific product sub-category.
- `unit_cost`: Manufacturing/acquisition cost per unit.
- `list_price`: Catalog base retail price.

#### 3.3 `dim_date`
Conformed calendar dimension covering full operational history (`2024-01-01` to `2026-12-31`).
- `date_key`: Smart surrogate key formatted as `YYYYMMDD` (e.g. `20250418`).
- `full_date`: Standard `DATE` format.
- `day_of_week`: Integer `1-7` (1 = Monday, 7 = Sunday).
- `day_name`: Full weekday name (`Monday`, `Tuesday`, etc.).
- `day_of_month`: Day of the month (`1-31`).
- `month_num`: Month integer (`1-12`).
- `month_name`: Full month name (`January`, `February`, etc.).
- `quarter`: Calendar quarter (`1-4`).
- `year`: Calendar year (`2024-2026`).
- `is_weekend`: Flag (`1` for Saturday/Sunday, `0` for weekdays).

#### 3.4 `dim_location`
Distinct geography combinations extracted from customer profiles.
- `location_key`: Surrogate integer primary key (`AUTO_INCREMENT`).
- `city`: Standardized city name.
- `state`: Standardized state/province name.
- `country`: Country name.
- `postal_code`: Postal / ZIP code.
- `UNIQUE KEY`: `(city, state, country, postal_code)`.

#### 3.5 `dim_payment`
Supported transaction checkout methods.
- `payment_key`: Surrogate integer primary key (`AUTO_INCREMENT`).
- `payment_method`: Distinct payment channel (`Credit Card`, `Debit Card`, `Net Banking`, `PayPal`, `UPI`).

#### 3.6 `fact_sales`
Central atomic sales line-item fact table.
- `sales_key`: Surrogate BigInt primary key (`AUTO_INCREMENT`).
- `order_id`: Degenerate dimension (order header identifier).
- `order_item_id`: Degenerate dimension & business key (`UNIQUE KEY`).
- Foreign Keys: `customer_key`, `product_key`, `date_key`, `location_key`, `payment_key`.
- Measures: All financial, operational status, and sequence measures detailed in Section 2.

---

### 4. Indexing & Query Optimization Strategy

1. **Foreign Key B-Tree Indexes**:
   - `idx_fact_sales_customer (customer_key)`
   - `idx_fact_sales_product (product_key)`
   - `idx_fact_sales_date (date_key)`
   - `idx_fact_sales_location (location_key)`
   - `idx_fact_sales_payment (payment_key)`
2. **Operational Status & Degenerate Key Indexes**:
   - `idx_fact_sales_status (order_status)`
   - `idx_fact_sales_order_id (order_id)`
3. **Composite Query Indexes**:
   - `idx_fact_sales_date_customer (date_key, customer_key)` for customer cohort analysis.
   - `idx_fact_sales_product_date (product_key, date_key)` for product sales velocity.

---

### 5. DDL and Analytical SQL Organization

- **DDL Scripts** (`sql/schema/`):
  - `00_init_warehouse.sql`: Complete teardown and recreation script.
  - `01_create_database.sql`: Database creation and character set configuration.
  - `02_create_dimensions.sql`: DDL for all 5 dimension tables.
  - `03_create_fact_sales.sql`: DDL for the atomic fact table.

- **Analytical SQL Scripts** (`sql/analytics/`):
  - `01_monthly_revenue_and_profit.sql`: Monthly revenue, COGS, profit, and margin trends.
  - `02_mom_revenue_growth.sql`: Month-over-month growth % and running revenue totals.
  - `03_product_category_rankings.sql`: Top products and categories ranked by profit.
  - `04_customer_ltv_repeat_purchases.sql`: Customer lifetime value and repeat purchase cohorts.
  - `05_rfm_segmentation_analysis.sql`: Star-schema SQL RFM quartile segmentation.
  - `06_regional_performance.sql`: State and city geographic revenue distribution.
  - `07_payment_and_order_status.sql`: Payment channel adoption and fulfillment attrition funnel.
