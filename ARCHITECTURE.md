# System Architecture & Technical Specification

## E-Commerce Sales and Customer Analytics

---

### 1. High-Level Architectural Diagram

```mermaid
flowchart TD
    subgraph Data Sources ["1. Operational Data Source Layer"]
        RAW_ORDERS["orders.csv / json<br/>(Transactional Header)"]
        RAW_ITEMS["order_items.csv<br/>(Line Item Detail)"]
        RAW_CUSTOMERS["customers.csv<br/>(Master Profile)"]
        RAW_PRODUCTS["products.csv<br/>(Catalog Master)"]
    end

    subgraph Ingestion_Validation ["2. Ingestion & Quality Gatekeeper"]
        INGEST["Ingestion Module<br/>(Hash Audit & Lineage Tracking)"]
        VALIDATOR{"Data Quality Gatekeeper<br/>- Null Key Checks<br/>- Non-Negative Monetary Checks<br/>- Date Format Checks<br/>- Referential Integrity Checks"}
        REJECTS[("/data/rejected/<br/>quarantine_records.csv<br/>+ reject_reason_code")]
    end

    subgraph Transformation_Spark ["3. Data Cleaning & PySpark Engine"]
        CLEAN["Data Cleaner & Normalizer<br/>(Canonicalization, Case Trim)"]
        PROCESSED_STAGING[("/data/processed/<br/>Standardized DataFrames")]
        SPARK_ENGINE["Apache PySpark Engine<br/>- Distributed Multi-way Joins<br/>- Revenue & Profit Computations<br/>- Window Functions (Running Totals, Rank)<br/>- Surrogate Key Preparation"]
    end

    subgraph Data_Warehouse ["4. Relational Data Warehouse (MySQL)"]
        direction TB
        DIM_DATE["dim_date<br/>(Calendar Hierarchy)"]
        DIM_CUST["dim_customer<br/>(Surrogate PK: customer_key)"]
        DIM_PROD["dim_product<br/>(Surrogate PK: product_key)"]
        DIM_LOC["dim_location<br/>(Surrogate PK: location_key)"]
        DIM_PAY["dim_payment<br/>(Surrogate PK: payment_key)"]
        FACT_SALES["fact_sales<br/>(Sales Measures, FKs to Dims)"]

        DIM_DATE --- FACT_SALES
        DIM_CUST --- FACT_SALES
        DIM_PROD --- FACT_SALES
        DIM_LOC --- FACT_SALES
        DIM_PAY --- FACT_SALES
    end

    subgraph Advanced_Analytics ["5. SQL Analytics & Machine Learning"]
        SQL_ANALYTICS["Analytical SQL Layer<br/>(CTEs, Window Funcs, Cohorts, CLV)"]
        RFM_KMEANS["Customer Segmentation Engine<br/>- Recency, Frequency, Monetary<br/>- Log Transform & StandardScaler<br/>- K-Means Clustering<br/>- Silhouette Score Optimization"]
    end

    subgraph Consumption_Layer ["6. Business Intelligence (Power BI)"]
        POWER_BI["Power BI Executive Dashboard<br/>- Sales & Profit Performance<br/>- Customer Cohorts & RFM Clusters<br/>- Regional Heatmap & Category Tree"]
    end

    subgraph Orchestration ["7. Pipeline Orchestration & Monitoring"]
        AIRFLOW["Apache Airflow DAG<br/>ecommerce_pipeline_dag<br/>- Task Dependencies<br/>- Automatic Retries & Alerts<br/>- Execution Auditing"]
    end

    Data Sources --> INGEST
    INGEST --> VALIDATOR
    VALIDATOR -- "Invalid Records" --> REJECTS
    VALIDATOR -- "Passed Records" --> CLEAN
    CLEAN --> PROCESSED_STAGING
    PROCESSED_STAGING --> SPARK_ENGINE
    SPARK_ENGINE --> Data_Warehouse
    Data_Warehouse --> SQL_ANALYTICS
    Data_Warehouse --> RFM_KMEANS
    RFM_KMEANS -. "Update Segment Labels" .-> DIM_CUST
    Data_Warehouse --> POWER_BI
    AIRFLOW -. "Orchestrates Entire Flow" .-> Ingestion_Validation
    AIRFLOW -. "Triggers" .-> Transformation_Spark
    AIRFLOW -. "Triggers" .-> Data_Warehouse
    AIRFLOW -. "Triggers" .-> Advanced_Analytics
```

---

### 2. Architectural Layers & Component Breakdown

#### Layer 1: Raw Data Sourcing & Lineage Auditing
- **Operational Reality**: In real e-commerce systems, orders, order items, customers, and product catalogs are generated across different services (Order Management System, Catalog Service, CRM).
- **Format Support**: Delimited files (CSV) and semi-structured payloads (JSON).
- **Ingestion Audit**: Every batch reads raw files, calculates file MD5 hash, records total ingestion timestamp, and counts raw records for zero-loss reconciliation.

#### Layer 2: Data Quality Gatekeeper & Quarantine Mechanism
- **Non-Silent Quarantine**: Flawed data is **never silently dropped**. Records failing validation are written to `/data/rejected/` with:
  - Original record payload.
  - `reject_stage`: e.g. `VALIDATION_CHECK`.
  - `reject_reason`: e.g. `NEGATIVE_PRICE`, `NULL_CUSTOMER_KEY`, `INVALID_DATE_FORMAT`, `FOREIGN_KEY_NOT_FOUND`.
  - `rejection_timestamp`: ISO 8601 UTC timestamp.
- **Validation Rules**:
  1. Primary Key Uniqueness (no duplicate `order_id` in order header, no duplicate `order_item_id`).
  2. Mandatory Non-Null Attributes (`customer_id`, `product_id`, `order_date`).
  3. Positive Monetary & Quantity Constraints (`quantity > 0`, `unit_price > 0`, `0 <= discount_percent <= 100`).
  4. Logical Date Checks (`order_date <= current_date`, valid calendar string).
  5. Referential Integrity (order items referencing existing orders and products).

#### Layer 3: Data Cleaning & Normalization
- **String Sanitization**: Trimming leading/trailing whitespace, normalizing casing (e.g. `Electronics` vs `electronics`), standardizing state/country codes.
- **Temporal Parsing**: Normalizing multiple date formats (e.g. `YYYY-MM-DD`, `DD/MM/YYYY`) into standard ISO `YYYY-MM-DD`.
- **Handling Missing Non-Critical Fields**: Handling optional attributes with graceful defaults (e.g., missing phone numbers replaced by `UNKNOWN` or null placeholder without dropping the customer).

#### Layer 4: PySpark Processing Engine
- **Why PySpark**: Demonstrates distributed data processing paradigms, decoupling compute from storage, handling large-scale joins efficiently via broadcast/hash joins, and computing analytical window functions before loading into relational storage.
- **Core Operations**:
  - Multi-DataFrame joins: Joining `orders` + `order_items` + `products` + `customers`.
  - Derived Business Measures:
    - $\text{gross\_revenue} = \text{quantity} \times \text{unit\_price}$
    - $\text{discount\_amount} = \text{gross\_revenue} \times \left(\frac{\text{discount\_percent}}{100}\right)$
    - $\text{net\_revenue} = \text{gross\_revenue} - \text{discount\_amount}$
    - $\text{total\_cost} = \text{quantity} \times \text{unit\_cost}$
    - $\text{profit} = \text{net\_revenue} - \text{total\_cost}$
  - Window Functions:
    - Customer cumulative purchase value: `sum(net_revenue) over (partition by customer_id order by order_date)`
    - Customer order sequence rank: `row_number() over (partition by customer_id order by order_date)`
    - Monthly running revenue totals.

#### Layer 5: Relational Data Warehouse (MySQL)
- **Star Schema Design**:
  - **1 Fact Table**: `fact_sales` containing core additive and semi-additive business metrics and foreign keys.
  - **5 Dimension Tables**:
    - `dim_customer`: Customer profile, geography reference, signup date, and dynamically assigned RFM cluster.
    - `dim_product`: Product catalog, hierarchical category and sub-category, unit cost, list price.
    - `dim_date`: Conformed calendar dimension with pre-calculated day of week, quarter, month name, weekend flags.
    - `dim_location`: Distinct geographic locations (city, state, country, postal code).
    - `dim_payment`: Distinct payment methods.
- **Integrity & Indexing**:
  - Primary Keys with auto-increment surrogate keys for dimensions.
  - Foreign Key constraints connecting `fact_sales` to all dimensions.
  - Composite B-Tree indexes on `(date_key, customer_key)` and `(product_key)` for lightning-fast join and aggregation execution.

#### Layer 6: Advanced SQL Analytics
- Production SQL queries demonstrating:
  - Common Table Expressions (CTEs) for multi-level aggregations.
  - Window functions (`LAG()`, `LEAD()`, `RANK()`, `SUM() OVER ()`).
  - Month-over-Month (MoM) revenue growth calculation.
  - Customer Lifetime Value (CLV) and repeat customer purchase cadence.
  - Category and product-level profit margin analysis.

#### Layer 7: Unsupervised Machine Learning (RFM + K-Means)
- **Methodology**:
  1. Extract RFM metrics from `fact_sales` and `dim_date`:
     - **Recency ($R$)**: Days since the customer's most recent order date relative to snapshot date.
     - **Frequency ($F$)**: Total distinct count of orders placed by customer.
     - **Monetary ($M$)**: Total net revenue generated by customer.
  2. Data Preprocessing:
     - Log transformation ($\log(x + 1)$) to mitigate extreme right-skewness.
     - `StandardScaler` to ensure zero mean and unit variance.
  3. Model Training & Evaluation:
     - Run K-Means across $k \in [2, 8]$.
     - Compute **Inertia (Elbow method)** and **Silhouette Score**.
     - Select optimal $k$ based on highest silhouette score with balanced cluster sizes.
  4. Empirical Labeling:
     - Inspect mean $R, F, M$ per cluster to assign business definitions (e.g. *Champions*, *Loyal High-Spenders*, *At-Risk / Lapsed*, *New / Low-Value*).
     - Persist the cluster assignments back into `dim_customer.rfm_segment`.

#### Layer 8: Semantic Data Model & Power BI Dashboards
- Designed for executive decision-makers and category managers:
  - Direct relational connection to MySQL dimensional warehouse or curated Parquet exports.
  - Star schema relationships automatically recognized (1-to-many single direction filtering).
  - Calculated DAX measures for Average Order Value (AOV), Repeat Customer Rate, Gross Margin %, and YoY/MoM revenue.

#### Layer 9: Orchestration & Monitoring (Apache Airflow)
- **Airflow DAG**: `ecommerce_etl_pipeline`
  - PythonOperator tasks with strict linear and branched dependencies.
  - Tasks:
    1. `check_system_health`: Checks DB connectivity and storage paths.
    2. `extract_and_ingest_data`: Reads source files and logs raw counts.
    3. `validate_and_quarantine`: Executes schema and business rules.
    4. `clean_and_normalize`: Produces standardized intermediate tables.
    5. `pyspark_transform_and_aggregate`: Runs distributed transformations.
    6. `load_dimensional_warehouse`: Inserts/updates MySQL dimensions and fact tables.
    7. `run_sql_analytics`: Executes validation queries and updates analytical views.
    8. `train_rfm_customer_segmentation`: Runs K-Means and updates customer segment column.
  - Built-in retry counts (3 retries with exponential backoff) and production logging.

---

### 3. Data Flow & Reject Handling Strategy

```
Raw Record Enters
       │
       ▼
[Validation Gate] ─── Fails rule? ───► YES ───► Append to /data/rejected/quarantine_records.csv
       │                                        Log Warning with reject reason code
       ▼ NO (Passed)
[Cleaning Stage]
       │
       ▼
[PySpark Processing]
       │
       ▼
[Surrogate Key Lookup & MySQL Loader]
       │
       ▼
[Star Schema DW]
```

---

### 4. Hardware & Software Requirements

- **Runtime**: Python 3.10+
- **Database**: MySQL Server 8.0+ (running locally or in Docker container)
- **Distributed Compute**: PySpark 3.4+ / Apache Spark (local master mode)
- **Orchestration**: Apache Airflow 2.7+ (local or Docker-compose)
- **Memory**: 8 GB RAM minimum (16 GB recommended for running Airflow + Spark + MySQL together)
- **OS**: Windows / Linux / macOS (all cross-platform path handling using Python `pathlib.Path`)
