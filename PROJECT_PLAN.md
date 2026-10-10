# E-Commerce Sales and Customer Analytics
## Master Project Plan & Execution Roadmap

---

### 1. Executive Summary & Objective

The **E-Commerce Sales and Customer Analytics** platform is a production-grade data platform designed to process, govern, model, analyze, and visualize high-volume e-commerce transactions. 

The primary business objectives are:
1. Ingest multi-entity e-commerce transactions (orders, items, customers, products).
2. Enforce strict data quality gates (schema validation, integrity constraints, reject logging).
3. Transform raw operational datasets into standardized, curated datasets.
4. Leverage distributed computing (Apache PySpark) for heavy transformations, aggregations, and window-based metrics.
5. Populate a dimensional star-schema data warehouse in MySQL with appropriate primary keys, foreign keys, surrogate keys, and indexes.
6. Run deep analytical SQL queries covering revenue growth, cohort retention, delivery/payment metrics, customer lifetime value (CLV), and profit margins.
7. Perform customer segmentation using Recency, Frequency, and Monetary (RFM) modeling combined with K-Means clustering (evaluated via Silhouette Score).
8. Expose analytical views and extract clean semantic datasets for executive dashboards in Power BI.
9. Orchestrate the entire end-to-end lineage via Apache Airflow with automated failure alerting and retry mechanisms.

---

### 2. End-to-End Architecture & Data Flow

```
[Operational Sources / Raw Data CSVs & JSONs]
                    │
                    ▼
     [1. Data Ingestion Module]
     - Reads raw files
     - Audits row counts, file hashes, timestamps
                    │
                    ▼
     [2. Data Validation Gatekeeper]
     - Schema validation (Great Expectations / Pydantic style)
     - Negative prices/quantities, null keys, corrupt dates
     - Segregates invalid rows into /data/rejected/ with reason codes
                    │
                    ▼
     [3. Data Cleaning & Normalization]
     - Type casting, string canonicalization, whitespace trimming
     - Replaces imputed nulls or logs drops
     - Emits curated datasets into /data/processed/
                    │
                    ▼
     [4. PySpark Processing Engine]
     - Distributed joins (orders + order_items + products)
     - Derived metrics: revenue = (qty * price) * (1 - discount)
     - Profit = revenue - (qty * unit_cost)
     - Window functions: running totals, customer order ranking
     - Generates enriched fact and dimensional staging files
                    │
                    ▼
     [5. MySQL Dimensional Data Warehouse]
     - Star Schema: 1 Fact (fact_sales) + 5 Dimensions
     - dim_customer, dim_product, dim_date, dim_location, dim_payment
     - Enforces PK/FK constraints and indexing
                    │
                    ▼
     [6. SQL Analytics & Metric Views]
     - Monthly revenue & MoM growth CTEs
     - CLV, repeat customer rate, category margins
                    │
                    ▼
     [7. Machine Learning / RFM Segmentation]
     - Calculates R, F, M metrics per customer
     - Robust scaling + K-Means clustering
     - Silhouette score optimization (Elbow + Silhouette analysis)
     - Enriches warehouse with customer_segment label
                    │
                    ▼
     [8. Power BI Executive Analytics]
     - Dimensional model connection
     - Executive KPI cards, trend charts, matrix visualizers
                    │
                    ▲
    ════════════════════════════════════════════════════
    [Orchestration: Apache Airflow DAG]
    (extract → validate → clean → transform → spark → load → analytics → ml)
```

---

### 3. Dimensional Data Warehouse Schema (Star Schema)

The target MySQL data warehouse uses a **Star Schema** pattern optimized for analytical query performance (OLAP), slice-and-dice operations, and BI tools.

#### 3.1 Dimensions

1. **`dim_customer`**
   - `customer_key` (INT, PK, AUTO_INCREMENT) - Surrogate Key
   - `customer_id` (VARCHAR(32), UNIQUE) - Natural Key
   - `customer_name` (VARCHAR(100))
   - `email` (VARCHAR(100))
   - `phone` (VARCHAR(30))
   - `signup_date` (DATE)
   - `rfm_segment` (VARCHAR(50), DEFAULT 'Unassigned')
   - `is_active` (BOOLEAN)

2. **`dim_product`**
   - `product_key` (INT, PK, AUTO_INCREMENT) - Surrogate Key
   - `product_id` (VARCHAR(32), UNIQUE) - Natural Key
   - `product_name` (VARCHAR(150))
   - `category` (VARCHAR(50))
   - `sub_category` (VARCHAR(50))
   - `unit_cost` (DECIMAL(10, 2))
   - `list_price` (DECIMAL(10, 2))

3. **`dim_date`**
   - `date_key` (INT, PK) - e.g. YYYYMMDD (20261008)
   - `full_date` (DATE, UNIQUE)
   - `day_of_week` (TINYINT)
   - `day_name` (VARCHAR(15))
   - `day_of_month` (TINYINT)
   - `month_num` (TINYINT)
   - `month_name` (VARCHAR(15))
   - `quarter` (TINYINT)
   - `year` (SMALLINT)
   - `is_weekend` (BOOLEAN)

4. **`dim_location`**
   - `location_key` (INT, PK, AUTO_INCREMENT) - Surrogate Key
   - `city` (VARCHAR(100))
   - `state` (VARCHAR(100))
   - `country` (VARCHAR(50))
   - `postal_code` (VARCHAR(20))
   - UNIQUE KEY (`city`, `state`, `country`, `postal_code`)

5. **`dim_payment`**
   - `payment_key` (INT, PK, AUTO_INCREMENT) - Surrogate Key
   - `payment_method` (VARCHAR(50), UNIQUE) - e.g. Credit Card, UPI, PayPal, Debit Card, Net Banking

#### 3.2 Fact Table

**`fact_sales`**
- `sales_key` (BIGINT, PK, AUTO_INCREMENT)
- `order_id` (VARCHAR(32)) - Degenerate Dimension
- `order_item_id` (VARCHAR(32), UNIQUE) - Degenerate Dimension
- `customer_key` (INT, FK -> dim_customer.customer_key)
- `product_key` (INT, FK -> dim_product.product_key)
- `date_key` (INT, FK -> dim_date.date_key)
- `location_key` (INT, FK -> dim_location.location_key)
- `payment_key` (INT, FK -> dim_payment.payment_key)
- `order_status` (VARCHAR(30))
- `quantity` (INT)
- `unit_price` (DECIMAL(10, 2))
- `discount_percent` (DECIMAL(5, 2))
- `gross_revenue` (DECIMAL(12, 2)) -- qty * unit_price
- `discount_amount` (DECIMAL(12, 2)) -- gross_revenue * discount_percent / 100
- `net_revenue` (DECIMAL(12, 2)) -- gross_revenue - discount_amount
- `total_cost` (DECIMAL(12, 2)) -- qty * dim_product.unit_cost
- `profit` (DECIMAL(12, 2)) -- net_revenue - total_cost
- `shipping_cost` (DECIMAL(10, 2))
- `created_at` (TIMESTAMP)

---

### 4. Implementation Phases

| Phase | Title | Scope & Deliverables | Status |
| :--- | :--- | :--- | :---: |
| **Phase 1** | **Repository Structure & Synthetic Data** | Directory tree, configuration (`config.yaml`), production logger, requirements, docker-compose, synthetic dirty data generator (11,867 records), immutable raw manifest. | **COMPLETED** |
| **Phase 2** | **Ingestion, Validation & Cleaning** | Ingestion pipeline with audit logs; validation rules detecting missing keys, negative values, malformed dates; quarantine router (`data/quarantine/`); cleaning & canonicalization functions. Conservation verified (11,308 valid + 559 quarantined = 11,867). | **COMPLETED** |
| **Phase 3** | **PySpark Processing Engine** | PySpark batch transformations, multi-table joins, derived column calculation, distributed window functions (running cumulative sales, rank), FOB Destination revenue recognition, curated Parquet outputs ($405,484.47 recognized revenue). | **COMPLETED** |
| **Phase 4** | **Data Warehouse (MySQL/SQLite) & SQL Analytics** | DDL scripts for Star Schema (1 Fact, 5 Dims), indexes, surrogate keys, transactional rollback. ETL loader with surrogate key lookups. 7 analytical SQL scripts (CTE, Window functions, CLV, MoM growth) with 100% financial conservation. | **COMPLETED** |
| **Phase 5** | **Apache Airflow Orchestration** | Production Airflow DAG (`ecommerce_analytics_dag.py`) orchestrating 9-task dependency chain with retries, fail-fast circuit breakers, Docker containerization (`docker-compose.yml`), and execution audit summary. | **COMPLETED** |
| **Phase 6** | **Customer Segmentation (RFM + K-Means)** | Feature extraction (Recency, Frequency, Monetary), log1p + StandardScaler normalization, K-Means elbow & silhouette score evaluation ($k=4$, silhouette=0.4873), cluster profiling, updating DWH `dim_customer.rfm_segment` for all 1,000 customers. | **COMPLETED** |
| **Phase 7** | **Power BI Semantic Modeling & Interactive Dashboards** | Star Schema Parquet/CSV data exports (`data/powerbi/`), 13 formal DAX measures (`dax_measures.dax`), semantic model schema (`semantic_model.json`), setup guide (`POWERBI_GUIDE.md`), high-contrast visual charts, interactive HTML dashboard. | **COMPLETED** |

---

### 5. Technology Justification & Responsibilities

| Technology | Role in Project | Justification (Why not something simpler?) |
| :--- | :--- | :--- |
| **Python** | Core Language | Unifies ingestion, data manipulation, validation, machine learning, and database connectivity. |
| **Pandas & NumPy** | Ingestion & Validation | High performance vectorized data manipulation for small-to-medium datasets, auditing, and cleaning. |
| **PySpark** | Distributed Processing | Simulates enterprise big-data scale; demonstrates proficiency in distributed Spark DataFrames, window aggregations, and resilient distributed transformations. |
| **MySQL / SQLite** | Relational DWH | Demonstrates enterprise relational star schema modeling, primary/foreign key integrity, B-Tree indexes, and complex analytical SQL. Dialect-adapted for local SQLite and containerized MySQL. |
| **Scikit-learn** | ML Segmentation | Provides rigorous unsupervised learning: StandardScaler, KMeans, silhouette_score for empirical clustering rather than arbitrary rule-of-thumb groupings. |
| **Apache Airflow**| Pipeline Orchestrator | Industry-standard DAG scheduling, dependency resolution, failure retry policies, execution logging, and SLA monitoring. |
| **Power BI** | Business Intelligence | Enterprise-grade semantic modeling, interactive dashboarding, slicing by dimensions, and C-suite KPI delivery. |
| **Docker** | Containerization | Ensures 100% reproducible deployment of MySQL, Airflow, and dependency stacks across development, testing, and production environments. |

---

### 6. Testing Strategy

The project enforces automated test coverage across all pipeline layers via `tests/run_tests.py` (58 tests passing, 0 failures, 0 errors):

1. **Data Ingestion & Quality Validation (`tests/test_validation.py` - 10 tests)**:
   - Primary and foreign key integrity, date format boundaries, negative quantities, prices, discounts, and quarantine isolation.
2. **Data Cleaning & Normalization (`tests/test_cleaning.py` - 7 tests)**:
   - Deduplication, case trimming, string canonicalization, and nullable customer phone preservation.
3. **PySpark Distributed Processing (`tests/test_spark_transformations.py` - 5 tests)**:
   - Multi-way join grain preservation, exact financial measure formulas, window rankings, and FOB Destination revenue recognition.
4. **Data Warehouse ETL & Storage (`tests/test_warehouse_loader.py` - 8 tests)**:
   - Surrogate key resolution, dimension row counts, foreign key integrity, idempotence on rerun, and transactional rollback.
5. **Analytical SQL Engine (`tests/test_analytical_sql.py` - 7 tests)**:
   - Execution of SQL scripts 01–07 against relational warehouse, CTE verification, window functions, and financial conservation.
6. **Airflow Orchestration (`tests/test_airflow_dag.py` - 8 tests)**:
   - DAG structure, default arguments, 9-task dependency chain, task execution, and error propagation.
7. **Customer Segmentation ML (`tests/test_rfm_segmentation.py` - 8 tests)**:
   - Purchase eligibility, log1p transformation, StandardScaler Z-scores, reproducibility, 100% customer coverage, and revenue conservation.
8. **Power BI Semantic Model (`tests/test_powerbi_semantic_model.py` - 5 tests)**:
   - Demand vs Recognized measures, 5-bucket conservation, atomic grain preservation across multi-way dimension joins, and filter isolation.

