# E-Commerce Sales and Customer Analytics

[![Python 3.12](https://img.shields.io/badge/Python-3.12-blue.svg)](https://www.python.org/)
[![Apache Spark](https://img.shields.io/badge/Apache_Spark-PySpark_4.2-E25A1C.svg)](https://spark.apache.org/)
[![MySQL](https://img.shields.io/badge/MySQL-8.0_Data_Warehouse-4479A1.svg)](https://www.mysql.com/)
[![SQLite](https://img.shields.io/badge/SQLite-Standalone_DWH-003B57.svg)](https://www.sqlite.org/)
[![SQLAlchemy](https://img.shields.io/badge/SQLAlchemy-2.0+-D71F00.svg)](https://www.sqlalchemy.org/)
[![Apache Airflow](https://img.shields.io/badge/Apache_Airflow-2.7+-017CEE.svg)](https://airflow.apache.org/)
[![Scikit-Learn](https://img.shields.io/badge/scikit--learn-RFM_K--Means-F7931E.svg)](https://scikit-learn.org/)
[![Power BI](https://img.shields.io/badge/Power_BI-Executive_Dashboards-F2C811.svg)](https://powerbi.microsoft.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT)

A modular, production-grade **Data Engineering and Advanced Analytics platform** built to ingest, validate, clean, warehouse, model, and visualize multi-entity e-commerce transactional data.

Designed both as an **advanced academic capstone** and as a **portfolio-grade project for Data Engineering and Analytics viva/interviews**.

---

## 📑 Table of Contents

1. [Project Overview](#-project-overview)
2. [Phase Completion Status](#-project-phase-completion-status)
3. [Technologies & Tools](#-technologies--tools)
4. [System Architecture](#-system-architecture)
5. [Dimensional Star Schema Architecture](#-dimensional-star-schema-architecture)
6. [Accounting Principles & Revenue Recognition (FOB Destination)](#-accounting-principles--revenue-recognition-fob-destination)
7. [Repository Structure](#-repository-structure)
8. [Prerequisites & Environment Setup](#-prerequisites--environment-setup)
9. [Step-by-Step Execution Guide](#-step-by-step-execution-guide)
10. [Apache Airflow Orchestration Architecture](#-apache-airflow-orchestration-architecture)
11. [Customer Segmentation with RFM & Machine Learning (K-Means)](#-customer-segmentation-with-rfm--machine-learning-k-means)
12. [Power BI Semantic Model & Executive Dashboards](#-power-bi-semantic-model--executive-dashboards)
13. [Analytical SQL Reference](#-analytical-sql-scripts-reference-sqlanalytics)
14. [Automated Test Suite & Verification](#-automated-test-suite--verification)
15. [Viva & Technical Interview Defense Highlights](#-viva--technical-interview-defense-highlights)

---

## 🌟 Project Overview

Modern e-commerce enterprises generate complex transactional data across customer profiles, product catalogs, order headers, and line-item fulfillments. Turning raw transactional logs into trusted executive reporting requires solving fundamental data engineering challenges:
- **Data Quality & Quarantine**: Identifying malformed keys, invalid date sequences, out-of-range quantities, negative unit costs, and erroneous discounts without halting upstream ingestion.
- **Distributed Big Data Processing**: Efficiently joining multi-table datasets, computing running time-series windows, and tracking customer spend using Apache Spark.
- **Relational Dimensional Modeling**: Designing a Kimball Star Schema with surrogate keys, conformed dimensions, and an atomic line-item fact table.
- **Strict Revenue Accounting (FOB Destination)**: Distinguishing commercial **Gross/Net Demand** from realized **Recognized Financials** (Delivered orders), isolating in-transit inventory, and auditing operational attrition (cancellations and returns).
- **Workflow Automation**: Orchestrating the end-to-end multi-tier pipeline using an idempotent, fail-fast Apache Airflow DAG.
- **Machine Learning Analytics**: Engineering Recency, Frequency, Monetary (RFM) behavioral features, performing log-normal variance stabilization, and training an unsupervised K-Means model to segment customers into actionable commercial cohorts.
- **Executive BI Modeling**: Delivering a clean Power BI semantic model with 13 verified DAX measures, star schema relationships, and interactive visualization dashboards.

---

## 🚦 Project Phase Completion Status

| Phase | Milestone | Scope / Deliverables | Verification Status |
| :---: | :--- | :--- | :---: |
| **Phase 1** | **Architecture & Data Engine** | Project scaffolding, configuration, 11,867 raw records across 4 entities | ✅ Verified & Tested |
| **Phase 2** | **Ingestion & Data Quality Gate** | Validation engine, quarantine router (559 defective records routed), 100% record conservation | ✅ Verified & Tested |
| **Phase 3** | **PySpark Processing Engine** | Multi-way joins, window functions, FOB Destination accounting ($405,484.47 recognized) | ✅ Verified & Tested |
| **Phase 4** | **Data Warehouse & Analytical SQL** | Kimball Star Schema (5 dimensions + 1 fact table), 7 analytical SQL scripts, zero unmapped keys | ✅ Verified & Tested |
| **Phase 5** | **Apache Airflow Orchestration** | 9-task fail-fast DAG, daily schedule, retries/timeouts, execution audit manifest | ✅ Verified & Tested |
| **Phase 6** | **Customer Segmentation (RFM & K-Means)** | Feature scaling (log1p + StandardScaler), k=4 clustering (Silhouette: 0.4873), 1,000 customers profiled | ✅ Verified & Tested |
| **Phase 7** | **Power BI Semantic Modeling & Dashboards** | 13 DAX measures, Star Schema extracts, demand vs recognized visuals, HTML dashboard | ✅ Verified & Tested |

---

## 🛠️ Technologies & Tools

| Technology | Version / Specification | Role in Architecture |
| :--- | :--- | :--- |
| **Python** | `3.10+` (Tested on `3.12`) | Core programming language across all pipeline modules |
| **Apache Spark / PySpark** | `3.4+` / `4.2` | Distributed DataFrame joins, window functions, and curated analytical datasets |
| **MySQL** | `8.0` | Primary relational Enterprise Data Warehouse (Dockerized & local) |
| **SQLite** | `3.x` | Standalone zero-configuration local Data Warehouse option for testing/offline demos |
| **SQLAlchemy** | `2.0+` | Relational ORM & database connectivity layer for schema initialization and ETL staging |
| **Apache Airflow** | `2.7+` | Production DAG workflow orchestrator with SLA management and fail-fast dependencies |
| **scikit-learn** | `1.3+` | RFM feature preprocessing, StandardScaler normalization, and K-Means clustering |
| **Pandas & NumPy** | `2.0+` / `1.24+` | Tabular data manipulation, vector math, and variance-stabilizing transforms |
| **Matplotlib & Seaborn** | `3.7+` / `0.12+` | Publication-quality chart generation for model evaluation and reporting |
| **Power BI Desktop & DAX** | `2024+` | Executive business intelligence, star-schema semantic modeling, and DAX KPI measures |
| **Docker & Docker Compose** | Compose Spec `3.8` | Multi-container development environment for MySQL 8.0 and Airflow Webserver |
| **PyTest / Unittest** | `7.4+` | Regression testing framework (58 automated unit and integration tests) |

---

## 🏛️ System Architecture

```
[Operational Sources] ──► [Ingestion & Lineage Audit] ──► [Quality Gatekeeper & Reject Router]
                                                                        │
        ┌───────────────────────────────────────────────────────────────┘
        ▼
[Data Cleaning & Normalization] ──► [PySpark Distributed Engine] ──► [Relational Star Schema DWH]
                                                                                │
        ┌───────────────────────────────────────────────────────────────────────┘
        ▼
[Advanced Analytical SQL Views] ──► [RFM + K-Means Clustering] ──► [Power BI Semantic Dashboard]
                                                                                ▲
                                                                                │
═══════════════════════════════ [Apache Airflow Orchestration] ═════════════════
```

---

## 🗄️ Dimensional Star Schema Architecture

The data warehouse implements a **Kimball Star Schema** centered on customer order line items:

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
       │     city, state, etc.  │  │   dim_date   │ │     payment_method     │
       └────────────────────────┘  ├──────────────┤ └────────────────────────┘
                                   │ PK date_key  │
                                   │    full_date │
                                   └──────────────┘
```

### Table Definitions & Summary

| Table | Type | Grain | Primary Key | Row Count | Purpose |
| :--- | :--- | :--- | :--- | :---: | :--- |
| `dim_date` | Conformed Dimension | 1 Day | `date_key` (YYYYMMDD) | 1,096 | Calendar hierarchy (2024–2026), quarter, weekend flags |
| `dim_location` | Dimension | 1 Metro Area | `location_key` (INT) | 15 | Customer geographic dimensions (City, State, Country, Postal) |
| `dim_customer` | Dimension | 1 Customer | `customer_key` (INT) | 1,000 | Customer profile, signup date, nullable phone, RFM segment |
| `dim_product` | Dimension | 1 SKU | `product_key` (INT) | 30 | Merchandising catalog, category, sub-category, unit costs |
| `dim_payment` | Dimension | 1 Payment Method | `payment_key` (INT) | 5 | Transaction checkout methods (UPI, Net Banking, Card, PayPal) |
| `fact_sales` | Atomic Fact | 1 Order Line Item | `sales_key` (BIGINT) | 6,379 | Line-item measures, volume, price, discounts, recognized revenue |

---

## 📈 Accounting Principles & Revenue Recognition (FOB Destination)

To maintain absolute alignment between data engineering, financial reporting, and executive BI, the platform enforces strict mathematical separation between **Gross Demand** and **Realized Financials**:

$$\text{Gross Demand} = \text{Total order items placed across all 5 statuses} = \mathbf{\$978,097.82} \text{ (Net Demand)}$$

$$\text{Recognized Revenue} = \text{Line items with status } \mathbf{Delivered} = \mathbf{\$405,484.47} \text{ (100\% match across PySpark, SQL, BI)}$$

### Financial Measure Conservation

$$\text{Recognized Revenue} + \text{In-Transit} + \text{Pipeline} + \text{Returned Loss} + \text{Cancelled Loss} \equiv \text{Net Demand}$$
$$\$405,484.47 + \$138,599.59 + \$146,949.52 + \$128,944.83 + \$158,119.41 = \mathbf{\$978,097.82}$$

- **Zero Double-Counting**: Joining line items with dimensions preserves the atomic grain of 6,379 items.
- **Header vs. Line-item Reconciliation**: 80 orders had all of their line items quarantined during Phase 2 validation (due to item-level defects). Therefore, the 6,379 line items in `fact_sales` represent exactly 3,819 distinct orders ($3,899 - 80 = 3,819$).

---

## 📂 Repository Structure

```
ecommerce-data-engineering/
├── airflow/
│   └── dags/
│       ├── airflow_compat.py          # Cross-platform Airflow compatibility shim
│       └── ecommerce_analytics_dag.py # 9-task fail-fast production Airflow DAG
├── config/
│   ├── config.yaml                    # Global configuration parameters
│   ├── logging_config.py              # Structured logging utility
│   └── pipeline_config.py             # Strongly-typed environment configuration
├── data/
│   ├── raw/                           # Immutable benchmark raw CSV datasets (4 entities)
│   ├── processed/                     # Validated & cleaned staging datasets
│   │   └── analytics/                 # Curated Parquet & CSV analytical datasets
│   ├── rejected/                      # Quarantined anomaly records & failure audit
│   ├── warehouse/                     # Relational Data Warehouse storage (.db / sqlite)
│   └── powerbi/                       # Dimensional extracts & BI semantic model feeds
├── docker/
│   └── .gitkeep                       # Docker service assets
├── docs/
│   ├── data_dictionary.md             # Granular column-level data dictionary
│   ├── star_schema.md                 # Detailed dimensional modeling specification
│   └── viva_defense_guide.md          # 11-section in-depth technical viva defense guide
├── logs/
│   └── .gitkeep                       # Runtime logs directory (pipeline.log ignored)
├── notebooks/
│   └── .gitkeep                       # Interactive exploratory Jupyter notebooks
├── powerbi/
│   ├── dax_measures.dax               # 13 verified DAX business measures
│   ├── interactive_dashboard.html     # Interactive HTML semantic dashboard viewer
│   ├── POWERBI_GUIDE.md               # Step-by-step Power BI Desktop setup guide
│   └── semantic_model.json            # Power BI semantic model metadata & relationships
├── reports/
│   ├── model_evaluation_report.md     # K-Means clustering evaluation summary
│   ├── model_evaluation_report.json   # Machine-readable cluster metrics
│   └── visualizations/                # Publication-quality PNG charts
│       ├── 01_elbow_silhouette_evaluation.png
│       ├── 02_rfm_scatter_distribution.png
│       ├── 03_segment_revenue_vs_customers.png
│       ├── 04_segment_rfm_profiles.png
│       ├── 05_revenue_by_order_status.png
│       └── 06_monthly_demand_vs_recognized.png
├── sql/
│   ├── schema/                        # Warehouse DDL (database, dimensions, fact table)
│   │   ├── 00_init_warehouse.sql
│   │   ├── 01_create_database.sql
│   │   ├── 02_create_dimensions.sql
│   │   └── 03_create_fact_sales.sql
│   └── analytics/                     # 7 analytical business SQL queries (window functions, CTEs)
│       ├── 01_monthly_revenue_and_profit.sql
│       ├── 02_mom_revenue_growth.sql
│       ├── 03_product_category_rankings.sql
│       ├── 04_customer_ltv_repeat_purchases.sql
│       ├── 05_rfm_segmentation_analysis.sql
│       ├── 06_regional_performance.sql
│       └── 07_payment_and_order_status.sql
├── src/
│   ├── ingestion/                     # Ingestion engine & synthetic data generator
│   │   ├── generate_raw_data.py
│   │   └── ingestion_engine.py
│   ├── validation/                    # Data quality gatekeeper & schema validation
│   │   └── data_validator.py
│   ├── transformation/                # Data cleaning & normalization engine
│   │   └── cleaner.py
│   ├── spark/                         # PySpark distributed processing & window functions
│   │   ├── spark_session.py
│   │   ├── transformations.py
│   │   └── run_spark_pipeline.py
│   ├── warehouse/                     # Kimball star-schema loader & SQL executor
│   │   ├── warehouse_loader.py
│   │   └── run_analytics_sql.py
│   ├── analytics/                     # ML segmentation & Power BI asset generator
│   │   ├── rfm_segmentation.py
│   │   └── generate_powerbi_assets.py
│   └── pipeline_runner.py             # CLI master orchestrator (--phase 1..7 / --phase all)
├── tests/                             # Comprehensive automated test suite (58 regression tests)
│   ├── run_tests.py                   # Master test runner
│   ├── test_validation.py             # Phase 2 validation unit tests
│   ├── test_cleaning.py               # Phase 2 cleaning unit tests
│   ├── test_spark_transformations.py  # Phase 3 PySpark integration tests
│   ├── test_warehouse_loader.py       # Phase 4 warehouse loader tests
│   ├── test_analytical_sql.py         # Phase 4 analytical SQL query tests
│   ├── test_airflow_dag.py            # Phase 5 Airflow DAG structure & execution tests
│   ├── test_rfm_segmentation.py       # Phase 6 RFM K-Means clustering tests
│   └── test_powerbi_semantic_model.py # Phase 7 semantic model & DAX tests
├── .env.example                       # Environment variables configuration template
├── .gitignore                         # Git exclusion rules
├── ARCHITECTURE.md                    # System architecture specification
├── docker-compose.yml                 # Multi-container orchestration (MySQL 8 + Airflow)
├── PROJECT_PLAN.md                    # Roadmap & phase tracking
├── README.md                          # Comprehensive project documentation
└── requirements.txt                   # Pinned Python package dependencies
```

---

## 💻 Prerequisites & Environment Setup

### 1. Prerequisites
- **Python**: Version `3.10` or higher (tested on Python `3.12`).
- **Java**: Java Development Kit (`JDK 8`, `JDK 11`, or `JDK 17`) installed with `JAVA_HOME` environment variable configured (required by Apache Spark / PySpark).
- **Git**: Installed for version control.
- **Docker & Docker Compose** *(Optional)*: Required only if running containerized MySQL 8.0 and Airflow Webserver.

### 2. Clone Repository & Setup Virtual Environment
```bash
# Clone the repository
git clone https://github.com/<your-username>/ecommerce-data-pipeline.git
cd ecommerce-data-pipeline

# Create virtual environment
python -m venv .venv

# Activate virtual environment
# On Windows (PowerShell):
.\.venv\Scripts\Activate.ps1
# On Linux / macOS:
source .venv/bin/activate

# Upgrade pip and install all required dependencies
pip install --upgrade pip
pip install -r requirements.txt
```

### 3. Configure Environment Variables
Copy the template configuration file:
```bash
# Windows:
copy .env.example .env
# Linux / macOS:
cp .env.example .env
```
The pipeline defaults to standalone local execution (using SQLite and local PySpark) requiring **zero configuration**. To connect to a live MySQL 8.0 instance, update `MYSQL_HOST`, `MYSQL_USER`, and `MYSQL_PASSWORD` in `.env`.

---

## 🚀 Step-by-Step Execution Guide

### Option A: Run the Complete End-to-End Pipeline in One Command
```bash
python src/pipeline_runner.py --phase all
```
This executes the entire pipeline sequentially:
1. Validates and ingests raw datasets.
2. Cleans records and routes anomalies to `data/rejected/`.
3. Runs PySpark transformations, window functions, and financial accounting.
4. Loads the Kimball Star Schema into the Data Warehouse.
5. Executes analytical SQL verification queries.
6. Trains K-Means ($k=4$) RFM customer segmentation and updates customer segments.
7. Exports Power BI extracts and generates publication charts in `reports/visualizations/`.

### Option B: Run Specific Pipeline Phases
```bash
# Phase 2: Ingestion, Validation & Cleaning
python src/pipeline_runner.py --phase 2

# Phase 3: PySpark Distributed Transformations
python src/pipeline_runner.py --phase 3

# Phase 4: Data Warehouse Load & SQL Analytics
python src/pipeline_runner.py --phase 4

# Phase 5: Airflow Orchestration Execution
python src/pipeline_runner.py --phase 5

# Phase 6: Customer RFM Segmentation & ML Clustering
python src/pipeline_runner.py --phase 6

# Phase 7: Power BI Semantic Assets & Visualizations
python src/analytics/generate_powerbi_assets.py
```

### Option C: Standalone Warehouse & SQL Execution
```bash
# Load data into standalone SQLite warehouse (offline mode)
python src/warehouse/warehouse_loader.py --sqlite --reset

# Load data into live MySQL warehouse (requires MySQL running)
python src/warehouse/warehouse_loader.py --reset

# Run all 7 analytical SQL queries with formatted CLI output
python src/warehouse/run_analytics_sql.py --sqlite
```

---

## 🌪️ Apache Airflow Orchestration Architecture

The pipeline is orchestrated using **Apache Airflow 2.7+** via the master DAG `ecommerce_analytics_pipeline` (`airflow/dags/ecommerce_analytics_dag.py`). The workflow enforces a linear, fail-fast sequence ensuring data integrity at every tier:

```
[start_pipeline] (EmptyOperator)
       │
       ▼
[validate_raw_data_integrity] (PythonOperator)
       │  • Checks SHA-256 file hashes & verifies 4 mandatory raw entities
       ▼
[data_quality_and_cleaning] (PythonOperator)
       │  • Validates PKs, FKs, types & business constraints
       │  • Routes 559 defect records to quarantine
       │  • Enforces conservation: 11,867 raw = 11,308 valid + 559 quarantined
       ▼
[pyspark_transformations] (PythonOperator)
       │  • Executes distributed joins on 6,379 valid order line items
       │  • Enforces FOB Destination revenue recognition ($405,484.47 recognized)
       │  • Computes window-based running metrics & customer RFM features
       ▼
[load_mysql_warehouse] (PythonOperator)
       │  • Idempotent dimensional load: resolves natural keys to surrogate keys
       │  • Populates 5 dimensions + 1 atomic line-item fact table (fact_sales)
       ▼
[verify_warehouse_reconciliation] (PythonOperator)
       │  • Audits referential integrity (0 orphan/null foreign keys)
       │  • Audits financial conservation: Net Demand = $978,097.82
       ▼
[customer_segmentation_rfm] (PythonOperator)
       │  • Evaluates k=2..6, fits k=4 K-Means (Silhouette: 0.4873, CH: 1,874.08)
       │  • Profiles 1,000 customers into 4 commercial cohorts
       │  • Updates dim_customer.rfm_segment in the Data Warehouse
       ▼
[generate_pipeline_summary] (PythonOperator)
       │  • Emits operational execution manifest to logs/pipeline_execution_summary.json
       ▼
[end_pipeline] (EmptyOperator)
```

### DAG SLA & Operational Policies
- **Schedule Interval**: `@daily` (runs daily at midnight UTC)
- **Concurrency**: `max_active_runs = 1` (prevents race conditions and duplicate warehouse loads)
- **Catchup**: `False` (avoids unexpected historical backfill storms)
- **Retries**: 2 retries with a 3-minute backoff delay (`retry_delay = timedelta(minutes=3)`)
- **Execution Timeout**: 30 minutes SLA per task (`execution_timeout = timedelta(minutes=30)`)
- **Fail-Fast Semantics**: If any task encounters a data corruption or conservation breach, downstream execution halts immediately.

### Running with Docker Compose
```bash
# Start MySQL 8 and Airflow standalone containers
docker compose up -d

# Access Airflow Web UI: http://localhost:8080 (Username: admin | Password: admin)
```

---

## 🤖 Customer Segmentation with RFM & Machine Learning (K-Means)

Phase 6 implements advanced customer segmentation using **scikit-learn K-Means clustering** over behavioral RFM features:

### 1. RFM Feature Engineering & Revenue Recognition
- **Recency ($R$)**: Days since customer's most recent qualifying purchase relative to snapshot date (`2026-09-30`). For non-purchasers, falls back to tenure from `signup_date`.
- **Frequency ($F$)**: Count of distinct qualifying purchases (`order_status == 'Delivered'`).
- **Monetary ($M$)**: Total recognized revenue from delivered orders ($405,484.47 total sum).
- **Fulfillment Attrition Rules**: Under FOB Destination accounting, orders that are `Cancelled` or `Returned` are strictly excluded from customer spend. In-transit orders (`Shipped`, `Processing`) are tracked in operational diagnostic features (`in_transit_spend`) but excluded from realized monetary value until delivery occurs.

### 2. Feature Preprocessing & Variance Stabilization
- **Log Transformation**: Raw RFM features exhibit substantial right-skewness (monetary skew = 1.38, frequency skew = 0.71). We apply a variance-stabilizing transform $x' = \log(1 + x)$ (`np.log1p`), dampening extreme high-value outliers without truncating customer data.
- **Z-Score Normalization**: Features are standardized using `StandardScaler` to ensure zero mean and unit variance ($\mu = 0, \sigma = 1$), preventing high-magnitude monetary figures from dominating distance metrics over order counts.

### 3. Model Selection & Cluster Evaluation ($k=2$ to $6$)

| Clusters ($k$) | Inertia (SSE) | Silhouette Score | Calinski-Harabasz Score | Davies-Bouldin Index | Evaluation |
| :---: | :---: | :---: | :---: | :---: | :--- |
| $k=2$ | 1,219.80 | 0.6103 | 1,456.50 | 0.4143 | Trivial bimodal split (zero-spend vs. non-zero) |
| $k=3$ | 658.43 | 0.5110 | 1,772.80 | 0.7585 | Under-segmented (blends active and lapsed accounts) |
| **$k=4$ (Selected)** | **451.48** | **0.4873** | **1,874.08 (Peak)** | **0.7831** | **Global CH Peak, Defensible Elbow, High Interpretability** |
| $k=5$ | 371.00 | 0.4594 | 1,762.99 | 0.8666 | Diminishing returns; over-fragments core shoppers |
| $k=6$ | 306.44 | 0.4733 | 1,747.54 | 0.8294 | Excessive fragmentation |

### 4. Customer Segment Behavioral Profiles & Commercial Strategies

| Cluster ID | Segment Name | Customer Count | Population Share (%) | Mean Recency | Mean Frequency | Mean Spend | Total Recognized Spend | Revenue Share (%) | Commercial Strategy |
| :---: | :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :--- |
| **0** | **Champions** | 296 | 29.6% | 247.8 days | 2.82 | $753.56 | $223,053.37 | **55.0%** | VIP loyalty programs, early access, retention |
| **1** | **Recent Active Shoppers** | 132 | 13.2% | 40.0 days | 2.42 | $603.53 | $79,665.33 | **19.6%** | Post-purchase cross-sell, nurture campaigns |
| **2** | **At-Risk / Lapsed Buyers** | 358 | 35.8% | 572.7 days | 1.23 | $287.06 | $102,765.77 | **25.3%** | Win-back discount incentives, churn surveys |
| **3** | **Unconverted / Inactive** | 214 | 21.4% | 915.4 days | 0.00 | $0.00 | $0.00 | **0.0%** | Onboarding welcome series, reactivation promos |
| **Total** | | **1,000** | **100.0%** | | | | **$405,484.47** | **100.0%** | |

### 5. Publication Visualizations (`reports/visualizations/`)
- `01_elbow_silhouette_evaluation.png`: Dual-axis plot of Inertia and Silhouette scores confirming $k=4$ elbow and CH peak.
- `02_rfm_scatter_distribution.png`: 2D scatter plots of Recency vs. Monetary and Frequency vs. Monetary color-coded by segment.
- `03_segment_revenue_vs_customers.png`: Pareto chart illustrating customer share vs. realized revenue contribution.
- `04_segment_rfm_profiles.png`: Multi-metric bar comparison illustrating distinct Recency, Frequency, and Monetary signatures.
- `05_revenue_by_order_status.png`: Net demand distribution by order status ($978k total demand vs $405k delivered).
- `06_monthly_demand_vs_recognized.png`: Monthly time-series comparison of Net Demand vs Recognized Revenue.

---

## 📊 Power BI Semantic Model & Executive Dashboards

The analytics layer includes a ready-to-use Power BI semantic model specification and DAX measures library:
- **Guide**: Step-by-step setup guide in [`powerbi/POWERBI_GUIDE.md`](powerbi/POWERBI_GUIDE.md).
- **DAX Measures**: 13 business-verified DAX measures in [`powerbi/dax_measures.dax`](powerbi/dax_measures.dax).
- **Interactive Dashboard**: Zero-install standalone browser dashboard in [`powerbi/interactive_dashboard.html`](powerbi/interactive_dashboard.html).
- **Semantic Model Schema**: Machine-readable star schema relations in [`powerbi/semantic_model.json`](powerbi/semantic_model.json).

---

## 📊 Analytical SQL Scripts Reference (`sql/analytics/`)

1. **`01_monthly_revenue_and_profit.sql`**: Monthly top-line demand, discounts, recognized revenue, COGS, and profit margin %.
2. **`02_mom_revenue_growth.sql`**: Time-series trajectory using `LAG()` and cumulative running revenue totals.
3. **`03_product_category_rankings.sql`**: Dense ranking (`DENSE_RANK()`) of categories and top 10 SKUs by realized profit.
4. **`04_customer_ltv_repeat_purchases.sql`**: Customer lifetime value (CLV) and repeat-purchase cohort distribution.
5. **`05_rfm_segmentation_analysis.sql`**: Star-schema RFM quartile scoring (`NTILE(4)`) and segment distribution.
6. **`06_regional_performance.sql`**: Geographic revenue and profit breakdown by State and City.
7. **`07_payment_and_order_status.sql`**: Payment channel market share and fulfillment attrition funnel analysis.

---

## 🧪 Automated Test Suite & Verification

The repository contains an enterprise-grade regression test suite verifying all 7 project phases:

```bash
python tests/run_tests.py
```

### Test Suite Execution Summary:
- **Total Tests**: `58`
- **Failures**: `0`
- **Errors**: `0`
- **Status**: `100% Passing`

```
  test_validation.py              [10/10 PASS]  Primary/foreign keys, date constraints, quantities, discounts
  test_cleaning.py                [ 7/7  PASS]  Deduplication, normalization, phone null handling
  test_spark_transformations.py   [ 5/5  PASS]  Multi-way joins, revenue recognition, running windows, RFM
  test_warehouse_loader.py        [ 8/8  PASS]  Surrogate keys, referential integrity, idempotence, rollback
  test_analytical_sql.py          [ 7/7  PASS]  Analytical SQL queries, CTEs, window functions
  test_airflow_dag.py             [ 8/8  PASS]  DAG imports, default args, 9-task dependency chain, fail-fast
  test_rfm_segmentation.py        [ 8/8  PASS]  RFM eligibility, scaling, reproducibility, coverage, profiles
  test_powerbi_semantic_model.py  [ 5/5  PASS]  Demand vs Recognized measures, 5-bucket conservation, grain
```

---

## 🎓 Viva & Technical Interview Defense Highlights

> 📘 **Full Comprehensive Guide**: See the complete 11-section technical viva defense specification in [`docs/viva_defense_guide.md`](docs/viva_defense_guide.md) covering end-to-end architecture trade-offs, dimensional modeling proofs, PySpark execution strategies, FOB Destination GAAP revenue recognition, and scenario troubleshooting exercises.

### Q1: Why Kimball Star Schema over 3NF for analytical reporting?
> **Answer**: 3NF is engineered for OLTP transaction integrity, requiring deep recursive joins that degrade analytical aggregation queries. A Kimball Star Schema denormalizes conformed dimensions around an atomic line-item fact table. This minimizes join depth to a single hop, maximizes B-Tree index efficiency, and allows downstream BI tools (Power BI) to slice measures across time, product, customer, and geography without complex SQL.

### Q2: How do you prevent double-counting order-level shipping cost or totals across line items?
> **Answer**: In our dimensional model, `fact_sales` is defined at the atomic **order-line-item grain**. Financial measures like `gross_revenue`, `discount_amount`, and `cogs` are calculated at the line-item level. For order-level measures like `shipping_cost`, we avoid naive `SUM(shipping_cost)` by using degenerate dimension grouping (`COUNT(DISTINCT order_id)`) or allocating costs proportionally.

### Q3: How is revenue recognized when orders are shipped, processing, cancelled, or returned?
> **Answer**: In compliance with enterprise revenue recognition standards (IFRS 15 / US GAAP FOB Destination), revenue is recognized **only when control transfers to the customer upon delivery (`Delivered`)**. Orders that are `Shipped` or `Processing` are segregated as in-transit or pipeline inventory. Orders that are `Cancelled` or `Returned` are isolated into operational loss measures. This guarantees that financial reports reflect actual realized earnings while operational dashboards track fulfillment attrition.

### Q4: How is surrogate key resolution handled during warehouse ETL?
> **Answer**: Natural keys from operational sources (e.g. `customer_id`, `product_id`) are never used as relational foreign keys in the fact table. The ETL loader loads dimension tables first, caching in-memory mappings between natural keys and auto-increment surrogate keys (`customer_key`, `product_key`, `location_key`, `payment_key`). During fact table staging, natural keys are mapped to surrogate keys, validating that zero unmapped or orphan foreign keys enter the data warehouse.

### Q5: Why use Apache Airflow instead of traditional cron scheduling?
> **Answer**: Traditional cron lacks dependency management, state persistence, centralized logs, failure retries, and backfill controls. If a 2:00 AM ingestion job fails under cron, a 2:30 AM warehouse job still runs blindly on corrupt or missing data. Airflow provides an explicit Directed Acyclic Graph (DAG) with fail-fast upstream short-circuiting, parameterizable retries with exponential backoffs, task timeouts, and visual execution tracking.

### Q6: How does the pipeline guarantee idempotence during DAG failure and retries?
> **Answer**: Every pipeline phase is built to be idempotent:
> 1. Raw validation and cleaning write deterministic target files overwriting previous staging output.
> 2. PySpark transformations overwrite target Parquet partitions deterministically.
> 3. The data warehouse loader uses natural business key uniqueness (`order_item_id`) and transactional UPSERT logic. Rerunning a task or an entire DAG multiple times results in the exact same warehouse row counts and financial totals without duplicate insertion.

### Q7: Why use K-Means unsupervised clustering rather than traditional manual RFM quartile scoring?
> **Answer**: Manual RFM scoring (e.g., binning R, F, M into 1–4 quartiles) forces artificial uniform bucket distributions, ignores natural multidimensional correlations between frequency and spend, and creates $4 \times 4 \times 4 = 64$ fragmented micro-segments that marketers cannot operationalize. K-Means discovers true geometrical clustering densities in 3D feature space, converging on mathematically optimal, highly separated centroids. In our dataset, $k=4$ automatically separates zero-spend accounts from core high-margin Champions without manual boundary guesswork.

### Q8: Why apply a log1p transform prior to scaling RFM features for K-Means?
> **Answer**: K-Means relies on Euclidean distance, which assumes spherical, isotropic clusters with roughly Gaussian feature distributions. E-commerce monetary spend and order frequency exhibit heavy right-skewed power-law distributions. Without transformation, extreme high spenders stretch the Euclidean space and distort centroid positions. Applying $x' = \log(1 + x)$ compresses the long right tail, stabilizes feature variances, and allows `StandardScaler` to produce balanced, robust Z-scores.

### Q9: How does the segmentation model preserve financial conservation with the data warehouse?
> **Answer**: The segmentation pipeline ingests curated customer metrics generated directly from the PySpark transformation layer, which enforces FOB Destination revenue recognition. The sum of customer monetary spend across all four clusters equals **$405,484.47**—matching the data warehouse's `fact_sales` recognized revenue to the cent. Furthermore, segment assignments are written back to `dim_customer.rfm_segment`, allowing downstream SQL queries and Power BI dashboards to slice verified financial measures by machine-learned clusters with zero discrepancies.

### Q10: How do you differentiate Demand Revenue from Recognized Revenue in the semantic model?
> **Answer**: We explicitly separate **Demand Revenue** (commercial gross/net demand representing checkout commitments across all order statuses: Delivered, Shipped, Processing, Cancelled, Returned) from **Recognized Revenue** (earned financial revenue recognized strictly upon delivery under FOB Destination rules). In our Star Schema and Power BI DAX layer, this is governed by explicit measures:
> - $\text{Net Demand Revenue} = \$978,097.82$ across 6,379 line items
> - $\text{Recognized Revenue} = \$405,484.47$ (41.46% realization rate)
> - $\text{In-Transit / Pipeline Demand} = \$285,549.11$ (Shipped + Processing)
> - $\text{Cancelled / Returned Demand Loss} = \$287,064.24$ (operational attrition)
> Because measures are aggregated directly from the atomic line-item grain (`order_item_id`) with 1-to-many dimension joins, there is zero double-counting, and the mathematical identity is conserved to the cent: $\text{Recognized} + \text{In-Transit} + \text{Pipeline} + \text{Cancelled} + \text{Returned} \equiv \text{Net Demand Revenue}$.

---

## 📜 License

This project is licensed under the MIT License - see the LICENSE file for details.
