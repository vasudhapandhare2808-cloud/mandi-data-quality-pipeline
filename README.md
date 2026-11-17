# Mandi Data Quality Pipeline

A Python data engineering project that processes historical Indian agricultural mandi price data, applies validation and transformation rules, loads a PostgreSQL warehouse, stores Parquet artifacts in Amazon S3, and serves market analytics through a Streamlit dashboard.

**Live dashboard:** https://mandi-market-intelligence.streamlit.app/

> **Data note:** This project currently processes the 2025 dataset from the India Mandi Daily Commodity Prices collection. The dataset is historical and derived from AGMARKNET data. It is not a live market-price feed.

## Project overview

The pipeline is designed to make mandi price data easier to validate, investigate, store, and analyze. It separates raw ingestion, cleaned records, rejected records, analytical warehouse tables, and operational monitoring data.

**Source dataset:** [India Mandi Daily Commodity Prices (2001–2026) on Kaggle](https://www.kaggle.com/datasets/khandelwalmanas/daily-commodity-prices-india)

## Architecture

```text
Kaggle CSV source (2025)
        |
        v
Python extraction
        |
        v
Bronze Parquet artifact ----------------------> Amazon S3: bronze/
        |
        v
Transformation and validation
        |
        +-------------------------------+
        |                               |
        v                               v
Accepted Silver Parquet           Rejected records Parquet
        |                               |
        v                               v
Amazon S3: silver/accepted/        Amazon S3: silver/rejected/
        |
        v
PostgreSQL warehouse
  ├── bronze: ingested source records
  ├── silver: accepted and rejected records
  ├── gold: analytical dimensions and fact table
  └── ops: run metadata, quality checks, reconciliation
        |
        v
Dashboard export utility
        |
        v
Amazon S3: dashboard/ (Parquet exports)
        |
        v
Streamlit dashboard
        |
        v
DuckDB queries over downloaded Parquet exports
```

### Dashboard data path

The Streamlit dashboard does **not** query PostgreSQL directly. The dashboard reads eight Parquet exports from the S3 `dashboard/` prefix, downloads them into `data/dashboard_exports/`, and creates DuckDB views over those files.

The exports currently used by the dashboard are:

- `gold_dim_date.parquet`
- `gold_dim_market.parquet`
- `gold_dim_commodity.parquet`
- `gold_fact_market_prices.parquet`
- `ops_dq_results.parquet`
- `ops_pipeline_runs.parquet`
- `ops_reconciliation.parquet`
- `silver_rejected_records.parquet`

This distinction matters when refreshing data: update the warehouse, regenerate and upload the dashboard exports, then use **Refresh dashboard** to reload the S3 exports.

## Technology stack

| Area | Tools |
|---|---|
| Language | Python |
| Data processing | pandas, PyArrow |
| File format | Parquet |
| Warehouse | PostgreSQL |
| Database access | SQLAlchemy, psycopg2 |
| Object storage | Amazon S3 |
| AWS integration | boto3, AWS CLI |
| Dashboard | Streamlit, Plotly |
| Dashboard query engine | DuckDB |
| Testing | pytest |
| Configuration | python-dotenv |

## Data pipeline

### 1. Extraction: CSV to Bronze

The extraction stage reads the year-specific CSV in chunks and writes a Bronze Parquet artifact. Chunked processing limits memory use when working with the large source file.

Example output:

```text
data/bronze/mandi_bronze_<run_id>.parquet
```

### 2. Transformation and validation: Bronze to Silver

The transformation stage standardizes fields, parses dates and numeric values, validates price relationships, and separates accepted records from rejected records. Duplicate records are also detected across processing batches.

Example outputs:

```text
data/silver/mandi_silver_<run_id>.parquet
data/rejected/mandi_rejected_<run_id>.parquet
```

Rejected records retain source values and rejection reasons for investigation.

### 3. PostgreSQL warehouse

The warehouse uses four schemas:

| Schema | Purpose |
|---|---|
| `bronze` | Ingested source records |
| `silver` | Accepted and rejected records |
| `gold` | Analytical dimensions and fact table |
| `ops` | Pipeline runs, data-quality results, and reconciliation records |

Gold tables:

- `gold.dim_date`
- `gold.dim_market`
- `gold.dim_commodity`
- `gold.fact_market_prices`

Operational tables:

- `ops.pipeline_runs`
- `ops.dq_results`
- `ops.reconciliation`

### 4. Data quality and reconciliation

The pipeline is designed to run seven data-quality checks and record reconciliation counts for the Bronze, accepted Silver, rejected, and Gold layers.

The supplied November 16 execution log confirms the Bronze-to-Silver transformation, PostgreSQL loads, and Gold population. It then records a failure during final data-quality validation: PostgreSQL rejected an `ON CONFLICT (run_id, rule_name)` operation because the matching unique constraint was missing. The supplied log therefore does not establish that this run completed all seven checks or finished successfully end to end. Apply and verify the relevant SQL constraint migration before describing this run as a successful full pipeline run.

The processed record counts add up as follows:

```text
5,811,082 accepted + 8,400 rejected = 5,819,482 Bronze records
```

### 5. Amazon S3

The project stores pipeline artifacts and dashboard exports in the `mandi-data-pipeline-vasudha` bucket in `ap-south-1` (Asia Pacific, Mumbai).

```text
s3://mandi-data-pipeline-vasudha/
├── raw/
├── bronze/
├── silver/
│   ├── accepted/
│   └── rejected/
├── gold/
└── dashboard/
    ├── gold_dim_date.parquet
    ├── gold_dim_market.parquet
    ├── gold_dim_commodity.parquet
    ├── gold_fact_market_prices.parquet
    ├── ops_dq_results.parquet
    ├── ops_pipeline_runs.parquet
    ├── ops_reconciliation.parquet
    └── silver_rejected_records.parquet
```

The project upload helper is documented as verifying uploaded object sizes against local files. The local upload utility uses the AWS CLI profile `mandi-pipeline`. The dashboard uses its own AWS credentials to download the Parquet exports from the S3 `dashboard/` prefix.

## Streamlit dashboard

The dashboard is titled **Indian Mandi Analytics** and uses a forest-green and warm-ivory visual theme.

Live app: https://mandi-market-intelligence.streamlit.app/

### Dashboard sections

- **Market overview:** filtered Gold price-record count, market count, commodity count, and average modal price.
- **Pipeline run metrics:** accepted records, rejected records, and acceptance rate for the selected successful run recorded in the warehouse.
- **Price movement:** monthly average modal price, with average minimum and maximum prices available in the query.
- **Commodity analysis:** the ten most frequently reported commodities, record counts, and average modal prices.
- **Market analysis:** markets with the highest number of price observations, including state, district, record count, and average modal price.
- **Rejected-record analysis:** rejection counts and percentages by validation reason, a downloadable rejection summary, and sample rejected rows.
- **Pipeline monitoring:** data-quality results, recent pipeline execution history, run statuses, and warehouse reconciliation records.

### Dashboard filters

The sidebar supports filtering by:

- Year
- State
- District
- Market
- Commodity

The dashboard query cache is set to five minutes. The **Refresh dashboard** button clears the query and connection caches and reloads the Parquet exports from S3. The S3 exports themselves must be regenerated and uploaded after warehouse data changes.

### Understanding the dashboard numbers

The dashboard contains two types of metrics:

1. **Market metrics** are calculated from `gold.fact_market_prices` and respond to the selected year, state, district, market, and commodity filters.
2. **Pipeline quality metrics** come from the selected successful record in `ops.pipeline_runs`, with rejected-record, data-quality, and reconciliation counts matched by `run_id`.

Therefore, filtered market metrics can change when you use the sidebar, while pipeline run metrics describe a particular processing run.

## Recorded processing run

The recorded processing run is `20261116_140820_205004`.

| Metric | Recorded result |
|---|---:|
| Bronze source records | 5,819,482 |
| Accepted Silver records | 5,811,082 |
| Rejected records | 8,400 |
| Acceptance rate | 99.86% |
| Markets | 5,469* |
| Commodities | 3,979* |

The Bronze, accepted Silver, and rejected counts are supported by the supplied transformation and load logs. The acceptance rate is calculated from those counts. The market and commodity counts are carried over from the project run notes and should be rechecked against the Gold dimension tables before being presented as independently verified run metrics.

*The supplied November 16 execution log shows that final data-quality validation failed because the database was missing a unique constraint required by the `ops.dq_results` upsert. It therefore does not prove that all seven data-quality checks or four reconciliation entries completed for this run. Do not describe this run as a successful end-to-end run until the constraint migration is applied and a successful run is visible in `ops.pipeline_runs`.

The project notes state that the Bronze, accepted Silver, and rejected Parquet artifacts were uploaded to S3 and verified by checking object sizes. The dashboard separately expects the eight Parquet exports listed above under the S3 `dashboard/` prefix. These are historical processing results, not live-feed figures.

## Repository structure

```text
indian-market-data-pipeline/
├── dashboard/
│   └── app.py
├── data/
│   ├── raw/
│   ├── bronze/
│   ├── silver/
│   ├── rejected/
│   └── dashboard_exports/
├── docs/
├── logs/
├── reports/
├── scripts/
│   └── export_dashboard_data.py
├── sql/
│   ├── 01_create_schemas.sql
│   ├── 02_create_ops_tables.sql
│   ├── 03_create_data_tables.sql
│   ├── 04_create_gold_tables.sql
│   ├── 05_update_rejected_table.sql
│   ├── 06_populate_gold.sql
│   ├── 07_populate_monitoring.sql
│   ├── 08_add_dq_unique_constraint.sql
│   └── 09_add_reconciliation_constraint.sql
├── src/
│   └── mandi_pipeline/
│       ├── extract.py
│       ├── transform.py
│       ├── load.py
│       ├── quality_checks.py
│       ├── reconciliation.py
│       ├── run_tracker.py
│       ├── s3_storage.py
│       └── pipeline.py
├── tests/
├── .env.example
├── requirements.txt
└── README.md
```

Generated data, local environments, logs, and credentials should remain out of version control.

## Local setup

### Prerequisites

- Python 3.11 or newer
- PostgreSQL
- AWS CLI for S3 access
- An AWS account and an S3 bucket for cloud-storage integration

### 1. Create and activate a virtual environment

```bash
python3 -m venv .venv
source .venv/bin/activate
```

### 2. Install dependencies

```bash
python -m pip install -r requirements.txt
```

### 3. Configure PostgreSQL

Create the project database and configure a local `.env` file. Example:

```env
DATABASE_URL=postgresql+psycopg2:///mandi_warehouse
```

Adjust the URL for your PostgreSQL installation. Do not commit `.env`.

Apply the SQL files in `sql/` in numeric order to the intended database. Before a full load, confirm that the target warehouse is empty. The full pipeline mode checks for an empty target warehouse and should not be run against a database that already contains the project data.

### 4. Place the source data

Download the 2025 CSV from the Kaggle dataset and place it here:

```text
data/raw/2025.csv
```

The extraction module expects a year-named CSV in `data/raw/`.

### 5. Configure AWS

Configure the local AWS CLI profile:

```bash
aws configure --profile mandi-pipeline
```

Use `ap-south-1` for the project bucket. Confirm access:

```bash
aws s3 ls s3://mandi-data-pipeline-vasudha/ --profile mandi-pipeline
```

The IAM identity should have only the S3 permissions required for this bucket.

For Streamlit Community Cloud, configure credentials in **Manage app → Settings → Secrets**:

```toml
[aws]
access_key_id = "YOUR_ACCESS_KEY_ID"
secret_access_key = "YOUR_SECRET_ACCESS_KEY"
```

Never put real credentials in Python source code, README files, screenshots, or Git. If a key has been exposed, revoke it and create a replacement.

## Running the pipeline

Run commands from the repository root with the virtual environment active.

### Full pipeline

```bash
PYTHONPATH=src python -m mandi_pipeline.pipeline --mode full --year 2025
```

A full run checks that the target warehouse is empty before loading. Do not run it against a warehouse that already contains the project data.

### Validate an existing warehouse

```bash
PYTHONPATH=src python -m mandi_pipeline.pipeline --mode validate-existing
```

This validates the existing warehouse without re-extracting the source dataset.

### Resume a failed run

```bash
PYTHONPATH=src python -m mandi_pipeline.pipeline --mode resume
```

The resume mode uses the run ID and Bronze file configured in `pipeline.py`. Confirm the warehouse state and those configured values before resuming.

## Exporting dashboard data

The dashboard expects the eight Parquet exports listed in the Dashboard data path section to exist under the S3 `dashboard/` prefix.

The repository includes:

```text
scripts/export_dashboard_data.py
```

Run the export utility according to its configured arguments and database connection, then verify that all eight expected objects exist under `s3://mandi-data-pipeline-vasudha/dashboard/`. The Streamlit app downloads these objects and queries them with DuckDB.

## Running the dashboard locally

With the required S3 dashboard exports available and AWS credentials configured:

```bash
python -m streamlit run dashboard/app.py --server.port 8503
```

Open the local URL printed by Streamlit.

## Tests and syntax checks

Run the test suite:

```bash
PYTHONPATH=src pytest
```

Compile the pipeline modules:

```bash
python -m py_compile src/mandi_pipeline/s3_storage.py
python -m py_compile src/mandi_pipeline/pipeline.py
python -m py_compile dashboard/app.py
```

## Security and data handling

- Keep `.env`, AWS keys, and local AWS credential files out of Git.
- Keep the S3 bucket private unless public access is explicitly required.
- Scope IAM permissions to the project bucket and required actions.
- Keep large raw CSV and generated Parquet files out of the Git repository.
- Treat the Kaggle dataset as historical source data, not as a real-time market API.

## Future improvements

Potential next steps include scheduled ingestion, automated CI tests, cloud monitoring, and lifecycle policies for older Parquet artifacts.
