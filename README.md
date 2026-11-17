# Mandi Data Quality Pipeline

A Python-based data engineering project that ingests Indian agricultural
mandi price data, validates and transforms records, loads a PostgreSQL
warehouse, stores Parquet pipeline artifacts in Amazon S3, and presents
analytical results through a Streamlit dashboard.

## Project overview

The pipeline uses the **India Mandi Daily Commodity Prices
(2001--2026)** dataset by Manas Khandelwal on Kaggle. The dataset is
historical and is derived from AGMARKNET data. It is not a live
market-price feed.

Dataset:
<https://www.kaggle.com/datasets/khandelwalmanas/daily-commodity-prices-india>

The project currently processes the 2025 dataset.

## Architecture

``` text
Kaggle CSV source
       |
       v
Python extraction
       |
       v
Bronze Parquet artifact ----------------------> Amazon S3: bronze/
       |
       v
Python transformation and validation
       |
       +--------------------------+
       |                          |
       v                          v
Accepted Silver Parquet      Rejected records Parquet
       |                          |
       v                          v
Amazon S3: silver/accepted/   Amazon S3: silver/rejected/
       |
       v
PostgreSQL warehouse
       |
       +--> Bronze schema
       +--> Silver schema
       +--> Gold schema
       +--> Ops metadata and quality results
       |
       v
Streamlit dashboard
```

The S3 upload helper verifies each uploaded object's size against the
local file. The existing Bronze, accepted Silver, and rejected Parquet
artifacts from the successful November 16, 2026 run were uploaded and
verified in S3. The updated pipeline code includes S3 upload calls for
full and resume modes; a fresh full end-to-end run using that updated
orchestration has not yet been performed.

## Technology stack

-   **Language:** Python
-   **Data processing:** pandas, PyArrow
-   **Storage formats:** Parquet
-   **Database:** PostgreSQL
-   **Database access:** SQLAlchemy, psycopg2
-   **Cloud object storage:** Amazon S3
-   **AWS SDK:** boto3
-   **Cloud CLI:** AWS CLI
-   **Dashboard:** Streamlit, Plotly
-   **Testing:** pytest
-   **Configuration:** python-dotenv

## Data pipeline

### 1. Extraction

The extraction stage reads the yearly CSV source in chunks and writes a
Bronze Parquet artifact.

Example output:

``` text
data/bronze/mandi_bronze_<run_id>.parquet
```

Chunked processing is used to keep memory use manageable for the large
source file.

### 2. Transformation and data quality

The transformation stage standardizes fields, parses dates and numeric
values, checks records, and separates accepted records from rejected
records. It also detects duplicate records across processing batches.

Outputs:

``` text
data/silver/mandi_silver_<run_id>.parquet
data/rejected/mandi_rejected_<run_id>.parquet
```

Rejected records retain raw source values alongside the rejection reason
to support investigation.

### 3. PostgreSQL warehouse

The warehouse is organized into layers:

  -----------------------------------------------------------------------
  Schema                              Purpose
  ----------------------------------- -----------------------------------
  `bronze`                            Raw ingested mandi records

  `silver`                            Clean accepted records and rejected
                                      records

  `gold`                              Analytical dimensions and fact
                                      table

  `ops`                               Pipeline runs, data-quality
                                      results, and reconciliation records
  -----------------------------------------------------------------------

Gold tables:

-   `gold.dim_date`
-   `gold.dim_market`
-   `gold.dim_commodity`
-   `gold.fact_market_prices`

Operational tables:

-   `ops.pipeline_runs`
-   `ops.dq_results`
-   `ops.reconciliation`

SQL setup and Gold population scripts are maintained in the `sql/`
directory.

### 4. Validation and reconciliation

The pipeline runs seven data-quality checks and compares row counts
across the Bronze, accepted Silver, rejected, and Gold layers.

The successful 2025 processing run reconciled as follows:

  Layer                    Rows
  ----------------- -----------
  Bronze              5,819,482
  Silver accepted     5,811,082
  Silver rejected         8,400
  Gold fact           5,811,082

The accepted and rejected Silver counts sum to the Bronze count.

### 5. Amazon S3 storage

S3 stores the Parquet artifacts in separate prefixes:

``` text
s3://mandi-data-pipeline-vasudha/
├── raw/
├── bronze/
│   └── mandi_bronze_<run_id>.parquet
├── silver/
│   ├── accepted/
│   │   └── mandi_silver_<run_id>.parquet
│   └── rejected/
│       └── mandi_rejected_<run_id>.parquet
└── gold/
```

AWS region: `ap-south-1` (Asia Pacific, Mumbai).

The Python upload utility uses the AWS CLI profile `mandi-pipeline` and
checks the S3 object size after upload. The IAM principal should have
only the S3 permissions required for this bucket.

Do not commit AWS access keys, secret keys, `.env` files, or local AWS
credential files to Git.

### 6. Streamlit dashboard

The dashboard provides views for:

-   Pipeline overview and key row counts
-   Monthly price trends
-   Commodity and market analysis
-   Rejected-record analysis
-   Data-quality results
-   Pipeline run history
-   Layer reconciliation

## Verified results

The successful 2025 run (`20261116_140820_205004`) produced:

  Metric                                Result
  -------------------------------- -----------
  Total source records processed     5,819,482
  Accepted records                   5,811,082
  Rejected records                       8,400
  Acceptance rate                       99.86%
  Markets                                5,469
  Commodities                            3,979
  Data-quality checks passed                 7
  Reconciliation entries                     4

These are results from the recorded successful run, not a claim that the
dataset is updated live.

The corresponding Bronze, accepted Silver, and rejected Parquet
artifacts were uploaded to S3 and verified by listing the bucket and
checking object sizes.

## Repository structure

``` text
indian-market-data-pipeline/
├── dashboard/
│   └── app.py
├── data/
│   ├── raw/
│   ├── bronze/
│   ├── silver/
│   └── rejected/
├── logs/
├── sql/
│   ├── 01_create_schemas.sql
│   ├── ...
│   └── 06_populate_gold.sql
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
├── .gitignore
└── README.md
```

The exact set of SQL scripts and test files may evolve as the project is
maintained. Generated data, logs, local environments, and credentials
should remain outside version control.

## Local setup

### Prerequisites

-   Python 3.11 or newer
-   PostgreSQL
-   AWS CLI (needed for S3 access)
-   An AWS account and an S3 bucket if you want to use the cloud-storage
    integration

### 1. Clone the repository

``` bash
git clone <your-repository-url>
cd indian-market-data-pipeline
```

### 2. Create and activate a virtual environment

macOS/Linux:

``` bash
python3 -m venv .venv
source .venv/bin/activate
```

### 3. Install dependencies

If a `requirements.txt` is maintained in the repository:

``` bash
python -m pip install -r requirements.txt
```

The project uses pandas, PyArrow, SQLAlchemy, psycopg2-binary,
python-dotenv, boto3, Streamlit, Plotly, and pytest.

### 4. Configure PostgreSQL

Create a PostgreSQL database for the project, then create a local `.env`
file:

``` env
DATABASE_URL=postgresql+psycopg2:///mandi_warehouse
```

Adjust the connection URL for your PostgreSQL installation. Do not
commit `.env`.

Apply the SQL schema scripts from the `sql/` directory in their intended
numeric order before running the pipeline. Confirm that the database is
the intended empty target before starting a full load.

### 5. Place the source data

Download the required yearly CSV from the Kaggle dataset and place it
in:

``` text
data/raw/2025.csv
```

The extraction module expects the year-named CSV in `data/raw/`.

## Running the pipeline

Run commands from the repository root with the virtual environment
active.

### Full pipeline

``` bash
PYTHONPATH=src python -m mandi_pipeline.pipeline --mode full --year 2025
```

A full run checks that the target warehouse is empty before loading. Do
not run it against a database that already contains the project data.

### Validate an existing warehouse

``` bash
PYTHONPATH=src python -m mandi_pipeline.pipeline --mode validate-existing
```

This mode runs validation against the existing warehouse. It does not
re-extract the source dataset.

### Resume a failed run

``` bash
PYTHONPATH=src python -m mandi_pipeline.pipeline --mode resume
```

The current resume implementation uses the run ID and Bronze file
configured in `pipeline.py`. Update those constants only when
intentionally resuming a different failed run, and verify that the
target warehouse is in the expected state first.

## AWS S3 setup

1.  Create an S3 bucket in your chosen AWS region.
2.  Configure an IAM identity with the minimum required bucket and
    object permissions.
3.  Configure an AWS CLI profile. For example:

``` bash
aws configure --profile mandi-pipeline
```

Set the region to `ap-south-1` if using the project bucket in Mumbai.

4.  Confirm access:

``` bash
aws s3 ls s3://<your-bucket-name>/ --profile mandi-pipeline
```

5.  Update `BUCKET_NAME`, `AWS_REGION`, and `AWS_PROFILE` in
    `src/mandi_pipeline/s3_storage.py` to match your own setup.

6.  Test an upload:

``` bash
PYTHONPATH=src python -c "from mandi_pipeline.s3_storage import upload_file; print(upload_file('path/to/file.parquet', 'bronze'))"
```

Allowed prefixes are `raw`, `bronze`, `silver`, `silver/accepted`,
`silver/rejected`, and `gold`.

The bucket name in this repository's current local configuration is
`mandi-data-pipeline-vasudha`. Use your own bucket name when deploying
your own copy.

## Running the dashboard

With PostgreSQL available and the database connection configured:

``` bash
DATABASE_URL="postgresql+psycopg2:///mandi_warehouse" python -m streamlit run dashboard/app.py --server.port 8503
```

Open the local URL printed by Streamlit in your browser.

If your dashboard uses a different database URL, replace the example
with the correct connection string.

## Tests and syntax checks

Run the test suite:

``` bash
PYTHONPATH=src pytest
```

Compile the pipeline modules:

``` bash
python -m py_compile src/mandi_pipeline/s3_storage.py
python -m py_compile src/mandi_pipeline/pipeline.py
```

## Security and data handling

-   Keep `.env` and AWS credentials out of Git.
-   Do not upload access keys or secret keys into source files,
    notebooks, screenshots, or documentation.
-   Keep the S3 bucket private unless public access is explicitly
    required.
-   Use IAM permissions scoped to the project bucket.
-   Treat the Kaggle dataset as historical source data, not as a
    real-time market API.
-   Keep large raw CSV and generated Parquet files out of the Git
    repository.

## Future improvements

Potential next steps include scheduled ingestion, automated CI tests,
cloud-based monitoring, and lifecycle policies for older S3 objects.
These are future enhancements and are not required for the current
project.

## Author

Built as a data engineering portfolio project using Python, PostgreSQL,
Parquet, Amazon S3, and Streamlit.
