
import csv
import os
from io import StringIO
from pathlib import Path

import pyarrow.parquet as pq
from dotenv import load_dotenv
from sqlalchemy import create_engine, text


# --------------------------------------------------
# CONFIGURATION
# --------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]
BATCH_SIZE = 25_000

load_dotenv(PROJECT_ROOT / ".env")

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise ValueError("DATABASE_URL not found in .env")

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True
)


# --------------------------------------------------
# TABLE CONFIGURATION
# --------------------------------------------------

TABLES = [
    {
        "name": "bronze.raw_mandi_prices",
        "file_pattern": "data/bronze/*.parquet",
        "columns": [
            "State", "District", "Market", "Commodity",
            "Variety", "Grade", "Arrival_Date",
            "Min_Price", "Max_Price", "Modal_Price",
            "Commodity_Code", "run_id", "source_file",
            "ingested_at"
        ],
    },
    {
        "name": "silver.mandi_prices_clean",
        "file_pattern": "data/silver/*.parquet",
        "columns": [
            "state", "district", "market", "commodity",
            "variety", "grade", "arrival_date",
            "min_price", "max_price", "modal_price",
            "commodity_code", "run_id", "source_file",
            "ingested_at"
        ],
    },
    {
        "name": "silver.rejected_records",
        "file_pattern": "data/rejected/*.parquet",
        "columns": [
            "state", "district", "market", "commodity",
            "variety", "grade", "arrival_date",
            "min_price", "max_price", "modal_price",
            "commodity_code", "run_id", "source_file",
            "ingested_at", "rejection_reason",
            "raw_state", "raw_district", "raw_market",
            "raw_commodity", "raw_variety", "raw_grade",
            "raw_arrival_date", "raw_min_price",
            "raw_max_price", "raw_modal_price",
            "raw_commodity_code", "raw_run_id",
            "raw_source_file", "raw_ingested_at"
        ],
    },
]


# --------------------------------------------------
# FIND LATEST PARQUET FILE
# --------------------------------------------------

def find_latest_file(pattern):

    files = list(PROJECT_ROOT.glob(pattern))

    if not files:
        raise FileNotFoundError(
            f"No Parquet file found for pattern: {pattern}"
        )

    return max(
        files,
        key=lambda file: file.stat().st_mtime
    )


# --------------------------------------------------
# LOAD A SPECIFIC PARQUET FILE INTO POSTGRESQL
# --------------------------------------------------

def load_table(table_config, parquet_path):

    table_name = table_config["name"]
    columns = table_config["columns"]

    parquet_path = Path(parquet_path)

    if not parquet_path.is_absolute():
        parquet_path = PROJECT_ROOT / parquet_path

    if not parquet_path.exists():
        raise FileNotFoundError(
            f"Parquet file not found: {parquet_path}"
        )

    parquet_file = pq.ParquetFile(parquet_path)
    expected_rows = parquet_file.metadata.num_rows

    # Verify required columns.
    available_columns = parquet_file.schema_arrow.names

    missing_columns = [
        column
        for column in columns
        if column not in available_columns
    ]

    if missing_columns:
        raise ValueError(
            f"Missing columns in {parquet_path.name}: "
            f"{missing_columns}"
        )

    print("\n" + "=" * 55)
    print(f"Loading table: {table_name}")
    print(f"Source file: {parquet_path.name}")
    print(f"Expected rows: {expected_rows:,}")
    print("=" * 55)

    # Prevent loading into a non-empty table.
    with engine.connect() as connection:

        existing_rows = connection.execute(
            text(f"SELECT COUNT(*) FROM {table_name}")
        ).scalar_one()

    if existing_rows != 0:
        raise RuntimeError(
            f"{table_name} already contains "
            f"{existing_rows:,} rows. "
            "Loading stopped to prevent duplicate data."
        )

    quoted_columns = ", ".join(columns)

    copy_sql = (
        f"COPY {table_name} ({quoted_columns}) "
        r"FROM STDIN WITH (FORMAT CSV, NULL E'\\N')"
    )

    raw_connection = engine.raw_connection()
    cursor = raw_connection.cursor()

    loaded_rows = 0

    try:

        for batch_number, batch in enumerate(
            parquet_file.iter_batches(
                batch_size=BATCH_SIZE,
                columns=columns,
                use_threads=False
            ),
            start=1
        ):

            df = batch.to_pandas()

            buffer = StringIO()

            df.to_csv(
                buffer,
                index=False,
                header=False,
                na_rep=r"\N",
                quoting=csv.QUOTE_MINIMAL,
                doublequote=True,
                lineterminator="\n"
            )

            buffer.seek(0)

            cursor.copy_expert(
                copy_sql,
                buffer
            )

            loaded_rows += len(df)

            if batch_number % 10 == 0:

                print(
                    f"Loaded {loaded_rows:,} / "
                    f"{expected_rows:,} rows"
                )

        raw_connection.commit()

    except Exception:

        raw_connection.rollback()
        raise

    finally:

        cursor.close()
        raw_connection.close()

    # Verify database row count.
    with engine.connect() as connection:

        actual_rows = connection.execute(
            text(f"SELECT COUNT(*) FROM {table_name}")
        ).scalar_one()

    if actual_rows != expected_rows:

        raise RuntimeError(
            f"Count mismatch in {table_name}: "
            f"expected {expected_rows:,}, "
            f"found {actual_rows:,}"
        )

    print(
        f"Successfully loaded: {actual_rows:,} rows"
    )

    return actual_rows


# --------------------------------------------------
# STANDALONE EXECUTION
# --------------------------------------------------

def main():

    print("\nMANDI DATA QUALITY PIPELINE")
    print("PostgreSQL bulk loading started")

    results = {}

    try:

        for table_config in TABLES:

            parquet_path = find_latest_file(
                table_config["file_pattern"]
            )

            results[table_config["name"]] = load_table(
                table_config,
                parquet_path
            )

        print("\n" + "=" * 55)
        print("DATABASE LOADING SUMMARY")
        print("=" * 55)

        for table_name, row_count in results.items():

            print(
                f"{table_name}: {row_count:,} rows"
            )

        print(
            "\nAll three data tables verified successfully."
        )

    finally:

        engine.dispose()


if __name__ == "__main__":
    main()
