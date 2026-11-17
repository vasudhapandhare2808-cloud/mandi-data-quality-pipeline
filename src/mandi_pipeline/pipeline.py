import argparse
import logging
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import text

from . import extract
from . import transform
from . import load
from . import quality_checks
from . import reconciliation
from . import run_tracker
from . import s3_storage


# --------------------------------------------------
# CONFIGURATION
# --------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SQL_DIR = PROJECT_ROOT / "sql"
GOLD_SQL_FILE = SQL_DIR / "06_populate_gold.sql"

load_dotenv(PROJECT_ROOT / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)

RESUME_RUN_ID = "20261116_140820_205004"

RESUME_BRONZE_FILE = (
    PROJECT_ROOT
    / "data"
    / "bronze"
    / f"mandi_bronze_{RESUME_RUN_ID}.parquet"
)


# --------------------------------------------------
# AWS S3 HELPERS
# --------------------------------------------------

def upload_bronze_to_s3(bronze_file):
    """Upload the Bronze Parquet artifact to S3."""
    logger.info("Uploading Bronze Parquet to AWS S3")

    s3_uri = s3_storage.upload_file(
        bronze_file,
        "bronze"
    )

    logger.info("Bronze upload verified: %s", s3_uri)
    return s3_uri


def upload_silver_to_s3(transform_result):
    """Upload accepted and rejected Silver Parquet artifacts to S3."""
    artifacts = [
        (
            "silver_file",
            "accepted_rows",
            "silver/accepted",
            "accepted Silver"
        ),
        (
            "rejected_file",
            "rejected_rows",
            "silver/rejected",
            "rejected records"
        ),
    ]

    uploaded = {}

    for file_key, count_key, prefix, label in artifacts:
        row_count = transform_result[count_key]
        file_path = Path(transform_result[file_key])

        if row_count == 0:
            logger.info(
                "Skipping %s S3 upload because the row count is zero",
                label
            )
            continue

        if not file_path.is_file():
            raise FileNotFoundError(
                f"{label} Parquet file is missing: {file_path}"
            )

        logger.info("Uploading %s Parquet to AWS S3", label)

        s3_uri = s3_storage.upload_file(
            file_path,
            prefix
        )

        uploaded[label] = s3_uri
        logger.info("%s upload verified: %s", label, s3_uri)

    return uploaded


# --------------------------------------------------
# DATABASE HELPERS
# --------------------------------------------------

def get_table_count(connection, table_name):

    return connection.execute(
        text(f"SELECT COUNT(*) FROM {table_name}")
    ).scalar_one()


def check_empty_warehouse():

    protected_tables = [
        "bronze.raw_mandi_prices",
        "silver.mandi_prices_clean",
        "silver.rejected_records",
        "gold.dim_date",
        "gold.dim_market",
        "gold.dim_commodity",
        "gold.fact_market_prices",
    ]

    with load.engine.connect() as connection:

        populated_tables = {}

        for table_name in protected_tables:

            count = get_table_count(
                connection,
                table_name
            )

            if count > 0:
                populated_tables[table_name] = count

    if populated_tables:

        details = "\n".join(
            f"{table}: {count:,} rows"
            for table, count in populated_tables.items()
        )

        raise RuntimeError(
            "Execution stopped. Target warehouse is not empty:\n"
            f"{details}"
        )

    logger.info("Target warehouse is empty.")


# --------------------------------------------------
# GOLD LAYER
# --------------------------------------------------

def populate_gold():

    if not GOLD_SQL_FILE.exists():
        raise FileNotFoundError(
            f"Gold SQL file not found: {GOLD_SQL_FILE}"
        )

    sql_script = GOLD_SQL_FILE.read_text(
        encoding="utf-8"
    )

    raw_connection = load.engine.raw_connection()
    cursor = raw_connection.cursor()

    try:
        logger.info("Populating Gold warehouse")
        cursor.execute(sql_script)
        raw_connection.commit()
        logger.info("Gold population completed")

    except Exception:
        raw_connection.rollback()
        raise

    finally:
        cursor.close()
        raw_connection.close()


# --------------------------------------------------
# VALIDATION
# --------------------------------------------------

def validate_warehouse(run_id):

    logger.info("Running data quality checks")

    quality_success = quality_checks.run_quality_checks(
        run_id
    )

    logger.info("Running warehouse reconciliation")

    reconciliation_success = reconciliation.run_reconciliation(
        run_id
    )

    if not quality_success:
        raise RuntimeError(
            "One or more data quality checks failed."
        )

    if not reconciliation_success:
        raise RuntimeError(
            "Warehouse reconciliation failed."
        )

    logger.info("All validation checks passed.")


# --------------------------------------------------
# PIPELINE COUNTS
# --------------------------------------------------

def get_pipeline_counts():

    with load.engine.connect() as connection:

        total_rows = get_table_count(
            connection,
            "bronze.raw_mandi_prices"
        )

        accepted_rows = get_table_count(
            connection,
            "silver.mandi_prices_clean"
        )

        rejected_rows = get_table_count(
            connection,
            "silver.rejected_records"
        )

    return total_rows, accepted_rows, rejected_rows


# --------------------------------------------------
# LOAD TRANSFORMED DATA AND FINISH
# --------------------------------------------------

def load_and_finish(
    run_id,
    bronze_file,
    transform_result,
    expected_total
):

    transformed_total = transform_result["total_rows"]
    accepted_rows = transform_result["accepted_rows"]
    rejected_rows = transform_result["rejected_rows"]

    if transformed_total != expected_total:
        raise RuntimeError(
            "Bronze and transformation row counts differ."
        )

    if transformed_total != accepted_rows + rejected_rows:
        raise RuntimeError(
            "Silver accepted and rejected counts do not reconcile."
        )

    logger.info(
        "Transformation completed: %s accepted, %s rejected",
        f"{accepted_rows:,}",
        f"{rejected_rows:,}"
    )

    # Upload transformed artifacts before loading PostgreSQL.
    # Each upload is verified by s3_storage.upload_file().
    upload_silver_to_s3(transform_result)

    bronze_config = next(
        item for item in load.TABLES
        if item["name"] == "bronze.raw_mandi_prices"
    )

    silver_config = next(
        item for item in load.TABLES
        if item["name"] == "silver.mandi_prices_clean"
    )

    rejected_config = next(
        item for item in load.TABLES
        if item["name"] == "silver.rejected_records"
    )

    logger.info("Loading Bronze table")
    load.load_table(bronze_config, bronze_file)

    if accepted_rows > 0:
        logger.info("Loading Silver accepted records")
        load.load_table(
            silver_config,
            transform_result["silver_file"]
        )

    if rejected_rows > 0:
        logger.info("Loading rejected records")
        load.load_table(
            rejected_config,
            transform_result["rejected_file"]
        )

    logger.info("Populating Gold layer")
    populate_gold()

    logger.info("Running final validation")
    validate_warehouse(run_id)

    run_tracker.complete_run(
        run_id,
        expected_total,
        accepted_rows,
        rejected_rows
    )

    logger.info("=" * 65)
    logger.info("FULL PIPELINE COMPLETED SUCCESSFULLY")
    logger.info("Run ID: %s", run_id)
    logger.info("=" * 65)


# --------------------------------------------------
# MODE 1: VALIDATE EXISTING WAREHOUSE
# --------------------------------------------------

def run_existing_validation():

    run_id = None

    try:
        logger.info("Starting warehouse validation")

        run_id = run_tracker.start_run(
            "existing_warehouse_validation"
        )

        validate_warehouse(run_id)

        total, accepted, rejected = get_pipeline_counts()

        run_tracker.complete_run(
            run_id,
            total,
            accepted,
            rejected
        )

        logger.info("Existing warehouse validation successful")

    except Exception as error:

        logger.exception("Warehouse validation failed")

        if run_id is not None:
            run_tracker.fail_run(run_id, str(error))

        raise


# --------------------------------------------------
# MODE 2: FULL ETL PIPELINE
# --------------------------------------------------

def run_full_pipeline(year=2025):

    run_id = None

    try:
        logger.info("Starting full ETL pipeline")

        check_empty_warehouse()

        source_file = f"{year}.csv"
        run_id = run_tracker.start_run(source_file)

        logger.info("STAGE 1: EXTRACTION")

        bronze_file, extracted_rows = (
            extract.extract_mandi_data(
                year=year,
                chunk_size=50000,
                run_id=run_id
            )
        )

        logger.info(
            "Extracted %s rows",
            f"{extracted_rows:,}"
        )

        # Upload Bronze immediately after extraction.
        upload_bronze_to_s3(bronze_file)

        logger.info("STAGE 2: TRANSFORMATION")

        transform_result = transform.transform_mandi_data(
            batch_size=50000,
            bronze_file=bronze_file,
            run_id=run_id
        )

        load_and_finish(
            run_id,
            bronze_file,
            transform_result,
            extracted_rows
        )

    except BaseException as error:

        if run_id is not None:
            try:
                run_tracker.fail_run(run_id, str(error))
            except Exception:
                logger.exception("Could not update failed run status")

        if isinstance(error, KeyboardInterrupt):
            logger.warning("Pipeline interrupted by user")
        else:
            logger.exception("Full pipeline execution failed")

        raise


# --------------------------------------------------
# MODE 3: RESUME EXISTING FAILED RUN
# --------------------------------------------------

def resume_pipeline():

    run_id = RESUME_RUN_ID
    bronze_file = RESUME_BRONZE_FILE

    try:
        logger.info("=" * 65)
        logger.info("RESUMING FAILED PIPELINE")
        logger.info("Run ID: %s", run_id)
        logger.info("=" * 65)

        # Check the existing Bronze file.
        if not bronze_file.is_file():
            raise FileNotFoundError(
                f"Existing Bronze file not found: {bronze_file}"
            )

        # Verify all target tables are empty.
        check_empty_warehouse()

        # Verify the run is FAILED before reactivating it.
        with run_tracker.engine.connect() as connection:

            run_status = connection.execute(
                text("""
                    SELECT status
                    FROM ops.pipeline_runs
                    WHERE run_id = :run_id
                """),
                {"run_id": run_id}
            ).scalar_one_or_none()

        if run_status != "FAILED":
            raise RuntimeError(
                f"Expected FAILED status, found: {run_status}"
            )

        # Verify Bronze row count and embedded run ID.
        import pyarrow.parquet as pq

        parquet_file = pq.ParquetFile(bronze_file)

        metadata_rows = parquet_file.metadata.num_rows
        schema_names = parquet_file.schema_arrow.names

        if "run_id" not in schema_names:
            raise RuntimeError(
                "Bronze file does not contain run_id metadata."
            )

        if "source_file" not in schema_names:
            raise RuntimeError(
                "Bronze file does not contain source_file metadata."
            )

        embedded_run_ids = set()

        for batch in parquet_file.iter_batches(
            batch_size=50000,
            columns=["run_id"]
        ):
            embedded_run_ids.update(
                value
                for value in batch.column(0).to_pylist()
                if value is not None
            )

            if len(embedded_run_ids) > 1:
                raise RuntimeError(
                    "Bronze file contains multiple run IDs."
                )

        if embedded_run_ids != {run_id}:
            raise RuntimeError(
                f"Bronze run ID mismatch: {embedded_run_ids}"
            )

        logger.info(
            "Verified existing Bronze file: %s rows",
            f"{metadata_rows:,}"
        )

        # Ensure the existing Bronze artifact is also present in S3.
        upload_bronze_to_s3(bronze_file)

        # Reactivate the original failed run.
        run_tracker.resume_run(run_id)

        logger.info("STAGE 2: TRANSFORMATION")

        transform_result = transform.transform_mandi_data(
            batch_size=50000,
            bronze_file=bronze_file,
            run_id=run_id
        )

        load_and_finish(
            run_id,
            bronze_file,
            transform_result,
            metadata_rows
        )

    except BaseException as error:

        try:
            run_tracker.fail_run(run_id, str(error))
        except Exception:
            logger.exception("Could not update resumed run status")

        if isinstance(error, KeyboardInterrupt):
            logger.warning("Resume interrupted by user")
        else:
            logger.exception("Resume execution failed")

        raise


# --------------------------------------------------
# COMMAND LINE INTERFACE
# --------------------------------------------------

def main():

    parser = argparse.ArgumentParser(
        description="Mandi Data Quality and ETL Pipeline"
    )

    parser.add_argument(
        "--mode",
        choices=[
            "validate-existing",
            "full",
            "resume"
        ],
        required=True
    )

    parser.add_argument(
        "--year",
        type=int,
        default=2025
    )

    args = parser.parse_args()

    try:

        if args.mode == "validate-existing":
            run_existing_validation()

        elif args.mode == "full":
            run_full_pipeline(args.year)

        elif args.mode == "resume":
            resume_pipeline()

    finally:

        run_tracker.close_connection()
        load.engine.dispose()
        quality_checks.engine.dispose()
        reconciliation.engine.dispose()


if __name__ == "__main__":
    main()
