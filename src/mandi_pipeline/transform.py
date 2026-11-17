
import logging
import json
import re
import sqlite3
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


# --------------------------------------------------
# CONFIGURATION
# --------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]

BRONZE_DIR = PROJECT_ROOT / "data" / "bronze"
SILVER_DIR = PROJECT_ROOT / "data" / "silver"
REJECTED_DIR = PROJECT_ROOT / "data" / "rejected"

SILVER_DIR.mkdir(parents=True, exist_ok=True)
REJECTED_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)


# --------------------------------------------------
# COLUMN CONFIGURATION
# --------------------------------------------------

COLUMN_MAP = {
    "State": "state",
    "District": "district",
    "Market": "market",
    "Commodity": "commodity",
    "Variety": "variety",
    "Grade": "grade",
    "Arrival_Date": "arrival_date",
    "Min_Price": "min_price",
    "Max_Price": "max_price",
    "Modal_Price": "modal_price",
    "Commodity_Code": "commodity_code",
}

TEXT_COLUMNS = [
    "state", "district", "market",
    "commodity", "variety", "grade"
]

PRICE_COLUMNS = [
    "min_price", "max_price", "modal_price"
]

REQUIRED_COLUMNS = [
    "state", "market", "commodity",
    "arrival_date", "modal_price"
]

DUPLICATE_KEY = [
    "state",
    "district",
    "market",
    "commodity",
    "commodity_code",
    "variety",
    "grade",
    "arrival_date",
]


# --------------------------------------------------
# NORMALIZATION HELPERS
# --------------------------------------------------

def normalize_key(values):

    normalized = []

    for value in values:

        if isinstance(value, pd.Timestamp):
            value = value.strftime("%Y-%m-%d")

        normalized.append(str(value).strip())

    return json.dumps(
        normalized,
        ensure_ascii=False
    )


def normalize_price(value):

    if pd.isna(value):
        return None

    return format(float(value), ".15g")


# --------------------------------------------------
# SILVER TRANSFORMATION
# --------------------------------------------------

def transform_mandi_data(
    batch_size=50000,
    bronze_file=None,
    run_id=None
):

    # Resolve Bronze file.
    if bronze_file is None:

        bronze_files = list(BRONZE_DIR.glob("*.parquet"))

        if not bronze_files:
            raise FileNotFoundError(
                "No Bronze Parquet file found"
            )

        bronze_file = max(
            bronze_files,
            key=lambda file: file.stat().st_mtime
        )

    else:

        bronze_file = Path(bronze_file)

        if not bronze_file.is_absolute():
            bronze_file = PROJECT_ROOT / bronze_file

    if not bronze_file.exists():
        raise FileNotFoundError(
            f"Bronze file not found: {bronze_file}"
        )

    # Resolve shared run ID.
    filename_run_id = bronze_file.stem.replace(
        "mandi_bronze_", "",
        1
    )

    if run_id is None:
        run_id = filename_run_id

    run_id = str(run_id)

    if not re.fullmatch(r"[A-Za-z0-9_-]+", run_id):
        raise ValueError(
            "Invalid run ID format."
        )

    if filename_run_id != run_id:
        raise ValueError(
            f"Run ID mismatch: {filename_run_id} != {run_id}"
        )

    # Output files.
    silver_file = (
        SILVER_DIR / f"mandi_silver_{run_id}.parquet"
    )

    rejected_file = (
        REJECTED_DIR / f"mandi_rejected_{run_id}.parquet"
    )

    if silver_file.exists() or rejected_file.exists():
        raise FileExistsError(
            f"Transformation outputs already exist for {run_id}"
        )

    parquet = pq.ParquetFile(bronze_file)

    required_source_columns = (
        list(COLUMN_MAP.keys())
        + ["run_id", "source_file", "ingested_at"]
    )

    missing_columns = [
        column
        for column in required_source_columns
        if column not in parquet.schema_arrow.names
    ]

    if missing_columns:
        raise ValueError(
            f"Missing Bronze columns: {missing_columns}"
        )

    logger.info("Starting Silver transformation")
    logger.info("Bronze file: %s", bronze_file.name)
    logger.info("Run ID: %s", run_id)
    logger.info("Batch size: %s", f"{batch_size:,}")

    # Counters.
    total_rows = 0
    accepted_rows = 0
    rejected_rows = 0

    identical_duplicates = 0
    conflicting_duplicates = 0

    valid_writer = None
    rejected_writer = None
    connection = None

    try:

        # --------------------------------------------------
        # SQLITE DUPLICATE TRACKING
        # --------------------------------------------------

        with tempfile.TemporaryDirectory() as temp_dir:

            db_path = Path(temp_dir) / "seen_keys.db"

            connection = sqlite3.connect(
                db_path,
                timeout=60
            )

            # Optimize temporary SQLite database.
            connection.execute("PRAGMA journal_mode = OFF")
            connection.execute("PRAGMA synchronous = OFF")
            connection.execute("PRAGMA temp_store = MEMORY")
            connection.execute("PRAGMA cache_size = -50000")

            connection.execute("""
                CREATE TABLE seen_keys (
                    record_key TEXT PRIMARY KEY,
                    min_price TEXT,
                    max_price TEXT,
                    modal_price TEXT
                ) WITHOUT ROWID
            """)

            # --------------------------------------------------
            # PROCESS PARQUET IN BATCHES
            # --------------------------------------------------

            for batch_number, batch in enumerate(
                parquet.iter_batches(
                    batch_size=batch_size,
                    use_threads=False
                ),
                start=1
            ):

                df = batch.to_pandas()

                # Preserve original source values.
                raw_df = df.copy()

                df = df.rename(columns=COLUMN_MAP)

                # --------------------------------------------------
                # TEXT STANDARDIZATION
                # --------------------------------------------------

                for column in TEXT_COLUMNS:

                    df[column] = (
                        df[column]
                        .astype("string")
                        .str.strip()
                    )

                # --------------------------------------------------
                # NUMERIC CONVERSION
                # --------------------------------------------------

                original_prices = df[
                    PRICE_COLUMNS
                ].copy()

                for column in PRICE_COLUMNS:

                    df[column] = pd.to_numeric(
                        df[column],
                        errors="coerce"
                    )

                # --------------------------------------------------
                # DATE CONVERSION
                # --------------------------------------------------

                df["arrival_date"] = pd.to_datetime(
                    df["arrival_date"],
                    errors="coerce",
                    format="%Y-%m-%d"
                )

                reasons = pd.Series(
                    "",
                    index=df.index,
                    dtype="string"
                )

                def add_reason(mask, reason):

                    nonlocal reasons

                    reasons.loc[mask] = (
                        reasons.loc[mask]
                        + reason
                        + ";"
                    )

                # --------------------------------------------------
                # REQUIRED FIELD VALIDATION
                # --------------------------------------------------

                for column in REQUIRED_COLUMNS:

                    mask = df[column].isna()

                    if column in TEXT_COLUMNS:
                        mask |= df[column].eq("")

                    add_reason(
                        mask,
                        f"missing_{column}"
                    )

                # --------------------------------------------------
                # PRICE VALIDATION
                # --------------------------------------------------

                for column in PRICE_COLUMNS:

                    invalid_numeric = (
                        df[column].isna()
                        & original_prices[column].notna()
                        & original_prices[column]
                            .astype(str)
                            .str.strip()
                            .ne("")
                    )

                    add_reason(
                        invalid_numeric,
                        f"invalid_{column}"
                    )

                    non_finite = (
                        df[column].notna()
                        & ~np.isfinite(df[column])
                    )

                    add_reason(
                        non_finite,
                        f"non_finite_{column}"
                    )

                    non_positive = (
                        df[column].notna()
                        & np.isfinite(df[column])
                        & (df[column] <= 0)
                    )

                    add_reason(
                        non_positive,
                        f"non_positive_{column}"
                    )

                # --------------------------------------------------
                # PRICE ORDERING VALIDATION
                # --------------------------------------------------

                min_modal_invalid = (
                    np.isfinite(df["min_price"])
                    & np.isfinite(df["modal_price"])
                    & (
                        df["min_price"]
                        > df["modal_price"]
                    )
                )

                modal_max_invalid = (
                    np.isfinite(df["modal_price"])
                    & np.isfinite(df["max_price"])
                    & (
                        df["modal_price"]
                        > df["max_price"]
                    )
                )

                add_reason(
                    min_modal_invalid,
                    "min_above_modal"
                )

                add_reason(
                    modal_max_invalid,
                    "modal_above_max"
                )

                # --------------------------------------------------
                # DATE VALIDATION
                # --------------------------------------------------

                today = pd.Timestamp.now().normalize()

                invalid_date = (
                    df["arrival_date"].isna()
                )

                future_date = (
                    df["arrival_date"].notna()
                    & (df["arrival_date"] > today)
                )

                add_reason(
                    invalid_date,
                    "invalid_arrival_date"
                )

                add_reason(
                    future_date,
                    "future_arrival_date"
                )

                # --------------------------------------------------
                # BUILD NATURAL KEYS
                # --------------------------------------------------

                row_keys = []
                price_signatures = []
                unique_keys = []

                key_values = df[
                    DUPLICATE_KEY
                ].itertuples(
                    index=False,
                    name=None
                )

                price_values = df[
                    PRICE_COLUMNS
                ].itertuples(
                    index=False,
                    name=None
                )

                for values, prices in zip(
                    key_values,
                    price_values
                ):

                    if any(
                        pd.isna(value)
                        for value in values
                    ):

                        row_keys.append(None)
                        price_signatures.append(None)
                        continue

                    if any(
                        str(value).strip() == ""
                        for value in values
                    ):

                        row_keys.append(None)
                        price_signatures.append(None)
                        continue

                    record_key = normalize_key(values)

                    signature = tuple(
                        normalize_price(value)
                        for value in prices
                    )

                    row_keys.append(record_key)
                    price_signatures.append(signature)

                    unique_keys.append(record_key)

                # Remove duplicate lookup keys.
                unique_keys = list(
                    dict.fromkeys(unique_keys)
                )

                # --------------------------------------------------
                # FETCH EXISTING KEYS FROM SQLITE
                # --------------------------------------------------

                canonical_signatures = {}

                for start in range(
                    0,
                    len(unique_keys),
                    900
                ):

                    key_chunk = unique_keys[
                        start:start + 900
                    ]

                    placeholders = ",".join(
                        "?" for _ in key_chunk
                    )

                    query = f"""
                        SELECT
                            record_key,
                            min_price,
                            max_price,
                            modal_price
                        FROM seen_keys
                        WHERE record_key IN ({placeholders})
                    """

                    rows = connection.execute(
                        query,
                        key_chunk
                    ).fetchall()

                    for row in rows:

                        canonical_signatures[row[0]] = (
                            row[1],
                            row[2],
                            row[3]
                        )

                # --------------------------------------------------
                # CLASSIFY DUPLICATES
                # --------------------------------------------------

                new_records = []

                identical_mask = pd.Series(
                    False,
                    index=df.index
                )

                conflicting_mask = pd.Series(
                    False,
                    index=df.index
                )

                for index, record_key, signature in zip(
                    df.index,
                    row_keys,
                    price_signatures
                ):

                    if record_key is None:
                        continue

                    if record_key in canonical_signatures:

                        if (
                            signature
                            == canonical_signatures[record_key]
                        ):

                            identical_mask.loc[index] = True

                        else:

                            conflicting_mask.loc[index] = True

                    else:

                        canonical_signatures[record_key] = (
                            signature
                        )

                        new_records.append((
                            record_key,
                            *signature
                        ))

                # --------------------------------------------------
                # BULK INSERT INTO SQLITE
                # --------------------------------------------------

                if new_records:

                    connection.executemany(
                        """
                        INSERT OR IGNORE INTO seen_keys
                        VALUES (?, ?, ?, ?)
                        """,
                        new_records
                    )

                    connection.commit()

                # --------------------------------------------------
                # ADD DUPLICATE REJECTION REASONS
                # --------------------------------------------------

                add_reason(
                    identical_mask,
                    "duplicate_identical_prices"
                )

                add_reason(
                    conflicting_mask,
                    "conflicting_duplicate_prices"
                )

                # --------------------------------------------------
                # SEPARATE ACCEPTED AND REJECTED RECORDS
                # --------------------------------------------------

                rejected_mask = reasons.ne("")

                accepted = df.loc[
                    ~rejected_mask
                ].copy()

                rejected = df.loc[
                    rejected_mask
                ].copy()

                rejected["rejection_reason"] = (
                    reasons.loc[rejected_mask]
                    .str.rstrip(";")
                )

                # Preserve original source columns.
                raw_rejected = raw_df.loc[
                    rejected_mask
                ].copy()

                raw_rejected.columns = [
                    f"raw_{column.lower()}"
                    for column in raw_rejected.columns
                ]

                rejected = pd.concat(
                    [rejected, raw_rejected],
                    axis=1
                )

                # --------------------------------------------------
                # WRITE SILVER PARQUET
                # --------------------------------------------------

                if not accepted.empty:

                    table = pa.Table.from_pandas(
                        accepted,
                        preserve_index=False
                    )

                    if valid_writer is None:

                        valid_writer = pq.ParquetWriter(
                            silver_file,
                            table.schema
                        )

                    valid_writer.write_table(table)

                # --------------------------------------------------
                # WRITE REJECTED PARQUET
                # --------------------------------------------------

                if not rejected.empty:

                    table = pa.Table.from_pandas(
                        rejected,
                        preserve_index=False
                    )

                    if rejected_writer is None:

                        rejected_writer = pq.ParquetWriter(
                            rejected_file,
                            table.schema
                        )

                    rejected_writer.write_table(table)

                # --------------------------------------------------
                # UPDATE COUNTERS
                # --------------------------------------------------

                total_rows += len(df)
                accepted_rows += len(accepted)
                rejected_rows += len(rejected)

                identical_duplicates += int(
                    identical_mask.sum()
                )

                conflicting_duplicates += int(
                    conflicting_mask.sum()
                )

                logger.info(
                    "Batch %s | Processed: %s | Accepted: %s "
                    "| Rejected: %s | Identical duplicates: %s "
                    "| Conflicting duplicates: %s",
                    batch_number,
                    f"{total_rows:,}",
                    f"{accepted_rows:,}",
                    f"{rejected_rows:,}",
                    f"{identical_duplicates:,}",
                    f"{conflicting_duplicates:,}"
                )

            connection.close()
            connection = None

        # --------------------------------------------------
        # CLOSE PARQUET WRITERS
        # --------------------------------------------------

        if valid_writer is not None:
            valid_writer.close()
            valid_writer = None

        if rejected_writer is not None:
            rejected_writer.close()
            rejected_writer = None

        # --------------------------------------------------
        # VERIFY OUTPUT COUNTS
        # --------------------------------------------------

        if total_rows != accepted_rows + rejected_rows:

            raise RuntimeError(
                "Transformation count mismatch: "
                "total rows do not equal accepted plus rejected."
            )

        if total_rows != parquet.metadata.num_rows:

            raise RuntimeError(
                "Bronze and transformation row counts differ."
            )

        logger.info("=" * 60)
        logger.info("SILVER TRANSFORMATION COMPLETED")
        logger.info("Total rows: %s", f"{total_rows:,}")
        logger.info("Accepted: %s", f"{accepted_rows:,}")
        logger.info("Rejected: %s", f"{rejected_rows:,}")
        logger.info(
            "Identical duplicates: %s",
            f"{identical_duplicates:,}"
        )
        logger.info(
            "Conflicting duplicates: %s",
            f"{conflicting_duplicates:,}"
        )
        logger.info("=" * 60)

        return {
            "total_rows": total_rows,
            "accepted_rows": accepted_rows,
            "rejected_rows": rejected_rows,
            "identical_duplicates": identical_duplicates,
            "conflicting_duplicates": conflicting_duplicates,
            "silver_file": silver_file,
            "rejected_file": rejected_file,
        }

    except BaseException:

        # Close any open resources.
        if connection is not None:
            connection.close()
            connection = None

        if valid_writer is not None:
            valid_writer.close()
            valid_writer = None

        if rejected_writer is not None:
            rejected_writer.close()
            rejected_writer = None

        # Remove incomplete outputs.
        if silver_file.exists():
            silver_file.unlink()

        if rejected_file.exists():
            rejected_file.unlink()

        raise


# --------------------------------------------------
# STANDALONE EXECUTION
# --------------------------------------------------

if __name__ == "__main__":

    result = transform_mandi_data()

    print("\n========== SILVER SUMMARY ==========")

    for key, value in result.items():
        print(f"{key}: {value}")

    print("====================================")
