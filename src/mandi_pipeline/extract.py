
import logging
import re
from pathlib import Path
from datetime import datetime

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


# --------------------------------------------------
# CONFIGURATION
# --------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]

RAW_DATA = PROJECT_ROOT / "data" / "raw"
BRONZE_DATA = PROJECT_ROOT / "data" / "bronze"

BRONZE_DATA.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)


# --------------------------------------------------
# EXTRACT MANDI DATA
# --------------------------------------------------

def extract_mandi_data(year=2025, chunk_size=50000, run_id=None):

    source_file = RAW_DATA / f"{year}.csv"

    if not source_file.exists():
        raise FileNotFoundError(
            f"Dataset not found: {source_file}"
        )

    # Use the orchestrator's run ID when supplied.
    # Otherwise, generate one for standalone execution.
    if run_id is None:
        run_id = datetime.now().strftime(
            "%Y%m%d_%H%M%S_%f"
        )

    # Keep the run ID safe for use in filenames.
    if not re.fullmatch(r"[A-Za-z0-9_-]+", str(run_id)):
        raise ValueError(
            "run_id can contain only letters, numbers, "
            "underscores, and hyphens."
        )

    run_id = str(run_id)

    output_file = (
        BRONZE_DATA / f"mandi_bronze_{run_id}.parquet"
    )

    if output_file.exists():
        raise FileExistsError(
            f"Output file already exists: {output_file}"
        )

    logger.info("Starting extraction: %s", source_file.name)
    logger.info("Pipeline run ID: %s", run_id)

    total_rows = 0
    writer = None

    try:

        for chunk in pd.read_csv(
            source_file,
            chunksize=chunk_size,
            dtype=str
        ):

            chunk["run_id"] = run_id
            chunk["source_file"] = source_file.name
            chunk["ingested_at"] = datetime.now().isoformat()

            table = pa.Table.from_pandas(
                chunk,
                preserve_index=False
            )

            if writer is None:
                writer = pq.ParquetWriter(
                    output_file,
                    table.schema
                )

            writer.write_table(table)

            total_rows += len(chunk)

            logger.info(
                "Processed %s rows",
                f"{total_rows:,}"
            )

        if writer is None:
            raise ValueError(
                f"No records found in {source_file}"
            )

    except Exception:

        if writer is not None:
            writer.close()
            writer = None

        if output_file.exists():
            output_file.unlink()

        raise

    finally:

        if writer is not None:
            writer.close()

    logger.info(
        "Extraction completed: %s rows",
        f"{total_rows:,}"
    )

    logger.info("Bronze file: %s", output_file)

    return output_file, total_rows


# --------------------------------------------------
# STANDALONE EXECUTION
# --------------------------------------------------

if __name__ == "__main__":

    output_file, total_rows = extract_mandi_data()

    print("\nExtraction successful!")
    print("Output file:", output_file)
    print("Total rows:", f"{total_rows:,}")
