
import os
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text


# --------------------------------------------------
# CONFIGURATION
# --------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]

load_dotenv(PROJECT_ROOT / ".env")

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise ValueError("DATABASE_URL not found in .env")

engine = create_engine(DATABASE_URL, pool_pre_ping=True)


# --------------------------------------------------
# START A NEW PIPELINE RUN
# --------------------------------------------------

def start_run(source_file):

    run_id = datetime.now(timezone.utc).strftime(
        "%Y%m%d_%H%M%S_%f"
    )

    with engine.begin() as connection:

        connection.execute(
            text("""
                INSERT INTO ops.pipeline_runs (
                    run_id,
                    source_file,
                    start_time,
                    status
                )
                VALUES (
                    :run_id,
                    :source_file,
                    CURRENT_TIMESTAMP,
                    'RUNNING'
                )
            """),
            {
                "run_id": run_id,
                "source_file": source_file
            }
        )

    print(f"Pipeline run started: {run_id}")

    return run_id


# --------------------------------------------------
# RESUME A FAILED PIPELINE RUN
# --------------------------------------------------

def resume_run(run_id):

    with engine.begin() as connection:

        result = connection.execute(
            text("""
                UPDATE ops.pipeline_runs
                SET
                    start_time = CURRENT_TIMESTAMP,
                    end_time = NULL,
                    duration_seconds = NULL,
                    status = 'RUNNING',
                    error_message = NULL
                WHERE run_id = :run_id
                  AND status = 'FAILED'
            """),
            {"run_id": run_id}
        )

        if result.rowcount != 1:
            raise ValueError(
                f"Cannot resume run. Run not found or not FAILED: {run_id}"
            )

    print(f"Pipeline run resumed: {run_id}")


# --------------------------------------------------
# COMPLETE A SUCCESSFUL RUN
# --------------------------------------------------

def complete_run(
    run_id,
    total_rows,
    accepted_rows,
    rejected_rows
):

    with engine.begin() as connection:

        result = connection.execute(
            text("""
                UPDATE ops.pipeline_runs
                SET
                    end_time = CURRENT_TIMESTAMP,
                    status = 'SUCCESS',
                    total_rows = :total_rows,
                    accepted_rows = :accepted_rows,
                    rejected_rows = :rejected_rows,
                    duration_seconds = ROUND(
                        EXTRACT(
                            EPOCH FROM (
                                CURRENT_TIMESTAMP - start_time
                            )
                        )::NUMERIC,
                        2
                    )
                WHERE run_id = :run_id
                  AND status = 'RUNNING'
            """),
            {
                "run_id": run_id,
                "total_rows": total_rows,
                "accepted_rows": accepted_rows,
                "rejected_rows": rejected_rows
            }
        )

        if result.rowcount != 1:
            raise ValueError(
                f"No active pipeline run found: {run_id}"
            )

    print(f"Pipeline run completed: {run_id}")


# --------------------------------------------------
# RECORD A FAILED RUN
# --------------------------------------------------

def fail_run(run_id, error_message):

    with engine.begin() as connection:

        result = connection.execute(
            text("""
                UPDATE ops.pipeline_runs
                SET
                    end_time = CURRENT_TIMESTAMP,
                    status = 'FAILED',
                    duration_seconds = ROUND(
                        EXTRACT(
                            EPOCH FROM (
                                CURRENT_TIMESTAMP - start_time
                            )
                        )::NUMERIC,
                        2
                    ),
                    error_message = :error_message
                WHERE run_id = :run_id
                  AND status = 'RUNNING'
            """),
            {
                "run_id": run_id,
                "error_message": str(error_message)[:5000]
            }
        )

        if result.rowcount != 1:
            raise ValueError(
                f"No active pipeline run found: {run_id}"
            )

    print(f"Pipeline run failed: {run_id}")


# --------------------------------------------------
# CLEANUP
# --------------------------------------------------

def close_connection():
    engine.dispose()
