
import os
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text


# Configuration
PROJECT_ROOT = Path(__file__).resolve().parents[2]

load_dotenv(PROJECT_ROOT / ".env")

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise ValueError("DATABASE_URL not found in .env")

engine = create_engine(DATABASE_URL, pool_pre_ping=True)




# Warehouse layers to reconcile
RECONCILIATION_QUERIES = {
    "bronze": "SELECT COUNT(*) FROM bronze.raw_mandi_prices",
    "silver_accepted": "SELECT COUNT(*) FROM silver.mandi_prices_clean",
    "silver_rejected": "SELECT COUNT(*) FROM silver.rejected_records",
    "gold_fact": "SELECT COUNT(*) FROM gold.fact_market_prices",
}


def run_reconciliation(run_id):

    print("\n" + "=" * 65)
    print("MANDI PIPELINE RECONCILIATION REPORT")
    print("=" * 65)

    results = {}

    with engine.begin() as connection:

        for layer_name, query in RECONCILIATION_QUERIES.items():

            row_count = connection.execute(
                text(query)
            ).scalar_one()

            results[layer_name] = row_count

            connection.execute(
                text("""
                    INSERT INTO ops.reconciliation (
                        run_id,
                        layer_name,
                        row_count,
                        recorded_at
                    )
                    VALUES (
                        :run_id,
                        :layer_name,
                        :row_count,
                        CURRENT_TIMESTAMP
                    )
                    ON CONFLICT (run_id, layer_name)
                    DO UPDATE SET
                        row_count = EXCLUDED.row_count,
                        recorded_at = CURRENT_TIMESTAMP
                """),
                {
                    "run_id": run_id,
                    "layer_name": layer_name,
                    "row_count": row_count
                }
            )

            print(f"{layer_name:<20} | {row_count:>12,} rows")

    bronze = results["bronze"]
    silver_total = (
        results["silver_accepted"]
        + results["silver_rejected"]
    )
    gold = results["gold_fact"]

    bronze_silver_pass = bronze == silver_total
    silver_gold_pass = (
        results["silver_accepted"] == gold
    )

    print("-" * 65)

    print(
        "Bronze-Silver:",
        "PASS" if bronze_silver_pass else "FAIL"
    )

    print(
        "Silver-Gold:",
        "PASS" if silver_gold_pass else "FAIL"
    )

    print("\nReconciliation results saved to ops.reconciliation")
    print("=" * 65)

    return bronze_silver_pass and silver_gold_pass


if __name__ == "__main__":

    try:
        success = run_reconciliation()
        engine.dispose()

        if not success:
            raise SystemExit(1)

    except Exception as error:
        engine.dispose()
        print(f"Reconciliation failed: {error}")
        raise SystemExit(1)
