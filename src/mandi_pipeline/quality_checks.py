
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text


PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise ValueError("DATABASE_URL not found in .env")

engine = create_engine(DATABASE_URL, pool_pre_ping=True)



QUALITY_CHECKS = [
    (
        "Bronze-Silver reconciliation",
        """
        SELECT ABS(
            (SELECT COUNT(*) FROM bronze.raw_mandi_prices)
            -
            (
                (SELECT COUNT(*) FROM silver.mandi_prices_clean)
                +
                (SELECT COUNT(*) FROM silver.rejected_records)
            )
        )
        """,
        "bronze.raw_mandi_prices"
    ),
    (
        "Missing mandatory fields in Silver",
        """
        SELECT COUNT(*)
        FROM silver.mandi_prices_clean
        WHERE state IS NULL OR TRIM(state) = ''
           OR district IS NULL OR TRIM(district) = ''
           OR market IS NULL OR TRIM(market) = ''
           OR commodity IS NULL OR TRIM(commodity) = ''
           OR arrival_date IS NULL
           OR min_price IS NULL
           OR max_price IS NULL
           OR modal_price IS NULL
        """,
        "silver.mandi_prices_clean"
    ),
    (
        "Non-positive prices in Silver",
        """
        SELECT COUNT(*)
        FROM silver.mandi_prices_clean
        WHERE min_price <= 0
           OR max_price <= 0
           OR modal_price <= 0
        """,
        "silver.mandi_prices_clean"
    ),
    (
        "Invalid price ranges in Silver",
        """
        SELECT COUNT(*)
        FROM silver.mandi_prices_clean
        WHERE min_price > modal_price
           OR modal_price > max_price
           OR min_price > max_price
        """,
        "silver.mandi_prices_clean"
    ),
    (
        "Silver-Gold fact reconciliation",
        """
        SELECT ABS(
            (SELECT COUNT(*) FROM silver.mandi_prices_clean)
            -
            (SELECT COUNT(*) FROM gold.fact_market_prices)
        )
        """,
        "silver.mandi_prices_clean"
    ),
    (
        "Duplicate records in Gold fact table",
        """
        SELECT COUNT(*)
        FROM (
            SELECT date_key, market_key, commodity_key
            FROM gold.fact_market_prices
            GROUP BY date_key, market_key, commodity_key
            HAVING COUNT(*) > 1
        ) duplicates
        """,
        "gold.fact_market_prices"
    ),
    (
        "Missing dimension references in Gold",
        """
        SELECT COUNT(*)
        FROM gold.fact_market_prices f
        LEFT JOIN gold.dim_date d
            ON f.date_key = d.date_key
        LEFT JOIN gold.dim_market m
            ON f.market_key = m.market_key
        LEFT JOIN gold.dim_commodity c
            ON f.commodity_key = c.commodity_key
        WHERE d.date_key IS NULL
           OR m.market_key IS NULL
           OR c.commodity_key IS NULL
        """,
        "gold.fact_market_prices"
    ),
]


def run_quality_checks(run_id):

    print("\n" + "=" * 75)
    print("MANDI DATA QUALITY REPORT")
    print("=" * 75)

    passed = 0
    failed = 0

    with engine.begin() as connection:

        for rule_name, query, source_table in QUALITY_CHECKS:

            violations = connection.execute(
                text(query)
            ).scalar_one()

            rows_checked = connection.execute(
                text(f"SELECT COUNT(*) FROM {source_table}")
            ).scalar_one()

            failure_percentage = (
                violations * 100.0 / rows_checked
                if rows_checked else 0
            )

            status = "PASS" if violations == 0 else "FAIL"

            if status == "PASS":
                passed += 1
            else:
                failed += 1

            connection.execute(
                text("""
                    INSERT INTO ops.dq_results (
                        run_id,
                        rule_name,
                        rows_checked,
                        rows_failed,
                        failure_percentage,
                        checked_at
                    )
                    VALUES (
                        :run_id,
                        :rule_name,
                        :rows_checked,
                        :rows_failed,
                        :failure_percentage,
                        CURRENT_TIMESTAMP
                    )
                    ON CONFLICT (run_id, rule_name)
                    DO UPDATE SET
                        rows_checked = EXCLUDED.rows_checked,
                        rows_failed = EXCLUDED.rows_failed,
                        failure_percentage = EXCLUDED.failure_percentage,
                        checked_at = CURRENT_TIMESTAMP
                """),
                {
                    "run_id": run_id,
                    "rule_name": rule_name,
                    "rows_checked": rows_checked,
                    "rows_failed": violations,
                    "failure_percentage": round(
                        failure_percentage, 3
                    )
                }
            )

            print(
                f"{status:<8} | {rule_name:<42} "
                f"| Violations: {violations:,}"
            )

    print("-" * 75)
    print(f"Total checks : {len(QUALITY_CHECKS)}")
    print(f"Passed       : {passed}")
    print(f"Failed       : {failed}")
    print("Results saved to ops.dq_results")

    return failed == 0


if __name__ == "__main__":

    try:
        success = run_quality_checks()
        engine.dispose()

        if not success:
            sys.exit(1)

    except Exception as error:
        engine.dispose()
        print(f"\nQuality check execution failed: {error}")
        sys.exit(1)
