
-- 1. Record the successful pipeline run

INSERT INTO ops.pipeline_runs (
    run_id,
    source_file,
    start_time,
    status,
    total_rows,
    accepted_rows,
    rejected_rows
)
SELECT
    '20261114_221025',
    '2025.csv',
    MIN(ingested_at),
    'SUCCESS',
    5819482,
    5811082,
    8400
FROM bronze.raw_mandi_prices
ON CONFLICT (run_id) DO NOTHING;


-- 2. Record reconciliation counts

INSERT INTO ops.reconciliation (
    run_id, layer_name, row_count
)
VALUES
    ('20261114_221025', 'bronze', 5819482),
    ('20261114_221025', 'silver_accepted', 5811082),
    ('20261114_221025', 'silver_rejected', 8400),
    ('20261114_221025', 'gold_fact', 5811082);


-- 3. Record data quality results

INSERT INTO ops.dq_results (
    run_id,
    rule_name,
    rows_checked,
    rows_failed,
    failure_percentage
)
SELECT
    '20261114_221025',
    rule_name,
    5819482,
    failed_rows,
    ROUND(
        failed_rows * 100.0 / 5819482,
        3
    )
FROM (
    SELECT
        'non_positive_min_price' AS rule_name,
        COUNT(*) AS failed_rows
    FROM silver.rejected_records
    WHERE rejection_reason LIKE '%non_positive_min_price%'

    UNION ALL

    SELECT
        'non_positive_max_price',
        COUNT(*)
    FROM silver.rejected_records
    WHERE rejection_reason LIKE '%non_positive_max_price%'

    UNION ALL

    SELECT
        'min_above_modal',
        COUNT(*)
    FROM silver.rejected_records
    WHERE rejection_reason LIKE '%min_above_modal%'

    UNION ALL

    SELECT
        'modal_above_max',
        COUNT(*)
    FROM silver.rejected_records
    WHERE rejection_reason LIKE '%modal_above_max%'

    UNION ALL

    SELECT
        'duplicate_identical_prices',
        COUNT(*)
    FROM silver.rejected_records
    WHERE rejection_reason LIKE '%duplicate_identical_prices%'

    UNION ALL

    SELECT
        'conflicting_duplicate_prices',
        COUNT(*)
    FROM silver.rejected_records
    WHERE rejection_reason LIKE '%conflicting_duplicate_prices%'
) AS quality_counts;
