
CREATE TABLE IF NOT EXISTS ops.pipeline_runs (
    run_id VARCHAR(50) PRIMARY KEY,
    source_file VARCHAR(255),
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    status VARCHAR(20) NOT NULL,
    total_rows BIGINT DEFAULT 0,
    accepted_rows BIGINT DEFAULT 0,
    rejected_rows BIGINT DEFAULT 0,
    duration_seconds NUMERIC(12, 2),
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS ops.dq_results (
    dq_result_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id VARCHAR(50) NOT NULL
        REFERENCES ops.pipeline_runs(run_id),
    rule_name VARCHAR(100) NOT NULL,
    rows_checked BIGINT DEFAULT 0,
    rows_failed BIGINT DEFAULT 0,
    failure_percentage NUMERIC(7, 3),
    checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ops.reconciliation (
    reconciliation_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id VARCHAR(50) NOT NULL
        REFERENCES ops.pipeline_runs(run_id),
    layer_name VARCHAR(30) NOT NULL,
    row_count BIGINT NOT NULL,
    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
