
ALTER TABLE silver.rejected_records
    ADD COLUMN IF NOT EXISTS raw_state TEXT,
    ADD COLUMN IF NOT EXISTS raw_district TEXT,
    ADD COLUMN IF NOT EXISTS raw_market TEXT,
    ADD COLUMN IF NOT EXISTS raw_commodity TEXT,
    ADD COLUMN IF NOT EXISTS raw_variety TEXT,
    ADD COLUMN IF NOT EXISTS raw_grade TEXT,
    ADD COLUMN IF NOT EXISTS raw_arrival_date TEXT,
    ADD COLUMN IF NOT EXISTS raw_min_price TEXT,
    ADD COLUMN IF NOT EXISTS raw_max_price TEXT,
    ADD COLUMN IF NOT EXISTS raw_modal_price TEXT,
    ADD COLUMN IF NOT EXISTS raw_commodity_code TEXT,
    ADD COLUMN IF NOT EXISTS raw_run_id TEXT,
    ADD COLUMN IF NOT EXISTS raw_source_file TEXT,
    ADD COLUMN IF NOT EXISTS raw_ingested_at TEXT;
