
-- BRONZE: Original source records stored as text

CREATE TABLE IF NOT EXISTS bronze.raw_mandi_prices (
    state TEXT,
    district TEXT,
    market TEXT,
    commodity TEXT,
    variety TEXT,
    grade TEXT,
    arrival_date TEXT,
    min_price TEXT,
    max_price TEXT,
    modal_price TEXT,
    commodity_code TEXT,
    run_id VARCHAR(50),
    source_file VARCHAR(255),
    ingested_at TIMESTAMP
);

-- SILVER: Cleaned and validated records

CREATE TABLE IF NOT EXISTS silver.mandi_prices_clean (
    state TEXT NOT NULL,
    district TEXT NOT NULL,
    market TEXT NOT NULL,
    commodity TEXT NOT NULL,
    variety TEXT,
    grade TEXT,
    arrival_date DATE NOT NULL,
    min_price NUMERIC(12,2) NOT NULL,
    max_price NUMERIC(12,2) NOT NULL,
    modal_price NUMERIC(12,2) NOT NULL,
    commodity_code TEXT,
    run_id VARCHAR(50),
    source_file VARCHAR(255),
    ingested_at TIMESTAMP
);

-- SILVER: Rejected records with original source values

CREATE TABLE IF NOT EXISTS silver.rejected_records (
    rejection_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    state TEXT,
    district TEXT,
    market TEXT,
    commodity TEXT,
    variety TEXT,
    grade TEXT,
    arrival_date TEXT,
    min_price TEXT,
    max_price TEXT,
    modal_price TEXT,
    commodity_code TEXT,
    run_id VARCHAR(50),
    source_file VARCHAR(255),
    ingested_at TIMESTAMP,
    rejection_reason TEXT NOT NULL
);
