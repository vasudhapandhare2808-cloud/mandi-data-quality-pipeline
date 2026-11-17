
CREATE TABLE IF NOT EXISTS gold.dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE UNIQUE NOT NULL,
    day INTEGER NOT NULL,
    month INTEGER NOT NULL,
    month_name VARCHAR(15) NOT NULL,
    quarter INTEGER NOT NULL,
    year INTEGER NOT NULL,
    day_of_week VARCHAR(15) NOT NULL
);

CREATE TABLE IF NOT EXISTS gold.dim_market (
    market_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    state TEXT NOT NULL,
    district TEXT NOT NULL,
    market TEXT NOT NULL,
    UNIQUE (state, district, market)
);

CREATE TABLE IF NOT EXISTS gold.dim_commodity (
    commodity_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    commodity TEXT NOT NULL,
    variety TEXT,
    grade TEXT,
    commodity_code TEXT,
    UNIQUE NULLS NOT DISTINCT
        (commodity, variety, grade, commodity_code)
);

CREATE TABLE IF NOT EXISTS gold.fact_market_prices (
    price_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    date_key INTEGER NOT NULL REFERENCES gold.dim_date(date_key),
    market_key BIGINT NOT NULL REFERENCES gold.dim_market(market_key),
    commodity_key BIGINT NOT NULL REFERENCES gold.dim_commodity(commodity_key),
    min_price NUMERIC(12,2) NOT NULL,
    max_price NUMERIC(12,2) NOT NULL,
    modal_price NUMERIC(12,2) NOT NULL,
    run_id VARCHAR(50),
    source_file VARCHAR(255),
    ingested_at TIMESTAMP
);
