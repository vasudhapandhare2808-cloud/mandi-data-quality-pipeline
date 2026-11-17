
-- 1. Populate the date dimension

INSERT INTO gold.dim_date (
    date_key, full_date, day, month,
    month_name, quarter, year, day_of_week
)
SELECT DISTINCT
    TO_CHAR(arrival_date, 'YYYYMMDD')::INTEGER,
    arrival_date,
    EXTRACT(DAY FROM arrival_date)::INTEGER,
    EXTRACT(MONTH FROM arrival_date)::INTEGER,
    TO_CHAR(arrival_date, 'FMMonth'),
    EXTRACT(QUARTER FROM arrival_date)::INTEGER,
    EXTRACT(YEAR FROM arrival_date)::INTEGER,
    TO_CHAR(arrival_date, 'FMDay')
FROM silver.mandi_prices_clean
ON CONFLICT DO NOTHING;


-- 2. Populate the market dimension

INSERT INTO gold.dim_market (
    state, district, market
)
SELECT DISTINCT
    state, district, market
FROM silver.mandi_prices_clean
ON CONFLICT DO NOTHING;


-- 3. Populate the commodity dimension

INSERT INTO gold.dim_commodity (
    commodity, variety, grade, commodity_code
)
SELECT DISTINCT
    commodity, variety, grade, commodity_code
FROM silver.mandi_prices_clean
ON CONFLICT DO NOTHING;


-- 4. Prevent duplicate fact records

CREATE UNIQUE INDEX IF NOT EXISTS
    uq_fact_market_prices_grain
ON gold.fact_market_prices (
    date_key, market_key, commodity_key
);


-- 5. Populate the fact table

INSERT INTO gold.fact_market_prices (
    date_key,
    market_key,
    commodity_key,
    min_price,
    max_price,
    modal_price,
    run_id,
    source_file,
    ingested_at
)
SELECT
    d.date_key,
    m.market_key,
    c.commodity_key,
    s.min_price,
    s.max_price,
    s.modal_price,
    s.run_id,
    s.source_file,
    s.ingested_at::TIMESTAMP
FROM silver.mandi_prices_clean s
JOIN gold.dim_date d
    ON s.arrival_date = d.full_date
JOIN gold.dim_market m
    ON s.state = m.state
    AND s.district = m.district
    AND s.market = m.market
JOIN gold.dim_commodity c
    ON s.commodity = c.commodity
    AND s.variety IS NOT DISTINCT FROM c.variety
    AND s.grade IS NOT DISTINCT FROM c.grade
    AND s.commodity_code IS NOT DISTINCT FROM c.commodity_code
ON CONFLICT (date_key, market_key, commodity_key)
DO NOTHING;
