CREATE TABLE IF NOT EXISTS crypto_prices_scd2 (
    id VARCHAR(50),
    symbol VARCHAR(20),
    name VARCHAR(100),
    current_price DOUBLE PRECISION,
    market_cap BIGINT,
    last_updated TIMESTAMP,
    price_change_percentage_24h DOUBLE PRECISION,
    is_high_value BOOLEAN,
    trend VARCHAR(20),
    load_timestamp TIMESTAMP,
    eff_start_date TIMESTAMP,
    eff_end_date TIMESTAMP,
    is_current BOOLEAN
);