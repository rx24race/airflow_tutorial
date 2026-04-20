BEGIN;

-- Step A: Expire records that have changed
UPDATE crypto_prices_scd2 target
SET is_current = False,
    eff_end_date = '{{ params.now }}'
FROM stg_crypto_prices stg
WHERE target.id = stg.id
  AND target.is_current = True
  AND (
      target.current_price <> stg.current_price OR
      target.trend <> stg.trend
  );

-- Step B: Insert new records
INSERT INTO crypto_prices_scd2 (
    id, symbol, name, current_price, market_cap, last_updated, 
    price_change_percentage_24h, is_high_value, trend, load_timestamp,
    eff_start_date, eff_end_date, is_current
)
SELECT 
    stg.id, stg.symbol, stg.name, stg.current_price, stg.market_cap, 
    CAST(stg.last_updated AS TIMESTAMP), stg.price_change_percentage_24h, 
    stg.is_high_value, stg.trend, CAST(stg.load_timestamp AS TIMESTAMP),
    '{{ params.now }}', '{{ params.max_date }}', True
FROM stg_crypto_prices stg
LEFT JOIN crypto_prices_scd2 target 
    ON stg.id = target.id AND target.is_current = True
WHERE target.id IS NULL;

COMMIT;