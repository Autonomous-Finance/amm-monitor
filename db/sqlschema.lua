local sqlschema = {}

-- ==================== TABLES & VIES ==================== --

sqlschema.create_transactions_table = [[
CREATE TABLE IF NOT EXISTS amm_transactions (
    id TEXT NOT NULL PRIMARY KEY,
    source TEXT NOT NULL CHECK (source IN ('gateway', 'message')),
    block_height INTEGER NOT NULL,
    block_id TEXT,
    sender TEXT NOT NULL,
    created_at_ts INTEGER,
    to_token TEXT NOT NULL,
    from_token TEXT NOT NULL,
    from_quantity TEXT NOT NULL,
    to_quantity TEXT NOT NULL,
    fee_percentage TEXT NOT NULL,
    reserves_token_a TEXT,
    reserves_token_b TEXT,
    amm_process TEXT NOT NULL,
    from_token_usd_price NUMERIC,
    to_token_usd_price NUMERIC
);
]]

--[[
  Records changes in the swap configuration of an AMM
  i.e. the factors that AFFECT THE PRICE of the next swap
  - changes in pool reserves (due to swaps or liquidity add/remove actions)
  - changes in pool fees
]]
sqlschema.create_amm_swap_params_changes_table = [[
CREATE TABLE IF NOT EXISTS amm_swap_params_changes (
    id TEXT NOT NULL PRIMARY KEY,
    source TEXT NOT NULL CHECK (source IN ('gateway', 'message')),
    block_height INTEGER NOT NULL,
    block_id TEXT,
    sender TEXT NOT NULL,
    created_at_ts INTEGER,
    cause TEXT NOT NULL CHECK (cause IN ('swap', 'swap-params-change')),
    reserves_0 TEXT NOT NULL,
    reserves_1 TEXT NOT NULL,
    fee_percentage TEXT NOT NULL,
    amm_process TEXT NOT NULL
);
]]

sqlschema.create_token_supply_changes_table = [[
CREATE TABLE IF NOT EXISTS token_supply_changes (
    id TEXT NOT NULL PRIMARY KEY,
    block_height INTEGER NOT NULL,
    block_id TEXT,
    supply_changed_at_ts INTEGER,
    token TEXT NOT NULL,
    total_supply TEXT NOT NULL
);
]]

--[[
  ! DEXI v1 - amm_base_token and amm_quote_token
  These are determined by DEXI according to DEXI's business logic, they are not defined as such by the AMMs.

  The labels 'quote' and 'base' are necessary in the context of market cap calculations.

  - in DEXI v1, BRK is the quote token for market cap calculations
  - we cannot assume that any offset token0 or token1 will be BRK, since we allow for non-bark pairs to be registered
  - when BRK is one of the tokens, we also cannot assume which of them (0 or 1) it will be
  - amms of non-BRK pairs are registered here WITHOUT base and quote token and are subsequently NOT INCLUDED in our market cap calculations

    ==> we always check for equality with BRK when registering an AMM, thereby
      - determining which is base and which is quote
      - ensuring we can filter out pools that don't enter the market cap calculation at all (non-BRK pairs)
]]
sqlschema.create_amm_registry_table = [[
CREATE TABLE IF NOT EXISTS amm_registry (
    amm_process TEXT NOT NULL PRIMARY KEY,
    amm_name TEXT NOT NULL,
    amm_token0 TEXT NOT NULL,
    amm_token1 TEXT NOT NULL,
    amm_base_token TEXT,
    amm_quote_token TEXT,
    amm_discovered_at_ts INTEGER
);
]]

-- table rather than view, since this will both change and be queried very frequently
sqlschema.create_amm_swap_params_table = [[
CREATE TABLE IF NOT EXISTS amm_swap_params (
    amm_process TEXT NOT NULL PRIMARY KEY,
    reserves_0 TEXT NOT NULL,
    reserves_1 TEXT NOT NULL,
    fee_percentage TEXT NOT NULL
);
]]

sqlschema.create_token_registry_table = [[
CREATE TABLE IF NOT EXISTS token_registry (
    token_process TEXT NOT NULL PRIMARY KEY,
    token_name TEXT NOT NULL,
    token_ticker TEXT NOT NULL,
    denominator INT NOT NULL,
    total_supply INT NOT NULL,
    fixed_supply BOOL NOT NULL,
    token_updated_at_ts INTEGER,
    token_discovered_at_ts INTEGER
);
]]

sqlschema.create_balances_table = [[
CREATE TABLE IF NOT EXISTS balances (
    process_id TEXT NOT NULL PRIMARY KEY,
    balance TEXT NOT NULL
);
]]

sqlschema.create_indicator_subscriptions_table = [[
CREATE TABLE IF NOT EXISTS indicator_subscriptions (
    process_id TEXT NOT NULL,
    amm_process_id TEXT NOT NULL,
    PRIMARY KEY (process_id),
    UNIQUE (process_id, amm_process_id)
);
]]


sqlschema.create_oracle_prices_table = [[
CREATE TABLE IF NOT EXISTS oracle_prices (
    process_id VARCHAR NOT NULL,
    ticker VARCHAR NOT NULL,
    price NUMERIC NOT NULL,
    last_update TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    PRIMARY KEY (process_id)
);
]]

sqlschema.create_top_n_subscriptions_table = [[
CREATE TABLE IF NOT EXISTS top_n_subscriptions (
    process_id TEXT NOT NULL,
    quote_token TEXT NOT NULL,
    top_n INTEGER NOT NULL,
    token_set TEXT NOT NULL DEFAULT '[]' CHECK (json_valid(token_set)),
    PRIMARY KEY (process_id),
    UNIQUE (process_id, quote_token)
);
]]


sqlschema.create_swap_subscription_table = [[
CREATE TABLE IF NOT EXISTS swap_subscriptions (
    process_id TEXT NOT NULL,
    amm_process_id TEXT NOT NULL,
    subscribed_at_ts INTEGER NOT NULL,
    PRIMARY KEY (process_id, amm_process_id)
);
]]


sqlschema.create_swap_params_subscription_table = [[
CREATE TABLE IF NOT EXISTS swap_params_subscriptions (
    process_id TEXT NOT NULL,
    amm_process_id TEXT NOT NULL,
    subscribed_at_ts INTEGER NOT NULL,
    PRIMARY KEY (process_id, amm_process_id)
);
]]

sqlschema.create_transactions_view = [[
CREATE VIEW amm_transactions_view AS
SELECT
  id,
  source,
  block_height,
  block_id,
  sender,
  created_at_ts,
  to_token,
  from_token,
  from_quantity,
  to_quantity,
  fee_percentage as fee,
  amm_process,
  CASE WHEN to_token = amm_token1 THEN 1 ELSE 0 END AS is_buy,
  ROUND(CASE
    WHEN from_quantity > 0 AND to_quantity > 0 THEN
      CASE
        WHEN to_token = amm_token1 THEN
          (from_quantity * 1.0 / to_quantity) * POWER(10, ABS(t0.denominator - tq.denominator))
        ELSE
          (to_quantity * 1.0 / from_quantity) * POWER(10, ABS(t0.denominator - tq.denominator))
      END
    ELSE NULL
  END, 12) AS price,
  ROUND(CASE
    WHEN from_quantity > 0 AND to_quantity > 0 THEN
      CASE
        WHEN to_token = amm_token1 THEN
          (from_quantity * 1.0 / to_quantity) * POWER(10, ABS(t0.denominator - tq.denominator)) * from_token_usd_price
        ELSE
          (to_quantity * 1.0 / from_quantity) * POWER(10, ABS(t0.denominator - tq.denominator)) * to_token_usd_price
      END
    ELSE NULL
  END, 5) AS usd_price,
  (CASE
    WHEN to_token = amm_token1 THEN from_quantity
    ELSE to_quantity
  END) * 1.0 / POWER(10, t0.denominator) AS volume,
  (CASE
    WHEN to_token = amm_token1
      THEN from_quantity  * 1.0 / POWER(10, t0.denominator) * from_token_usd_price
      ELSE to_quantity  * 1.0 / POWER(10, t0.denominator) * to_token_usd_price
  END) AS volume_usd,
  POWER(10, ABS(t0.denominator - tq.denominator)) AS denominator_conversion,
  t0.denominator AS quote_denominator,
  tq.denominator AS base_denominator,
  amm_token0 as quote_token_process,
  amm_token1 as base_token_process,
  amm_token0,
  amm_token1,
  t0.token_name as quote_token_name,
  tq.token_name as base_token_name,
  reserves_token_a AS reserves_0,
  reserves_token_b AS reserves_1
FROM amm_transactions
LEFT JOIN amm_registry USING (amm_process)
LEFT JOIN token_registry t0 ON t0.token_process = amm_token0
LEFT JOIN token_registry tq ON tq.token_process = amm_token1
]]


--! only includes token pairs with QUOTE_TOKEN
sqlschema.create_market_cap_view = [[
CREATE VIEW amm_market_cap_view AS
WITH current_prices AS (
  SELECT
    amm_process,
    (SELECT price FROM amm_transactions_view WHERE amm_process = r.amm_process ORDER BY created_at_ts DESC LIMIT 1) AS price
  FROM amm_registry r
)
SELECT
  r.amm_base_token AS token_process,
  t.total_supply * cp.price AS market_cap,
  r.amm_quote_token AS quote_token_process,
  rank() OVER (ORDER BY t.total_supply * cp.price DESC) AS market_cap_rank
FROM amm_registry r
LEFT JOIN token_registry t ON t.token_process = r.amm_base_token
LEFT JOIN current_prices cp ON cp.amm_process = r.amm_process
WHERE r.amm_quote_token IS NOT NULL
ORDER BY market_cap DESC
LIMIT 100
]]

return sqlschema
