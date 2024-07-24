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
    amm_process TEXT NOT NULL,
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
    token TEXT NOT NULL,
    reserves_0 TEXT NOT NULL,
    reserves_1 TEXT NOT NULL,
    fee_percentage TEXT NOT NULL,
    amm_process TEXT NOT NULL,
);
]]

sqlschema.create_token_supply_changes_table = [[
CREATE TABLE IF NOT EXISTS token_supply_changes (
    id TEXT NOT NULL PRIMARY KEY,
    block_height INTEGER NOT NULL,
    block_id TEXT,
    supply_changed_at_ts INTEGER,
    token TEXT NOT NULL,
    total_supply TEXT NOT NULL,
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
    token_0 TEXT NOT NULL,
    token_1 TEXT NOT NULL,
    reserves_0 TEXT NOT NULL,
    reserves_1 TEXT NOT NULL,
    fee_percentage TEXT NOT NULL
);
]]

sqlschema.create_token_registry_table = [[
CREATE TABLE IF NOT EXISTS token_registry (
    token_process TEXT NOT NULL PRIMARY KEY,
    token_name TEXT NOT NULL,
    denominator INT NOT NULL,
    total_supply INT NOT NULL,
    fixed_supply BOOL NOT NULL,
    token_updated_at_ts INTEGER,
    token_discovered_at_ts INTEGER
);
]]

sqlschema.create_balances_table = [[
CREATE TABLE IF NOT EXISTS balances (
    owner_id TEXT NOT NULL PRIMARY KEY,
    token_id TEXT NOT NULL,
    balance INT NOT NULL
);
]]

sqlschema.create_indicator_subscriptions_table = [[
CREATE TABLE IF NOT EXISTS indicator_subscriptions (
    process_id TEXT NOT NULL PRIMARY KEY,
    owner_id TEXT NOT NULL,
    amm_process_id TEXT NOT NULL
);
]]

sqlschema.create_top_n_subscriptions_table = [[
CREATE TABLE IF NOT EXISTS top_n_subscriptions (
    process_id TEXT NOT NULL PRIMARY KEY,
    owner_id TEXT NOT NULL,
    quote_token TEXT NOT NULL,
    top_n INTEGER NOT NULL,
    token_set TEXT NOT NULL DEFAULT '[]' CHECK (json_valid(token_set))
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
  CASE
    WHEN to_token = amm_token1 THEN from_quantity
    ELSE to_quantity
  END AS volume,
  POWER(10, ABS(t0.denominator - tq.denominator)) AS denominator_conversion,
  t0.denominator AS quote_denominator,
  tq.denominator AS base_denominator
FROM amm_transactions
LEFT JOIN amm_registry USING (amm_process)
LEFT JOIN token_registry t0 ON t0.token_process = amm_token0
LEFT JOIN token_registry tq ON tq.token_process = amm_token1
]]


--! only includes token pairs with BRK
sqlschema.create_market_cap_view = [[
CREATE VIEW market_cap_view AS
SELECT
  r.amm_base_token AS token_process,
  t.total_supply * current_price AS market_cap,
  r.amm_quote_token AS quote_token_process,
  rank() OVER (ORDER BY t.total_supply * current_price DESC) AS market_cap_rank,
FROM amm_registry r
WHERE r.amm_quote_token IS NOT NULL
LEFT JOIN token_registry t ON t.token_process = r.amm_base_token
ORDER BY market_cap DESC
LIMIT 100
]]


-- TODO functions below to be moved / eliminated once we integrate the subscribable

function sqlschema.registerIndicatorSubscriber(processId, ownerId, ammProcessId)
  local stmt = db:prepare [[
    INSERT INTO indicator_subscriptions (process_id, owner_id, amm_process_id)
    VALUES (:process_id, :owner_id, :amm_process_id)
    ON CONFLICT(process_id) DO UPDATE SET
    owner_id = excluded.owner_id,
    amm_process_id = excluded.amm_process_id;
  ]]
  if not stmt then
    error("Failed to prepare SQL statement for registering process: " .. db:errmsg())
  end
  stmt:bind_names({
    process_id = processId,
    owner_id = ownerId,
    amm_process_id = ammProcessId
  })
  local result, err = stmt:step()
  stmt:finalize()
  if err then
    error("Err: " .. db:errmsg())
  end
end

function sqlschema.registerTopNSubscriber(processId, ownerId, quoteToken, topN)
  local stmt = db:prepare [[
    INSERT INTO top_n_subscriptions (process_id, owner_id, quote_token, top_n)
    VALUES (:process_id, :owner_id, :quote_token, :top_n)
    ON CONFLICT(process_id) DO UPDATE SET
    owner_id = excluded.owner_id,
    quote_token = excluded.quote_token,
    top_n = excluded.top_n;
  ]]
  if not stmt then
    error("Failed to prepare SQL statement for registering process: " .. db:errmsg())
  end
  stmt:bind_names({
    process_id = processId,
    owner_id = ownerId,
    quote_token = quoteToken,
    top_n = topN
  })
  local result, err = stmt:step()
  stmt:finalize()
  if err then
    error("Err: " .. db:errmsg())
  end
end

function sqlschema.updateBalance(ownerId, tokenId, amount, isCredit)
  local stmt = db:prepare [[
    INSERT INTO balances (owner, token_id, balance)
    VALUES (:owner_id, :token_id, :amount)
    ON CONFLICT(owner) DO UPDATE SET
      balance = CASE
        WHEN :is_credit THEN balances.balance + :amount
        ELSE balances.balance - :amount
      END
    WHERE balances.token_id = :token_id;
  ]]
  if not stmt then
    error("Failed to prepare SQL statement for updating balance: " .. db:errmsg())
  end
  stmt:bind_names({
    owner_id = ownerId,
    token_id = tokenId,
    amount = math.abs(amount), -- Ensure amount is positive
    is_credit = isCredit
  })
  local result, err = stmt:step()
  stmt:finalize()
  if err then
    error("Error updating balance: " .. db:errmsg())
  end
end

return sqlschema
