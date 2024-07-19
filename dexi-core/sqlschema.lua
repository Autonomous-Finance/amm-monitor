local sqlschema = {}

sqlschema.create_transactions_table = [[
CREATE TABLE IF NOT EXISTS amm_transactions (
    id TEXT PRIMARY KEY,
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
    id TEXT PRIMARY KEY,
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

sqlschema.create_amm_registry_table = [[
CREATE TABLE IF NOT EXISTS amm_registry (
    amm_process TEXT PRIMARY KEY,
    amm_name TEXT NOT NULL,
    amm_token0 TEXT NOT NULL,
    amm_token1 TEXT NOT NULL,
    amm_discovered_at_ts INTEGER
);
]]

sqlschema.create_token_registry_table = [[
CREATE TABLE IF NOT EXISTS token_registry (
    token_process TEXT PRIMARY KEY,
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
    owner_id TEXT PRIMARY KEY,
    token_id TEXT NOT NULL,
    balance INT NOT NULL
);
]]

sqlschema.create_indicator_subscriptions_table = [[
CREATE TABLE IF NOT EXISTS indicator_subscriptions (
    process_id TEXT PRIMARY KEY,
    owner_id TEXT NOT NULL,
    amm_process_id TEXT NOT NULL
);
]]

sqlschema.create_top_n_subscriptions_table = [[
CREATE TABLE IF NOT EXISTS top_n_subscriptions (
    process_id TEXT PRIMARY KEY,
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

sqlschema.create_amm_swap_params_view = [[
CREATE VIEW amm_swap_params_view AS
SELECT
  amm_process,
  reserves_0,
  reserves_1,
  fee_percentage
FROM amm_liquidity_changes
WHERE created_at_ts = (SELECT MAX(created_at_ts) FROM amm_liquidity_changes WHERE amm_process = amm_process)
]]


-- assuming all pairs have the same token0 (quote token), in which the market cap is expressed
sqlschema.create_market_cap_view = [[
CREATE VIEW market_cap_view AS
SELECT
  r.amm_token1 AS token_process,
  t.total_supply * current_price AS market_cap,
FROM amm_registry r
LEFT JOIN token_registry t ON t.token_process = r.amm_token1
ORDER BY market_cap DESC
LIMIT 100
]]

function sqlschema.createTableIfNotExists(db)
  db:exec(sqlschema.create_transactions_table)

  db:exec(sqlschema.create_amm_swap_params_changes_table)

  db:exec(sqlschema.create_amm_registry_table)
  print("Err: " .. db:errmsg())

  db:exec("DROP VIEW IF EXISTS amm_transactions_view;")
  print("Err: " .. db:errmsg())

  db:exec(sqlschema.create_transactions_view)
  print("Err: " .. db:errmsg())

  db:exec("DROP VIEW IF EXISTS amm_swap_params_view;")
  print("Err: " .. db:errmsg())

  db:exec(sqlschema.create_amm_swap_params_view)
  print("Err: " .. db:errmsg())

  db:exec("DROP VIEW IF EXISTS amm_market_cap_view;")
  print("Err: " .. db:errmsg())

  db:exec(sqlschema.create_market_cap_view)
  print("Err: " .. db:errmsg())

  db:exec(sqlschema.create_balances_table)
  print("Err: " .. db:errmsg())

  db:exec(sqlschema.create_indicator_subscriptions_table)
  print("Err: " .. db:errmsg())

  db:exec(sqlschema.create_top_n_subscriptions_table)
  print("Err: " .. db:errmsg())

  db:exec(sqlschema.create_token_registry_table)
  print("Err: " .. db:errmsg())

  sqlschema.updateAMMs()
  sqlschema.updateTokens()
end

function sqlschema.dropAndRecreateTableIfOwner(db)
  db:exec("DROP TABLE IF EXISTS amm_transactions;")
  sqlschema.createTableIfNotExists()
end

function sqlschema.queryMany(stmt)
  local rows = {}
  for row in stmt:nrows() do
    table.insert(rows, row)
  end
  stmt:reset()
  return rows
end

function sqlschema.queryOne(stmt)
  return sqlschema.queryMany(stmt)[1]
end

function sqlschema.rawQuery(query)
  local stmt = db:prepare(query)
  if not stmt then
    error("Err: " .. db:errmsg())
  end
  return sqlschema.queryMany(stmt)
end

function sqlschema.registerAMM(name, processId, token0, token1, discoveredAt)
  print({
    "process", processId,
    "name", name,
    "token0", token0,
    "token1", token1,
  })
  local stmt = db:prepare [[
  INSERT OR REPLACE INTO amm_registry (amm_process, amm_name, amm_token0, amm_token1, amm_discovered_at_ts)
  VALUES
    (:process, :amm_name, :token0, :token1, :discovered_at);
  ]]
  if not stmt then
    error("Err: " .. db:errmsg())
  end
  stmt:bind_names({
    process = processId,
    amm_name = name,
    token0 = token0,
    token1 = token1,
    discovered_at = discoveredAt
  })
  stmt:step()
  print("Err: " .. db:errmsg())
  stmt:reset()
end

function sqlschema.getRegisteredAMMs()
  return sqlschema.rawQuery("SELECT * FROM amm_registry")
end

function sqlschema.getQuoteTokens()
  return sqlschema.rawQuery("SELECT DISTINCT amm_token0 FROM amm_registry")
end

function sqlschema.isQuoteTokenAvailable(token0)
  local stmt = db:prepare("SELECT COUNT(*) FROM amm_registry WHERE amm_token0 = :token0")
  if not stmt then
    error("Err: " .. db:errmsg())
  end
  stmt:bind_names({ token0 = token0 })
  local result = sqlschema.queryOne(stmt)
  return result["COUNT(*)"] > 0
end

function sqlschema.updateAMMs()
  sqlschema.registerAMM('TRUNK/AOCRED', 'vn5lUv8OaevTb45iI_qykad_d9MP69kuYg5mZW1zCHE',
    'Sa0iBLPNyJQrwpTTG-tWLQU-1QeUAJA73DdxGGiKoJc', 'OT9qTE2467gcozb2g8R6D6N3nQS94ENcaAIJfUzHCww', 1712737395)
  sqlschema.registerAMM('0rbit/AOCRED', '2bKo3vwB1Mo5TItmxuUQzZ11JgKauU_n2IZO1G13AIk',
    'Sa0iBLPNyJQrwpTTG-tWLQU-1QeUAJA73DdxGGiKoJc', 'BUhZLMwQ6yZHguLtJYA5lLUa9LQzLXMXRfaq9FVcPJc', 1712737395)
  sqlschema.registerAMM('BARK/AOCRED', 'U3Yy3MQ41urYMvSmzHsaA4hJEDuvIm-TgXvSm-wz-X0',
    'Sa0iBLPNyJQrwpTTG-tWLQU-1QeUAJA73DdxGGiKoJc', '8p7ApPZxC_37M06QHVejCQrKsHbcJEerd3jWNkDUWPQ', 1712737395)
  sqlschema.registerAMM('AFT/AOCRED', 'DCQJwfEQCD-OQYmfgNH4Oh6uGo9eQJbEn6WbNvtrI_k',
    'Sa0iBLPNyJQrwpTTG-tWLQU-1QeUAJA73DdxGGiKoJc', 'SpzpFLkqPGvr5ZFZPbvyAtizthmrJ13lL4VBQIBL0dg', 1712737395)
  sqlschema.registerAMM('EXP/AOCRED', 'IMcN3R14yThfHzgbYzBDuuSpzmow7zGyBHRE3Gwrtsk',
    'Sa0iBLPNyJQrwpTTG-tWLQU-1QeUAJA73DdxGGiKoJc', 'aYrCboXVSl1AXL9gPFe3tfRxRf0ZmkOXH65mKT0HHZw', 1712737395)
end

function sqlschema.updateTokens()
  sqlschema.registerToken('OT9qTE2467gcozb2g8R6D6N3nQS94ENcaAIJfUzHCww', 'TRUNK', 3, 34198, false, 1712737395)
  sqlschema.registerToken('8p7ApPZxC_37M06QHVejCQrKsHbcJEerd3jWNkDUWPQ', 'BARK', 3, 201047011, false, 1712737395)
  sqlschema.registerToken('SpzpFLkqPGvr5ZFZPbvyAtizthmrJ13lL4VBQIBL0dg', 'AFT', 12, 10000, false, 1712737395)
  sqlschema.registerToken('BUhZLMwQ6yZHguLtJYA5lLUa9LQzLXMXRfaq9FVcPJc', '0rbit', 12, 100109630, false, 1712737395)
  sqlschema.registerToken('aYrCboXVSl1AXL9gPFe3tfRxRf0ZmkOXH65mKT0HHZw', 'EXP', 6, 2782410, false, 1716217288)
  sqlschema.registerToken('Sa0iBLPNyJQrwpTTG-tWLQU-1QeUAJA73DdxGGiKoJc', 'AOCRED', 3, 2782410, false, 1716217288)
end

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

function sqlschema.registerToken(processId, name, denominator, totalSupply, fixedSupply, updatedAt)
  local stmt = db:prepare [[
    INSERT INTO token_registry (token_process, token_name, denominator, total_supply, fixed_supply, token_updated_at_ts)
    VALUES (:process_id, :token_name, :denominator, :total_supply, :fixed_supply, :token_updated_at_ts)
    ON CONFLICT(token_process) DO UPDATE SET
    token_name = excluded.token_name,
    denominator = excluded.denominator,
    total_supply = excluded.total_supply,
    fixed_supply = excluded.fixed_supply,
    token_updated_at_ts = excluded.token_updated_at_ts;
  ]]
  if not stmt then
    error("Failed to prepare SQL statement for registering token: " .. db:errmsg())
  end
  stmt:bind_names({
    process_id = processId,
    token_name = name,
    denominator = denominator,
    total_supply = totalSupply,
    fixed_supply = fixedSupply,
    token_updated_at_ts = updatedAt
  })
  local result, err = stmt:step()
  stmt:finalize()
  if err then
    error("Err: " .. db:errmsg())
  end
end

function sqlschema.updateTokenSupply(processId, totalSupply, fixedSupply, updatedAt)
  local stmt = db:prepare [[
    UPDATE token_registry
    SET total_supply = :total_supply, fixed_supply = :fixed_supply, token_updated_at_ts = :token_updated_at_ts
    WHERE token_process = :token_process;
  ]]
  if not stmt then
    error("Failed to prepare SQL statement for updating token supply: " .. db:errmsg())
  end
  stmt:bind_names({
    token_process = processId,
    total_supply = totalSupply,
    fixed_supply = fixedSupply,
    token_updated_at_ts = updatedAt
  })
  local result, err = stmt:step()
  stmt:finalize()
  if err then
    error("Err: " .. db:errmsg())
  end

  --[[
      supply change affects
          the market cap of this token =>
            the overall token ranking by market cap =>
              the top N token sets
    ]]
  sqlschema.updateTopNTokenSet()
end

--[[
 For subscribersStmt to top N market data, update the token set
 ]]
---@param specificSubscriber string | nil if nil, token set is updated for each subscriber
function sqlschema.updateTopNTokenSet(specificSubscriber)
  local specificSubscriberClause = specificSubscriber
      and " AND process_id = :process_id"
      or ""
  local stmt = db:prepare [[
    UPDATE top_n_subscriptions
    SET token_set = (
      SELECT json_group_array(token_process)
      FROM (
        SELECT token_process
        FROM market_cap_view
        LIMIT top_n
      )
    )
    WHERE EXISTS (
      SELECT 1
      FROM market_cap_view
      LIMIT top_n
    ) ]] .. specificSubscriberClause .. [[;
  ]]

  if not stmt then
    error("Failed to prepare SQL statement for updating top N token sets: " .. db:errmsg())
  end

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

function sqlschema.isKnownAmm(processId)
  local stmt = 'SELECT TRUE FROM amm_registry WHERE amm_process = :amm_process'
  local stmt = db:prepare(stmt)
  stmt:bind_names({ amm_process = processId })

  local row = sqlschema.queryOne(stmt)
  return row ~= nil
end

return sqlschema
