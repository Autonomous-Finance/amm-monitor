local sqlschema = {}

sqlschema.create_table = [[
CREATE TABLE IF NOT EXISTS amm_transactions (
    id TEXT PRIMARY KEY,
    source TEXT NOT NULL CHECK (source IN ('gateway', 'message')),
    block_height INTEGER NOT NULL,
    block_id TEXT,
    sender TEXT NOT NULL,
    created_at_ts INTEGER,
    to_token TEXT NOT NULL,
    from_token TEXT NOT NULL,
    from_quantity INT NOT NULL,
    to_quantity INT NOT NULL,
    fee INT INT NULL,
    amm_process TEXT NOT NULL,
    reserves_0 TEXT NOT NULL DEFAULT "",
    reserves_1 TEXT NOT NULL DEFAULT "",
    fee_percentage TEXT NOT NULL DEFAULT ""
);
]]

sqlschema.should_alter_table_add_reserves = function()
  local stmt = db:prepare("PRAGMA table_info(amm_transactions);")
  if not stmt then
    error("Err: " .. db:errmsg())
  end
  local hasReserves0 = false
  local hasReserves1 = false
  local hasFeePercentage = false
  for row in stmt:nrows() do
    if row.name == "reserves_0" then
      hasReserves0 = true
    elseif row.name == "reserves_1" then
      hasReserves1 = true
    elseif row.name == "fee_percentage" then
      hasFeePercentage = true
    end
  end
  stmt:reset()
  return not hasReserves0 or not hasReserves1 or not hasFeePercentage
end

sqlschema.alter_table_add_reserves = [[
ALTER TABLE amm_transactions
  ADD COLUMN reserves_0 TEXT NOT NULL DEFAULT "",
  ADD COLUMN reserves_1 TEXT NOT NULL DEFAULT "",
  ADD COLUMN fee_percentage TEXT NOT NULL DEFAULT "";
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

sqlschema.create_subscriptions_table = [[
CREATE TABLE IF NOT EXISTS subscriptions (
    process_id TEXT PRIMARY KEY,
    owner_id TEXT NOT NULL,
    amm_process_id TEXT NOT NULL
);
]]

sqlschema.create_top_n_subscriptions_table = [[
CREATE TABLE IF NOT EXISTS top_n_subscriptions (
    process_id TEXT PRIMARY KEY,
    owner_id TEXT NOT NULL,
    quote_token_process_id TEXT NOT NULL,
    last_push_at INTEGER DEFAULT 0,
    push_interval INTEGER DEFAULT 0
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
  fee,
  amm_process,
  reserves_0,
  reserves_1,
  fee_percentage,
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

function sqlschema.createTableIfNotExists(db)
  db:exec(sqlschema.create_table)

  if sqlschema.should_alter_table_add_reserves() then
    db:exec(sqlschema.alter_table_add_reserves)
  end

  db:exec("DROP VIEW IF EXISTS amm_transactions_view;")
  print("Err: " .. db:errmsg())

  db:exec(sqlschema.create_amm_registry_table)
  print("Err: " .. db:errmsg())

  db:exec(sqlschema.create_transactions_view)
  print("Err: " .. db:errmsg())

  db:exec(sqlschema.create_balances_table)
  print("Err: " .. db:errmsg())

  db:exec(sqlschema.create_subscriptions_table)
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
    "token1", token1
  })
  local stmt = db:prepare [[
  INSERT OR REPLACE INTO amm_registry (amm_process, amm_name, amm_token0, amm_token1, amm_discovered_at_ts)
  VALUES
    (:process, :amm_name, :token0, :token1, :discovered_at)
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

function sqlschema.getOverview(now, orderBy)
  local orderByClause = "amm_discovered_at_ts DESC"

  if orderBy == "volume" then
    orderByClause = "volume DESC"
  elseif orderBy == "transactions" then
    orderByClause = "transactions DESC"
  elseif orderBy == "market_cap" then
    orderByClause = "market_cap DESC"
  end

  local stmt = db:prepare(string.format([[
  WITH stats AS (
    SELECT
      amm_process,
      COUNT(*) AS transactions,
      SUM(volume) AS volume
    FROM amm_transactions_view
    WHERE created_at_ts >= :now - 86400
    GROUP BY 1
  ), current_prices AS (
    SELECT
      amm_process,
      (SELECT price FROM amm_transactions_view WHERE amm_process = r.amm_process ORDER BY created_at_ts DESC LIMIT 1) AS current_price
    FROM amm_registry r
  )
  SELECT
    rank() OVER (ORDER BY t.total_supply * current_price DESC) AS market_cap_rank,
    r.amm_name as amm_name,
    r.amm_process as amm_process,
    r.amm_token0 AS token0,
    r.amm_token1 AS token1,
    transactions,
    volume,
    t.token_name AS token_name,
    t.total_supply AS total_supply,
    t.fixed_supply AS fixed_supply,
    t.total_supply * current_price AS market_cap,
    (SELECT price FROM amm_transactions_view WHERE amm_process = r.amm_process ORDER BY created_at_ts DESC LIMIT 1) AS current_price,
    (SELECT price FROM amm_transactions_view WHERE amm_process = r.amm_process AND created_at_ts <= :now - 300 ORDER BY created_at_ts DESC LIMIT 1) AS price_5m_ago,
    (SELECT price FROM amm_transactions_view WHERE amm_process = r.amm_process AND created_at_ts <= :now - 3600 ORDER BY created_at_ts DESC LIMIT 1) AS price_1h_ago,
    (SELECT price FROM amm_transactions_view WHERE amm_process = r.amm_process AND created_at_ts <= :now - 21600 ORDER BY created_at_ts DESC LIMIT 1) AS price_6h_ago,
    (SELECT price FROM amm_transactions_view WHERE amm_process = r.amm_process AND created_at_ts <= :now - 86400 ORDER BY created_at_ts DESC LIMIT 1) AS price_24h_ago
  FROM amm_registry r
  LEFT JOIN stats s ON s.amm_process = r.amm_process
  LEFT JOIN current_prices c ON c.amm_process = r.amm_process
  LEFT JOIN token_registry t ON t.token_process = r.amm_token1
  ORDER BY %s
  LIMIT 100
  ]], 'market_cap DESC'))

  if not stmt then
    error("Err: " .. db:errmsg())
  end

  stmt:bind_names({
    now = now
  })

  return sqlschema.queryMany(stmt)
end

function sqlschema.getTopNMarketData(token0)
  local orderByClause = "market_cap DESC"
  local stmt = db:prepare([[
  WITH current_prices AS (
    SELECT
      amm_process,
      (SELECT price FROM amm_transactions_view WHERE amm_process = r.amm_process ORDER BY created_at_ts DESC LIMIT 1) AS current_price
      (SELECT reserves_0 FROM amm_transactions_view WHERE amm_process = r.amm_process ORDER BY created_at_ts DESC LIMIT 1) AS current_reserves_0
      (SELECT reserves_1 FROM amm_transactions_view WHERE amm_process = r.amm_process ORDER BY created_at_ts DESC LIMIT 1) AS current_reserves_1
      (SELECT fee_percentage FROM amm_transactions_view WHERE amm_process = r.amm_process ORDER BY created_at_ts DESC LIMIT 1) AS current_fee_percentage
    FROM amm_registry r
  )
  SELECT
    rank() OVER (ORDER BY t.total_supply * current_price DESC) AS market_cap_rank,
    r.amm_name as amm_name,
    r.amm_process as pool,
    r.amm_token1 AS token,
    t.token_name AS ticker,
    t.denominator as denomination,
    c.current_price AS current_price,
    c.reserves_0 AS reserves_0,
    c.reserves_1 AS reserves_1,
    c.fee_percentage AS fee_percentage
  FROM amm_registry r
  LEFT JOIN current_prices c ON c.amm_process = r.amm_process
  LEFT JOIN token_registry t ON t.token_process = r.amm_token1
  WHERE r.amm_token0 = :token0
  LIMIT 100
  ]], orderByClause)

  if not stmt then
    error("Err: " .. db:errmsg())
  end

  stmt:bind_names({
    token0 = token0,
    pricePrecision = PRICE_PRECISION
  })
  return sqlschema.queryMany(stmt)
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

function sqlschema.getIndicators(timestampFrom, timestampTo, ammProcess)
  local stmt = db:prepare([[
    SELECT
      *
    FROM amm_transactions_view
    WHERE created_at_ts BETWEEN :date_from AND :date_to
      AND amm_process = :amm_process
  ]])

  if not stmt then
    error("Err: " .. db:errmsg())
  end

  stmt:bind_names({
    date_from = timestampFrom,
    date_to = timestampTo,
    amm_process = ammProcess
  })

  return sqlschema.queryMany(stmt)
end

function sqlschema.registerProcess(processId, ownerId, ammProcessId)
  local stmt = db:prepare [[
    INSERT INTO subscriptions (process_id, owner_id, amm_process_id)
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

function sqlschema.registerTopNConsumer(processId, ownerId, quoteToken)
  local stmt = db:prepare [[
    INSERT INTO top_n_subscriptions (process_id, owner_id, quote_token_process_id)
    VALUES (:process_id, :owner_id, :quote_token_process_id)
    ON CONFLICT(process_id) DO UPDATE SET
    owner_id = excluded.owner_id,
    quote_token_process_id = excluded.quote_token_process_id;
  ]]
  if not stmt then
    error("Failed to prepare SQL statement for registering process: " .. db:errmsg())
  end
  stmt:bind_names({
    process_id = processId,
    owner_id = ownerId,
    quote_token_process_id = quoteToken
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
    UPDATE token_registry SET total_supply = :total_supply, fixed_supply = :fixed_supply, token_updated_at_ts = :token_updated_at_ts WHERE token_process = :token_process;
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
