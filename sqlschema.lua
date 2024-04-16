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
    amm_process TEXT NOT NULL
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
  CASE WHEN to_token = amm_token1 THEN 1 ELSE 0 END AS is_buy,
  ROUND(CASE 
    WHEN from_quantity > 0 AND to_quantity > 0 THEN
      CASE 
        WHEN to_token = amm_token1 THEN from_quantity * 1.0 / to_quantity
        ELSE to_quantity * 1.0 / from_quantity
      END
    ELSE NULL
  END, 12) AS price,
  CASE
    WHEN to_token = amm_token1 THEN from_quantity
    ELSE to_quantity
  END AS volume
FROM amm_transactions
LEFT JOIN amm_registry USING (amm_process)
]]


function sqlschema.createTableIfNotExists(db)
    db:exec(sqlschema.create_table)

    db:exec("DROP VIEW IF EXISTS amm_transactions_view;")
    print("Err: " .. db:errmsg())

    db:exec(sqlschema.create_amm_registry_table)
    print("Err: " .. db:errmsg())
  
    db:exec(sqlschema.create_transactions_view)
    print("Err: " .. db:errmsg())

    sqlschema.updateAMMs()
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
  local stmt = db:prepare[[
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

function sqlschema.getOverview(now, orderBy)
  local orderByClause = "amm_discovered_at_ts DESC"
  
  if orderBy == "volume" then
    orderByClause = "volume DESC"
  elseif orderBy == "transactions" then
    orderByClause = "transactions DESC"
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
  )
  SELECT
    r.amm_name as amm_name,
    r.amm_process as amm_process,
    r.amm_token0 AS token0,
    r.amm_token1 AS token1,
    transactions,
    volume,
    (SELECT price FROM amm_transactions_view WHERE amm_process = r.amm_process ORDER BY created_at_ts DESC LIMIT 1) AS current_price,
    (SELECT price FROM amm_transactions_view WHERE amm_process = r.amm_process AND created_at_ts <= :now - 300 ORDER BY created_at_ts DESC LIMIT 1) AS price_5m_ago,
    (SELECT price FROM amm_transactions_view WHERE amm_process = r.amm_process AND created_at_ts <= :now - 900 ORDER BY created_at_ts DESC LIMIT 1) AS price_15m_ago,
    (SELECT price FROM amm_transactions_view WHERE amm_process = r.amm_process AND created_at_ts <= :now - 21600 ORDER BY created_at_ts DESC LIMIT 1) AS price_6h_ago,
    (SELECT price FROM amm_transactions_view WHERE amm_process = r.amm_process AND created_at_ts <= :now - 86400 ORDER BY created_at_ts DESC LIMIT 1) AS price_24h_ago
  FROM amm_registry r
  LEFT JOIN stats s ON s.amm_process = r.amm_process
  ORDER BY %s
  LIMIT 100
  ]], orderByClause))

  if not stmt then
    error("Err: " .. db:errmsg())
  end

  stmt:bind_names({
    now = now
  })

  return sqlschema.queryMany(stmt)
end

function sqlschema.updateAMMs()
  sqlschema.registerAMM('TRUNK/AOCRED', 'vn5lUv8OaevTb45iI_qykad_d9MP69kuYg5mZW1zCHE', 'Sa0iBLPNyJQrwpTTG-tWLQU-1QeUAJA73DdxGGiKoJc', 'OT9qTE2467gcozb2g8R6D6N3nQS94ENcaAIJfUzHCww', 1712737395)
  sqlschema.registerAMM('0rbit/AOCRED', '2bKo3vwB1Mo5TItmxuUQzZ11JgKauU_n2IZO1G13AIk', 'Sa0iBLPNyJQrwpTTG-tWLQU-1QeUAJA73DdxGGiKoJc', 'BUhZLMwQ6yZHguLtJYA5lLUa9LQzLXMXRfaq9FVcPJc', 1712737395)
  sqlschema.registerAMM('BARK/AOCRED', 'U3Yy3MQ41urYMvSmzHsaA4hJEDuvIm-TgXvSm-wz-X0', 'Sa0iBLPNyJQrwpTTG-tWLQU-1QeUAJA73DdxGGiKoJc', '8p7ApPZxC_37M06QHVejCQrKsHbcJEerd3jWNkDUWPQ', 1712737395)
  sqlschema.registerAMM('AFT/AOCRED', 'DCQJwfEQCD-OQYmfgNH4Oh6uGo9eQJbEn6WbNvtrI_k', 'Sa0iBLPNyJQrwpTTG-tWLQU-1QeUAJA73DdxGGiKoJc', 'SpzpFLkqPGvr5ZFZPbvyAtizthmrJ13lL4VBQIBL0dg', 1712737395)
end

return sqlschema

