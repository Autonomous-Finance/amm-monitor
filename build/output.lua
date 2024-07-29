do
local _ENV = _ENV
package.preload[ "db.seed" ] = function( ... ) local arg = _G.arg;
local sqlschema = require('db.sqlschema')
local dexiCore = require('dexi-core.dexi-core')

local dbSeed = {}

function dbSeed.createMissingTables()
  db:exec(sqlschema.create_transactions_table)

  db:exec(sqlschema.create_amm_swap_params_changes_table)

  db:exec(sqlschema.create_amm_swap_params_table)
  print("create_amm_swap_params_table: " .. db:errmsg() == 'not an error' and '✅' or db:errmsg())

  db:exec(sqlschema.create_token_supply_changes_table)
  print("create_token_supply_changes_table: " .. db:errmsg() == 'not an error' and '✅' or db:errmsg())

  db:exec(sqlschema.create_amm_registry_table)
  print("create_amm_registry_table: " .. (db:errmsg() == 'not an error' and '✅' or db:errmsg()))

  db:exec("DROP VIEW IF EXISTS amm_transactions_view;")
  print("DROP amm_transactions_view : " .. (db:errmsg() == 'not an error' and '✅' or db:errmsg()))

  db:exec(sqlschema.create_transactions_view)
  print("create_transactions_view: " .. (db:errmsg() == 'not an error' and '✅' or db:errmsg()))

  db:exec("DROP VIEW IF EXISTS amm_market_cap_view;")
  print("DROP amm_market_cap_view " .. (db:errmsg() == 'not an error' and '✅' or db:errmsg()))

  db:exec(sqlschema.create_market_cap_view)
  print("create_market_cap_view: " .. (db:errmsg() == 'not an error' and '✅' or db:errmsg()))

  db:exec(sqlschema.create_balances_table)
  print("create_balances_table: " .. (db:errmsg() == 'not an error' and '✅' or db:errmsg()))

  db:exec(sqlschema.create_indicator_subscriptions_table)
  print("create_indicator_subscriptions_table: " .. (db:errmsg() == 'not an error' and '✅' or db:errmsg()))

  db:exec(sqlschema.create_top_n_subscriptions_table)
  print("create_top_n_subscriptions_table: " .. (db:errmsg() == 'not an error' and '✅' or db:errmsg()))

  db:exec(sqlschema.create_token_registry_table)
  print("create_token_registry_table: " .. (db:errmsg() == 'not an error' and '✅' or db:errmsg()))
end

local function seedAMMs()
  dexiCore.registerAMM('TRUNK/AOCRED', 'vn5lUv8OaevTb45iI_qykad_d9MP69kuYg5mZW1zCHE',
    'Sa0iBLPNyJQrwpTTG-tWLQU-1QeUAJA73DdxGGiKoJc', 'OT9qTE2467gcozb2g8R6D6N3nQS94ENcaAIJfUzHCww', 1712737395)
  dexiCore.registerAMM('0rbit/AOCRED', '2bKo3vwB1Mo5TItmxuUQzZ11JgKauU_n2IZO1G13AIk',
    'Sa0iBLPNyJQrwpTTG-tWLQU-1QeUAJA73DdxGGiKoJc', 'BUhZLMwQ6yZHguLtJYA5lLUa9LQzLXMXRfaq9FVcPJc', 1712737395)
  dexiCore.registerAMM('BARK/AOCRED', 'U3Yy3MQ41urYMvSmzHsaA4hJEDuvIm-TgXvSm-wz-X0',
    'Sa0iBLPNyJQrwpTTG-tWLQU-1QeUAJA73DdxGGiKoJc', '8p7ApPZxC_37M06QHVejCQrKsHbcJEerd3jWNkDUWPQ', 1712737395)
  dexiCore.registerAMM('AFT/AOCRED', 'DCQJwfEQCD-OQYmfgNH4Oh6uGo9eQJbEn6WbNvtrI_k',
    'Sa0iBLPNyJQrwpTTG-tWLQU-1QeUAJA73DdxGGiKoJc', 'SpzpFLkqPGvr5ZFZPbvyAtizthmrJ13lL4VBQIBL0dg', 1712737395)
  dexiCore.registerAMM('EXP/AOCRED', 'IMcN3R14yThfHzgbYzBDuuSpzmow7zGyBHRE3Gwrtsk',
    'Sa0iBLPNyJQrwpTTG-tWLQU-1QeUAJA73DdxGGiKoJc', 'aYrCboXVSl1AXL9gPFe3tfRxRf0ZmkOXH65mKT0HHZw', 1712737395)
end

local function seedTokens()
  dexiCore.registerToken('OT9qTE2467gcozb2g8R6D6N3nQS94ENcaAIJfUzHCww', 'TRUNK', 3, 34198, false, 1712737395)
  dexiCore.registerToken('8p7ApPZxC_37M06QHVejCQrKsHbcJEerd3jWNkDUWPQ', 'BARK', 3, 201047011, false, 1712737395)
  dexiCore.registerToken('SpzpFLkqPGvr5ZFZPbvyAtizthmrJ13lL4VBQIBL0dg', 'AFT', 12, 10000, false, 1712737395)
  dexiCore.registerToken('BUhZLMwQ6yZHguLtJYA5lLUa9LQzLXMXRfaq9FVcPJc', '0rbit', 12, 100109630, false, 1712737395)
  dexiCore.registerToken('aYrCboXVSl1AXL9gPFe3tfRxRf0ZmkOXH65mKT0HHZw', 'EXP', 6, 2782410, false, 1716217288)
  dexiCore.registerToken('Sa0iBLPNyJQrwpTTG-tWLQU-1QeUAJA73DdxGGiKoJc', 'AOCRED', 3, 2782410, false, 1716217288)
end

function dbSeed.seed()
  seedAMMs()
  seedTokens()
end

function dbSeed.handleResetDBState(msg)
  if msg.From ~= Owner and msg.From ~= ao.id then
    error('Only the owner and the process itself can reset-and-seed the database')
  end

  db:exec("DROP TABLE IF EXISTS amm_transactions;")
  dbSeed.createMissingTables()
  dbSeed.seed()
end

return dbSeed
end
end

do
local _ENV = _ENV
package.preload[ "db.sqlschema" ] = function( ... ) local arg = _G.arg;
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
    amm_process TEXT NOT NULL
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
    process_id TEXT NOT NULL,
    owner_id TEXT NOT NULL,
    amm_process_id TEXT NOT NULL,
    PRIMARY KEY (process_id),
    UNIQUE (process_id, amm_process_id)
);
]]

sqlschema.create_top_n_subscriptions_table = [[
CREATE TABLE IF NOT EXISTS top_n_subscriptions (
    process_id TEXT NOT NULL,
    owner_id TEXT NOT NULL,
    quote_token TEXT NOT NULL,
    top_n INTEGER NOT NULL,
    token_set TEXT NOT NULL DEFAULT '[]' CHECK (json_valid(token_set)),
    PRIMARY KEY (process_id),
    UNIQUE (process_id, quote_token)
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
CREATE VIEW amm_market_cap_view AS
SELECT
  r.amm_base_token AS token_process,
  t.total_supply * current_price AS market_cap,
  r.amm_quote_token AS quote_token_process,
  rank() OVER (ORDER BY t.total_supply * current_price DESC) AS market_cap_rank
FROM amm_registry r
LEFT JOIN token_registry t ON t.token_process = r.amm_base_token
WHERE r.amm_quote_token IS NOT NULL
ORDER BY market_cap DESC
LIMIT 100
]]

return sqlschema
end
end

do
local _ENV = _ENV
package.preload[ "db.utils" ] = function( ... ) local arg = _G.arg;
local dbUtils = {}

function dbUtils.queryMany(stmt)
  local rows = {}
  for row in stmt:nrows() do
    table.insert(rows, row)
  end
  stmt:reset()
  return rows
end

function dbUtils.queryOne(stmt)
  return dbUtils.queryMany(stmt)[1]
end

function dbUtils.rawQuery(query)
  local stmt = db:prepare(query)
  if not stmt then
    error("Err: " .. db:errmsg())
  end
  return dbUtils.queryMany(stmt)
end

return dbUtils
end
end

do
local _ENV = _ENV
package.preload[ "dexi-core.candles" ] = function( ... ) local arg = _G.arg;
local intervals = require('dexi-core.intervals')
local dbUtils = require('db.utils')

local candles = {}

function candles.generateCandlesForXDaysInIntervalY(xDays, yInterval, endTime, ammProcessId)
  local intervalSeconds = intervals.IntervalSecondsMap[yInterval]
  if not intervalSeconds then
    error("Invalid interval specified")
    return
  end

  -- Determine the GROUP BY clause based on the interval
  local groupByClause
  if yInterval == '15m' then
    groupByClause = "strftime('%Y-%m-%d %H:%M', \"created_at_ts\" / 900 * 900, 'unixepoch')"
  elseif yInterval == '1h' then
    groupByClause = "strftime('%Y-%m-%d %H', \"created_at_ts\", 'unixepoch')"
  elseif yInterval == '4h' then
    groupByClause = "strftime('%Y-%m-%d %H', \"created_at_ts\" / 14400 * 14400, 'unixepoch')"
  elseif yInterval == '1d' then
    groupByClause = "strftime('%Y-%m-%d', \"created_at_ts\", 'unixepoch')"
  else
    error("Unsupported interval for grouping")
    return
  end

  local stmt = db:prepare(string.format([[
    SELECT
      %s AS candle_time,
      MIN(created_at_ts) AS start_timestamp,
      MAX(created_at_ts) AS end_timestamp,
      (SELECT price FROM amm_transactions WHERE created_at_ts = (SELECT MIN(created_at_ts) FROM amm_transactions_view WHERE created_at_ts >= :start_time AND created_at_ts < :end_time AND amm_process = :amm_process)) AS open,
      MAX(price) AS high,
      MIN(price) AS low,
      (SELECT price FROM amm_transactions WHERE created_at_ts = (SELECT MAX(created_at_ts) FROM amm_transactions_view WHERE created_at_ts >= :start_time AND created_at_ts < :end_time AND amm_process = :amm_process)) AS close,
      SUM(volume) / POWER(10, quote_denominator) AS volume
    FROM
      amm_transactions_view AS t1
    WHERE created_at_ts >= :start_time AND created_at_ts < :end_time AND amm_process = :amm_process
    GROUP BY
      1
    ORDER BY
      candle_time ASC
  ]], groupByClause))

  local startTime = endTime - (xDays * 24 * 3600)

  stmt:bind_names({
    start_time = startTime,
    end_time = endTime,
    amm_process = ammProcessId
  })

  local candles = dbUtils.queryMany(stmt)

  for i = 2, #candles do
    candles[i].open = candles[i - 1].close
  end

  if #candles > 0 then
    candles[1].open = 0
  end

  return candles
end

return candles
end
end

do
local _ENV = _ENV
package.preload[ "dexi-core.dexi-core" ] = function( ... ) local arg = _G.arg;
local json = require('json')

local dbUtils = require('db.utils')
local overview = require('dexi-core.overview')
local intervals = require('dexi-core.intervals')
local stats = require('dexi-core.stats')
local candles = require('dexi-core.candles')
local priceAround = require('dexi-core.price-around')
local topN = require('top-n.top-n')


local dexiCore = {}

-- ---------------- SQL

local sql = {}

function sql.registerAMM(name, processId, token0, token1, discoveredAt)
  print('Registering AMM:')
  print({
    "process", processId,
    "name", name,
    "token0", token0,
    "token1", token1,
  })
  local stmt = db:prepare [[
  INSERT OR REPLACE INTO amm_registry (amm_process, amm_name, amm_token0, amm_token1, amm_quote_token, amm_base_token, amm_discovered_at_ts)
  VALUES
    (:process, :amm_name, :token0, :token1, :quote_token, :base_token, :discovered_at);
  ]]
  if not stmt then
    error("Err: " .. db:errmsg())
  end
  stmt:bind_names({
    process = processId,
    amm_name = name,
    token0 = token0,
    token1 = token1,
    quote_token = token0 == QUOTE_TOKEN_PROCESS and token0 or token1,
    base_token = token0 == QUOTE_TOKEN_PROCESS and token1 or token0,
    discovered_at = discoveredAt
  })
  local result, err = stmt:step()
  if err then
    print("Err: " .. db:errmsg())
  end
  stmt:reset()
end

function sql.registerToken(processId, name, denominator, totalSupply, fixedSupply, updatedAt)
  print('Registering Token:')
  print({
    "process", processId,
    "name", name,
    "denominator", denominator,
    "totalSupply", totalSupply,
    "fixedSupply", fixedSupply,
    "updatedAt", updatedAt
  })
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

function sql.getRegisteredAMMs()
  return dbUtils.rawQuery("SELECT * FROM amm_registry")
end

---@param supply_changed_at_ts number @timestamp of when the change occurred in the token contract - typically earlier than the time at which DEXI records this change
function sql.updateTokenSupply(id, block_height, block_id, supply_changed_at_ts, token, total_supply, now_ts)
  db:execute('BEGIN;')

  local changesStmt = db:prepare [[
    INSERT INTO token_supply_changes
    (id, block_height, block_id, supply_changed_at_ts, token, total_supply)
    VALUES (:id, :block_height, :block_id, :supply_changed_at_ts, :token, :total_supply);
  ]]
  if not changesStmt then
    error("Failed to prepare SQL statement for updating token supply: " .. db:errmsg())
  end
  changesStmt:bind_names({
    id = id,
    block_height = block_height,
    block_id = block_id,
    supply_changed_at_ts = supply_changed_at_ts,
    token = token,
    total_supply = total_supply
  })
  local _, errChanges = changesStmt:step()
  changesStmt:finalize()
  if errChanges then
    error("Err: " .. db:errmsg())
  end

  local registryStmt = db:prepare [[
    UPDATE token_registry
    SET total_supply = :total_supply, token_updated_at_ts = :token_updated_at_ts
    WHERE token_process = :token_process;
  ]]
  if not registryStmt then
    error("Failed to prepare SQL statement for updating token supply: " .. db:errmsg())
  end
  registryStmt:bind_names({
    token_process = token,
    total_supply = total_supply,
    token_updated_at_ts = now_ts
  })
  local _, errRegistry = registryStmt:step()
  registryStmt:finalize()
  if errRegistry then
    error("Err: " .. db:errmsg())
  end

  db:execute('COMMIT;')
end

function sql.isKnownAmm(processId)
  local stmt = db:prepare('SELECT TRUE FROM amm_registry WHERE amm_process = :amm_process')
  stmt:bind_names({ amm_process = processId })

  local row = dbUtils.queryOne(stmt)
  return row ~= nil
end

-- ---------------- EXPORT

dexiCore.registerToken = function(processId, name, denominator, totalSupply, fixedSupply, updatedAt)
  sql.registerToken(processId, name, denominator, totalSupply, fixedSupply, updatedAt)
end

dexiCore.registerAMM = function(name, processId, token0, token1, discoveredAt)
  sql.registerAMM(name, processId, token0, token1, discoveredAt)
end

dexiCore.handleGetRegisteredAMMs = function(msg)
  ao.send({
    ['App-Name'] = 'Dexi',
    ['Payload'] = 'Registered-AMMs',
    Target = msg.From,
    Data = json.encode(sql.getRegisteredAMMs())
  })
end

dexiCore.handleGetOverview = function(msg)
  local now = msg.Timestamp / 1000
  local orderBy = msg.Tags['Order-By']
  ao.send({
    ['App-Name'] = 'Dexi',
    ['Payload'] = 'Overview',
    Target = msg.From,
    Data = json.encode(overview.getOverview(now, orderBy))
  })
end

function dexiCore.handleGetPricesInBatch(msg)
  local amms = json.decode(msg.Tags['AMM-List'])
  local ammPrices = {}
  for _, amm in ipairs(amms) do
    local price = priceAround.findPriceAroundTimestamp(msg.Timestamp / 1000, amm)
    ammPrices[amm] = price
  end

  ao.send({
    Target = msg.From,
    Action = 'Price-Batch-Response',
    Data = json.encode(ammPrices)
  })
end

function dexiCore.handleGetStats(msg)
  local aggStats = stats.getAggregateStats(0, msg.Tags.AMM)
  local now = math.floor(msg.Timestamp / 1000)

  local priceNow = priceAround.findPriceAroundTimestamp(now, msg.Tags.AMM)
  local price24HAgo = priceAround.findPriceAroundTimestamp(now - intervals.IntervalSecondsMap['1d'], msg.Tags.AMM)
  local price6HAgo = priceAround.findPriceAroundTimestamp(now - intervals.IntervalSecondsMap['6h'], msg.Tags.AMM)
  local price1HAgo = priceAround.findPriceAroundTimestamp(now - intervals.IntervalSecondsMap['1h'], msg.Tags.AMM)

  ao.send({
    Target = msg.From,
    ['App-Name'] = 'Dexi',
    ['Payload'] = 'Stats',
    ['AMM'] = msg.Tags.AMM,
    ['Total-Volume'] = tostring(aggStats.total_volume),
    ['Buy-Volume'] = tostring(aggStats.buy_volume),
    ['Sell-Volume'] = tostring(aggStats.sell_volume),
    ['Buy-Count'] = tostring(aggStats.buy_count),
    ['Sell-Count'] = tostring(aggStats.sell_count),
    ['Buyers'] = tostring(aggStats.distinct_buyers),
    ['Sellers'] = tostring(aggStats.distinct_sellers),
    ['Total-Traders'] = tostring(aggStats.distinct_traders),
    ['Latest-Price'] = tostring(priceNow),
    ['Price-24H-Ago'] = tostring(price24HAgo),
    ['Price-6H-Ago'] = tostring(price6HAgo),
    ['Price-1H-Ago'] = tostring(price1HAgo)
  })
end

function dexiCore.handleGetCandles(msg)
  local days = msg.Tags.Days and tonumber(msg.Tags.Days) or 30
  local candles = candles.generateCandlesForXDaysInIntervalY(days, msg.Tags.Interval, msg.Timestamp / 1000,
    msg.Tags.AMM)
  ao.send({
    Target = msg.From,
    ['App-Name'] = 'Dexi',
    ['Payload'] = 'Candles',
    ['AMM'] = msg.Tags.AMM,
    ['Interval'] = msg.Tags.Interval or '15m',
    ['Days'] = tostring(msg.Tags.Days),
    Data = json.encode(candles)
  })
end

function dexiCore.handleUpdateTokenSupply(msg)
  if msg.From ~= SUPPLY_UPDATES_PROVIDER then
    error('Unauthorized')
  end

  local id = msg.id
  local block_height = msg.Tags["Block-Height"]
  local block_id = msg.Tags["Block-Id"]
  local token = msg.Tags["Process-Id"]
  local total_supply = msg.Tags["Total-Supply"]
  local supply_changed_at_ts = msg.Tags["Supply-Changed-At"]
  local now_ts = math.floor(msg.Timestamp / 1000)
  sql.updateTokenSupply(id, block_height, block_id, supply_changed_at_ts, token, total_supply, now_ts)

  --[[
      supply change affects
          the market cap of this token =>
            the overall token ranking by market cap =>
              the top N token sets
    ]]
  topN.updateTopNTokenSet()
end

function dexiCore.isKnownAmm(processId)
  return sql.isKnownAmm(processId)
end

return dexiCore
end
end

do
local _ENV = _ENV
package.preload[ "dexi-core.intervals" ] = function( ... ) local arg = _G.arg;
local intervals = {}

intervals.IntervalSecondsMap = {
  ["5m"] = 300,
  ["15m"] = 900,
  ["1h"] = 3600,
  ["4h"] = 14400,
  ["12h"] = 57600,
  ["6h"] = 21600,
  ["1d"] = 86400,
  ["7d"] = 86400 * 7,
  ["1M"] = 2592000
}

function intervals.getIntervalStart(timestamp, interval)
  timestamp = math.floor(timestamp) -- Ensure timestamp is an integer
  local date = os.date("!*t", timestamp)

  if interval == "1h" then
    date.min = 0
    date.sec = 0
  elseif interval == "15m" then
    date.min = 0
    date.sec = 0
  elseif interval == "4h" then
    date.min = 0
    date.sec = 0
    date.hour = date.hour - (date.hour % 4)
  elseif interval == "1d" then
    date.hour = 0
    date.min = 0
    date.sec = 0
  elseif interval == "1M" then
    date.day = 1
    date.hour = 0
    date.min = 0
    date.sec = 0
  else
    error("Unsupported interval: " .. interval)
  end

  return os.time(date)
end

return intervals
end
end

do
local _ENV = _ENV
package.preload[ "dexi-core.overview" ] = function( ... ) local arg = _G.arg;
local dbUtils = require('db.utils')

local overview = {}

function overview.getOverview(now, orderBy)
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

  return dbUtils.queryMany(stmt)
end

return overview
end
end

do
local _ENV = _ENV
package.preload[ "dexi-core.price-around" ] = function( ... ) local arg = _G.arg;
local sqlschema = require('db.sqlschema')

local priceAround = {}

function priceAround.findPriceAroundTimestamp(targetTimestampBefore, ammProcessId)
  local stmt = db:prepare [[
    SELECT price
    FROM amm_transactions_view
    WHERE created_at_ts <= :target_timestamp_before
    AND amm_process = :amm_process_id
    ORDER BY created_at_ts DESC
    LIMIT 1;
  ]]

  if not stmt then
    error("Failed to prepare SQL statement: " .. db:errmsg())
  end

  stmt:bind_names({
    target_timestamp_before = targetTimestampBefore,
    amm_process_id = ammProcessId
  })


  local row = sqlschema.queryOne(stmt)
  local price = row and row.price or nil

  return price
end

return priceAround
end
end

do
local _ENV = _ENV
package.preload[ "dexi-core.stats" ] = function( ... ) local arg = _G.arg;
local dbUtils = require('db.utils')

local stats = {}

function stats.getAggregateStats(minTimestamp, ammProcessId)
  print(1)
  local stmt, err = db:prepare [[
    SELECT
      SUM(volume) AS total_volume,
      ROUND(SUM(CASE WHEN is_buy = 1 THEN volume ELSE 0 END)) AS buy_volume,
      ROUND(SUM(CASE WHEN is_buy = 0 THEN volume ELSE 0 END)) AS sell_volume,
      SUM(is_buy) AS buy_count,
      SUM(1 - is_buy) AS sell_count,
      COUNT(DISTINCT CASE WHEN is_buy = 1 THEN sender END) AS distinct_buyers,
      COUNT(DISTINCT CASE WHEN is_buy = 0 THEN sender END) AS distinct_sellers,
      COUNT(DISTINCT sender) AS distinct_traders
    FROM amm_transactions_view
    WHERE created_at_ts >= :min_ts
    AND amm_process = :amm;
  ]]

  if not stmt then
    error("Failed to prepare SQL statement: " .. db:errmsg())
  end

  print(stmt, err)
  stmt:bind_names({
    min_ts = minTimestamp,
    amm = ammProcessId
  })


  return dbUtils.queryOne(stmt)
end

return stats
end
end

do
local _ENV = _ENV
package.preload[ "indicators.calc" ] = function( ... ) local arg = _G.arg;
local calc = {}

function calc.calculateSMAs(dailyStats)
  local smas = {}
  for i = 1, #dailyStats do
    local sma10 = 0
    local sma20 = 0
    local sma50 = 0
    local sma100 = 0
    local sma150 = 0
    local sma200 = 0

    for j = math.max(1, i - 9), i do
      sma10 = sma10 + dailyStats[j].close
    end
    sma10 = sma10 / math.min(10, i)

    for j = math.max(1, i - 19), i do
      sma20 = sma20 + dailyStats[j].close
    end
    sma20 = sma20 / math.min(20, i)

    for j = math.max(1, i - 49), i do
      sma50 = sma50 + dailyStats[j].close
    end
    sma50 = sma50 / math.min(50, i)

    for j = math.max(1, i - 99), i do
      sma100 = sma100 + dailyStats[j].close
    end
    sma100 = sma100 / math.min(100, i)

    for j = math.max(1, i - 149), i do
      sma150 = sma150 + dailyStats[j].close
    end
    sma150 = sma150 / math.min(150, i)

    for j = math.max(1, i - 199), i do
      sma200 = sma200 + dailyStats[j].close
    end
    sma200 = sma200 / math.min(200, i)

    smas[i] = {
      sma10 = sma10,
      sma20 = sma20,
      sma50 = sma50,
      sma100 = sma100,
      sma150 = sma150,
      sma200 = sma200
    }
  end

  return smas
end

function calc.calculateEMAs(dailyStats)
  local ema12 = {}
  local ema26 = {}

  for i = 1, #dailyStats do
    if i == 1 then
      ema12[i] = dailyStats[i].close
      ema26[i] = dailyStats[i].close
    else
      ema12[i] = (dailyStats[i].close - ema12[i - 1]) * 2 / 13 + ema12[i - 1]
      ema26[i] = (dailyStats[i].close - ema26[i - 1]) * 2 / 27 + ema26[i - 1]
    end
  end

  return ema12, ema26
end

function calc.calculateMACD(ema12, ema26)
  local macd = {}
  local signalLine = {}
  local histogram = {}

  for i = 1, #ema12 do
    macd[i] = ema12[i] - ema26[i]

    if i == 1 then
      signalLine[i] = macd[i]
    else
      signalLine[i] = (macd[i] - signalLine[i - 1]) * 2 / 10 + signalLine[i - 1]
    end

    histogram[i] = macd[i] - signalLine[i]
  end

  return macd, signalLine, histogram
end

function calc.calculateBollingerBands(dailyStats, smas)
  local upperBand = {}
  local lowerBand = {}

  for i = 1, #dailyStats do
    local sum = 0
    local count = 0

    for j = math.max(1, i - 19), i do
      sum = sum + (dailyStats[j].close - smas[i].sma20) ^ 2
      count = count + 1
    end

    local stdDev = math.sqrt(sum / count)
    upperBand[i] = smas[i].sma20 + 2 * stdDev
    lowerBand[i] = smas[i].sma20 - 2 * stdDev
  end

  return upperBand, lowerBand
end

return calc
end
end

do
local _ENV = _ENV
package.preload[ "indicators.indicators" ] = function( ... ) local arg = _G.arg;
local dbUtils = require('db.utils')
local calc = require('indicators.calc')
local json = require("json")

local indicators = {}

-- ---------------- SQL

local sql = {}

function sql.getDiscoveredAt(ammProcessId)
  local ammStmt = db:prepare([[
    SELECT amm_discovered_at_ts
    FROM amm_registry
    WHERE amm_process = :amm_process_id
  ]])
  if not ammStmt then
    error("Err: " .. db:errmsg())
  end

  ammStmt:bind_names({ amm_process_id = ammProcessId })

  local row = ammStmt:step()
  ammStmt:finalize()

  return row.amm_discovered_at_ts
end

function sql.getDailyStats(ammProcessId, startDate, endDate)
  local stmt = db:prepare([[
    SELECT
      date(created_at_ts, 'unixepoch') AS date,
      MIN(price) AS low,
      MAX(price) AS high,
      (SELECT price FROM amm_transactions_view sub
       WHERE sub.amm_process = :amm_process AND date(sub.created_at_ts, 'unixepoch') = date(main.created_at_ts, 'unixepoch')
       ORDER BY sub.created_at_ts LIMIT 1) AS open,
      (SELECT price FROM amm_transactions_view sub
       WHERE sub.amm_process = :amm_process AND date(sub.created_at_ts, 'unixepoch') = date(main.created_at_ts, 'unixepoch')
       ORDER BY sub.created_at_ts DESC LIMIT 1) AS close,
      SUM(volume) AS volume
    FROM amm_transactions_view main
    WHERE amm_process = :amm_process AND date(created_at_ts, 'unixepoch') BETWEEN :start_date AND :end_date
    GROUP BY date(created_at_ts, 'unixepoch')
  ]])
  stmt:bind_names({
    amm_process = ammProcessId,
    start_date = startDate,
    end_date = endDate
  })
  return dbUtils.queryMany(stmt)
end

function sql.getSubscribersToProcess(ammProcessId)
  local subscribersStmt = db:prepare([[
      SELECT s.process_id
      FROM indicator_subscriptions s
      JOIN balances b ON s.owner_id = b.owner_id AND b.balance > 0
      WHERE amm_process_id = :amm_process_id
    ]])
  if not subscribersStmt then
    error("Err: " .. db:errmsg())
  end
  subscribersStmt:bind_names({ amm_process_id = ammProcessId })

  local processes = {}
  for row in subscribersStmt:nrows() do
    table.insert(processes, row.process_id)
  end
  subscribersStmt:finalize()

  return processes
end

-- ---------------- INTERNAL

local function fillMissingDates(dailyStats, startDate, endDate)
  local filledDailyStats = {}
  local currentTimestamp = os.time({ year = startDate:sub(1, 4), month = startDate:sub(6, 7), day = startDate:sub(9, 10) })
  local endTimestamp = os.time({ year = endDate:sub(1, 4), month = endDate:sub(6, 7), day = endDate:sub(9, 10) })
  local lastClose = 0

  while currentTimestamp <= endTimestamp do
    local currentDate = os.date("%Y-%m-%d", currentTimestamp)
    local found = false
    for _, row in ipairs(dailyStats) do
      if row.date == currentDate then
        filledDailyStats[#filledDailyStats + 1] = row
        lastClose = row.close
        found = true
        break
      end
    end

    if not found then
      filledDailyStats[#filledDailyStats + 1] = {
        date = currentDate,
        low = lastClose,
        high = lastClose,
        open = lastClose,
        close = lastClose,
        volume = 0
      }
    end

    currentTimestamp = currentTimestamp + 24 * 60 * 60 -- Add one day in seconds
  end

  return filledDailyStats
end

local function generateIndicatorsData(ammProcessId, startTimestamp, endTimestamp)
  local endDate = os.date("!%Y-%m-%d", endTimestamp)
  local startDate = os.date("!%Y-%m-%d", startTimestamp)

  local dailyStats = sql.getDailyStats(ammProcessId, startDate, endDate)
  local filledDailyStats = fillMissingDates(dailyStats, startDate, endDate)
  local smas = calc.calculateSMAs(filledDailyStats)
  -- local ema12, ema26 = calculations.calculateEMAs(filledDailyStats)
  -- local macd, signalLine, histogram = calculations.calculateMACD(ema12, ema26)
  -- local upperBand, lowerBand = calculations.calculateBollingerBands(filledDailyStats, smas)

  local result = {}
  for i = 1, #filledDailyStats do
    result[i] = {
      date = filledDailyStats[i].date,
      open = filledDailyStats[i].open,
      high = filledDailyStats[i].high,
      low = filledDailyStats[i].low,
      close = filledDailyStats[i].close,
      volume = filledDailyStats[i].volume,
      -- ema12 = ema12[i],
      -- ema26 = ema26[i],
      -- macd = macd[i],
      -- signalLine = signalLine[i],
      -- histogram = histogram[i],
      -- upperBand = upperBand[i],
      -- lowerBand = lowerBand[i],
      sma10 = smas[i].sma10,
      sma20 = smas[i].sma20,
      sma50 = smas[i].sma50,
      sma100 = smas[i].sma100,
      sma150 = smas[i].sma150,
      sma200 = smas[i].sma200
    }
  end

  return result
end

local function getIndicators(ammProcessId, now)
  local discoveredAt = sql.getDiscoveredAt(ammProcessId)
  local oneWeekAgo = now - (7 * 24 * 60 * 60)
  local startTimestamp = math.max(discoveredAt, oneWeekAgo)

  return generateIndicatorsData(ammProcessId, startTimestamp, now)
end

-- ---------------- EXPORT

function indicators.handleGetIndicators(msg)
  local ammProcessId = msg.Tags['AMM']
  local now = math.floor(msg.Timestamp / 1000)
  ao.send({
    Target = msg.From,
    ['App-Name'] = 'Dexi',
    ['Response-For'] = 'Get-Indicators',
    ['AMM'] = ammProcessId,
    Data = json.encode(getIndicators(ammProcessId, now))
  })
end

function indicators.dispatchIndicatorsForAMM(now, ammProcessId)
  local discoveredAt = sql.getDiscoveredAt(ammProcessId)
  local oneWeekAgo = now - (7 * 24 * 60 * 60)
  local startTimestamp = math.max(discoveredAt, oneWeekAgo)

  local processes = sql.getSubscribersToProcess(ammProcessId)

  local indicatorsResults = getIndicators(ammProcessId, startTimestamp)

  if not DISPATCH_ACTIVE then
    if LOGGING_ACTIVE then
      ao.send({
        Target = ao.id,
        Action = 'Log',
        Data = 'Skipping Dispatch for Indicators (AMM: ' .. ammProcessId .. ')'
      })
    end
    return
  end

  print('sending indicators to ' .. #processes .. ' processes')

  local message = {
    ['Target'] = ao.id,
    ['App-Name'] = 'Dexi',
    ['Assignments'] = processes,
    ['Action'] = 'IndicatorsUpdate',
    ['AMM'] = ammProcessId,
    ['Data'] = json.encode(indicatorsResults)
  }
  ao.send(message)

  print('Dispatched indicators for all AMMs')
end

return indicators
end
end

do
local _ENV = _ENV
package.preload[ "ingest.ingest" ] = function( ... ) local arg = _G.arg;
local json = require('json')

local validationSchemas = require('validation.validation-schemas')
local dbUtils = require('db.utils')
local dexiCore = require('dexi-core.dexi-core')
local indicators = require('indicators.indicators')
local topN = require('top-n.top-n')

local ingest = {}


-- ==================== SQL ===================== --

local ingestSql = {}

function ingestSql.getGatewayHeight(msg)
  local stmt = db:prepare [[
    SELECT MAX(block_height) AS max_height
    FROM amm_transactions
    WHERE source = 'gateway' AND amm_process = :amm;
  ]]

  stmt:bind_names({ amm = msg.Tags.AMM })

  local row = dbUtils.queryOne(stmt)
  local gatewayHeight = row and row.max_height or 0

  stmt:reset()

  return gatewayHeight
end

function ingestSql.recordSwap(entry)
  local stmt, err = db:prepare [[
    REPLACE INTO amm_transactions (
      id, source, block_height, block_id, sender, created_at_ts,
      to_token, from_token, from_quantity, to_quantity, fee_percentage, amm_process
    ) VALUES (:id, :source, :block_height, :block_id, :sender, :created_at_ts,
              :to_token, :from_token, :from_quantity, :to_quantity, :fee_percentage, :amm_process);
  ]]

  if not stmt then
    error("Failed to prepare SQL statement: " .. db:errmsg())
  end

  -- going for brevity - this will be more robust with teal
  stmt:bind_names(entry)

  stmt:step()
  stmt:reset()
end

function ingestSql.recordChangeInSwapParams(entry)
  local stmt, err = db:prepare [[
    REPLACE INTO amm_swap_params_changes (
      id, source, block_height, block_id, sender, created_at_ts, cause
      reserves_0, reserves_1, fee_percentage, amm_process
    ) VALUES (:id, :source, :block_height, :block_id, :sender, :created_at_ts, :cause,
              :reserves_0, :reserves_1, :fee_percentage, :amm_process);
  ]]

  if not stmt then
    error("Failed to prepare SQL statement: " .. db:errmsg())
  end

  -- going for brevity - this will be more robust with teal
  stmt:bind_names(entry)

  stmt:step()
  stmt:reset()
end

function ingestSql.updateCurrentSwapParams(entry)
  local stmt, err = db:prepare [[
    REPLACE INTO amm_swap_params (
      amm_process, reserves_0, reserves_1, fee_percentage
    ) VALUES (
     :amm_process, :reserves_0, :reserves_1, :fee_percentage
    );
  ]]

  if not stmt then
    error("Failed to prepare SQL statement: " .. db:errmsg())
  end

  -- going for brevity - this will be more robust with teal
  stmt:bind_names(entry)

  stmt:step()
  stmt:reset()
end

-- ==================== INTERNAL ===================== --

local function recordChangeInSwapParams(msg, source, sourceAmm, cause)
  local reserves_0 = msg.Tags["Reseves-Token-A"]
  local reserves_1 = msg.Tags["Reseves-Token-B"]
  local fee_percentage = msg.Tags["Fee-Percentage"]

  local valid, err = validationSchemas.swapParamsSchema(reserves_0, reserves_1, fee_percentage)
  assert(valid, 'Invalid input amm swap params data' .. json.encode(err))

  local entry = {
    id = msg.Id,
    source = source,
    block_height = msg['Block-Height'],
    block_id = msg['Block-Id'] or '',
    sender = msg.recipient or '',
    created_at_ts = math.floor(msg.Timestamp / 1000),
    cause = cause,
    fee_percentage = fee_percentage,
    reserves_0 = reserves_0,
    reserves_1 = reserves_1,
    amm_process = sourceAmm
  }

  ingestSql.recordChangeInSwapParams(entry)
  ingestSql.updateCurrentSwapParams(entry)
end

local function recordSwap(msg, source, sourceAmm)
  local valid, err = validationSchemas.inputMessageSchema(msg)
  assert(valid, 'Invalid input transaction data' .. json.encode(err))

  local entry = {
    id = msg.Id,
    source = source,
    block_height = msg['Block-Height'],
    block_id = msg['Block-Id'] or '',
    sender = msg.recipient or '',
    created_at_ts = msg.Timestamp / 1000,
    to_token = msg.Tags['To-Token'],
    from_token = msg.Tags['From-Token'],
    from_quantity = tonumber(msg.Tags['From-Quantity']),
    to_quantity = tonumber(msg.Tags['To-Quantity']),
    fee_percentage = tonumber(msg.Tags['Fee-Percentage']),
    amm_process = sourceAmm
  }
  ingestSql.recordSwap(entry)
  --[[
      the new swap affects
        the latest price =>
          the market cap of this amm's base token =>
            the overall ranking by market cap =>
              the top N token sets
    ]]
  topN.updateTopNTokenSet()
end

-- ==================== EXPORT ===================== --

function ingest.getCurrentHeight(msg)
  ao.send({
    Target = msg.From,
    Height = tostring(ingestSql.getGatewayHeight(msg))
  })
end

-- INGEST SWAP PARAMS CHANGES

function ingest.handleMonitorIngestSwapParamsChange(msg)
  local ammProcessId = dexiCore.isKnownAmm(msg.From)
      and msg.From
      or (msg.From == Owner and msg.Tags["AMM"] or nil)
  if ammProcessId then
    local now = math.floor(msg.Timestamp / 1000)
    recordChangeInSwapParams(msg, 'message', ammProcessId, 'swap-params-change')
    topN.dispatchMarketDataIncludingAMM(now, ammProcessId)
  end
end

function ingest.handleFeedIngestSwapParamsChange(msg)
  if msg.From == OFFCHAIN_FEED_PROVIDER then
    local data = json.decode(msg.Data)
    for _, liquidityUpdate in ipairs(data) do
      recordChangeInSwapParams(liquidityUpdate, 'gateway', liquidityUpdate.Tags['AMM'], 'swap-params-change')
    end

    local isLatestSwapParamsChange = false -- TODO implement; check if data goes up to present and this is the latest data entry;
    if isLatestSwapParamsChange then
      local now = math.floor(msg.Timestamp / 1000)
      topN.dispatchMarketDataIncludingAMM(now, msg.Tags['AMM'])
    end
  end
end

-- INGEST SWAPS

function ingest.handleMonitorIngestSwap(msg)
  local ammProcessId = dexiCore.isKnownAmm(msg.From)
      and msg.From
      or (msg.From == Owner and msg.Tags["AMM"] or nil)
  if ammProcessId then
    local now = math.floor(msg.Timestamp / 1000)

    recordSwap(msg, 'message', ammProcessId)

    -- the new swap affects indicators for this amm
    indicators.dispatchIndicatorsForAMM(ammProcessId, now)

    recordChangeInSwapParams(msg, 'message', ammProcessId, 'swap')

    topN.dispatchMarketDataIncludingAMM(now, ammProcessId)
  end
end

function ingest.handleFeedIngestSwaps(msg)
  if msg.From == OFFCHAIN_FEED_PROVIDER then
    local data = json.decode(msg.Data)
    for _, swap in ipairs(data) do
      local ammProcessId = swap.Tags['AMM']
      local now = math.floor(msg.Timestamp / 1000)

      recordSwap(swap, 'gateway', ammProcessId)

      local isLatestSwap = false -- TODO: implement; check if data goes up to present and this is the latest data entry;
      if isLatestSwap then
        -- the new swap affects indicators for this amm
        indicators.dispatchIndicatorsForAMM(ammProcessId, now)

        --[[
          the new swap affects
            the latest price =>
              the market cap of this amm's base token =>
                the overall ranking by market cap =>
                  the top N token sets
        ]]
        topN.updateTopNTokenSet()

        recordChangeInSwapParams(msg, 'gateway', swap.Tags['AMM'], 'swap')
      end
    end
  end
end

return ingest
end
end

