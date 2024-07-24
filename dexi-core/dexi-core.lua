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
    quote_token = token0 == BARK_TOKEN_PROCESS and token0 or token1,
    base_token = token0 == BARK_TOKEN_PROCESS and token1 or token0,
    discovered_at = discoveredAt
  })
  stmt:step()
  print("Err: " .. db:errmsg())
  stmt:reset()
end

function sql.registerToken(processId, name, denominator, totalSupply, fixedSupply, updatedAt)
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

function sql.updateTokenSupply(processId, totalSupply, fixedSupply, updatedAt)
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

  local processId = msg.Tags["Process-Id"]
  local totalSupply = msg.Tags["Total-Supply"]
  local updatedAt = msg.Tags["Updated-At"]
  sql.updateTokenSupply(processId, totalSupply, updatedAt)
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
