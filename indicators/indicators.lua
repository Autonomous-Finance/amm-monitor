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

  local row = dbUtils.queryOne(ammStmt)

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

function sql.getActiveSubscribersToAMM(ammProcessId)
  local subscribersStmt = db:prepare([[
      SELECT s.process_id
      FROM indicator_subscriptions s
      JOIN balances b ON s.process_id = b.process_id AND CAST(b.balance AS REAL) > 0
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

  local processes = sql.getActiveSubscribersToAMM(ammProcessId)

  -- local indicatorsResults = getIndicators(ammProcessId, startTimestamp)

  -- if not DISPATCH_ACTIVE then
  --   if LOGGING_ACTIVE then
  --     ao.send({
  --       Target = ao.id,
  --       Action = 'Log',
  --       Data = 'Skipping Dispatch for Indicators (AMM: ' .. ammProcessId .. ')'
  --     })
  --   end
  --   return
  -- end

  -- print('sending indicators to ' .. #processes .. ' processes')

  -- local message = {
  --   ['Target'] = ao.id,
  --   ['App-Name'] = 'Dexi',
  --   ['Assignments'] = processes,
  --   ['Action'] = 'IndicatorsUpdate',
  --   ['AMM'] = ammProcessId,
  --   ['Data'] = json.encode(indicatorsResults)
  -- }
  -- ao.send(message)

  -- print('Dispatched indicators for all AMMs')
end

return indicators
