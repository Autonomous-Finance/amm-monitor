local sqlschema = require('sqlschema')
local indicators = {}

function indicators.getDailyStats(ammProcessId, startDate, endDate)
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
  return sqlschema.queryMany(stmt)
end

function indicators.fillMissingDates(dailyStats, startDate, endDate)
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

function indicators.calculateSMAs(dailyStats)
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

function indicators.calculateEMAs(dailyStats)
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

function indicators.calculateMACD(ema12, ema26)
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

function indicators.calculateBollingerBands(dailyStats, smas)
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

function indicators.getIndicators(ammProcessId, startTimestamp, endTimestamp)
  local endDate = os.date("!%Y-%m-%d", endTimestamp)
  local startDate = os.date("!%Y-%m-%d", startTimestamp)

  local dailyStats = indicators.getDailyStats(ammProcessId, startDate, endDate)
  local filledDailyStats = indicators.fillMissingDates(dailyStats, startDate, endDate)
  local smas = indicators.calculateSMAs(filledDailyStats)
  -- local ema12, ema26 = indicators.calculateEMAs(filledDailyStats)
  -- local macd, signalLine, histogram = indicators.calculateMACD(ema12, ema26)
  -- local upperBand, lowerBand = indicators.calculateBollingerBands(filledDailyStats, smas)

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

function indicators.dispatchIndicatorsMessage(ammProcessId, startTimestamp, endTimestamp)
  local subscribersStmt = db:prepare([[
      SELECT s.process_id
      FROM subscriptions s
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

  local indicatorsResults = indicators.getIndicators(ammProcessId, startTimestamp, endTimestamp)

  local json = require("json")

  print('sending indicators to ' .. #processes .. ' processes')

  local message = {
    ['Target'] = ao.id,
    ['Assignments'] = processes,
    ['Action'] = 'IndicatorsUpdate',
    ['AMM'] = ammProcessId,
    ['Data'] = json.encode(indicatorsResults)
  }
  ao.send(message)

  -- for _, processId in ipairs(processes) do
  --   message.Target = processId
  -- ao.send(message)
  -- end
end

function indicators.dispatchIndicatorsForAMM(now, ammProcessId)
  local ammStmt = db:prepare([[
      SELECT amm_discovered_at_ts
      FROM amm_registry
      WHERE amm_process = :amm_process_id
    ]])
  if not ammStmt then
    error("Err: " .. db:errmsg())
  end

  ammStmt:bind_names({ amm_process_id = ammProcessId })

  local oneWeekAgo = now - (7 * 24 * 60 * 60)

  local row = ammStmt:step()
  local discoveredAt = row.amm_discovered_at_ts

  local startTimestamp = math.max(discoveredAt, oneWeekAgo)
  indicators.dispatchIndicatorsMessage(ammProcessId, startTimestamp, now)

  ammStmt:finalize()
  print('Dispatched indicators for all AMMs')
end

return indicators
