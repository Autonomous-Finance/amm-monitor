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
