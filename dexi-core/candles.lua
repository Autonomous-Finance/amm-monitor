local intervals = require('dexi-core.intervals')
local dbUtils = require('db.utils')
local hopper = require('hopper.hopper')
local lookups = require('dexi-core.lookups')

local candles = {}

function candles.generateCandlesForXDaysInIntervalY(xDays, yInterval, endTime, ammProcessId, convertToUsd)
  local intervalSeconds = intervals.IntervalSecondsMap[yInterval]
  if not intervalSeconds then
    error("Invalid interval specified")
    return
  end

  local conversionPrice = 1

  if convertToUsd then
    local ammInfo = lookups.ammInfo(ammProcessId)
    if ammInfo then
      local hopperPrice = hopper.getPrice(ammInfo.amm_token0, 'USD')
      if hopperPrice then
        conversionPrice = hopperPrice
      else
        conversionPrice = 0
      end
    end
  end

  -- Determine the GROUP BY clause based on the interval
  local candleTime
  if yInterval == '15m' then
    candleTime = 60 * 15
  elseif yInterval == '1h' then
    candleTime = 60 * 60
  elseif yInterval == '4h' then
    candleTime = 60 * 60 * 4
  elseif yInterval == '1d' then
    candleTime = 60 * 60 * 24
  else
    error("Unsupported interval for grouping")
    return
  end

  local stmt = [[
    SELECT
        t1.price * :conversion_price AS open,
        m.high * :conversion_price as high,
        m.low * :conversion_price as low,
        t2.price * :conversion_price as close,
        strftime('%Y-%m-%d %H:%M', min_time, 'unixepoch') as candle_time,
        min_time as start_timestamp,
        max_time as end_timestamp,
        m.volume
  FROM (SELECT
          MAX(amm_process) AS amm_process,
          MIN(created_at_ts) AS min_time,
          MAX(created_at_ts) AS max_time,
          MIN(price) as low,
          MAX(price) as high,
          FLOOR(amm_transactions_view.created_at_ts/:candle_time) as open_time,
          SUM(volume) * :conversion_price AS volume
        FROM amm_transactions_view
        WHERE created_at_ts >= :start_time AND created_at_ts < :end_time AND amm_process = :amm_process
        GROUP BY open_time) m
  JOIN amm_transactions_view t1 ON t1.created_at_ts = min_time AND t1.amm_process = m.amm_process
  JOIN amm_transactions_view t2 ON t2.created_at_ts = max_time AND t2.amm_process = m.amm_process
  ORDER BY min_time ASC
  ]]

  local startTime = endTime - (xDays * 24 * 3600)

  local params = {
    start_time = startTime,
    end_time = endTime,
    amm_process = ammProcessId,
    candle_time = candleTime,
    conversion_price = conversionPrice
  }

  local candles = dbUtils.queryManyWithParams(stmt, params)

  for i = 2, #candles do
    candles[i].open = candles[i - 1].close
  end

  return candles
end

return candles
