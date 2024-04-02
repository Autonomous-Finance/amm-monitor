local intervals = require('intervals')
local candles = {}

function candles.generateCandlesForXDaysInIntervalY(xDays, yInterval, endTime)
    local intervalSeconds = intervals.IntervalSecondsMap[yInterval]
    if not intervalSeconds then
      error("Invalid interval specified")
      return
    end
  
    local startTime = endTime - (xDays * 24 * 3600) -- Calculate start time based on x days ago
  
    local candles = {}
    local candleStartTimestamp = intervals.getIntervalStart(startTime, yInterval)
    local candleEndTimestamp = candleStartTimestamp + intervalSeconds
  
    local openPrice, closePrice, highPrice, lowPrice, totalVolume = nil, nil, nil, nil, 0
  
    for _, transaction in ipairs(Transactions) do
      if transaction.Timestamp and transaction.Timestamp >= startTime and transaction.Timestamp <= endTime then
        -- Initialize the first candle's open price
        if not openPrice then
          openPrice = transaction.Price
        end
  
        -- Check if the transaction belongs to the current candle
        if transaction.Timestamp < candleEndTimestamp then
          closePrice = transaction.Price -- Update close price with each transaction
          highPrice = (not highPrice or transaction.Price > highPrice) and transaction.Price or highPrice
          lowPrice = (not lowPrice or transaction.Price < lowPrice) and transaction.Price or lowPrice
          totalVolume = totalVolume + (transaction.Volume or 0)
        else
          -- Save the completed candle
          table.insert(candles, {
            open = openPrice,
            close = closePrice,
            high = highPrice,
            low = lowPrice,
            volume = totalVolume,
            startTimestamp = candleStartTimestamp,
            endTimestamp = candleEndTimestamp - 1,
            startTime = os.date("%Y-%m-%d %H:%M:%S", candleStartTimestamp),
            endTime = os.date("%Y-%m-%d %H:%M:%S", candleEndTimestamp - 1)
          })
  
          -- Prepare for the next candle
          while transaction.Timestamp >= candleEndTimestamp do
            candleStartTimestamp = candleEndTimestamp
            candleEndTimestamp = candleStartTimestamp + intervalSeconds
          end
  
          -- Set the open price of the new candle to the close price of the last candle
          openPrice, highPrice, lowPrice, totalVolume = closePrice, transaction.Price, transaction.Price, transaction.Volume or 0
          closePrice = transaction.Price -- Ensure the close price is updated for the new candle
        end
      end
    end
  
    -- Add the last candle if it has any data
    if openPrice then
      table.insert(candles, {
        open = openPrice,
        close = closePrice,
        high = highPrice,
        low = lowPrice,
        volume = totalVolume,
        startTimestamp = candleStartTimestamp,
        endTimestamp = candleEndTimestamp - 1,
        startTime = os.date("%Y-%m-%d %H:%M:%S", candleStartTimestamp),
        endTime = os.date("%Y-%m-%d %H:%M:%S", candleEndTimestamp - 1)
      })
    end
  
    return candles
end

return candles

