local json = require('json')

local overview = require('dexi-core.overview')
local intervals = require('dexi-core.intervals')
local stats = require('dexi-core.stats')
local candles = require('dexi-core.candles')
local priceAround = require('dexi-core.price-around')


local historicalQueries = {}

-- ---------------- EXPORT

historicalQueries.getOverview = function(msg)
  local now = msg.Timestamp / 1000
  local orderBy = msg.Tags['Order-By']
  ao.send({
    ['App-Name'] = 'Dexi',
    ['Payload'] = 'Overview',
    Target = msg.From,
    Data = json.encode(overview.getOverview(now, orderBy))
  })
end

function historicalQueries.getPricesInBatch(msg)
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

function historicalQueries.getStats(msg)
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

function historicalQueries.getCandles(msg)
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

return historicalQueries
