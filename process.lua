local json = require("json")


local intervals = require "intervals"
local candles = require "candles"
local stats = require "stats"
local schemas = require "schemas"


OFFCHAIN_FEED_PROVIDER = 'iC5mu-_GkholDuxBrzI-rm1gIUagPrBOWhqzUwKBosk'
TOKEN = ao.env.Process.Tags["Base-Token"]
AMM =  ao.env.Process.Tags["Monitor-For"]

Transactions = Transactions or {}
TransactionIdsSet = TransactionIdsSet or {}


local function insertSingleMessage(msg, source)
  local valid, err = schemas.inputMessageSchema(msg)
  assert(valid, 'Invalid input transaction data' .. json.encode(err))

  local isBuy = msg.Tags['To-Token'] == TOKEN
  local nodeId = msg.Id
  local fromQuantity = tonumber(msg.Tags['From-Quantity'])
  local toQuantity = tonumber(msg.Tags['To-Quantity'])
  local price = nil
  local volume = nil
  if fromQuantity and toQuantity then
    price = isBuy and (toQuantity / fromQuantity) or (fromQuantity / toQuantity)
    volume = isBuy and fromQuantity or toQuantity
  end

  if not TransactionIdsSet[nodeId] then
    local node = {
      Id = msg.Id,
      ['Block-Height'] = msg['Block-Height'],
      From = msg.From,
      Source = source,
      IsBuy = isBuy,
      Timestamp = math.floor(msg.Timestamp / 1000),
      Price = price,
      Volume = volume,
      Fee = tonumber(msg.Tags['Fee']),
      ['To-Token'] = msg.Tags['To-Token'],
      ['From-Token'] = msg.Tags['From-Token'],
      ['From-Quantity'] = fromQuantity,
      ['To-Quantity'] = toQuantity
    }

    local valid, err = schemas.outputMessageSchema(node)
    assert(valid, 'Invalid output transaction data' .. json.encode(err))
    table.insert(Transactions, node)
    TransactionIdsSet[nodeId] = true
  end
end

function insertManyTransactions(transactions, source)
    for _, transaction in ipairs(transactions) do
        insertSingleMessage(transaction, source)
    end
    table.sort(Transactions, function(a, b) return a.Timestamp < b.Timestamp end)
end

local function findPriceAroundTimestamp(targetTimestampBefore)
  for i = #Transactions, 1, -1 do
    local transaction = Transactions[i]
    if transaction.Timestamp and transaction.Timestamp < targetTimestampBefore and transaction.Price then
      return transaction.Price
    end
  end
  return nil
end


Handlers.add(
  "get-stats",
  Handlers.utils.hasMatchingTag("Action", "Get-Stats"),
  function (msg)
    local totalVolume, buyVolume, sellVolume, buyCount, sellCount, distinctBuyers, distinctSellers, distinctTraders, latestPrice = stats.getAggregateStats(0)
    local now = msg.Timestamp / 1000

    local price24HAgo = findPriceAroundTimestamp(now - intervals.IntervalSecondsMap['1d'])
    local price6HAgo = findPriceAroundTimestamp(now - intervals.IntervalSecondsMap['6h'])
    local price1HAgo = findPriceAroundTimestamp(now - intervals.IntervalSecondsMap['1h'])
    local candles = candles.generateCandlesForXDaysInIntervalY(30, '15m', now)
    
    ao.send({
      Target = msg.From, 
      ['Total-Volume'] = tostring(totalVolume),
      ['Buy-Volume'] = tostring(buyVolume),
      ['Sell-Volume'] = tostring(sellVolume),
      ['Buy-Count'] = tostring(buyCount),
      ['Sell-Count'] = tostring(sellCount),
      ['Buyers'] = tostring(distinctBuyers),
      ['Sellers'] = tostring(distinctSellers),
      ['Total-Traders'] = tostring(distinctTraders),
      ['Latest-Price'] = tostring(latestPrice),
      ['Price-24H-Ago'] = tostring(price24HAgo),
      ['Price-6H-Ago'] = tostring(price6HAgo),
      ['Price-1H-Ago'] = tostring(price1HAgo),
      Data = json.encode(candles)
    })
  end
)

Handlers.add(
  "GetCandles",
  Handlers.utils.hasMatchingTag("Action", "Get-Candles"),
  function (msg)
    local candles = candles.generateCandlesForXDaysInIntervalY(30, '15m', msg.Timestamp / 1000)
    ao.send({
      Target = msg.From,
      Data = json.encode(candles)
    })
  end
)

Handlers.add(
  "UpdateLocalState", -- handler name
  Handlers.utils.hasMatchingTag("Action", "Order-Confirmation-Monitor"),
  function (msg)
    if msg.From == AMM then
      insertManyTransactions({msg}, 'message')
    end
  end
)


Handlers.add(
  "ReceiveOffchainFeed", -- handler name
  Handlers.utils.hasMatchingTag("Action", "Receive-Offchain-Feed"),
  function (msg)
    if msg.From == OFFCHAIN_FEED_PROVIDER then
      local data = json.decode(msg.Data)
      insertManyTransactions(data, 'gateway')
    end
  end
)


Handlers.add(
  "GetCurrentHeight",
  Handlers.utils.hasMatchingTag("Action", "Get-Current-Height"),
  function (msg)
    local gatewayHeight = 0
    for _, transaction in ipairs(Transactions) do
      if transaction.Source == 'gateway' then
        gatewayHeight = math.max(gatewayHeight, transaction['Block-Height'])
      end
    end

    ao.send({
      Target = msg.From,
      Height = gatewayHeight
    })
  end
)


Handlers.add(
  "GetAMM", -- handler name
  Handlers.utils.hasMatchingTag("Action", "Get-AMM"), -- handler pattern to identify cron message
  function (msg)
    ao.send({
      Target = msg.From,
      AMM = AMM
    })
  end
)



-- Handlers.add(
--   "receive-data-feed",
--   Handlers.utils.hasMatchingTag("Action", "Receive-data-feed"),
--   function (msg)
--     local data = json.decode(msg.Data)
--     if data.data.transactions then
--       updateTransactions(data.data.transactions.edges)
--       print('transactions updated')
--       if #data.data.transactions.edges > 0 then
--         requestTransactions(100)
--       end
--       requestBlocks()
--     elseif data.data.blocks then
--       updateBlockTimestamps(data.data.blocks.edges)
--       print('blocks updated')
--     end
--   end
-- )