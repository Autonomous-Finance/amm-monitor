local json = require("json")
local sqlite3 = require("lsqlite3")

local overview = require("dexi-core.overview")
local dexiCore = require("dexi-core.dexi-core")
local ingest = require("ingest.ingest")
local sqlschema = require("dexi-core.sqlschema")
local indicators = require("indicators.indicators")
local topN = require("top-n.top-n")
local debug = require("utils.debug")

db = db or sqlite3.open_memory()

sqlschema.createTableIfNotExists(db)

-- eliminate warnings
Owner = Owner or ao.env.Process.Owner
Handlers = Handlers or {}
ao = ao or {}

DEXI_TOKEN = "eM6NGBSgwyDTqQ0grng1fQvBF-5HcMeshLcz9QPE-0A"
TOKEN = ao.env.Process.Tags["Base-Token"]
AMM = ao.env.Process.Tags["Monitor-For"]

-- ================== HANDLER LOGIC ================= --
OFFCHAIN_FEED_PROVIDER = OFFCHAIN_FEED_PROVIDER or ao.env.Process.Tags["Offchain-Feed-Provider"]
BARK_TOKEN_PROCESS = BARK_TOKEN_PROCESS or ao.env.Process.Tags["Bark-Token-Process"]


-- -------------- SUBSCRIPTIONS -------------- --

local recordPayment = function(msg)
  if msg.From == 'Sa0iBLPNyJQrwpTTG-tWLQU-1QeUAJA73DdxGGiKoJc' then
    sqlschema.updateBalance(msg.Tags.Sender, msg.From, tonumber(msg.Tags.Quantity), true)
  end
end

local subscribeForIndicators = function(msg)
  local processId = msg.Tags['Subscriber-Process-Id']
  local ownerId = msg.Tags['Owner-Id']
  local ammProcessId = msg.Tags['AMM-Process-Id']

  print('Registering subscriber to indicator data: ' ..
    processId .. ' for amm: ' .. ammProcessId .. ' with owner: ' .. ownerId)
  indicators.registerIndicatorSubscriber(processId, ownerId, ammProcessId)

  ao.send({
    Target = ao.id,
    Assignments = { ownerId, processId },
    Action = 'Dexi-Indicator-Subscription-Confirmation',
    AMM = ammProcessId,
    Process = processId,
    OK = 'true'
  })
end

local subscribeForTopN = function(msg)
  local processId = msg.Tags['Subscriber-Process-Id']
  local ownerId = msg.Tags['Owner-Id']
  local quoteToken = msg.Tags['Quote-Token']

  if not quoteToken then
    error('Quote-Token is required')
  end

  if quoteToken ~= BARK_TOKEN_PROCESS then
    error('Quote token not available (only BRK): ' .. quoteToken)
  end

  print('Registering subscriber to top N market data: ' ..
    processId .. ' for quote token: ' .. quoteToken .. ' with owner: ' .. ownerId)
  topN.registerTopNSubscriber(processId, ownerId, quoteToken)

  -- determine top N token set for this subscriber
  topN.updateTopNTokenSet(processId)

  ao.send({
    Target = ao.id,
    Assignments = { ownerId, processId },
    Action = 'Dexi-Top-N-Subscription-Confirmation',
    QuoteToken = quoteToken,
    Process = processId,
    OK = 'true'
  })
end

local recordRegisterAMMPayment = function(msg)
  assert(msg.Tags.Quantity, 'Credit notice data must contain a valid quantity')
  assert(msg.Tags.Sender, 'Credit notice data must contain a valid sender')
  assert(msg.Tags["AMM-Process"], 'Credit notice data must contain a valid amm-process')
  assert(msg.Tags["Token-A"], 'Credit notice data must contain a valid token-a')
  assert(msg.Tags["Token-B"], 'Credit notice data must contain a valid token-b')
  assert(msg.Tags["Name"], 'Credit notice data must contain a valid fee-percentage')

  -- send Register-Subscriber to amm process
  ao.send({
    Target = msg.Tags["AMM-Process"],
    Action = "Register-Subscriber",
    Tags = {
      ["Subscriber-Process-Id"] = ao.id,
      ["Owner-Id"] = msg.Tags.Sender,
      ['Topics'] = json.encode({ "order-confirmation", "liquidity-change" })
    }
  })

  -- Pay for the Subscription
  ao.send({
    Target = DEXI_TOKEN,
    Action = "Transfer",
    Tags = {
      Receiver = msg.Tags["AMM-Process"],
      Quantity = msg.Tags.Quantity,
      ["X-Action"] = "Pay-For-Subscription"
    }
  })

  sqlschema.registerAMM(
    msg.Tags["Name"],
    msg.Tags["AMM-Process"],
    msg.Tags["Token-A"],
    msg.Tags["Token-B"],
    msg.Timestamp
  )

  -- send confirmation to sender
  ao.send({
    Target = msg.Tags.Sender,
    Action = "Dexi-AMM-Registration-Confirmation",
    Tags = {
      ["AMM-Process"] = msg.Tags["AMM-Process"],
      ["Token-A"] = msg.Tags["Token-A"],
      ["Token-B"] = msg.Tags["Token-B"],
      ["Name"] = msg.Tags["Name"]
    }
  
  })
end

-- -------------------------------------------- --

Handlers.add(
  "GetRegisteredAMMs",
  Handlers.utils.hasMatchingTag("Action", "Get-Registered-AMMs"),
  dexiCore.getRegisteredAMMs
)

Handlers.add(
  "GetStats",
  Handlers.utils.hasMatchingTag("Action", "Get-Stats"),
  dexiCore.getStats
)

Handlers.add(
  "GetCandles",
  Handlers.utils.hasMatchingTag("Action", "Get-Candles"),
  dexiCore.getCandles
)

Handlers.add(
  "UpdateLocalState-Swap",
  Handlers.utils.hasMatchingTag("Action", "Swap-Monitor"),
  ingest.monitorIngestSwap
)

Handlers.add(
  "UpdateLocalState-Swap-Params-Change",
  Handlers.utils.hasMatchingTag("Action", "Swap-Params-Change"),
  ingest.monitorIngestSwapParamsChange
)

Handlers.add(
  "ReceiveOffchainFeed-Swaps",
  Handlers.utils.hasMatchingTag("Action", "Receive-Offchain-Feed-Swaps"),
  ingest.feedIngestSwaps
)

Handlers.add(
  "ReceiveOffchainFeed-Swap-Params-Changes",
  Handlers.utils.hasMatchingTag("Action", "Receive-Offchain-Feed-Swap-Params-Changes"),
  ingest.feedIngestSwapParamsChange
)

Handlers.add(
  "GetOverview",
  Handlers.utils.hasMatchingTag("Action", "Get-Overview"),
  overview.getOverview
)

Handlers.add(
  "GetTopNMarketData",
  Handlers.utils.hasMatchingTag("Action", "Get-Top-N-Market-Data"),
  topN.getTopNMarketData
)

Handlers.add(
  "SubscribeIndicators",
  Handlers.utils.hasMatchingTag("Action", "Subscribe-Indicators"),
  subscribeForIndicators
)

Handlers.add(
  "SubscribeTopN",
  Handlers.utils.hasMatchingTag("Action", "Subscribe-Top-N"),
  subscribeForTopN
)

Handlers.add(
  "BatchRequestPrices",
  Handlers.utils.hasMatchingTag("Action", "Price-Batch-Request"),
  dexiCore.getPricesInBatch
)

Handlers.add(
  "CreditNotice",
  Handlers.utils.hasMatchingTag("Action", "Credit-Notice"),
  recordPayment
)

Handlers.add(
  "CreditNoticeRegisterAMM",
  function(msg)
    return Handlers.utils.hasMatchingTag("Action", "Credit-Notice")(msg) and
        Handlers.utils.hasMatchingTag("X-Action", "Register-AMM")(msg) and
        msg.From == DEXI_TOKEN
  end,
  recordRegisterAMMPayment
)

Handlers.add(
  "GetCurrentHeight",
  Handlers.utils.hasMatchingTag("Action", "Get-Current-Height"),
  ingest.getCurrentHeight
)

Handlers.add(
  "DumpTableToCSV",
  Handlers.utils.hasMatchingTag("Action", "Dump-Table-To-CSV"),
  debug.dumpToCSV
)


function Trusted(msg)
  local mu = "fcoN_xJeisVsPXA-trzVAuIiqO3ydLQxM-L4XbrQKzY"
  -- return false if trusted
  if msg.Owner == mu then
    return false
  end
  if msg.From == msg.Owner then
    return false
  end
  return true
end

Handlers.prepend("qualify message",
  Trusted,
  function(msg)
    print("This Msg is not trusted!")
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
