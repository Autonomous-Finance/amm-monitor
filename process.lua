local sqlite3 = require("lsqlite3")

local dexiCore = require("dexi-core.dexi-core")
local sqlschema = require("db.sqlschema")
local seeder = require("db.seed")
local ingest = require("ingest.ingest")
local indicators = require("indicators.indicators")
local topN = require("top-n.top-n")
local debug = require("utils.debug")

db = db or sqlite3.open_memory()

seeder.createMissingTables()
seeder.seed() -- TODO eliminate in production

-- eliminate warnings
Owner = Owner or ao.env.Process.Owner
Handlers = Handlers or {}
ao = ao or {}

OFFCHAIN_FEED_PROVIDER = OFFCHAIN_FEED_PROVIDER or ao.env.Process.Tags["Offchain-Feed-Provider"]
QUOTE_TOKEN_PROCESS = QUOTE_TOKEN_PROCESS or ao.env.Process.Tags["Quote-Token-Process"]
SUPPLY_UPDATES_PROVIDER = SUPPLY_UPDATES_PROVIDER or
    ao.env.Process.Tags["Offchain-Supply-Updates-Provider"]

-- -------------- SUBSCRIPTIONS -------------- --
-- TODO move out or remove with refactoring that integrates subscribable package

local recordPayment = function(msg)
  if msg.From == 'Sa0iBLPNyJQrwpTTG-tWLQU-1QeUAJA73DdxGGiKoJc' then
    sqlschema.updateBalance(msg.Tags.Sender, msg.From, tonumber(msg.Tags.Quantity), true)
  end
end

local handleSubscribeForIndicators = function(msg)
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

local handleSubscribeForTopN = function(msg)
  local processId = msg.Tags['Subscriber-Process-Id']
  local ownerId = msg.Tags['Owner-Id']
  local quoteToken = msg.Tags['Quote-Token']

  if not quoteToken then
    error('Quote-Token is required')
  end

  if quoteToken ~= QUOTE_TOKEN_PROCESS then
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

-- -------------------------------------------- --

-- CORE --

Handlers.add(
  "GetRegisteredAMMs",
  Handlers.utils.hasMatchingTag("Action", "Get-Registered-AMMs"),
  dexiCore.handleGetRegisteredAMMs
)

Handlers.add(
  "GetStats",
  Handlers.utils.hasMatchingTag("Action", "Get-Stats"),
  dexiCore.handleGetStats
)

Handlers.add(
  "GetCandles",
  Handlers.utils.hasMatchingTag("Action", "Get-Candles"),
  dexiCore.handleGetCandles
)

Handlers.add(
  "GetOverview",
  Handlers.utils.hasMatchingTag("Action", "Get-Overview"),
  dexiCore.handleGetOverview
)

Handlers.add(
  "BatchRequestPrices",
  Handlers.utils.hasMatchingTag("Action", "Price-Batch-Request"),
  dexiCore.handleGetPricesInBatch
)

Handlers.add(
  'Update-Total-Supply',
  Handlers.utils.hasMatchingTag("Action", "Update-Total-Supply"),
  dexiCore.handleUpdateTotalSupply
)

-- SWAP & SWAP PARAMS CHANGES INGESTION --

Handlers.add(
  "UpdateLocalState-Swap",
  Handlers.utils.hasMatchingTag("Action", "Swap-Monitor"),
  ingest.handleMonitorIngestSwap
)

Handlers.add(
  "UpdateLocalState-Swap-Params-Change",
  Handlers.utils.hasMatchingTag("Action", "Swap-Params-Change"),
  ingest.handleMonitorIngestSwapParamsChange
)

Handlers.add(
  "ReceiveOffchainFeed-Swaps",
  Handlers.utils.hasMatchingTag("Action", "Receive-Offchain-Feed-Swaps"),
  ingest.handleFeedIngestSwaps
)

Handlers.add(
  "ReceiveOffchainFeed-Swap-Params-Changes",
  Handlers.utils.hasMatchingTag("Action", "Receive-Offchain-Feed-Swap-Params-Changes"),
  ingest.handleFeedIngestSwapParamsChange
)

Handlers.add(
  "GetCurrentHeight",
  Handlers.utils.hasMatchingTag("Action", "Get-Current-Height"),
  ingest.getCurrentHeight
)

-- INDICATORS --

Handlers.add(
  "SubscribeIndicators",
  Handlers.utils.hasMatchingTag("Action", "Subscribe-Indicators"),
  handleSubscribeForIndicators
)

-- TOP N --

Handlers.add(
  "Get-Top-N-Market-Data",
  Handlers.utils.hasMatchingTag("Action", "Get-Top-N-Market-Data"),
  topN.handleGetTopNMarketData
)

Handlers.add(
  "Subscribe-Top-N",
  Handlers.utils.hasMatchingTag("Action", "Subscribe-Top-N"),
  handleSubscribeForTopN
)

-- PAYMENTS

Handlers.add(
  "CreditNotice",
  Handlers.utils.hasMatchingTag("Action", "Credit-Notice"),
  recordPayment
)

-- DEBUG

Handlers.add(
  "Reset-DB-State",
  Handlers.utils.hasMatchingTag("Action", "Reset-DB-State"),
  seeder.handleResetDBState
)

Handlers.add(
  "DumpTableToCSV",
  Handlers.utils.hasMatchingTag("Action", "Dump-Table-To-CSV"),
  debug.dumpToCSV
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
