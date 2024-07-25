local sqlite3 = require("lsqlite3")

local dexiCore = require("dexi-core.dexi-core")
local subscriptions = require("subscriptions.subscriptions")
local seeder = require("db.seed")
local ingest = require("ingest.ingest")
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
QUOTE_TOKEN_TICKER = QUOTE_TOKEN_TICKER or ao.env.Process.Tags["Quote-Token-Ticker"]
SUPPLY_UPDATES_PROVIDER = SUPPLY_UPDATES_PROVIDER or
    ao.env.Process.Tags["Offchain-Supply-Updates-Provider"]
PAYMENT_TOKEN_PROCESS = PAYMENT_TOKEN_PROCESS or ao.env.Process.Tags["Payment-Token-Process"]
PAYMENT_TOKEN_TICKER = PAYMENT_TOKEN_TICKER or ao.env.Process.Tags["Payment-Token-Ticker"]

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
  dexiCore.handleUpdateTokenSupply
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
  "Get-Indicators",
  Handlers.utils.hasMatchingTag("Action", "Get-Indicators"),
  indicators.handleGetIndicators
)

Handlers.add(
  "Subscribe-Indicators",
  Handlers.utils.hasMatchingTag("Action", "Subscribe-Indicators"),
  subscriptions.handleSubscribeForIndicators
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
  subscriptions.handleSubscribeForTopN
)

-- PAYMENTS

Handlers.add(
  "CreditNotice",
  Handlers.utils.hasMatchingTag("Action", "Credit-Notice"),
  subscriptions.recordPayment
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
