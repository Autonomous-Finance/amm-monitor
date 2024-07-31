local sqlite3 = require("lsqlite3")

local dexiCore = require("dexi-core.dexi-core")
local subscriptions = require("subscriptions.subscriptions")
local indicators = require("indicators.indicators")
local seeder = require("db.seed")
local ingest = require("ingest.ingest")
local topN = require("top-n.top-n")
local debug = require("utils.debug")
local register_amm = require("register-amm.register-amm")

db = db or sqlite3.open_memory()

seeder.createMissingTables()
seeder.seed() -- TODO eliminate in production

-- eliminate warnings
Owner = Owner or ao.env.Process.Owner
Handlers = Handlers or {}
ao = ao or {}

-- OWNABLE --
Ownable = require "ownable.ownable"

OFFCHAIN_FEED_PROVIDER = OFFCHAIN_FEED_PROVIDER or ao.env.Process.Tags["Offchain-Feed-Provider"]
QUOTE_TOKEN_PROCESS = QUOTE_TOKEN_PROCESS or ao.env.Process.Tags["Quote-Token-Process"]
QUOTE_TOKEN_TICKER = QUOTE_TOKEN_TICKER or ao.env.Process.Tags["Quote-Token-Ticker"]
SUPPLY_UPDATES_PROVIDER = SUPPLY_UPDATES_PROVIDER or
    ao.env.Process.Tags["Offchain-Supply-Updates-Provider"]
PAYMENT_TOKEN_PROCESS = PAYMENT_TOKEN_PROCESS or ao.env.Process.Tags["Payment-Token-Process"]
PAYMENT_TOKEN_TICKER = PAYMENT_TOKEN_TICKER or ao.env.Process.Tags["Payment-Token-Ticker"]

DISPATCH_ACTIVE = DISPATCH_ACTIVE or true
LOGGING_ACTIVE = LOGGING_ACTIVE or true

OPERATOR = OPERATOR or ao.env.Process.Tags["Operator"]

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
  function(msg)
    return Handlers.utils.hasMatchingTag("Action", "Notify-On-Topic")(msg)
        and
        Handlers.utils.hasMatchingTag("Topic", "order-confirmation")(msg) -- TODO add check that msg.From is am AMM registered as topics provider
  end,
  ingest.handleMonitorIngestSwap
)

Handlers.add(
  "UpdateLocalState-Swap-Params-Change",
  function(msg)
    return Handlers.utils.hasMatchingTag("Action", "Notify-On-Topic")(msg)
        and
        Handlers.utils.hasMatchingTag("Topic", "swap-params-change")(msg) -- TODO add check that msg.From is am AMM registered as topics provider
  end,
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

Handlers.add(
  "Unsubscribe-Indicators",
  Handlers.utils.hasMatchingTag("Action", "Unsubscribe-Indicators"),
  subscriptions.handleUnsubscribeForIndicators
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

Handlers.add(
  "Unsubscribe-Top-N",
  Handlers.utils.hasMatchingTag("Action", "Unsubscribe-Top-N"),
  subscriptions.handleUnsubscribeForTopN
)

-- PAYMENTS

Handlers.add(
  "Receive-Payment-For-Subscriber",
  function(msg)
    return Handlers.utils.hasMatchingTag("Action", "Credit-Notice")(msg)
        and Handlers.utils.hasMatchingTag("X-Action", "Pay-For-Subscriptions")(msg)
        and msg.From == PAYMENT_TOKEN_PROCESS
  end,
  function(msg)
    subscriptions.recordPayment(msg)
  end
)

Handlers.add(
  "Receive-Payment-For-AMM-Subscription",
  function(msg)
    return Handlers.utils.hasMatchingTag("Action", "Credit-Notice")(msg)
        and Handlers.utils.hasMatchingTag("X-Action", "Register-AMM")(msg)
        and msg.From == PAYMENT_TOKEN_PROCESS
  end,
  function(msg)
      register_amm.handlePayForSubscriptions(msg)
  end
)

-- REGISTER AMM Subscriber

Handlers.add(
  "Register-AMM-Subscriber",
  Handlers.utils.hasMatchingTag("Action", "Register-AMM-Subscriber"),
  register_amm.handleRegisterSubscriber
)

-- MAINTENANCE

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

Handlers.add(
  "Debug-Table",
  Handlers.utils.hasMatchingTag("Action", "Debug-Table"),
  debug.debugTransactions
)

Handlers.add(
  "Toggle-Dispatch-Active",
  Handlers.utils.hasMatchingTag("Action", "Toggle-Dispatch-Active"),
  function(msg)
    assert(msg.From == OPERATOR, "Only the operator can toggle dispatching")
    DISPATCH_ACTIVE = not DISPATCH_ACTIVE
    ao.send({
      Target = msg.From,
      Data = "Dispatching toggled to " .. tostring(not DISPATCH_ACTIVE)
    })
  end
)

Handlers.add(
  "Toggle-Logging-Active",
  Handlers.utils.hasMatchingTag("Action", "Toggle-Logging-Active"),
  function(msg)
    assert(msg.From == OPERATOR, "Only the operator can toggle logging")
    LOGGING_ACTIVE = not LOGGING_ACTIVE
    ao.send({
      Target = msg.From,
      Data = "Logging toggled to " .. tostring(not LOGGING_ACTIVE)
    })
  end
)
