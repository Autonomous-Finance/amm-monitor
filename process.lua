local sqlite3 = require("lsqlite3")

local dexiCore = require("dexi-core.dexi-core")
local usdPrice = require("dexi-core.usd-price")
local subscriptions = require("subscriptions.subscriptions")
local indicators = require("indicators.indicators")
local swapSubscribers = require("swap-subscribers.main")
local seeder = require("db.seed")
local ingest = require("ingest.ingest")
local topN = require("top-n.top-n")
local debug = require("utils.debug")
local integrateAmm = require("integrate-amm.integrate-amm")
local emergency = require("ops.emergency")
local configOps = require("ops.config-ops")
local initialize = require("ops.initialize")
local analytics = require("amm-analytics.main")

db = db or sqlite3.open_memory()

seeder.createMissingTables()

-- eliminate warnings
Owner = Owner or ao.env.Process.Owner
Handlers = Handlers or {}
ao = ao or {}

-- OWNABLE --
Ownable = require "ownable.ownable"

QUOTE_TOKEN = QUOTE_TOKEN or {
  ProcessId = nil,
  Ticker = nil,
  Denominator = nil,
  TotalSupply = nil,
}

PAYMENT_TOKEN_PROCESS = PAYMENT_TOKEN_PROCESS or nil
PAYMENT_TOKEN_TICKER = PAYMENT_TOKEN_TICKER or nil

SUPPLY_UPDATES_PROVIDER = SUPPLY_UPDATES_PROVIDER or nil
OFFCHAIN_FEED_PROVIDER = OFFCHAIN_FEED_PROVIDER or nil

DISPATCH_ACTIVE = DISPATCH_ACTIVE or true
LOGGING_ACTIVE = LOGGING_ACTIVE or true

OPERATOR = OPERATOR or ao.env.Process.Tags["Operator"]

Initialized = Initialized or false

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

Handlers.add(
  'Receive-RedStone-Prices',
  Handlers.utils.hasMatchingTag("Action", "Receive-RedStone-Prices"),
  usdPrice.updateUsdPrice
)


Handlers.add(
  'Get-Oracle-Price',
  Handlers.utils.hasMatchingTag("Action", "Get-Oracle-Price"),
  function(msg)
    assert(msg.Tags["Process-Id"], "Process-Id is required")

    local price = usdPrice.getUsdPriceForToken(msg.Tags["Process-Id"])
    if price then
      ao.send({
        Target = msg.From,
        ResponseFor = msg.Action,
        ['Process-Id'] = msg.Tags['Process-Id'],
        Price = tostring(price)
      })
    else
      ao.send({
        Target = msg.From,
        ResponseFor = msg.Action,
        ['Process-Id'] = msg.Tags['Process-Id'],
        Error = "Price not found"
      })
    end
  end
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

-- SWAP SUBSCRIBERS
-- Handlers.add(
--   "Subscribe-Swaps",
--   Handlers.utils.hasMatchingTag("Action", "Subscribe-Swaps"),
--   swapSubscribers.handleSubscribeForSwaps
-- )

-- Handlers.add(
--   "Unsubscribe-Swaps",
--   Handlers.utils.hasMatchingTag("Action", "Unsubscribe-Swaps"),
--   swapSubscribers.handleUnsubscribeForSwaps
-- )

-- TOP N --

Handlers.add(
  "Get-Top-N-Market-Data",
  Handlers.utils.hasMatchingTag("Action", "Get-Top-N-Market-Data"),
  topN.handleGetTopNMarketData
)

Handlers.add(
  "Get-Top-N-Token-Set",
  Handlers.utils.hasMatchingTag("Action", "Get-Top-N-Token-Set"),
  topN.handleGetTopNTokenSet
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

-- PAYMENT for Dexi Subscribers

Handlers.add(
  "Receive-Payment-For-Subscriber",
  function(msg)
    return Handlers.utils.hasMatchingTag("Action", "Credit-Notice")(msg)
        and Handlers.utils.hasMatchingTag("X-Action", "Pay-For-Subscription")(msg)
        and msg.From == PAYMENT_TOKEN_PROCESS
  end,
  function(msg)
    subscriptions.recordPayment(msg)
  end
)

-- AMM Registration

Handlers.add(
  "Receive-Payment-For-AMM-Registration",
  function(msg)
    return Handlers.utils.hasMatchingTag("Action", "Credit-Notice")(msg)
        and Handlers.utils.hasMatchingTag("X-Action", "Register-AMM")(msg)
        and msg.From == PAYMENT_TOKEN_PROCESS
  end,
  integrateAmm.handlePayForAmmRegistration
)

Handlers.add(
  "Receive-AMM-Info",
  function(msg)
    return Handlers.utils.hasMatchingTag("Response-For", "Info")(msg)
        and integrateAmm.hasPendingAmmInfo(msg)
  end,
  integrateAmm.handleInfoResponseFromAmm
)

Handlers.add(
  "Receive-Token-Info",
  function(msg)
    return Handlers.utils.hasMatchingTag("Response-For", "Info")(msg)
        and integrateAmm.hasPendingTokenInfo(msg)
  end,
  integrateAmm.handleTokenInfoResponse
)

Handlers.add(
  "Subscription-Confirmation",
  Handlers.utils.hasMatchingTag("Response-For", "Subscribe-To-Topics"),
  integrateAmm.handleSubscriptionConfirmationFromAmm
)

Handlers.add(
  "Payment-Confirmation-From-AMM",
  Handlers.utils.hasMatchingTag("Response-For", "Pay-For-Subscription"),
  integrateAmm.handlePaymentConfirmationFromAmm
)

Handlers.add(
  "Get-AMM-Registration-Status",
  Handlers.utils.hasMatchingTag("Action", "Get-AMM-Registration-Status"),
  integrateAmm.handleGetRegistrationStatus
)

-- OPS

Handlers.add(
  "Initialize",
  Handlers.utils.hasMatchingTag("Action", "Initialize"),
  initialize.handleInitialize
)

Handlers.add(
  "Toggle-Dispatch-Active",
  Handlers.utils.hasMatchingTag("Action", "Toggle-Dispatch-Active"),
  emergency.toggleDispatchActive
)

Handlers.add(
  "Toggle-Logging-Active",
  Handlers.utils.hasMatchingTag("Action", "Toggle-Logging-Active"),
  emergency.toggleLoggingActive
)

Handlers.add(
  "Set-Quote-Token",
  Handlers.utils.hasMatchingTag("Action", "Set-Quote-Token"),
  configOps.handleSetQuoteToken
)

Handlers.add(
  "Set-Payment-Token",
  Handlers.utils.hasMatchingTag("Action", "Set-Payment-Token"),
  configOps.handleSetPaymentToken
)

Handlers.add(
  "Set-Offchain-Feed-Provider",
  Handlers.utils.hasMatchingTag("Action", "Set-Offchain-Feed-Provider"),
  configOps.handleSetOffchainFeedProvider
)

Handlers.add(
  "Set-Supply-Updates-Provider",
  Handlers.utils.hasMatchingTag("Action", "Set-Supply-Updates-Provider"),
  configOps.handleSetSupplyUpdatesProvider
)

-- MAINTENANCE

Handlers.add(
  "Get-Config",
  Handlers.utils.hasMatchingTag("Action", "Get-Config"),
  debug.handleGetConfig
)

Handlers.add(
  "Remove-AMM",
  Handlers.utils.hasMatchingTag("Action", "Remove-AMM"),
  integrateAmm.handleRemoveAmm
)

Handlers.add(
  "Remove-Token",
  Handlers.utils.hasMatchingTag("Action", "Remove-Token"),
  dexiCore.handleRemoveToken
)

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

-- ANALYTICS

Handlers.add(
  "Get-Daily-Volume",
  Handlers.utils.hasMatchingTag("Action", "Get-Daily-Volume"),
  analytics.getDailyVolume
)

Handlers.add(
  "Get-Pool-Overview",
  Handlers.utils.hasMatchingTag("Action", "Get-Pool-Overview"),
  analytics.getPoolOverview
)
