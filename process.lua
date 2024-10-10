local sqlite3 = require("lsqlite3")

local dexiCore = require("dexi-core.dexi-core")
local usdPrice = require("dexi-core.usd-price")
local subscriptions = require("subscriptions.subscriptions")
local indicators = require("indicators.indicators")
local swapSubscribers = require("swap-subscribers.main")
local reserveSubscribers = require("swap-subscribers.reserves")
local seeder = require("db.seed")
local ingest = require("ingest.ingest")
local topN = require("top-n.top-n")
local debug = require("utils.debug")
local integrateAmm = require("integrate-amm.integrate-amm")
local emergency = require("ops.emergency")
local configOps = require("ops.config-ops")
local initialize = require("ops.initialize")
local analytics = require("amm-analytics.main")
local hopper = require("hopper.hopper")
local updateToken = require("update-token.update-token")
local lookups = require("dexi-core.lookups")
local ingestTokenLock = require("ingest.ingest-token-lock")
local json = require("json")
local utils = require(".utils")
local dbUtils = require("db.utils")

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
TOKEN_LOCKER = 'jxiKuu_21_KjNga8KxH1h8fJeoCl9DzcEjGBiKN66DY'

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
        Action = msg.Action .. "-Response",
        ['Process-Id'] = msg.Tags['Process-Id'],
        Price = tostring(price)
      })
    else
      ao.send({
        Target = msg.From,
        Action = msg.Action .. "-Response",
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
        (Handlers.utils.hasMatchingTag("Topic", "swap-params-change")(msg)
          or
          Handlers.utils.hasMatchingTag("Topic", "liquidity-add-remove")(msg))
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
Handlers.add(
  "Subscribe-Swaps",
  Handlers.utils.hasMatchingTag("Action", "Subscribe-Swaps"),
  swapSubscribers.registerSwapSubscriberHandler
)

Handlers.add(
  "Unsubscribe-Swaps",
  Handlers.utils.hasMatchingTag("Action", "Unsubscribe-Swaps"),
  swapSubscribers.unregisterSwapSubscriberHandler
)


-- RESERVES SUBSCRIBERS
Handlers.add(
  "Subscribe-Reserve-Changes",
  Handlers.utils.hasMatchingTag("Action", "Subscribe-Reserve-Changes"),
  reserveSubscribers.registerSwapParamsSubscriberHandler
)

Handlers.add(
  "Unsubscribe-Reserve-Changes",
  Handlers.utils.hasMatchingTag("Action", "Unsubscribe-Reserve-Changes"),
  reserveSubscribers.unregisterSwapParamsSubscriberHandler
)


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


-- TOKEN LOCKER --
Handlers.add(
  "Notify-Claimed-Tokens",
  function(msg)
    return Handlers.utils.hasMatchingTag("Action", "Notify-Claimed-Tokens")(msg)
        and msg.From == TOKEN_LOCKER
  end,
  ingestTokenLock.handleClaimNotification
)

Handlers.add(
  "Notify-Locked-Tokens",
  function(msg)
    return Handlers.utils.hasMatchingTag("Action", "Notify-Locked-Tokens")(msg)
        and msg.From == TOKEN_LOCKER
  end,
  ingestTokenLock.handleLockNotification
)

Handlers.add(
  "Get-Locked-Share",
  Handlers.utils.hasMatchingTag("Action", "Get-Locked-Share"),
  function(msg)
    local ammProcess = msg.Tags['AMM-Process']
    local lockedShare = analytics.getOneYearLockedShare(ammProcess)
    local currentTvl = analytics.getCurrentTvl(ammProcess)
    local aggregateLockedTokens = analytics.getAggregateLockedTokens(ammProcess)
    -- todo return total locked liquidity
    local totalLockedLiquidity = utils.reduce(function(acc, curr)
      return acc + curr.locked_tokens
    end, 0, aggregateLockedTokens)

    ao.send({
      Target = msg.From,
      ResponseFor = msg.Action,
      ['One-Year-Locked-Share'] = lockedShare,
      ['Current-Tvl'] = currentTvl,
      ['One-Year-Locked-Liquidity'] = currentTvl * lockedShare,
      Data = json.encode({
        ['One-Year-Locked-Share'] = lockedShare,
        ['Current-Tvl'] = currentTvl,
        ['One-Year-Locked-Liquidity'] = currentTvl * lockedShare,
        ['Aggregate-Locked-Tokens'] = aggregateLockedTokens
      })
    })
  end
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
  "Register-AMM",
  function(msg)
    return Handlers.utils.hasMatchingTag("Action", "Credit-Notice")(msg)
        and Handlers.utils.hasMatchingTag("X-Action", "Register-AMM")(msg)
        and msg.From == PAYMENT_TOKEN_PROCESS
  end,
  integrateAmm.handleRegisterAmm
)

Handlers.add(
  "Get-AMM-Registration-Status",
  Handlers.utils.hasMatchingTag("Action", "Get-AMM-Registration-Status"),
  integrateAmm.handleGetRegistrationStatus
)

Handlers.add("Get-AMM-Details",
  Handlers.utils.hasMatchingTag("Action", "Get-AMM-Details"),
  integrateAmm.handleGetAmmDetails
)

-- AMM Activate user public subscription

Handlers.add(
  "Activate-Public-AMM",
  function(msg)
    return Handlers.utils.hasMatchingTag("Action", "Credit-Notice")(msg)
        and Handlers.utils.hasMatchingTag("X-Action", "Activate-AMM")(msg)
  end,
  integrateAmm.handleActivateAmm
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


Handlers.add(
  "Get-Transaction-Ids",
  debug.getTransactionIds
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

Handlers.add(
  "Get-Price-For-Token",
  Handlers.utils.hasMatchingTag("Action", "Get-Price-For-Token"),
  hopper.getPriceForTokenHandler
)

-- COMMUNITY APPROVED TOKENS

Handlers.add(
  "Get-Community-Approved-Tokens",
  Handlers.utils.hasMatchingTag("Action", "Get-Community-Approved-Tokens"),
  function(msg)
    local tokens = dbUtils.queryManyWithParams('SELECT * FROM community_approved_tokens', {})
    local processes = utils.map(function(token) return token.id end, tokens)

    ao.send({
      Target = msg.From,
      ResponseFor = msg.Action,
      Tokens = processes
    })
  end
)

-- Token Profile Update

Handlers.add(
  "Receive-Payment-For-Token-Profile-Update",
  function(msg)
    return Handlers.utils.hasMatchingTag("Action", "Credit-Notice")(msg)
        and Handlers.utils.hasMatchingTag("X-Action", "Update-Token-Profile")(msg)
  end,
  updateToken.handlePayForUpdateToken
)

Handlers.add(
  "Get-Token-Update-Price",
  Handlers.utils.hasMatchingTag("Action", "Get-Token-Update-Price"),
  updateToken.handleGetPriceForUpdate
)

Handlers.add(
  "Get-Pool-Pnl-History",
  Handlers.utils.hasMatchingTag("Action", "Get-Pool-Pnl-History"),
  analytics.getPoolPnlHistoryForUser
)

Handlers.add(
  "Find-Best-Pairs-For-Token",
  Handlers.utils.hasMatchingTag("Action", "Find-Best-Pairs-For-Token"),
  analytics.findBestPairsForToken
)


-- LOOKUPS

Handlers.add(
  "Get-Price-For-Tokens",
  Handlers.utils.hasMatchingTag("Action", "Get-Price-For-Tokens"),
  function(msg)
    local tokens = json.decode(msg.Tags["Tokens"])
    local prices = {}
    for _, token in ipairs(tokens) do
      prices[token] = lookups.getPriceFromLastTransaction(token) or lookups.tryGetHopperPrice(token)
    end
    ao.send({
      Target = msg.From,
      ResponseFor = msg.Action,
      Prices = prices
    })
  end
)

Handlers.add(
  "Get-Historical-Volume",
  Handlers.utils.hasMatchingTag("Action", "Get-Historical-Volume"),
  function(msg)
    local agentType = msg.Tags['Agent-Type']
    assert(agentType, 'Agent-Type is required')

    local volumeQuery = [[
        SELECT
            COALESCE(SUM(amm_transactions_view.volume_usd), 0) AS total_volume_usd
        FROM amm_transactions_view
        JOIN agents ON agents.agent_id = amm_transactions_view.sender
        WHERE agents.agent_type = :agent_type;
    ]]

    local stmt = db:prepare(volumeQuery)
    stmt:bind_names({ agent_type = agentType })
    local volumeData = dbUtils.queryOne(stmt)

    ao.send({
      Target = msg.From,
      ResponseFor = msg.Action,
      TotalVolumeUSD = tostring(volumeData.total_volume_usd)
    })
  end
)
