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

DEXI_TOKEN = "eM6NGBSgwyDTqQ0grng1fQvBF-5HcMeshLcz9QPE-0A"
TOKEN = ao.env.Process.Tags["Base-Token"]
AMM = ao.env.Process.Tags["Monitor-For"]

-- ================== HANDLER LOGIC ================= --
OFFCHAIN_FEED_PROVIDER = OFFCHAIN_FEED_PROVIDER or ao.env.Process.Tags["Offchain-Feed-Provider"]
QUOTE_TOKEN_PROCESS = QUOTE_TOKEN_PROCESS or ao.env.Process.Tags["Quote-Token-Process"]
SUPPLY_UPDATES_PROVIDER = SUPPLY_UPDATES_PROVIDER or
    ao.env.Process.Tags["Offchain-Supply-Updates-Provider"]

-- -------------- SUBSCRIPTIONS -------------- --
-- TODO move out or remove with refactoring that integrates subscribable package

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

local recordPayment = function(msg)
  if msg.From == 'Sa0iBLPNyJQrwpTTG-tWLQU-1QeUAJA73DdxGGiKoJc' then
    sqlschema.updateBalance(msg.Tags.Sender, msg.From, tonumber(msg.Tags.Quantity), true)
  end

  if msg.From == DEXI_TOKEN and msg.Tags["X-Action"] == "Register-AMM" then
    recordRegisterAMMPayment(msg)
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
