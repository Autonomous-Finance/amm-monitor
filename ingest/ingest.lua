local json = require('json')
local dbUtils = require('db.utils')

local validationSchemas = require('validation.validation-schemas')
local dbUtils = require('db.utils')
local dexiCore = require('dexi-core.dexi-core')
local indicators = require('indicators.indicators')
local topN = require('top-n.top-n')
local usdPrice = require('dexi-core.usd-price')
local swapSubscribers = require('swap-subscribers.main')
local reserveSubscribers = require('swap-subscribers.reserves')
local lookups = require('dexi-core.lookups')

local ingest = {}


-- ==================== SQL ===================== --

local ingestSql = {}

function ingestSql.getGatewayHeight(msg)
  local stmt = db:prepare [[
    SELECT MAX(block_height) AS max_height
    FROM amm_transactions
    WHERE source = 'gateway' AND amm_process = :amm;
  ]]

  stmt:bind_names({ amm = msg.Tags.AMM })

  local row = dbUtils.queryOne(stmt)
  local gatewayHeight = row and row.max_height or 0

  stmt:reset()

  return gatewayHeight
end

function ingestSql.recordLiquidityChange(entry)
  local stmt = db:prepare [[
    INSERT INTO reserve_changes (
      id, reserves_token_a, reserves_token_b, delta_token_a, delta_token_b,
      action, delta_pool_tokens, total_pool_tokens, token_a, token_b,
      original_message_id, transfer_quantity, recipient, sender, created_at_ts, created_at_ts_ms, amm_process, tvl_in_usd, token_a_price, token_b_price
    ) VALUES (
      :id, :reserves_token_a, :reserves_token_b, :delta_token_a, :delta_token_b,
      :action, :delta_pool_tokens, :total_pool_tokens, :token_a, :token_b,
      :original_message_id, :transfer_quantity, :recipient, :sender, :created_at_ts, :created_at_ts_ms, :amm_process, :tvl_in_usd, :token_a_price, :token_b_price
    );
  ]]

  if not stmt then
    error("Failed to prepare SQL statement: " .. db:errmsg())
  end

  stmt:bind_names(entry)
  dbUtils.execute(stmt, "ingestSql.recordLiquidityChange")
end

function ingestSql.recordSwap(entry)
  local stmt = db:prepare [[
    INSERT OR REPLACE INTO amm_transactions (
      id, source, block_height, block_id, sender, created_at_ts, created_at_ts_ms,
      to_token, from_token, from_quantity, to_quantity, fee_percentage, amm_process, from_token_usd_price, to_token_usd_price, reserves_token_a, reserves_token_b, token_a_price, token_b_price
    ) VALUES (:id, :source, :block_height, :block_id, :sender, :created_at_ts, :created_at_ts_ms,
              :to_token, :from_token, :from_quantity, :to_quantity, :fee_percentage, :amm_process, :from_token_usd_price, :to_token_usd_price, :reserves_token_a, :reserves_token_b, :token_a_price, :token_b_price);
  ]]

  if not stmt then
    error("Failed to prepare SQL statement: " .. db:errmsg())
  end

  -- going for brevity - this will be more robust with teal
  stmt:bind_names(entry)
  dbUtils.execute(stmt, "ingestSql.recordSwap")
end

function ingestSql.recordChangeInSwapParams(entry)
  --
  -- :source, :block_height, :block_id, :sender, :created_at_ts, :cause, :reserves_0, :reserves_1, :fee_percentage, :amm_process

  local stmt = db:prepare [[
    INSERT OR REPLACE INTO amm_swap_params_changes (
      id, source, block_height, block_id, sender, created_at_ts, created_at_ts_ms,
      cause, reserves_0, reserves_1, amm_process
    ) VALUES (:id, :source, :block_height, :block_id, :sender, :created_at_ts, :created_at_ts_ms,
              :cause, :reserves_0, :reserves_1, :amm_process);
  ]]

  if not stmt then
    error("Failed to prepare SQL statement: " .. db:errmsg())
  end

  -- going for brevity - this will be more robust with teal
  stmt:bind_names(entry)
  dbUtils.execute(stmt, "ingestSql.recordChangeInSwapParams")
end

function ingestSql.updateCurrentSwapParams(entry)
  local stmt = db:prepare [[
    INSERT OR REPLACE INTO amm_swap_params (
      amm_process, reserves_0, reserves_1
    ) VALUES (
     :amm_process, :reserves_0, :reserves_1
    );
  ]]
  if not stmt then
    error("Failed to prepare SQL statement: " .. db:errmsg())
  end

  -- going for brevity - this will be more robust with teal
  stmt:bind_names(entry)
  dbUtils.execute(stmt, "ingestSql.updateCurrentSwapParams")
end

-- ==================== INTERNAL ===================== --

local function recordChangeInSwapParams(msg, payload, source, sourceAmm, cause)
  assert(msg.Id, 'Missing Id')
  assert(msg['Block-Height'], 'Missing Block-Height')
  assert(msg.From, 'Missing From')
  assert(msg.Timestamp, 'Missing Timestamp')

  local reserves_0 = payload["Reserves-Token-A"]
  local reserves_1 = payload["Reserves-Token-B"]

  assert(reserves_0, 'Missing Reserves-Token-A')
  assert(reserves_1, 'Missing Reserves-Token-B')

  local entry = {
    id = msg.Id,
    source = source,
    block_height = msg['Block-Height'],
    block_id = msg['Block-Id'] or '',
    sender = msg.recipient or '',
    created_at_ts = math.floor(msg.Timestamp / 1000),
    created_at_ts_ms = msg.Timestamp,
    cause = cause,
    reserves_0 = reserves_0,
    reserves_1 = reserves_1,
    amm_process = sourceAmm
  }

  print('Recording change in swap params ' .. json.encode(entry))
  ingestSql.recordChangeInSwapParams(entry)
  ingestSql.updateCurrentSwapParams(entry)
  reserveSubscribers.dispatchSwapParamsNotifications(msg.Id, sourceAmm)
end

local function getDenominator(token)
  local stmt = db:prepare [[
    SELECT denominator FROM token_registry WHERE token_process = :token;
  ]]
  stmt:bind_names({ token = token })
  if not stmt then
    error("Failed to prepare SQL statement: " .. db:errmsg())
  end
  local row = dbUtils.queryOne(stmt)
  return row and row.denominator
end


local function recordLiquidityChange(msg)
  local changeData = json.decode(msg.Data)
  if not changeData['Delta-Token-A'] then
    print('old style message, skipping')
    return
  end

  local tokenAInfo = lookups.tryGetHopperPrice(changeData['Token-A'])
  local tokenBInfo = lookups.tryGetHopperPrice(changeData['Token-B'])

  local tokenAPrice = tokenAInfo and tokenAInfo.price or nil
  local tokenBPrice = tokenBInfo and tokenBInfo.price or nil

  local tokenADenominator = tokenAInfo and tokenAInfo.denominator or nil
  local tokenBDenominator = tokenBInfo and tokenBInfo.denominator or nil

  local tvlInUsd
  if tokenAPrice and tokenBPrice then
    tvlInUsd = ((tonumber(changeData['Reserves-Token-A']) * tokenAPrice / 10 ^ tokenADenominator) +
      (tonumber(changeData['Reserves-Token-B']) * tokenBPrice / 10 ^ tokenBDenominator))
  else
    tvlInUsd = nil
  end

  local entry = {
    id = msg.Id,
    amm_process = msg.From,
    reserves_token_a = changeData["Reserves-Token-A"],
    reserves_token_b = changeData["Reserves-Token-B"],
    delta_token_a = changeData["Delta-Token-A"],
    delta_token_b = changeData["Delta-Token-B"],
    action = changeData["Action"],
    delta_pool_tokens = changeData["Delta-Pool-Tokens"],
    total_pool_tokens = changeData["Total-Pool-Tokens"],
    token_a = changeData["Token-A"],
    token_b = changeData["Token-B"],
    original_message_id = changeData["Original-Message-Id"],
    transfer_quantity = changeData["Transfer-Quantity"],
    recipient = changeData["Recipient"],
    sender = changeData["Sender"],
    created_at_ts = math.floor(msg.Timestamp / 1000),
    created_at_ts_ms = msg.Timestamp,
    tvl_in_usd = tvlInUsd,
    token_a_price = tokenAPrice,
    token_b_price = tokenBPrice,
  }

  print('Recording liquidity change ' .. json.encode(entry))
  ingestSql.recordLiquidityChange(entry)
end

local function recordSwap(msg, swapData, source, sourceAmm)
  assert(msg.Id, 'Missing Id')
  assert(msg['Block-Height'], 'Missing Block-Height')
  assert(msg.From, 'Missing From')
  assert(msg.Timestamp, 'Missing Timestamp')
  assert(swapData['To-Token'], 'Missing To-Token')
  assert(swapData['From-Token'], 'Missing From-Token')
  assert(swapData['From-Quantity'], 'Missing From-Quantity')
  assert(swapData['To-Quantity'], 'Missing To-Quantity')
  assert(swapData['Fee-Percentage'], 'Missing Fee-Percentage')
  assert(swapData['Reserves-Token-A'], 'Missing Reserves-Token-A')
  assert(swapData['Reserves-Token-B'], 'Missing Reserves-Token-B')


  local tokenAInfo = lookups.tryGetHopperPrice(swapData['Token-A'])
  local tokenBInfo = lookups.tryGetHopperPrice(swapData['Token-B'])

  local tokenAPrice = tokenAInfo and tokenAInfo.price or nil
  local tokenBPrice = tokenBInfo and tokenBInfo.price or nil

  local fromTokenUsdPrice = swapData['Token-A'] == swapData['From-Token'] and tokenAPrice or tokenBPrice
  local toTokenUsdPrice = swapData['Token-A'] == swapData['To-Token'] and tokenAPrice or tokenBPrice

  local entry = {
    id = msg.Id,
    source = source,
    block_height = msg['Block-Height'],
    block_id = msg['Block-Id'] or '',
    sender = swapData['User'] or '',
    created_at_ts = math.floor(msg.Timestamp / 1000),
    created_at_ts_ms = msg.Timestamp,
    to_token = swapData['To-Token'],
    from_token = swapData['From-Token'],
    from_quantity = tonumber(swapData['From-Quantity']),
    to_quantity = tonumber(swapData['To-Quantity']),
    fee_percentage = tonumber(swapData['Fee-Percentage']),
    amm_process = sourceAmm,
    from_token_usd_price = fromTokenUsdPrice,
    to_token_usd_price = toTokenUsdPrice,
    reserves_token_a = swapData['Reserves-Token-A'],
    reserves_token_b = swapData['Reserves-Token-B'],
    lp_fee_percentage = swapData['LP-Fee'],
    protocol_fee_percentage = swapData['Protocol-Fee'],
    token_a_price = tokenAPrice,
    token_b_price = tokenBPrice,
  }
  ingestSql.recordSwap(entry)

  swapSubscribers.dispatchSwapNotifications(msg.Id, sourceAmm)

  --[[
      the new swap affects
        the latest price =>
          the market cap of this amm's base token =>
            the overall ranking by market cap =>
              the top N token sets
    ]]
  topN.updateTopNTokenSet() -- TODO put back in once fixed
end

-- ==================== EXPORT ===================== --

function ingest.getCurrentHeight(msg)
  ao.send({
    Target = msg.From,
    Height = tostring(ingestSql.getGatewayHeight(msg))
  })
end

-- INGEST SWAP PARAMS CHANGES

function ingest.handleMonitorIngestSwapParamsChange(msg)
  print('Receiving swap params change notification ' .. json.encode(msg))
  local ammProcessId = dexiCore.isKnownAmm(msg.From)
      and msg.From
      or (msg.From == Owner and msg.Tags["AMM"] or nil)
  if ammProcessId then
    local now = math.floor(msg.Timestamp / 1000)
    recordChangeInSwapParams(msg, json.decode(msg.Data), 'message', ammProcessId, 'liquidity-add-remove')
    -- disable for now TODO!!!
    -- topN.dispatchMarketDataIncludingAMM(now, ammProcessId)
    recordLiquidityChange(msg)
  end
end

function ingest.handleFeedIngestSwapParamsChange(msg)
  if msg.From == OFFCHAIN_FEED_PROVIDER then
    local data = json.decode(msg.Data)
    for _, liquidityUpdate in ipairs(data) do
      -- TODO rework considering the msg / payload separation
      recordChangeInSwapParams(liquidityUpdate, 'gateway', liquidityUpdate.Tags['AMM'], 'liquidity-add-remove')
    end

    local isLatestSwapParamsChange = false -- TODO implement; check if data goes up to present and this is the latest data entry;
    if isLatestSwapParamsChange then
      local now = math.floor(msg.Timestamp / 1000)
      topN.dispatchMarketDataIncludingAMM(now, msg.Tags['AMM'])
    end
  end
end

-- INGEST SWAPS

function ingest.handleMonitorIngestSwap(msg)
  print('Receiving swap notification ' .. json.encode(msg))
  local ammProcessId = dexiCore.isKnownAmm(msg.From)
      and msg.From
      or (msg.From == Owner and msg.Tags["AMM"] or nil)
  if ammProcessId then
    local now = math.floor(msg.Timestamp / 1000)

    recordSwap(msg, json.decode(msg.Data), 'message', ammProcessId)



    -- the new swap affects indicators for this amm
    -- indicators.dispatchIndicatorsForAMM(ammProcessId, now)

    recordChangeInSwapParams(msg, json.decode(msg.Data), 'message', ammProcessId, 'swap')
    -- TODO: re-enable once fixed
    -- topN.dispatchMarketDataIncludingAMM(now, ammProcessId)
  end
end

function ingest.handleFeedIngestSwaps(msg)
  -- TODO rework considering the msg / payload separation
  if msg.From == OFFCHAIN_FEED_PROVIDER then
    local data = json.decode(msg.Data)
    for _, swap in ipairs(data) do
      local ammProcessId = swap.Tags['AMM']
      local now = math.floor(msg.Timestamp / 1000)

      recordSwap(swap, 'gateway', ammProcessId)

      local isLatestSwap = false -- TODO: implement; check if data goes up to present and this is the latest data entry;
      if isLatestSwap then
        -- the new swap affects indicators for this amm
        -- indicators.dispatchIndicatorsForAMM(ammProcessId, now)

        --[[
          the new swap affects
            the latest price =>
              the market cap of this amm's base token =>
                the overall ranking by market cap =>
                  the top N token sets
        ]]
        topN.updateTopNTokenSet()

        recordChangeInSwapParams(msg, 'gateway', swap.Tags['AMM'], 'swap')
      end
    end
  end
end

return ingest
