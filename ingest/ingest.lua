local json = require('json')

local validationSchemas = require('validation.validation-schemas')
local dbUtils = require('db.utils')
local dexiCore = require('dexi-core.dexi-core')
local indicators = require('indicators.indicators')
local topN = require('top-n.top-n')

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

function ingestSql.recordSwap(entry)
  local stmt = db:prepare [[
    INSERT OR REPLACE INTO amm_transactions (
      id, source, block_height, block_id, sender, created_at_ts,
      to_token, from_token, from_quantity, to_quantity, fee_percentage, amm_process
    ) VALUES (:id, :source, :block_height, :block_id, :sender, :created_at_ts,
              :to_token, :from_token, :from_quantity, :to_quantity, :fee_percentage, :amm_process);
  ]]

  if not stmt then
    error("Failed to prepare SQL statement: " .. db:errmsg())
  end

  -- going for brevity - this will be more robust with teal
  stmt:bind_names(entry)

  local result, err = stmt:step()
  stmt:finalize()
  if err then
    error("Failed to insert swap: " .. err)
  end
end

function ingestSql.recordChangeInSwapParams(entry)
  local stmt = db:prepare [[
    INSERT OR REPLACE INTO amm_swap_params_changes (
      id, source, block_height, block_id, sender, created_at_ts, cause,
      reserves_0, reserves_1, fee_percentage, amm_process
    ) VALUES (:id, :source, :block_height, :block_id, :sender, :created_at_ts, :cause,
              :reserves_0, :reserves_1, :fee_percentage, :amm_process);
  ]]

  if not stmt then
    error("Failed to prepare SQL statement: " .. db:errmsg())
  end

  -- going for brevity - this will be more robust with teal
  stmt:bind_names(entry)

  local result = stmt:finalize()
  print(db:errmsg())
  print('result: ' .. result)
end

function ingestSql.updateCurrentSwapParams(entry)
  local stmt = db:prepare [[
    INSERT OR REPLACE INTO amm_swap_params (
      amm_process, reserves_0, reserves_1, fee_percentage
    ) VALUES (
     :amm_process, :reserves_0, :reserves_1, :fee_percentage
    );
  ]]

  if not stmt then
    error("Failed to prepare SQL statement: " .. db:errmsg())
  end

  -- going for brevity - this will be more robust with teal
  stmt:bind_names(entry)

  local result, err = stmt:step()
  if err then
    error("Failed to insert swap params: " .. err)
  end
  stmt:finalize()
end

-- ==================== INTERNAL ===================== --

local function recordChangeInSwapParams(msg, payload, source, sourceAmm, cause)
  assert(msg.Id, 'Missing Id')
  assert(msg['Block-Height'], 'Missing Block-Height')
  assert(msg.From, 'Missing From')
  assert(msg.Timestamp, 'Missing Timestamp')

  local reserves_0 = payload["Reserves-Token-A"]
  local reserves_1 = payload["Reserves-Token-B"]
  local fee_percentage = payload["TotalFee"]

  assert(reserves_0, 'Missing Reserves-Token-A')
  assert(reserves_1, 'Missing Reserves-Token-B')
  assert(fee_percentage, 'Missing TotalFee')

  local entry = {
    id = msg.Id,
    source = source,
    block_height = msg['Block-Height'],
    block_id = msg['Block-Id'] or '',
    sender = msg.recipient or '',
    created_at_ts = math.floor(msg.Timestamp / 1000),
    cause = cause,
    fee_percentage = fee_percentage,
    reserves_0 = reserves_0,
    reserves_1 = reserves_1,
    amm_process = sourceAmm
  }

  print('Recording change in swap params ' .. json.encode(entry))
  ingestSql.recordChangeInSwapParams(entry)
  ingestSql.updateCurrentSwapParams(entry)
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

  local entry = {
    id = msg.Id,
    source = source,
    block_height = msg['Block-Height'],
    block_id = msg['Block-Id'] or '',
    sender = msg.recipient or '',
    created_at_ts = msg.Timestamp / 1000,
    to_token = swapData['To-Token'],
    from_token = swapData['From-Token'],
    from_quantity = tonumber(swapData['From-Quantity']),
    to_quantity = tonumber(swapData['To-Quantity']),
    fee_percentage = tonumber(swapData['Fee-Percentage']),
    amm_process = sourceAmm
  }
  ingestSql.recordSwap(entry)
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
    recordChangeInSwapParams(msg, json.decode(msg.Data), 'message', ammProcessId, 'swap-params-change')
    -- disable for now TODO!!!
    -- topN.dispatchMarketDataIncludingAMM(now, ammProcessId)
  end
end

function ingest.handleFeedIngestSwapParamsChange(msg)
  if msg.From == OFFCHAIN_FEED_PROVIDER then
    local data = json.decode(msg.Data)
    for _, liquidityUpdate in ipairs(data) do
      -- TODO rework considering the msg / payload separation
      recordChangeInSwapParams(liquidityUpdate, 'gateway', liquidityUpdate.Tags['AMM'], 'swap-params-change')
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
    indicators.dispatchIndicatorsForAMM(ammProcessId, now)

    recordChangeInSwapParams(msg, json.decode(msg.Data), 'message', ammProcessId, 'swap')

    topN.dispatchMarketDataIncludingAMM(now, ammProcessId)
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
        indicators.dispatchIndicatorsForAMM(ammProcessId, now)

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
