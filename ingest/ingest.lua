local json = require('json')

local validationSchemas = require('validation.validation-schemas')
local topNMarketData = require('top-n.top-n')
local sqlschema = require('dexi-core.sqlschema')
local indicators = require('indicators.indicators')

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

  local row = sqlschema.queryOne(stmt)
  local gatewayHeight = row and row.max_height or 0

  stmt:reset()

  return gatewayHeight
end

function ingestSql.recordSwap(entry)
  local stmt, err = db:prepare [[
    REPLACE INTO amm_transactions (
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

  stmt:step()
  stmt:reset()
end

function ingestSql.recordChangeInSwapParams(entry)
  local stmt, err = db:prepare [[
    REPLACE INTO amm_swap_params_changes (
      id, source, block_height, block_id, sender, created_at_ts, cause
      reserves_0, reserves_1, fee_percentage, amm_process
    ) VALUES (:id, :source, :block_height, :block_id, :sender, :created_at_ts, :cause,
              :reserves_0, :reserves_1, :fee_percentage, :amm_process);
  ]]

  if not stmt then
    error("Failed to prepare SQL statement: " .. db:errmsg())
  end

  -- going for brevity - this will be more robust with teal
  stmt:bind_names(entry)

  stmt:step()
  stmt:reset()
end

function ingestSql.updateCurrentSwapParams(entry)
  local stmt, err = db:prepare [[
    REPLACE INTO amm_swap_params (
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

  stmt:step()
  stmt:reset()
end

-- ==================== INTERNAL ===================== --

local function recordChangeInSwapParams(msg, source, sourceAmm, cause)
  local reserves_0 = msg.Tags["Reseves-Token-A"]
  local reserves_1 = msg.Tags["Reseves-Token-B"]
  local fee_percentage = msg.Tags["Fee-Percentage"]

  local valid, err = validationSchemas.swapParamsSchema(reserves_0, reserves_1, fee_percentage)
  assert(valid, 'Invalid input amm swap params data' .. json.encode(err))

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

  ingestSql.recordChangeInSwapParams(entry)
  ingestSql.updateCurrentSwapParams(entry)
end

local function recordSwap(msg, source, sourceAmm)
  local valid, err = validationSchemas.inputMessageSchema(msg)
  assert(valid, 'Invalid input transaction data' .. json.encode(err))

  local entry = {
    id = msg.Id,
    source = source,
    block_height = msg['Block-Height'],
    block_id = msg['Block-Id'] or '',
    sender = msg.recipient or '',
    created_at_ts = msg.Timestamp / 1000,
    to_token = msg.Tags['To-Token'],
    from_token = msg.Tags['From-Token'],
    from_quantity = tonumber(msg.Tags['From-Quantity']),
    to_quantity = tonumber(msg.Tags['To-Quantity']),
    fee_percentage = tonumber(msg.Tags['Fee-Percentage']),
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
  sqlschema.updateTopNTokenSet()
end

-- ==================== EXPORT ===================== --

function ingest.getCurrentHeight(msg)
  ao.send({
    Target = msg.From,
    Height = tostring(ingestSql.getGatewayHeight(msg))
  })
end

-- INGEST SWAP PARAMS CHANGES

function ingest.monitorIngestSwapParamsChange(msg)
  local ammProcessId = sqlschema.isKnownAmm(msg.From)
      and msg.From
      or (msg.From == Owner and msg.Tags["AMM"] or nil)
  if ammProcessId then
    local now = math.floor(msg.Timestamp / 1000)
    recordChangeInSwapParams(msg, 'message', ammProcessId, 'swap-params-change')
    topNMarketData.dispatchMarketDataIncludingAMM(now, ammProcessId)
  end
end

function ingest.feedIngestSwapParamsChange(msg)
  if msg.From == OFFCHAIN_FEED_PROVIDER then
    local data = json.decode(msg.Data)
    for _, liquidityUpdate in ipairs(data) do
      recordChangeInSwapParams(liquidityUpdate, 'gateway', liquidityUpdate.Tags['AMM'], 'swap-params-change')
    end

    local isLatestSwapParamsChange = false -- TODO implement; check if data goes up to present and this is the latest data entry;
    if isLatestSwapParamsChange then
      local now = math.floor(msg.Timestamp / 1000)
      topNMarketData.dispatchMarketDataIncludingAMM(now, msg.Tags['AMM'])
    end
  end
end

-- INGEST SWAPS

function ingest.monitorIngestSwap(msg)
  local ammProcessId = sqlschema.isKnownAmm(msg.From)
      and msg.From
      or (msg.From == Owner and msg.Tags["AMM"] or nil)
  if ammProcessId then
    local now = math.floor(msg.Timestamp / 1000)

    recordSwap(msg, 'message', ammProcessId)

    -- the new swap affects indicators for this amm
    indicators.dispatchIndicatorsForAMM(ammProcessId, now)

    recordChangeInSwapParams(msg, 'message', ammProcessId, 'swap')

    topNMarketData.dispatchMarketDataIncludingAMM(now, ammProcessId)
  end
end

function ingest.feedIngestSwaps(msg)
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
        sqlschema.updateTopNTokenSet()

        recordChangeInSwapParams(msg, 'gateway', swap.Tags['AMM'], 'swap')
      end
    end
  end
end

return ingest