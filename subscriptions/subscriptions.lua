local indicators = require('indicators.indicators')
local topN = require('top-n.top-n')
local bint = require('.bint')(256)
local dbUtils = require('db.utils')

local subscriptions = {}

-- ------------------- SQL

local sql = {}

-- INDICATORS SQL

function sql.registerIndicatorSubscriber(processId, ammProcessId)
  local stmt = db:prepare [[
    INSERT INTO indicator_subscriptions (process_id, amm_process_id)
    VALUES (:process_id, :amm_process_id)
  ]]
  if not stmt then
    error("Failed to prepare SQL statement for registering process: " .. db:errmsg())
  end
  stmt:bind_names({
    process_id = processId,
    amm_process_id = ammProcessId
  })
  local _, err = stmt:step()
  stmt:finalize()
  if err then
    error("Err: " .. db:errmsg())
  end
end

function sql.getIndicatorsSubscriber(processId, ammProcessId)
  local stmt = db:prepare [[
    SELECT *
    FROM indicator_subscriptions
    WHERE process_id = :process_id AND amm_process_id = :amm_process_id;
  ]]
  if not stmt then
    error("Failed to prepare SQL statement for getting indicators subscription owner: " .. db:errmsg())
  end
  stmt:bind_names({
    process_id = processId,
    amm_process_id = ammProcessId
  })
  return dbUtils.queryOne(stmt)
end

function sql.unregisterIndicatorsSubscriber(processId, ammProcessId)
  local stmt = db:prepare [[
    DELETE FROM indicator_subscriptions
    WHERE process_id = :process_id AND amm_process_id = :amm_process_id;
  ]]
  if not stmt then
    error("Failed to prepare SQL statement for unregistering process: " .. db:errmsg())
  end
  stmt:bind_names({
    process_id = processId,
    amm_process_id = ammProcessId
  })
  local _, err = stmt:step()
  stmt:finalize()
  if err then
    error("Err: " .. db:errmsg())
  end
end

-- TOP N SQL

function sql.registerTopNSubscriber(processId, quoteToken, nInTopN)
  local stmt = db:prepare [[
    INSERT INTO top_n_subscriptions (process_id, quote_token, top_n)
    VALUES (:process_id, :quote_token, :top_n)
  ]]
  if not stmt then
    error("Failed to prepare SQL statement for registering process: " .. db:errmsg())
  end
  stmt:bind_names({
    process_id = processId,
    quote_token = quoteToken,
    top_n = nInTopN
  })
  local _, err = stmt:step()
  stmt:finalize()
  if err then
    error("Err: " .. db:errmsg())
  end
end

function sql.getTopNSubscription(processId, quoteToken)
  local stmt = db:prepare [[
    SELECT *
    FROM top_n_subscriptions
    WHERE process_id = :process_id AND quote_token = :quote_token;
  ]]
  if not stmt then
    error("Failed to prepare SQL statement for getting top N subscription owner: " .. db:errmsg())
  end
  stmt:bind_names({
    process_id = processId,
    quote_token = quoteToken
  })
  return dbUtils.queryOne(stmt)
end

function sql.unregisterTopNSubscriber(processId, quoteToken)
  local stmt = db:prepare [[
    DELETE FROM top_n_subscriptions
    WHERE process_id = :process_id AND quote_token = :quote_token;
  ]]
  if not stmt then
    error("Failed to prepare SQL statement for unregistering process: " .. db:errmsg())
  end
  stmt:bind_names({
    process_id = processId,
    quote_token = quoteToken,
  })
  local _, err = stmt:step()
  stmt:finalize()
  if err then
    error("Err: " .. db:errmsg())
  end
end

function sql.updateBalance(processId, amount, isCredit)
  local currentBalance = bint(sql.getBalance(processId))
  local diff = isCredit and bint(amount) or -bint(amount)
  local newBalance = tostring(currentBalance + diff)

  local stmt = db:prepare [[
    INSERT INTO balances (process_id, balance)
    VALUES (:process_id, :amount)
    ON CONFLICT(process_id) DO UPDATE SET balance = :new_balance
    WHERE balances.token_id = :token_id;
  ]]
  if not stmt then
    error("Failed to prepare SQL statement for updating balance: " .. db:errmsg())
  end
  stmt:bind_names({
    process_id = processId,
    new_balance = newBalance,
    amount = math.abs(amount), -- Ensure amount is positive
    is_credit = isCredit
  })
  local _, err = stmt:step()
  stmt:finalize()
  if err then
    error("Error updating balance: " .. db:errmsg())
  end
end

function sql.getBalance(processId)
  local stmt = db:prepare [[
  SELECT * FROM balances WHERE process_id = :process_id
]]
  if not stmt then
    error("Failed to prepare SQL statement for getting balance entry: " .. db:errmsg())
  end
  stmt:bind_names({ process_id = processId })
  local balanceRow = dbUtils.queryOne(stmt)
  return balanceRow and balanceRow.balance or "0"
end

-- ------------------- EXPORT

subscriptions.handleSubscribeForIndicators = function(msg)
  local processId = msg.Tags['Subscriber-Process-Id']
  local ownerId = msg.Tags['Owner-Id']
  local ammProcessId = msg.Tags['AMM-Process-Id']

  if not processId then
    error('Subscriber-Process-Id is required')
  end

  if not ammProcessId then
    error('AMM-Process-Id is required')
  end

  if not ownerId then
    error('Owner-Id is required')
  end

  if sql.getIndicatorsSubscriber(processId, ammProcessId) then
    error('Indicators subscription already exists for process: ' .. processId .. ' and amm: ' .. ammProcessId)
  end

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

local unsubscribeForInidicators = function(processId, ammProcessId, asResponse)
  if not sql.getIndicatorsSubscriber(processId, ammProcessId) then
    error('No indicator subscription found for process: ' .. processId .. ' and amm: ' .. ammProcessId)
  end

  print('Unsubscribing subscriber from indicator data: ' ..
    processId .. ' for amm: ' .. ammProcessId)

  indicators.unregisterIndicatorSubscriber(processId, ammProcessId)

  local tag = asResponse and 'Response-For' or 'Action'

  ao.send({
    Target = processId,
    [tag] = 'Dexi-Indicator-Unsubscription-Confirmation',
    AMM = ammProcessId,
    OK = 'true'
  })
end

subscriptions.handleUnsubscribeForIndicators = function(msg)
  local processId = msg.From
  local ammProcessId = msg.Tags['AMM-Process-Id']
  local asResponse = true
  unsubscribeForInidicators(processId, ammProcessId, asResponse)
end

subscriptions.handleOperatorUnsubscribeForIndicators = function(msg)
  if msg.From ~= OPERATOR then
    error('Only the operator is allowed')
  end
  local processId = msg.Tags['Subscriber-Process-Id']
  local ammProcessId = msg.Tags['AMM-Process-Id']
  local asResponse = false
  unsubscribeForInidicators(processId, ammProcessId, asResponse)
end

subscriptions.handleSubscribeForTopN = function(msg)
  local processId = msg.Tags['Subscriber-Process-Id']
  local ownerId = msg.Tags['Owner-Id']
  local quoteToken = msg.Tags['Quote-Token']
  local nInTopN = msg.Tags['Top-N']

  if not quoteToken then
    error('Quote-Token is required')
  end

  if quoteToken ~= QUOTE_TOKEN.ProcessId then
    error('Quote token not available (only ' .. QUOTE_TOKEN.Ticker .. '): ' .. quoteToken)
  end

  if not nInTopN then
    error('Top-N is required')
  end

  if sql.getTopNSubscription(processId, quoteToken) then
    error('Top N subscription already exists for process: ' .. processId .. ' and quote token: ' .. quoteToken)
  end

  print('Registering subscriber to top N market data: ' ..
    processId .. ' for quote token: ' .. quoteToken .. ' with owner: ' .. ownerId)
  sql.registerTopNSubscriber(processId, quoteToken, nInTopN)

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

local function unsubscribeForTopN(processId, quoteToken, asResponse)
  if not sql.getTopNSubscription(processId, quoteToken) then
    error('No top N subscription found for process: ' .. processId .. ' and quote token: ' .. quoteToken)
  end

  print('Unsubscribing subscriber from top N market data: ' ..
    processId .. ' for quote token: ' .. quoteToken)
  sql.unregisterTopNSubscriber(processId, quoteToken)

  local tag = asResponse and 'Response-For' or 'Action'

  ao.send({
    Target = processId,
    [tag] = 'Dexi-Top-N-Unsubscription-Confirmation',
    QuoteToken = quoteToken,
    OK = 'true'
  })
end

subscriptions.handleUnsubscribeForTopN = function(msg)
  local processId = msg.From
  local quoteToken = msg.Tags['Quote-Token']
  local asResponse = true
  unsubscribeForTopN(processId, quoteToken, asResponse)
end

subscriptions.handleOperatorUnsubscribeForTopN = function(msg)
  if msg.From ~= OPERATOR then
    error('Only the operator is allowed')
  end
  local processId = msg.Tags['Subscriber-Process-Id']
  local quoteToken = msg.Tags['Quote-Token']
  local asResponse = false
  unsubscribeForTopN(processId, quoteToken, asResponse)
end

subscriptions.recordPayment = function(msg)
  sql.updateBalance(msg.Tags.Sender, tonumber(msg.Tags.Quantity), true)
end

return subscriptions
