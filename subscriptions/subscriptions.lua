local indicators = require('indicators.indicators')
local topN = require('top-n.top-n')

local subscriptions = {}

-- ------------------- SQL

local sql = {}

-- INDICATORS SQL

function sql.registerIndicatorSubscriber(processId, ownerId, ammProcessId)
  local stmt = db:prepare [[
    INSERT INTO indicator_subscriptions (process_id, owner_id, amm_process_id)
    VALUES (:process_id, :owner_id, :amm_process_id)
  ]]
  if not stmt then
    error("Failed to prepare SQL statement for registering process: " .. db:errmsg())
  end
  stmt:bind_names({
    process_id = processId,
    owner_id = ownerId,
    amm_process_id = ammProcessId
  })
  local result, err = stmt:step()
  stmt:finalize()
  if err then
    error("Err: " .. db:errmsg())
  end
end

function sql.hasIndicatorsSubscription(processId, ammProcessId)
  local stmt = db:prepare [[
    SELECT 1
    FROM indicator_subscriptions
    WHERE process_id = :process_id AND amm_process_id = :amm_process_id;
  ]]
  if not stmt then
    error("Failed to prepare SQL statement for checking indicators subscription: " .. db:errmsg())
  end
  stmt:bind_names({ process_id = processId, amm_process_id = ammProcessId })
  local row = stmt:step()
  stmt:finalize()
  return row and true or false
end

function sql.getIndicatorsSubscriptionOwner(processId, ammProcessId)
  local stmt = db:prepare [[
    SELECT owner_id
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
  local row = stmt:step()
  stmt:finalize()
  return row.owner_id
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
  local result, err = stmt:step()
  stmt:finalize()
  if err then
    error("Err: " .. db:errmsg())
  end
end

-- TOP N SQL

function sql.registerTopNSubscriber(processId, ownerId, quoteToken, nInTopN)
  local stmt = db:prepare [[
    INSERT INTO top_n_subscriptions (process_id, owner_id, quote_token, top_n)
    VALUES (:process_id, :owner_id, :quote_token, :top_n)
    ON CONFLICT(process_id) DO UPDATE SET
    owner_id = excluded.owner_id,
    quote_token = excluded.quote_token,
    top_n = excluded.top_n;
  ]]
  if not stmt then
    error("Failed to prepare SQL statement for registering process: " .. db:errmsg())
  end
  stmt:bind_names({
    process_id = processId,
    owner_id = ownerId,
    quote_token = quoteToken,
    top_n = nInTopN
  })
  local result, err = stmt:step()
  stmt:finalize()
  if err then
    error("Err: " .. db:errmsg())
  end
end

function sql.hasTopNSubscription(processId, quoteToken)
  local stmt = db:prepare [[
    SELECT 1
    FROM top_n_subscriptions
    WHERE process_id = :process_id AND quote_token = :quote_token;
  ]]
  if not stmt then
    error("Failed to prepare SQL statement for checking top N subscription: " .. db:errmsg())
  end
  stmt:bind_names({
    process_id = processId,
    quote_token = quoteToken,
  })
  local row = stmt:step()
  stmt:finalize()
  return row and true or false
end

function sql.getTopNSubscriptionOwner(processId, quoteToken)
  local stmt = db:prepare [[
    SELECT owner_id
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
  local row = stmt:step()
  stmt:finalize()
  return row.owner_id
end

function sql.unregisterTopNSubscriber(processId, ownerId, quoteToken, nInTopN)
  local stmt = db:prepare [[
    DELETE FROM top_n_subscriptions
    WHERE process_id = :process_id AND owner_id = :owner_id AND quote_token = :quote_token AND top_n = :top_n;
  ]]
  if not stmt then
    error("Failed to prepare SQL statement for unregistering process: " .. db:errmsg())
  end
  stmt:bind_names({
    process_id = processId,
    owner_id = ownerId,
    quote_token = quoteToken,
    top_n = nInTopN
  })
  local result, err = stmt:step()
  stmt:finalize()
  if err then
    error("Err: " .. db:errmsg())
  end
end

function sql.updateBalance(ownerId, tokenId, amount, isCredit)
  local stmt = db:prepare [[
    INSERT INTO balances (owner, token_id, balance)
    VALUES (:owner_id, :token_id, :amount)
    ON CONFLICT(owner) DO UPDATE SET
      balance = CASE
        WHEN :is_credit THEN balances.balance + :amount
        ELSE balances.balance - :amount
      END
    WHERE balances.token_id = :token_id;
  ]]
  if not stmt then
    error("Failed to prepare SQL statement for updating balance: " .. db:errmsg())
  end
  stmt:bind_names({
    owner_id = ownerId,
    token_id = tokenId,
    amount = math.abs(amount), -- Ensure amount is positive
    is_credit = isCredit
  })
  local result, err = stmt:step()
  stmt:finalize()
  if err then
    error("Error updating balance: " .. db:errmsg())
  end
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

  if sql.hasIndicatorsSubscription(processId, ammProcessId) then
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

subscriptions.handleUnsubscribeForIndicators = function(msg)
  local processId = msg.Tags['Subscriber-Process-Id']
  local ownerId = msg.Tags['Owner-Id']
  local ammProcessId = msg.Tags['AMM-Process-Id']

  local owner = sql.getIndicatorsSubscriptionOwner(processId)

  if not owner then
    error('No indicator subscription found for process: ' .. processId .. ' and amm: ' .. ammProcessId)
  end

  if owner ~= ownerId then
    error('Provided Owner-Id owns no indicator subscription for the process ' ..
      processId .. ' and amm: ' .. ammProcessId)
  end

  if owner ~= msg.From and msg.From ~= OPERATOR then
    error('Only an owner can unsubscribe its owned subscription. Indicator subscription for process ' ..
      processId .. ' is owned by ' ..
      owner .. ' not ' .. msg.From .. '(you)')
  end

  print('Unsubscribing subscriber from indicator data: ' ..
    processId .. ' for amm: ' .. ammProcessId .. ' with owner: ' .. ownerId)
  indicators.unregisterIndicatorSubscriber(processId, ammProcessId)

  ao.send({
    Target = ao.id,
    Assignments = { ownerId, processId },
    Action = 'Dexi-Indicator-Unsubscription-Confirmation',
    AMM = ammProcessId,
    Process = processId,
    OK = 'true'
  })
end

subscriptions.handleSubscribeForTopN = function(msg)
  local processId = msg.Tags['Subscriber-Process-Id']
  local ownerId = msg.Tags['Owner-Id']
  local quoteToken = msg.Tags['Quote-Token']
  local nInTopN = msg.Tags['Top-N']

  if not quoteToken then
    error('Quote-Token is required')
  end

  if quoteToken ~= QUOTE_TOKEN_PROCESS then
    error('Quote token not available (only BRK): ' .. quoteToken)
  end

  if not nInTopN then
    error('Top-N is required')
  end

  if sql.hasTopNSubscription(processId, quoteToken) then
    error('Top N subscription already exists for process: ' .. processId .. ' and quote token: ' .. quoteToken)
  end

  print('Registering subscriber to top N market data: ' ..
    processId .. ' for quote token: ' .. quoteToken .. ' with owner: ' .. ownerId)
  sql.registerTopNSubscriber(processId, ownerId, quoteToken, nInTopN)

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

function subscriptions.handleUnsubscribeForTopN(msg)
  local processId = msg.Tags['Subscriber-Process-Id']
  local ownerId = msg.Tags['Owner-Id']
  local quoteToken = msg.Tags['Quote-Token']

  local owner = sql.getTopNSubscriptionOwner(processId, quoteToken)

  if not owner then
    error('No top N subscription found for process: ' .. processId .. ' and quote token: ' .. quoteToken)
  end

  if owner ~= ownerId then
    error('Provided Owner-Id owns no top N subscription for the process ' ..
      processId .. ' and quote token: ' .. quoteToken)
  end

  if owner ~= msg.From and msg.From ~= OPERATOR then
    error('Only an owner can unsubscribe its owned subscription. Top N subscription for process ' ..
      processId .. ' is owned by ' ..
      owner .. ' not ' .. msg.From .. '(you)')
  end

  print('Unsubscribing subscriber from top N market data: ' ..
    processId .. ' for quote token: ' .. quoteToken .. ' with owner: ' .. ownerId)
  sql.unregisterTopNSubscriber(processId, ownerId, quoteToken)

  ao.send({
    Target = ao.id,
    Assignments = { ownerId, processId },
    Action = 'Dexi-Top-N-Unsubscription-Confirmation',
    QuoteToken = quoteToken,
    Process = processId,
    OK = 'true'
  })
end

subscriptions.recordPayment = function(msg)
  sql.updateBalance(msg.Tags.Sender, msg.From, tonumber(msg.Tags.Quantity), true)
end

return subscriptions
