local indicators = require('indicators.indicators')
local topN = require('top-n.top-n')

local subscriptions = {}

-- ------------------- SQL

local sql = {}

function sql.registerIndicatorSubscriber(processId, ownerId, ammProcessId)
  local stmt = db:prepare [[
    INSERT INTO indicator_subscriptions (process_id, owner_id, amm_process_id)
    VALUES (:process_id, :owner_id, :amm_process_id)
    ON CONFLICT(process_id) DO UPDATE SET
    owner_id = excluded.owner_id,
    amm_process_id = excluded.amm_process_id;
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

subscriptions.recordPayment = function(msg)
  if msg.From == PAYMENT_TOKEN_PROCESS then
    sql.updateBalance(msg.Tags.Sender, msg.From, tonumber(msg.Tags.Quantity), true)
  end
end

return subscriptions
