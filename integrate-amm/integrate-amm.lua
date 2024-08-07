local json = require("json")
local dexiCore = require("dexi-core.dexi-core")

local integrateAmm = {}

--[[
  {
    [processId] = {
      requester = requesterId,         -- whoever paid for subscribing to the AMM
      status = subscriptionStatus,     -- 'received-request--initializing' | 'initialized--subscribing' | 'subscribed--paying' | 'paid--complete'
      ammDetails = {
        name = ammName,
        tokenA = {
          processId = tokenAProcessId,
          pendingInfo = true,
          tokenName = tokenAName,
          tokenTicker = tokenATicker,
          denominator = tokenADenominator,
          totalSupply = tokenATotalSupply,
          fixedSupply = false,
        },
        tokenB = {
          ...
        },
      },
    }
  }
]]
AmmSubscriptions = AmmSubscriptions or {}


--[[
  Associate token Info responses with pending AMM registrations
  {
    [tokenProcessId] = ammProcessId
  }
]]
TokenInfoRequests = TokenInfoRequests or {}


-- ----------------------- INTERNAL

local updateStatus = function(ammProcessId, newStatus)
  local registrationData = AmmSubscriptions[ammProcessId]
  registrationData.status = newStatus
  ao.send({
    Target = registrationData.requester,
    Action = "Dexi-AMM-Registration-Confirmation",
    AMM = ammProcessId,
    Status = newStatus
  })
end

local getAmmInfo = function(ammProcessId)
  ao.send({
    Target = ammProcessId,
    Action = "Get-Amm-Info"
  })
end

local getTokenInfo = function(tokenProcessId)
  ao.send({
    Target = tokenProcessId,
    Action = "Info"
  })
end

local subscribeToAmm = function(ammProcessId)
  ao.send({
    Target = ammProcessId,
    Action = "Register-Subscriber",
    Topics = json.encode({ "order-confirmation", "swap-params-change" })
  })
end

local unsubscribeAmm = function(ammProcessId)
  ao.send({
    Target = ammProcessId,
    Action = "Unsubscribe-From-Topics",
    Topics = json.encode({ "order-confirmation", "swap-params-change" })
  })
end

local registerAMM = function(ammProcessId, now)
  local registrationData = AmmSubscriptions[ammProcessId]
  dexiCore.registerAMM(
    registrationData.ammDetails.name,
    ammProcessId,
    registrationData.ammDetails.tokenA,
    registrationData.ammDetails.tokenB,
    now
  )
end

local payForSubscription = function(ammProcessId)
  ao.send({
    Target = PAYMENT_TOKEN_PROCESS,
    Action = 'Transfer',
    Recipient = ammProcessId,
    Quantity = 1,
    ["X-Subscriber-Process-Id"] = ao.id
  })
end

-- --------------------- EXPORT
-- Registration Steps in the order of succession

-- 1. Receive a Payment that kicks off the registration process
integrateAmm.handlePayForAmmRegistration = function(msg)
  assert(msg.Tags.Quantity, 'Credit notice data must contain a valid quantity')
  assert(msg.Tags.Sender, 'Credit notice data must contain a valid sender')
  assert(msg.Tags["X-AMM-Process"], 'Credit notice data must contain a valid amm-process')

  local ammProcessId = msg.Tags["X-AMM-Process"]

  -- if a successful AMM registration is already in place, refund the requester;
  -- incomplete AMM registration will be overwritten by a initiating a new one (Dexi payment that was made with the previous one remains unused)
  -- TODO eventually we will delete AmmSubscriptions entries upon successful registration, and the check here will be made with dexiCore (is AMM registered)
  local existing = AmmSubscriptions[ammProcessId]
  if existing and existing.status == 'paid--complete' then
    local errorMsg = 'AMM registration already exists for process: ' .. ammProcessId
    ao.send({
      Target = msg.From,
      Recipient = msg.Tags.Sender,
      Action = 'Transfer',
      Quantity = msg.Tags.Quantity,
      ["X-Refund-Reason"] = errorMsg
    })
  end

  AmmSubscriptions[ammProcessId] = {
    requester = msg.Tags.Sender
  }

  getAmmInfo(msg.Tags["X-AMM-Process"])
  updateStatus(ammProcessId, 'received-request--initializing')
end

-- 2A. Receive the AMM Info to initialize the registration with correct data
integrateAmm.handleInfoResponseFromAmm = function(msg)
  local ammProcessId = msg.From
  local registrationData = AmmSubscriptions[ammProcessId]
  if not registrationData then
    error('No subscription request found for amm: ' .. ammProcessId)
  end

  assert(msg.Tags.Name, 'AMM info data must contain a valid Name tag')
  assert(msg.Tags["Token-A"], 'AMM info data must contain a valid Token-A tag')
  assert(msg.Tags["Token-B"], 'AMM info data must contain a valid Token-B tag')

  registrationData.ammDetails = {
    name = msg.Tags.Name,
    tokenA = {
      processId = msg.Tags["Token-A"],
      pendingInfo = false
    },
    tokenB = {
      processId = msg.Tags["Token-B"],
      pendingInfo = false
    },
  }

  local ammDetails = registrationData.ammDetails

  for _, token in ipairs({ msg.Tags["Token-A"], msg.Tags["Token-B"] }) do
    if not dexiCore.isKnownToken(token) then
      TokenInfoRequests[token] = ammProcessId
      local index = token == ammDetails.tokenA.processId and 'tokenA' or 'tokenB'
      ammDetails[index].pendingInfo = true
      getTokenInfo(token)
    end
  end

  -- if no info requests are pending, proceed with AMM subscription
  if not ammDetails.tokenA.pendingInfo and not ammDetails.tokenB.pendingInfo then
    subscribeToAmm(ammProcessId)
    updateStatus(ammProcessId, 'initialized--subscribing')
  end
end

integrateAmm.hasPendingTokenInfo = function(msg)
  return TokenInfoRequests[msg.From] ~= nil
end

-- 2B. Receive the Token Info to include the token registration in the AMM registration
integrateAmm.handleTokenInfoResponse = function(msg)
  local tokenProcessId = msg.From
  local ammProcessId = TokenInfoRequests[tokenProcessId]
  local registrationData = AmmSubscriptions[ammProcessId]

  assert(msg.Tags.Name, 'Token info data must contain a valid Name tag')
  assert(msg.Tags.Ticker, 'Token info data must contain a valid Ticker tag')
  assert(msg.Tags.Denomination, 'Token info data must contain a valid Denominator tag')
  assert(msg.Tags.TotalSupply, 'Token info data must contain a valid TotalSupply tag')

  local tokenInfo = {
    processId = tokenProcessId,
    tokenName = msg.Tags.Name,
    tokenTicker = msg.Tags.Ticker,
    denominator = msg.Tags.Denomination,
    totalSupply = msg.Tags.TotalSupply,
    fixedSupply = false,
    pendingInfo = false
  }

  dexiCore.registerToken(
    tokenInfo.processId,
    tokenInfo.tokenName,
    tokenInfo.denominator,
    tokenInfo.totalSupply,
    tokenInfo.fixedSupply,
    math.floor(msg.Timestamp / 1000)
  )

  local ammDetails = registrationData.ammDetails

  if ammDetails.tokenA.processId == tokenInfo.processId then
    ammDetails.tokenA = tokenInfo
    TokenInfoRequests[tokenInfo.processId] = nil
  elseif ammDetails.tokenB.processId == tokenInfo.processId then
    ammDetails.tokenB = tokenInfo
    TokenInfoRequests[tokenInfo.processId] = nil
  else
    error('Token info does not match any of the AMM tokens: ' .. json.encode(tokenInfo))
  end

  -- when both token info responses have been received, proceeed within AMM registration
  if not ammDetails.tokenA.pendingInfo and not ammDetails.tokenB.pendingInfo then
    subscribeToAmm(ammProcessId)
    updateStatus(ammProcessId, 'initialized--subscribing')
  end
end


-- 3. Receive Subscription Confirmation from AMM
integrateAmm.handleSubscriptionConfirmationFromAmm = function(msg)
  assert(msg.Tags.OK == 'true', 'Subscription failed for amm: ' .. msg.From)
  assert(msg.Tags["Updated-Topics"], 'Subscription confirmation data must contain a valid updated-topics')

  local topics = json.decode(msg.Tags["Updated-Topics"])

  assert(
    #topics == 2
    and (topics[1] == 'order-confirmation' and topics[2] == 'swap-params-change')
    or (topics[1] == 'swap-params-change' and topics[2] == 'order-confirmation'),
    'Invalid topics received from amm: ' .. msg.From .. ' - ' .. json.encode(topics))

  local ammProcessId = msg.From

  if not AmmSubscriptions[ammProcessId] then
    error('No subscription request found for amm: ' .. ammProcessId)
  end

  payForSubscription(ammProcessId)
  updateStatus(ammProcessId, 'subscribed--paying')
end

-- 4. Receive Payment Confirmation from AMM
integrateAmm.handlePaymentConfirmationFromAmm = function(msg)
  if not msg.Tags.OK == 'true' then
    error('Payment failed for amm: ' .. msg.From)
  end

  local ammProcessId = msg.From
  if not AmmSubscriptions[ammProcessId] then
    error('No subscription request found for amm: ' .. ammProcessId)
  end

  local now = math.floor(msg.Timestamp / 1000)
  registerAMM(ammProcessId, now)
  updateStatus(ammProcessId, 'paid--complete')

  AmmSubscriptions[ammProcessId] = nil
end

integrateAmm.handleRemoveAmm = function(msg)
  local ammProcessId = msg.Tags["Process-Id"]
  dexiCore.handleRemoveAmm(ammProcessId)
  unsubscribeAmm(ammProcessId)
end

integrateAmm.handleGetRegistrationStatus = function(msg)
  local ammProcessId = msg.Tags["Process-Id"]
  local registrationData = AmmSubscriptions[ammProcessId]
  if not registrationData then
    error('No subscription request found for amm: ' .. ammProcessId)
  end
  ao.send({
    Target = msg.From,
    Action = "Get-AMM-Registration-Status",
    AMM = ammProcessId,
    Status = registrationData.status
  })
end

return integrateAmm
