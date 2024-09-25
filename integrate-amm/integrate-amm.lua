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

local unsubscribeAmm = function(ammProcessId)
  ao.send({
    Target = ammProcessId,
    Action = "Unsubscribe-From-Topics",
    Topics = json.encode({ "order-confirmation", "liquidity-add-remove" })
  })
end

-- returns true if we can proceed with the registration
local validateRegisterAMM = function(msg)
  assert(msg.Tags.Quantity, 'Credit notice data must contain a valid quantity')
  assert(msg.Tags.Sender, 'Credit notice data must contain a valid sender')
  assert(msg.Tags["X-AMM-Process"], 'Credit notice data must contain a valid amm-process')

  local ammProcessId = msg.Tags["X-AMM-Process"]

  -- if a successful AMM registration is already in place, refund the requester;
  -- incomplete AMM registration will be overwritten by a initiating a new one (Dexi payment that was made with the previous one remains unused)
  -- TODO eventually we will delete AmmSubscriptions entries upon successful registration, and the check here will be made with dexiCore (is AMM registered)
  local existing = AmmSubscriptions[ammProcessId]
  return not (existing and existing.status == 'paid--complete')
end

local refundRegistrationPayment = function(msg)
  local ammProcessId = msg.Tags["X-AMM-Process"]
  local errorMsg = 'AMM registration already exists for process: ' .. ammProcessId
  ao.send({
    Target = msg.From,
    Recipient = msg.Tags.Sender,
    Action = 'Transfer',
    Quantity = msg.Tags.Quantity,
    ["X-Refund-Reason"] = errorMsg
  })
end

local initializeRegisterAMM = function(msg)
  local ammProcessId = msg.Tags["X-AMM-Process"]
  AmmSubscriptions[ammProcessId] = {
    requester = msg.Tags.Sender
  }
end

local getAmmInfoToRegisterAMM = function(msg)
  local ammProcessId = msg.Tags["X-AMM-Process"]
  ao.send({
    Target = ammProcessId,
    Action = "Info"
  })
  local ammInfoResponse = Receive(function(m)
    return m.Tags['From-Process'] == ammProcessId
        and m.Tags["Response-For"] == 'Info'
        and AmmSubscriptions[ammProcessId] ~= nil
        and AmmSubscriptions[ammProcessId].status == 'received-request--initializing'
  end)

  local ammName = ammInfoResponse.Tags.Name
  local ammTokenA = ammInfoResponse.Tags["TokenA"]
  local ammTokenB = ammInfoResponse.Tags["TokenB"]

  assert(ammName, 'AMM info data must contain a valid Name tag')
  assert(ammTokenA, 'AMM info data must contain a valid TokenA tag')
  assert(ammTokenB, 'AMM info data must contain a valid TokenB tag')

  local registrationData = AmmSubscriptions[ammProcessId]
  registrationData.ammDetails = {
    name = ammName,
    tokenA = {
      processId = ammTokenA,
    },
    tokenB = {
      processId = ammTokenB,
    },
  }
end

local getTokensInfoToRegisterAMM = function(msg)
  local ammProcessId = msg.Tags["X-AMM-Process"]
  local ammDetails = AmmSubscriptions[ammProcessId].ammDetails
  local ammTokenA = ammDetails.tokenA.processId
  local ammTokenB = ammDetails.tokenB.processId

  -- GET TOKENS INFO

  local reqs = 0
  for _, token in ipairs({ ammTokenA, ammTokenB }) do
    if not dexiCore.isKnownToken(token) then
      reqs = reqs + 1
      TokenInfoRequests[token] = ammProcessId
      ao.send({
        Target = token,
        Action = "Info"
      })
    end
  end

  while reqs > 0 do
    local tokenInfoResponse = Receive(function(m)
      local isFromToken = m.Tags['From-Process'] == ammTokenA or m.Tags['From-Process'] == ammTokenB
      if not isFromToken then
        return false
      end

      local token = m.Tags['From-Process']
      return m.Tags["Response-For"] == 'Info' and TokenInfoRequests[token] == ammProcessId
    end)
    reqs = reqs - 1

    local processId = tokenInfoResponse.Tags['From-Process']
    local tokenName = tokenInfoResponse.Tags.Name
    local tokenTicker = tokenInfoResponse.Tags.Ticker
    local tokenDenominator = tokenInfoResponse.Tags.Denomination
    local tokenTotalSupply = tokenInfoResponse.Tags.TotalSupply

    assert(tokenName, 'Token info data must contain a valid Name tag')
    assert(tokenTicker, 'Token info data must contain a valid Ticker tag')
    assert(tokenDenominator, 'Token info data must contain a valid Denomination tag')
    assert(tokenTotalSupply, 'Token info data must contain a valid TotalSupply tag')

    local tokenInfo = {
      processId = processId,
      tokenName = tokenName,
      tokenTicker = tokenTicker,
      denominator = tokenDenominator,
      totalSupply = tokenTotalSupply,
      fixedSupply = false,
      pendingInfo = false
    }

    dexiCore.registerToken(
      tokenInfo.processId,
      tokenInfo.tokenName,
      tokenInfo.tokenTicker,
      tokenInfo.denominator,
      tokenInfo.totalSupply,
      tokenInfo.fixedSupply,
      math.floor(msg.Timestamp / 1000)
    )

    if ammDetails.tokenA.processId == processId then
      ammDetails.tokenA = tokenInfo
      TokenInfoRequests[processId] = nil
    elseif ammDetails.tokenB.processId == processId then
      ammDetails.tokenB = tokenInfo
      TokenInfoRequests[processId] = nil
    else
      error('Token info does not match any of the AMM tokens: ' .. json.encode(tokenInfo))
    end
  end
end

local function subscribeToRegisterAMM(msg)
  local ammProcessId = msg.Tags["X-AMM-Process"]
  ao.send({
    Target = ammProcessId,
    Action = "Register-Subscriber",
    Topics = json.encode({ "order-confirmation", "liquidity-add-remove" })
  })

  local subscriptionConfirmation = Receive(function(m)
    return m.Tags['From-Process'] == ammProcessId
        and m.Tags["Response-For"] == 'Subscribe-To-Topics'
  end)

  assert(subscriptionConfirmation.Tags.OK == 'true', 'Subscription failed for amm: ' .. ammProcessId)
  assert(subscriptionConfirmation.Tags["Updated-Topics"],
    'Subscription confirmation data must contain a valid updated-topics')

  local topics = json.decode(subscriptionConfirmation.Tags["Updated-Topics"])
  assert(
    #topics == 2
    and (topics[1] == 'order-confirmation' and topics[2] == 'liquidity-add-remove')
    or (topics[1] == 'liquidity-add-remove' and topics[2] == 'order-confirmation'),
    'Invalid topics received from amm: ' .. ammProcessId .. ' - ' .. json.encode(topics))
end

local paySubscriptionToRegisterAMM = function(msg)
  local ammProcessId = msg.Tags["X-AMM-Process"]
  ao.send({
    Target = PAYMENT_TOKEN_PROCESS,
    Action = 'Transfer',
    Recipient = ammProcessId,
    Quantity = "1",
    ["X-Action"] = "Pay-For-Subscription",
    ["X-Subscriber-Process-Id"] = ao.id
  })

  local ammPaymentResponse = Receive(function(m)
    return m.Tags['From-Process'] == ammProcessId
        and m.Tags["Response-For"] == 'Pay-For-Subscription'
  end)

  if not ammPaymentResponse.Tags.OK == 'true' then
    error('Payment failed for amm: ' .. ammProcessId)
  end
end


local finalizeRegisterAMM = function(ammProcessId)
  local registrationData = AmmSubscriptions[ammProcessId]
  local now = math.floor(os.time() / 1000)
  print("REGISTERING AMM")
  dexiCore.registerAMM(
    registrationData.ammDetails.name,
    ammProcessId,
    registrationData.ammDetails.tokenA.processId,
    registrationData.ammDetails.tokenB.processId,
    now
  )
  print("REGISTERED AMM")
end


-- --------------------- EXPORT
integrateAmm.handleRegisterAmm = function(msg)
  if not validateRegisterAMM(msg) then
    refundRegistrationPayment(msg)
    return
  end

  local ammProcessId = msg.Tags["X-AMM-Process"]

  initializeRegisterAMM(msg)

  updateStatus(ammProcessId, 'received-request--initializing')

  getAmmInfoToRegisterAMM(msg)
  getTokensInfoToRegisterAMM(msg)

  updateStatus(ammProcessId, 'initialized--subscribing')

  subscribeToRegisterAMM(msg)

  updateStatus(ammProcessId, 'subscribed--paying')

  paySubscriptionToRegisterAMM(msg)

  updateStatus(ammProcessId, 'paid--complete')

  finalizeRegisterAMM(ammProcessId)
end

integrateAmm.handleRemoveAmm = function(msg)
  local ammProcessId = msg.Tags["Process-Id"]
  dexiCore.handleRemoveAmm(msg)
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
