local json = require("json")
local dexiCore = require("dexi-core.dexi-core")
local updateToken = require("update-token.update-token")
local hopper = require("hopper.hopper")

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


-- Set activate price in USD
PriceInUSD = 50;

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

  assert(msg.From == PAYMENT_TOKEN_PROCESS,
    'AMM registration request payment must be in DEXI tokens. DEXI ID : ' .. PAYMENT_TOKEN_PROCESS .. ' ')

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
  local ammInfoResponse = ao.send({
    Target = ammProcessId,
    Action = "Info"
  }).receive()

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

  for _, token in ipairs({ ammTokenA, ammTokenB }) do
    if not dexiCore.isKnownToken(token) then
      local tokenInfoResponse = ao.send({
        Target = token,
        Action = "Info"
      }).receive()

      local tokenName = tokenInfoResponse.Tags.Name
      local tokenTicker = tokenInfoResponse.Tags.Ticker
      local tokenDenominator = tokenInfoResponse.Tags.Denomination
      local tokenTotalSupply = tokenInfoResponse.Tags.TotalSupply

      assert(tokenName, 'Token info data must contain a valid Name tag')
      assert(tokenTicker, 'Token info data must contain a valid Ticker tag')
      assert(tokenDenominator, 'Token info data must contain a valid Denomination tag')
      assert(tokenTotalSupply, 'Token info data must contain a valid TotalSupply tag')

      local tokenInfo = {
        processId = token,
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

      if ammDetails.tokenA.processId == token then
        ammDetails.tokenA = tokenInfo
      elseif ammDetails.tokenB.processId == token then
        ammDetails.tokenB = tokenInfo
      else
        error('Token info does not match any of the AMM tokens: ' .. json.encode(tokenInfo))
      end
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
        and m.Tags.Action == 'Subscribe-To-Topics-Confirmation'
  end)

  assert(subscriptionConfirmation.Tags.Status == 'OK', 'Subscription failed for amm: ' .. ammProcessId)
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
        and m.Tags.Action == "Pay-For-Subscription-Confirmation"
  end)

  if not ammPaymentResponse.Tags.Status == 'OK' then
    error('Payment failed for amm: ' .. ammProcessId)
  end
end

local finalizeRegisterAMM = function(ammProcessId)
  local registrationData = AmmSubscriptions[ammProcessId]
  local now = math.floor(os.time() / 1000)
  dexiCore.registerAMM(
    registrationData.ammDetails.name,
    ammProcessId,
    registrationData.ammDetails.tokenA.processId,
    registrationData.ammDetails.tokenB.processId,
    now
  )
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

  -- DEXI SUBSCRIPTION TO THIS AMM
  unsubscribeAmm(ammProcessId)

  -- delete everything related to this AMM (status quo, history, subscriptions to DEXI about this AMM)
  dexiCore.handleRemoveAmm(msg)
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

integrateAmm.handleActivateAmm = function(msg)
  assert(msg.Tags["X-AMM-Process"], "AMM activation data must contain a valid X-AMM-Process tag")

  -- get denomiator for payment token
  local denominator = updateToken.get_token_denominator(msg.From)

  -- Get hopper price for the payment token
  local priceResponse = hopper.getPrice("USD", msg.From)
  local totalCost = priceResponse * PriceInUSD * 10 ^ denominator
  local quantity = tonumber(msg.Tags.Quantity)

  -- if less funds received refund the user and send back the reason
  if (quantity < totalCost) then
    -- Send back the funds
    ao.send({
      Target = msg.From,
      Action = "Transfer",
      Quantity = msg.Tags.Quantity,
      Recipient = msg.Sender
    })

    ao.send({
      Target = msg.Sender,
      Action = 'Activate-AMM-Result',
      Success = "false",
      ["AMM-Process"] = msg.Tags["X-AMM-Process"],
      ["Reason"] = "Insufficient funds",
      ["Received-Quantity"] = tostring(msg.Tags.Quantity),
      ["Total-Cost"] = tostring(totalCost)
    })

    -- break the execution
    return false
  end

  -- if more funds received refund the difference to user
  if (quantity > totalCost) then
    -- Send back the funds
    ao.send({
      Target = msg.From,
      Action = "Transfer",
      Quantity = quantity - totalCost,
      Recipient = msg.Sender
    })
  end

  -- Update the AMM in sql with status "public"
  dexiCore.activateAMM(msg.Tags["X-AMM-Process"])

  ao.send({
    Target = msg.Sender,
    Action = 'Activate-AMM-Result',
    Success = "true",
    ["AMM-Process"] = msg.Tags["X-AMM-Process"],
    Data = "true"
  })
end

integrateAmm.handleGetAmmDetails = function(msg)
  assert(msg.Tags["AMM-Process"], "AMM status data must contain a valid AMM-Process tag")

  local details = dexiCore.getRegisteredAMM(msg.Tags["AMM-Process"])

  ao.send({
    Target = msg.Sender,
    Action = 'Get-AMM-Details-Result',
    ["AMM-Process"] = msg.Tags["AMM-Process"],
    Data = json.encode(details)
  })
end

return integrateAmm
