local json = require("json")
local dexiCore = require("dexi-core.dexi-core")

local register_amm = {}

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
          processId = tokenBProcessId,
          tokenName = ,
          tokenTicker = ,
          denominator = ,
          totalSupply = ,
          fixedSupply = false,
        },
      },
    }
  }
]]
AmmSubscriptions = AmmSubscriptions or {}


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

local subscribeToAmm = function(ammProcessId)
  ao.send({
    Target = ammProcessId,
    Action = "Register-Subscriber",
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
register_amm.handlePayForAmmRegistration = function(msg)
  assert(msg.Tags.Quantity, 'Credit notice data must contain a valid quantity')
  assert(msg.Tags.Sender, 'Credit notice data must contain a valid sender')
  assert(msg.Tags["X-AMM-Process"], 'Credit notice data must contain a valid amm-process')

  local ammProcessId = msg.Tags["X-AMM-Process"]

  AmmSubscriptions[ammProcessId] = {
    requester = msg.Tags.Sender
  }

  getAmmInfo(msg.Tags["X-AMM-Process"])
  updateStatus(ammProcessId, 'received-request--initializing')
end

-- 2. Receive the AMM Info to initialize the registration with correct data
register_amm.handleInfoResponseFromAmm = function(msg)
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
    tokenA = msg.Tags["Token-A"],
    tokenB = msg.Tags["Token-B"]
  }

  subscribeToAmm(ammProcessId)
  updateStatus(ammProcessId, 'initilized--subscribing')
end

-- 3. Receive Subscription Confirmation from AMM
register_amm.handleSubscriptionConfirmationFromAmm = function(msg)
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
register_amm.handlePaymentConfirmationFromAmm = function(msg)
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
end

return register_amm
