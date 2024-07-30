local json = require("json")
local dexiCore = require("dexi-core.dexi-core")

local register_amm = {}

register_amm.handleRegisterSubscriber = function(msg)
  -- send Register-Subscriber to amm process
  ao.send({
    Target = msg.Tags["AMM-Process"],
    Action = "Register-Subscriber",
    Tags = {
      ["Subscriber-Process-Id"] = ao.id,
      ["Owner-Id"] = msg.From,
      ['Topics'] = json.encode({ "order-confirmation", "swap-params-change" })
    }
  })

  -- send confirmation to sender
  ao.send({
    Target = msg.From,
    Action = "Dexi-AMM-Subscriber-Registration-Confirmation",
    Tags = {
      ["AMM-Process"] = msg.Tags["AMM-Process"]
    }
  })
end

register_amm.handlePayForSubscriptions = function(msg)
  assert(msg.Tags.Quantity, 'Credit notice data must contain a valid quantity')
  assert(msg.Tags.Sender, 'Credit notice data must contain a valid sender')
  assert(msg.Tags["X-AMM-Process"], 'Credit notice data must contain a valid amm-process')
  assert(msg.Tags["X-Token-A"], 'Credit notice data must contain a valid token-a')
  assert(msg.Tags["X-Token-B"], 'Credit notice data must contain a valid token-b')
  assert(msg.Tags["X-Name"], 'Credit notice data must contain a valid fee-percentage')

  -- Pay for the Subscription
  ao.send({
    Target = PAYMENT_TOKEN_PROCESS,
    Action = "Transfer",
    Tags = {
      Recipient = msg.Tags["X-AMM-Process"],
      Quantity = msg.Tags.Quantity,
      ["X-Action"] = "Pay-For-Subscription"
    }
  })

  dexiCore.registerAMM(
    msg.Tags["X-Name"],
    msg.Tags["X-AMM-Process"],
    msg.Tags["X-Token-A"],
    msg.Tags["X-Token-B"],
    msg.Timestamp
  )

  -- send confirmation to sender
  ao.send({
    Target = msg.Tags.Sender,
    Action = "Dexi-AMM-Registration-Confirmation",
    Tags = {
      ["AMM-Process"] = msg.Tags["X-AMM-Process"],
      ["Token-A"] = msg.Tags["X-Token-A"],
      ["Token-B"] = msg.Tags["X-Token-B"],
      ["Name"] = msg.Tags["X-Name"]
    }
  })
end

return register_amm
