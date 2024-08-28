local json = require "json"
local bint = require ".bint" (256)

local function newmodule(selfId, name, initialBalances)
  local token = {}

  local ao = require "ao" (selfId)

  token.balance = initialBalances or {}
  token.name = name

  local function transfer(from, to, amount, forwardTags)
    local currFrom = bint(token.balance[from] or "0")
    local currTo = bint(token.balance[to] or "0")

    token.balance[to] = currTo + bint(amount)
    token.balance[from] = currFrom - bint(amount)
    local payloadCredit = {
      Target = to,
      Action = "Credit-Notice",
      Sender = from,
      Quantity = amount
    }
    for k, v in pairs(forwardTags) do
      payloadCredit[k] = v
    end

    ao.send(payloadCredit)

    local payloadDebit = {
      Target = from,
      Action = "Debit-Notice",
      Recipient = to,
      Quantity = amount
    }
    for k, v in pairs(forwardTags) do
      payloadDebit[k] = v
    end

    ao.send(payloadDebit)
  end

  function token.handle(msg)
    if msg.Tags.Action == "Transfer" then
      if msg.Tags.Quantity == '0' then
        error('0 Quantity in transfer of ' .. ao.id .. ' from ' .. msg.From .. ' to ' .. msg.Recipient)
      end
      local fwTags = {
        ["X-Action"] = msg.Tags["X-Action"],
        ["X-Slippage-Tolerance"] = msg.Tags["X-Slippage-Tolerance"],
        ["X-Expected-Output"] = msg.Tags["X-Expected-Output"],
      }
      transfer(msg.From, msg.Recipient, msg.Tags.Quantity, fwTags)
    else
      printVerb(2)('⚠️ ' .. token.name .. ' token handler not implemented for Action ' .. tostring(msg.Tags.Action))
    end
  end

  return token
end
return newmodule
