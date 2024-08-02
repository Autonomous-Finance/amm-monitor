local mod = {}

function mod.handleSetQuoteToken(msg)
  assert(msg.From == OPERATOR, "Only the operator can set the quote token")
  assert(msg.Tags["Quote-Token-Process"], "Quote-Token-Process is required")
  assert(msg.Tags["Quote-Token-Ticker"], "Quote-Token-Ticker is required")
  assert(msg.Tags["Quote-Token-Denominator"], "Quote-Token-Denominator is required")
  assert(msg.Tags["Quote-Token-Total-Supply"], "Quote-Token-Total-Supply is required")

  QUOTE_TOKEN.ProcessId = msg.Tags["Quote-Token-Process"]
  QUOTE_TOKEN.Ticker = msg.Tags["Quote-Token-Ticker"]
  QUOTE_TOKEN.Denominator = msg.Tags["Quote-Token-Denominator"]
  QUOTE_TOKEN.TotalSupply = msg.Tags["Quote-Token-Total-Supply"]
  ao.send({
    Target = msg.From,
    Data = "Quote token set to " .. QUOTE_TOKEN.ProcessId .. " (" .. QUOTE_TOKEN.Ticker .. ")" ..
        " with a total supply of " .. QUOTE_TOKEN.TotalSupply .. " and a denominator of " .. QUOTE_TOKEN.Denominator
  })
end

function mod.handleSetPaymentToken(msg)
  assert(msg.From == OPERATOR, "Only the operator can set the payment token")
  assert(msg.Tags["Payment-Token-Process"], "Payment-Token-Process is required")
  assert(msg.Tags["Payment-Token-Ticker"], "Payment-Token-Ticker is required")
  PAYMENT_TOKEN_PROCESS = msg.Tags["Payment-Token-Process"]
  PAYMENT_TOKEN_TICKER = msg.Tags["Payment-Token-Ticker"]
  ao.send({
    Target = msg.From,
    Data = "Payment token set to " .. PAYMENT_TOKEN_PROCESS .. " (" .. PAYMENT_TOKEN_TICKER .. ")"
  })
end

function mod.handleSetOffchainFeedProvider(msg)
  assert(msg.From == OPERATOR, "Only the operator can set the offchain feed provider")
  assert(msg.Tags["Offchain-Feed-Provider"], "Offchain-Feed-Provider is required")
  OFFCHAIN_FEED_PROVIDER = msg.Tags["Offchain-Feed-Provider"]
  ao.send({
    Target = msg.From,
    Data = "Offchain feed provider set to " .. OFFCHAIN_FEED_PROVIDER
  })
end

function mod.handleSetSupplyUpdatesProvider(msg)
  assert(msg.From == OPERATOR, "Only the operator can set the supply updates provider")
  assert(msg.Tags["Supply-Updates-Provider"], "Supply-Updates-Provider is required")
  SUPPLY_UPDATES_PROVIDER = msg.Tags["Supply-Updates-Provider"]
  ao.send({
    Target = msg.From,
    Data = "Supply updates provider set to " .. SUPPLY_UPDATES_PROVIDER
  })
end

return mod
