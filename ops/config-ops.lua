local mod = {}

function mod.handleSetQuoteToken(msg)
  assert(msg.From == OPERATOR, "Only the operator can set the quote token")
  assert(msg.Tags["Quote-Token-Process"], "Quote-Token-Process is required")
  assert(msg.Tags["Quote-Token-Ticker"], "Quote-Token-Ticker is required")
  QUOTE_TOKEN_PROCESS = msg.Tags["Quote-Token-Process"]
  QUOTE_TOKEN_TICKER = msg.Tags["Quote-Token-Ticker"]
  ao.send({
    Target = msg.From,
    Data = "Quote token set to " .. QUOTE_TOKEN_PROCESS .. " (" .. QUOTE_TOKEN_TICKER .. ")"
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
