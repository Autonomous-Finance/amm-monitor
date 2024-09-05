local dexiCore = require "dexi-core.dexi-core"

local mod = {}

mod.handleInitialize = function(msg)
  if msg.From ~= OPERATOR then
    error('Unauthorized')
  end

  if Initialized then
    return
  end

  assert(msg.Tags.QuoteTokenProcess, 'QuoteTokenProcess is required for Dexi initialization')
  assert(msg.Tags.QuoteTokenTicker, 'QuoteTokenTicker is required for Dexi initialization')
  assert(msg.Tags.QuoteTokenDenominator, 'QuoteTokenDenominator is required for Dexi initialization')
  assert(msg.Tags.QuoteTokenTotalSupply, 'QuoteTokenTotalSupply is required for Dexi initialization')
  assert(msg.Tags.PaymentTokenProcess, 'PaymentTokenProcess is required for Dexi initialization')
  assert(msg.Tags.PaymentTokenTicker, 'PaymentTokenTicker is required for Dexi initialization')
  assert(msg.Tags.SupplyUpdatesProvider, 'SupplyUpdatesProvider is required for Dexi initialization')

  QUOTE_TOKEN.ProcessId = msg.Tags.QuoteTokenProcess
  QUOTE_TOKEN.Ticker = msg.Tags.QuoteTokenTicker
  QUOTE_TOKEN.Denominator = msg.Tags.QuoteTokenDenominator
  QUOTE_TOKEN.TotalSupply = msg.Tags.QuoteTokenTotalSupply
  PAYMENT_TOKEN_PROCESS = msg.Tags.PaymentTokenProcess
  PAYMENT_TOKEN_TICKER = msg.Tags.PaymentTokenTicker
  SUPPLY_UPDATES_PROVIDER = msg.Tags.SupplyUpdatesProvider

  dexiCore.registerToken(
    QUOTE_TOKEN.ProcessId,
    QUOTE_TOKEN.Name,
    QUOTE_TOKEN.Ticker,
    QUOTE_TOKEN.Denominator,
    QUOTE_TOKEN.TotalSupply,
    true, -- assuming fixed supply
    math.floor(msg.Timestamp / 1000)
  )

  Initialized = true
  ao.send({
    Target = ao.id,
    Event = 'Initialized'
  })
end

return mod
