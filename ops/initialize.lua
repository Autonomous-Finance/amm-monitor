local mod = {}

mod.triggerInitialize = function()
  assert(QUOTE_TOKEN.ProcessId, 'QUOTE_TOKEN.ProcessId is required for Dexi initialization')
  assert(QUOTE_TOKEN.Ticker, 'QUOTE_TOKEN.Name is required for Dexi initialization')
  assert(QUOTE_TOKEN.Denominator, 'QUOTE_TOKEN.Denominator is required for Dexi initialization')
  assert(QUOTE_TOKEN.TotalSupply, 'QUOTE_TOKEN.TotalSupply is required for Dexi initialization')
  assert(PAYMENT_TOKEN_PROCESS, 'PAYMENT_TOKEN_PROCESS is required for Dexi initialization')
  assert(SUPPLY_UPDATES_PROVIDER, 'SUPPLY_UPDATES_PROVIDER is required for Dexi initialization')

  ao.send({
    Target = ao.id,
    Action = "Dexi-Initialize",
  })
end

mod.handleInitialize = function(msg)
  if msg.From ~= ao.id then
    error('Unauthorized')
  end

  mod.registerToken(
    QUOTE_TOKEN.ProcessId,
    QUOTE_TOKEN.Name,
    QUOTE_TOKEN.Denominator,
    QUOTE_TOKEN.TotalSupply,
    true,
    math.floor(msg.Timestamp / 1000)
  )

  Initialized = true
  ao.send({
    Target = ao.id,
    Event = 'Dexi-Initialized'
  })
end

return mod
