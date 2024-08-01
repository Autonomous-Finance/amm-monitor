local mod = {}

function mod.toggleDispatchActive(msg)
  assert(msg.From == OPERATOR, "Only the operator can toggle dispatching")
  DISPATCH_ACTIVE = not DISPATCH_ACTIVE
  ao.send({
    Target = msg.From,
    Data = "Dispatching toggled to " .. tostring(not DISPATCH_ACTIVE)
  })
end

function mod.toggleLoggingActive(msg)
  assert(msg.From == OPERATOR, "Only the operator can toggle logging")
  LOGGING_ACTIVE = not LOGGING_ACTIVE
  ao.send({
    Target = msg.From,
    Data = "Logging toggled to " .. tostring(not LOGGING_ACTIVE)
  })
end

return mod
