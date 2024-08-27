local mod = {}

function mod.open_memory()
  return {
    close = function() end,
    execute = function() end,
    exec = function() end,
    prepare = function() end,
    rows = function() end,
  }
end

return mod
