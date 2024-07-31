---@diagnostic disable: duplicate-set-field
require("test.setup")()

_G.VerboseTests = 0
_G.TestStartTime = os.time()
_G.VirtualTime = os.time()
_G.printVerb = function(level)
  level = level or 2
  return function(...) -- define here as global so we can use it in application code too
    if _G.VerboseTests >= level then print(table.unpack({ ... })) end
  end
end

_G.Owner = '123Owner321'
_G.MainProcessId = 'xyzAgentzyx'

_G.ao = require "ao" (_G.MainProcessId) -- make global so that the main process and its non-mocked modules can use it
-- => every ao.send({}) in this test file effectively appears as if the message comes the main process

_G.Handlers = require "handlers"
local dexi = require "process" -- require so that process handlers are loaded, as well as global variables


-- MOCK IDs

_G.MockTokenProcessIds = {
  "Token-1-XYZ",
  "Token-2-XYZ",
  "Token-3-XYZ",
  "Token-4-XYZ",
  "Token-5-XYZ",
  "Quote-Token-XYZ"
}

_G.MockDenomination = 6

_G.MockTotalSupplies = {
  tostring(10 ^ _G.MockDenomination * 1000 * 1),
  tostring(10 ^ _G.MockDenomination * 1000 * 2),
  tostring(10 ^ _G.MockDenomination * 1000 * 3),
  tostring(10 ^ _G.MockDenomination * 1000 * 4),
  tostring(10 ^ _G.MockDenomination * 1000 * 5),
  tostring(10 ^ _G.MockDenomination * 1000 * 6),
}

_G.MockPoolProcessIds = {
  "Pool-1-XYZ",
  "Pool-2-XYZ",
  "Pool-3-XYZ",
  "Pool-4-XYZ",
  "Pool-5-XYZ",
}

local json = require "json"
local bint = require ".bint" (256)

local dexiCore = require "dexi-core.dexi-core"

_G.AgentLogInfos = _G.AgentLogInfos or {}
_G.AgentLogErrors = _G.AgentLogErrors or {}

-- UNIT TESTS

describe("status quo after seeding tokens and pool", function()
  setup
  (function()
    for index, value in ipairs(_G.MockTokenProcessIds) do
      dexiCore.registerToken(
        value, 'Token' .. index, _G.MockDenomination, _G.MockTotalSupplies, false, 1712000000 + index
      )
    end
    for index, value in ipairs(_G.MockPoolProcessIds) do
      dexiCore.registerAMM(
        'Pool' .. index, value, _G.MockTokenProcessIds[6], _G.MockTokenProcessIds[index], 1712999000 + index)
    end
  end)

  teardown(function()

  end)


  it("should start with an empty token set and no market data", function()

  end)
end)
