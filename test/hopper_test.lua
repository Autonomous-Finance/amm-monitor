---@diagnostic disable: duplicate-set-field
require("test.setup")()

local hopper = require "hopper.hopper"

_G.IsInUnitTest = true -- prevent ao.send from attempting to execute
_G.VerboseTests = 0

local resetGlobals = function()
  _G.db = {
    nrows = function(query)
      -- Simulate the rows returned by the query
      local data = {
        { token0 = "bark", token1 = "mockAO", reserve0 = 1, reserve1 = 2 },
        { token0 = "lola", token1 = "bark", reserve0 = 1, reserve1 = 2 },
        { token0 = "dog", token1 = "lola", reserve0 = 1, reserve1 = 2 },
        { token0 = "dog", token1 = "cat", reserve0 = 1, reserve1 = 2 },
      }

      -- Iterator function that mimics db:nrows behavior
      local i = 0
      return function()
        i = i + 1
        if data[i] then
          return data[i]
        end
      end
    end,
    close = function() end,
  }
end

describe("hopper lib", function()
  it("should get price for token", function()
    resetGlobals()

    local result = hopper.getPriceForToken({
      Tags = {
        ["Quote-Token-Process"] = "mockAO",
        ["Base-Token-Process"] = "bark"
      }
    })

    local expected_result = { baseToken = "bark", quoteToken = "mockAO", price = 2 }

    assert.are.same(result, expected_result)
  end)

  it("should not get price for token", function()
    resetGlobals()

    local result = hopper.getPriceForToken({
      Tags = {
        ["Quote-Token-Process"] = "mockAO",
        ["Base-Token-Process"] = "dexi-token"
      }
    })

    local expected_result = { baseToken = "dexi-token", quoteToken = "mockAO", price = nil }

    assert.are.same(result, expected_result)
  end)

  it("should get price for token with 1 hop", function()
    resetGlobals()

    local result = hopper.getPriceForToken({
      Tags = {
        ["Quote-Token-Process"] = "mockAO",
        ["Base-Token-Process"] = "lola"
      }
    })

    local expected_result = { baseToken = "lola", quoteToken = "mockAO", price = 4 }

    assert.are.same(result, expected_result)
  end)

  it("should get price for token with 2 hops", function()
    resetGlobals()

    local result = hopper.getPriceForToken({
      Tags = {
        ["Quote-Token-Process"] = "mockAO",
        ["Base-Token-Process"] = "dog"
      }
    })

    local expected_result = { baseToken = "dog", quoteToken = "mockAO", price = 8 }

    assert.are.same(result, expected_result)
  end)

  it("should get price for token with 3 hops", function()
    resetGlobals()

    local result = hopper.getPriceForToken({
      Tags = {
        ["Quote-Token-Process"] = "mockAO",
        ["Base-Token-Process"] = "cat"
      }
    })

    local expected_result = { baseToken = "cat", quoteToken = "mockAO", price = 4 }

    assert.are.same(result, expected_result)
  end)

  it("should get price for token with 3 hops", function()
    resetGlobals()

    local result = hopper.getPriceForToken({
      Tags = {
        ["Quote-Token-Process"] = "cat",
        ["Base-Token-Process"] = "mockAO"
      }
    })

    local expected_result = { baseToken = "mockAO", quoteToken = "cat", price = 0.25 }

    assert.are.same(result, expected_result)
  end)
end)
