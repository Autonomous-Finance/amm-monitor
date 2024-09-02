---@diagnostic disable: duplicate-set-field
require("test.setup")()
local busted = require("busted")

local hopper_mock = require("hopper.hopper")

_G.IsInUnitTest = true -- prevent ao.send from attempting to execute
_G.VerboseTests = 0

describe("hopper lib", function()
  local fetch_pools_stub
  local fetch_oracle_pools_stub

  before_each(function()
    fetch_pools_stub = busted.stub(hopper_mock, "fetch_pools")
    fetch_pools_stub.returns({
      { token0 = "bark", token1 = "mockAO", reserve0 = 1, reserve1 = 2 },
      { token0 = "lola", token1 = "bark",   reserve0 = 1, reserve1 = 2 },
      { token0 = "dog",  token1 = "lola",   reserve0 = 1, reserve1 = 2 },
      { token0 = "dog",  token1 = "cat",    reserve0 = 1, reserve1 = 2 }
    })

    fetch_oracle_pools_stub = busted.stub(hopper_mock, "fetch_oracle_pools")
    fetch_oracle_pools_stub.returns({
      {
        token0 = "mockAO",
        token1 = 'USD',
        reserve0 = 1,
        reserve1 = 50
      },
      {
        token0 = 'USD',
        token1 = 'USD',
        reserve0 = 1,
        reserve1 = 1
      }
    })
  end)

  it("should get price for token", function()
    local result = hopper_mock.getPrice("bark", "mockAO")

    print('result', result)

    local expected_result = 2

    assert.are.same(result, expected_result)
  end)

  it("should not get price for token", function()
    local result = hopper_mock.getPrice("dexi-token", "mockAO")

    local expected_result = nil

    assert.are.same(result, expected_result)
  end)

  it("should get price for token with 1 hop", function()
    local result = hopper_mock.getPrice("lola", "mockAO")

    local expected_result = 4

    assert.are.same(result, expected_result)
  end)

  it("should get price for token with 2 hops", function()
    local result = hopper_mock.getPrice("dog", "mockAO")

    local expected_result = 8

    assert.are.same(result, expected_result)
  end)

  it("should get price for token with 3 hops", function()
    local result = hopper_mock.getPrice("cat", "mockAO")

    local expected_result = 4

    assert.are.same(result, expected_result)
  end)

  it("should get price for token with 3 hops - USD", function()
    local result = hopper_mock.getPrice("cat", "USD")

    local expected_result = 200

    assert.are.same(result, expected_result)
  end)

  it("should get price for token with 3 hops reversed", function()
    local result = hopper_mock.getPrice("mockAO", "cat")

    local expected_result = 0.25

    assert.are.same(result, expected_result)
  end)
end)
