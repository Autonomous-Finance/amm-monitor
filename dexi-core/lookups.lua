local dbUtils = require("db.utils")
local hopper = require("hopper.hopper")
local mod = {}

function mod.tokenInfo(tokenProcess)
    local sql = [[
    SELECT * FROM token_registry WHERE token_process = :token_process"
    ]]
    return dbUtils.queryOneWithParams(sql, { token_process = tokenProcess }, "mod.tokenInfo")
end

function mod.ammInfo(ammProcess)
    local sql = [[
    SELECT
        amm_registry.*,
        t0.token_name AS token_0_name,
        t0.denominator AS token_0_denominator,
        t0.total_supply AS token_0_total_supply,
        t0.fixed_supply AS token_0_fixed_supply,
        t0.token_updated_at_ts AS token_0_token_updated_at_ts,
        t0.token_discovered_at_ts AS token_0_token_discovered_at_ts,
        t0.token_ticker AS token_0_ticker,
        t1.token_name AS token_1_name,
        t1.denominator AS token_1_denominator,
        t1.total_supply AS token_1_total_supply,
        t1.fixed_supply AS token_1_fixed_supply,
        t1.token_updated_at_ts AS token_1_token_updated_at_ts,
        t1.token_discovered_at_ts AS token_1_token_discovered_at_ts,
        t1.token_ticker AS token_1_ticker
    FROM amm_registry
    JOIN token_registry t0 ON t0.token_process = amm_registry.amm_token0
    JOIN token_registry t1 ON t1.token_process = amm_registry.amm_token1
    WHERE amm_process = :amm_process
    ]]
    return dbUtils.queryOneWithParams(sql, { amm_process = ammProcess }, "mod.ammInfo")
end

function mod.getPriceFromLastTransaction(token)
    local sql = [[
    SELECT
        amm_token0,
        amm_token1,
        token0_denominator,
        token1_denominator,
        token0_usd_price,
        token1_usd_price
    FROM amm_transactions_view
    WHERE amm_token0 = :token OR amm_token1 = :token
    ORDER BY created_at_ts DESC
    LIMIT 1
    ]]
    local result = dbUtils.queryOneWithParams(sql, { token = token }, "mod.getPriceFromLastTransaction")

    if result then
        if result.amm_token0 == token then
            return {
                price = result.token0_usd_price,
                denominator = result.token0_denominator
            }
        else
            return {
                price = result.token1_usd_price,
                denominator = result.token1_denominator
            }
        end
    end

    return nil
end

function mod.tryGetHopperPrice(token)
    local success, price = pcall(function()
        local denominator = lookups.tokenInfo(token).denominator
        return {
            price = hopper.getPrice(token, 'USD'),
            denominator = denominator
        }
    end)
    if not success then
        print('Error retrieving price for ' .. token .. ': ' .. price)
        return nil
    end
    return price
end

return mod
