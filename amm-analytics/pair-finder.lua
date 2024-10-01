local dbUtils = require('db.utils')
local analytics = {}
local lookups = require('dexi-core.lookups')
local responses = require('utils.responses')

local pairFinderQuery = [[
    SELECT
        t1.token_ticker AS t1_ticker,
        t0.token_ticker AS t0_ticker,
        t1.token_name AS t1_name,
        t0.token_name AS t0_name,
        t0.token_process AS t0_process,
        t1.token_process AS t1_process,
        asp.amm_process AS amm_process,
        reserves_0,
        reserves_1
    FROM amm_swap_params asp
    LEFT JOIN amm_registry ar USING (amm_process)
    LEFT JOIN token_registry t0 ON t0.token_process = ar.amm_token0
    LEFT JOIN token_registry t1 ON t1.token_process = ar.amm_token1
    WHERE amm_token0 = :token OR amm_token1 = :token
]]

function analytics.findBestPairs(tokenProcess)
    local result = dbUtils.queryManyWithParams(pairFinderQuery, { token = tokenProcess })

    for _, pair in ipairs(result) do
        local token0Price = lookups.getPriceFromLastTransaction(pair.t0_process)
        local token1Price = lookups.getPriceFromLastTransaction(pair.t1_process)

        if token0Price and token1Price then
            local reserves0 = pair.reserves_0 / (10 ^ token0Price.denominator)
            local reserves1 = pair.reserves_1 / (10 ^ token1Price.denominator)

            pair.tvl_in_usd = reserves0 * token0Price.price + reserves1 * token1Price.price
        else
            pair.tvl_in_usd = nil
        end
    end

    -- Sort the result table by tvl_in_usd in descending order
    table.sort(result, function(a, b)
        -- Handle cases where tvl_in_usd might be nil
        if a.tvl_in_usd == nil and b.tvl_in_usd == nil then
            return false -- Consider them equal
        elseif a.tvl_in_usd == nil then
            return false -- nil values go to the end
        elseif b.tvl_in_usd == nil then
            return true  -- nil values go to the end
        else
            return a.tvl_in_usd > b.tvl_in_usd
        end
    end)

    return result
end

function analytics.findBestPairsForToken(msg)
    local tokenProcess = msg.Tags['Token']
    local pairs = analytics.findBestPairs(tokenProcess)
    if not pairs then
        local replyData = {
            {
                ticker = 'mockAO',
                name = 'mockAO',
                process = 'j7w28CJQHYwamMsotkhE7x0aVUggGwrBtdO5-MQ80uU',
                tvl_in_usd = 100000000
            }
        }
        local replyTags = { ['No-Pools-Found'] = true }
        responses.sendReply(msg, replyData, replyTags)
        return
    end

    local replyData = pairs
    responses.sendReply(msg, replyData)
end

return analytics
