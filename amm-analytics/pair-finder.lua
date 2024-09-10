local dbUtils = require('db.utils')
local analytics = {}
local json = require('json')

local pairFinderQuery = [[
    SELECT
        t1.token_ticker AS t1_ticker,
        t0.token_ticker AS t0_ticker,
        t1.token_name AS t1_name,
        t0.token_name AS t0_name,
        t0.token_process AS t0_process,
        t1.token_process AS t1_process,
        asp.amm_process AS amm_process,
        (reserves_0 / pow(10, token0_denominator) * token0_usd_price
                + reserves_1 / pow(10, token1_denominator) * token1_usd_price) tvl_in_usd
    FROM amm_swap_params asp
    LEFT JOIN amm_registry ar USING (amm_process)
    LEFT JOIN token_registry t0 ON t0.token_process = ar.amm_token0
    LEFT JOIN token_registry t1 ON t1.token_process = ar.amm_token1
    WHERE t0.token_process = :token OR t1.token_process = :token
    ORDER BY tvl_in_usd DESC
]]

function analytics.findBestPairs(tokenProcess)
    return dbUtils.queryManyWithParams(pairFinderQuery, { token = tokenProcess })
end

function analytics.findBestPairsForToken(msg)
    local tokenProcess = msg.Tags['Token']
    local pairs = analytics.findBestPairs(tokenProcess)
    if not pairs then
        ao.send({
            ['Response-For'] = 'Find-Best-Pairs-For-Token',
            ['No-Pools-Found'] = true,
            ['Target'] = msg.From,
            ['Data'] = json.encode({
                {
                    ticker = 'mockAO',
                    name = 'mockAO',
                    process = 'j7w28CJQHYwamMsotkhE7x0aVUggGwrBtdO5-MQ80uU',
                    tvl_in_usd = 100000000
                }
            })
        })
        return
    end
    ao.send({
        ['Response-For'] = 'Find-Best-Pairs-For-Token',
        ['Target'] = msg.From,
        ['Data'] = json.encode(pairs)
    })
end

return analytics
