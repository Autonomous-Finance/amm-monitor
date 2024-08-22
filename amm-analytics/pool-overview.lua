local dbUtils = require("db.utils")
local json = require("json")

local analytics = {}

local overviewQuery = [[
WITH latest_transactions_for_pool AS (
    SELECT
        row_number() over (PARTITION BY amm_process ORDER BY created_at_ts DESC) as seq,
        amm_process,
        price,
        quote_denominator
    FROM amm_transactions_view
), tx_counts AS (
    SELECT amm_process, count(1) cnt FROM amm_transactions_view GROUP BY 1
), fees30d AS (
    SELECT amm_process, SUM(volume * .2) AS fees_in_quote_30d FROM amm_transactions_view WHERE created_at_ts >= :now - 2592000 GROUP BY 1
), preagg AS (
    SELECT
        t1.token_ticker AS base_token_ticker,
        t0.token_ticker AS quote_token_ticker,
        t1.token_name AS base_token_name,
        t0.token_name AS quote_token_name,
        t0.token_process AS quote_token_process,
        t1.token_process AS base_token_process,
        asp.amm_process AS amm_process,
        ((CAST(reserves_0 AS REAL) + (CAST(reserves_1 AS REAL) * coalesce(ltfp.price, 0))) / POW(10, quote_denominator)) tvl_in_quote,
        ((CAST(reserves_0 AS REAL) + (CAST(reserves_1 AS REAL)) * coalesce(ltfp.price, 0)) / POW(10, quote_denominator)) * op.price tvl_in_usd,
        fees30d.fees_in_quote_30d / tvl_in_quote AS apr_30d,
        tx_counts.cnt AS tx_count
    FROM amm_swap_params asp
    LEFT JOIN latest_transactions_for_pool ltfp ON ltfp.amm_process = asp.amm_process AND seq = 1
    LEFT JOIN tx_counts USING (amm_process)
    LEFT JOIN amm_registry ar USING (amm_process)
    LEFT JOIN oracle_prices op ON op.process_id = ar.amm_token0
    LEFT JOIN fees30d v30 ON v30.amm_process = ar.amm_process
    LEFT JOIN token_registry t0 ON t0.token_process = ar.amm_token0
    LEFT JOIN token_registry t1 ON t1.token_process = ar.amm_token1
), sorted AS (
    SELECT
        *,
        row_number() over (ORDER BY tvl_in_usd DESC) as tvl_rank
    FROM preagg
)
SELECT *
FROM sorted
WHERE tvl_rank BETWEEN :start_rank AND :end_rank
]]


function analytics.getPoolOverview(msg)
    local startRank = tonumber(msg.Tags['Start-Rank'])
    local endRank = tonumber(msg.Tags['End-Rank'])

    assert(startRank and endRank, "Start and end ranks are required")

    local stmt = db:prepare(overviewQuery)

    if not stmt then
        error("Err: " .. db:errmsg())
    end

    stmt:bind_names({
        start_rank = startRank,
        end_rank = endRank,
    })

    local result = dbUtils.queryMany(stmt)

    ao.send({
        ['Response-For'] = 'Get-Pool-Overview',
        ['Target'] = msg.From,
        ['Data'] = json.encode(result)
    })
end

return analytics
