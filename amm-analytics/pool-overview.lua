local dbUtils = require("db.utils")
local json = require("json")
local responses = require('utils.responses')

local analytics = {}

local overviewQuery = [[
WITH tx_counts AS (
    SELECT amm_process, count(1) cnt FROM amm_transactions_view GROUP BY 1
), fees30d AS (
    SELECT amm_process, SUM(volume_usd * .25) AS fees_in_usd_30d FROM amm_transactions_view WHERE created_at_ts >= :now - 2592000 GROUP BY 1
), tvl AS (
    SELECT amm_process, tvl_in_usd, row_number() over (PARTITION BY amm_process ORDER BY created_at_ts DESC) AS seq FROM reserve_changes
),
preagg AS (
    SELECT
        t1.token_ticker AS base_token_ticker,
        t0.token_ticker AS quote_token_ticker,
        t1.token_name AS base_token_name,
        t0.token_name AS quote_token_name,
        t0.token_process AS quote_token_process,
        t1.token_process AS base_token_process,
        asp.amm_process AS amm_process,
        -- ((CAST(reserves_0 AS REAL) + (CAST(reserves_1 AS REAL) * coalesce(ltfp.price, 0))) / POW(10, quote_denominator)) tvl_in_quote,
        tvl_in_usd,
        f30.fees_in_usd_30d / tvl_in_usd AS apr_30d,
        tx_counts.cnt AS tx_count
    FROM amm_swap_params asp
    LEFT JOIN tx_counts USING (amm_process)
    LEFT JOIN tvl ON tvl.amm_process = asp.amm_process AND tvl.seq = 1
    LEFT JOIN amm_registry ar USING (amm_process)
    LEFT JOIN fees30d f30 ON f30.amm_process = ar.amm_process
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
-- WHERE tvl_rank BETWEEN :start_rank AND :end_rank
ORDER BY tvl_rank
LIMIT :end_rank
OFFSET :start_rank - 1
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
        now = math.floor(msg.Timestamp / 1000)
    })

    local result = dbUtils.queryMany(stmt)

    local replyData = result
    responses.sendReply(msg, replyData)
end

return analytics
