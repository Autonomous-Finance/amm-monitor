local poolTokenPnl = require("amm-analytics.pool-token-pnl")
local dbUtils = require("db.utils")

local mod = {}

function mod.getOneYearLockedTokens(ammProcess)
    local stmt = [[
        SELECT
            SUM(CAST(current_locked_value AS REAL)) AS total_locked_tokens
    FROM locked_tokens
        WHERE locked_token = :amm_process AND locked_until > :current_timestamp + (60 * 60 * 24 * 364 * 1000)
    ]]

    local result = dbUtils.queryOneWithParams(stmt, { amm_process = ammProcess, current_timestamp = os.time() })
    return result and result.total_locked_tokens or 0
end

function mod.getAggregateLockedTokens(ammProcess)
    local stmt = [[
        SELECT
            strftime('%Y-%m-%d', locked_at_ts / 1000, 'unixepoch') AS locked_at_date,
            SUM(CAST(current_locked_value AS REAL)) AS locked_tokens
        FROM locked_tokens
        WHERE locked_token = :amm_process
        GROUP BY locked_at_date
    ]]

    return dbUtils.queryManyWithParams(stmt, { amm_process = ammProcess })
end

function mod.getOneYearLockedShare(ammProcess)
    local lockedAmount = mod.getOneYearLockedTokens(ammProcess)
    local poolTokenAmount = poolTokenPnl.getLastPoolTokenAmount(ammProcess)
    return lockedAmount / poolTokenAmount
end

return mod