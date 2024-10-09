local poolTokenPnl = require("amm-analytics.pool-token-pnl")
local dbUtils = require("db.utils")

local function getOneYearLockedTokens(ammProcess)
    local stmt = [[
        SELECT
            SUM(CAST(current_locked_value AS INTEGER)) AS total_locked_tokens
    FROM locked_tokens
        WHERE locked_token = :amm_process AND locked_until > :current_timestamp + (60 * 60 * 24 * 365 * 1000)
    ]]

    local result = dbUtils.queryOneWithParams(stmt, { amm_process = ammProcess, current_timestamp = os.time() })
    return result.total_locked_tokens
end

local function getAggregateLockedTokens(ammProcess)
    local stmt = [[
        SELECT
            strftime('%Y-%m-%d', ceil(created_at_ts / 1000)) AS date,
            SUM(CAST(current_locked_value AS INTEGER)) AS total_locked_tokens
        FROM locked_tokens
        WHERE locked_token = :amm_process
        GROUP BY date
    ]]

    return dbUtils.query(stmt, { amm_process = ammProcess })
end

local function getOneYearLockedShare(ammProcess)
    local lockedAmount = getOneYearLockedTokens(ammProcess)
    local poolTokenAmount = poolTokenPnl.getLastPoolTokenAmount(ammProcess)
    return lockedAmount / poolTokenAmount
end
