local poolTokenPnl = require("amm-analytics.pool-token-pnl")
local dbUtils = require("db.utils")
local utils = require(".utils")
local bint = require(".bint")

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
            strftime('%Y-%m-%d', locked_until / 1000, 'unixepoch') AS locked_till_date,
            GROUP_CONCAT(CAST(current_locked_value AS TEXT)) AS locked_tokens
        FROM locked_tokens
        WHERE locked_token = :amm_process
        GROUP BY locked_till_date
    ]]

    local results = dbUtils.queryManyWithParams(stmt, { amm_process = ammProcess })

    -- Convert locked_tokens string to an array
    for _, result in ipairs(results) do
        result.locked_tokens = dbUtils.splitString(result.locked_tokens, ",")
        result.locked_tokens = utils.reduce(function(acc, curr)
            return acc + bint(curr)
        end, bint(0), result.locked_tokens)

        result.locked_tokens = tostring(result.locked_tokens)
    end

    return results
end

function mod.getOneYearLockedShare(ammProcess)
    local lockedAmount = mod.getOneYearLockedTokens(ammProcess)
    local poolTokenAmount = poolTokenPnl.getLastPoolTokenAmount(ammProcess)
    return lockedAmount / poolTokenAmount
end

return mod
