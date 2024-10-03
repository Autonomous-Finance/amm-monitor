local dbUtils = require("db.utils")
local responses = require('utils.responses')

local analytics = {}

local volumeQuery = [[
    WITH RECURSIVE date_range(date) AS (
        SELECT :start_timestamp
        UNION ALL
        SELECT date + 86400 -- add one day in seconds
        FROM date_range
        WHERE date < :end_timestamp
    ), process_id AS (
        SELECT :amm_process_id AS amm_process_id
    )
    SELECT
        DATE(date_range.date, 'unixepoch') as date,
        quote_token_process,
        COALESCE(SUM(volume), 0) AS daily_volume_quote_token,
        COALESCE(SUM(volume_usd), 0) AS daily_volume_usd
    FROM date_range
    LEFT JOIN amm_transactions_view ON DATE(created_at_ts, 'unixepoch') = DATE(date_range.date, 'unixepoch')
    JOIN process_id ON TRUE
    WHERE
        date_range.date >= :start_timestamp
        AND CASE WHEN amm_process_id IS NOT NULL THEN amm_process_id = amm_process_id ELSE TRUE END
    GROUP BY 1, 2
    ORDER BY 1 DESC;
]]

function analytics.getDailyVolume(msg)
    local startTimestamp = tonumber(msg.Tags['Start-Timestamp']) or math.floor(os.time() / 1000) - 7 * 24 * 60 * 60
    local endTimestamp = tonumber(msg.Tags['End-Timestamp']) or math.floor(os.time() / 1000)
    local ammProcessId = msg.Tags['Amm-Process-Id'] or nil

    assert(endTimestamp - startTimestamp <= 14 * 24 * 60 * 60, "No more then 14 days")

    assert(startTimestamp and endTimestamp, "Start and end timestamps are required")
    -- assert start date and end date are valid dates
    local startDate = os.date("!%Y-%m-%d", startTimestamp)
    local endDate = os.date("!%Y-%m-%d", endTimestamp)
    assert(startDate and endDate, "Start and end dates are required")

    local result = dbUtils.queryManyWithParams(volumeQuery, {
        start_timestamp = startTimestamp,
        end_timestamp = endTimestamp,
        amm_process_id = ammProcessId
    })
    responses.sendReply(msg, result)
end

return analytics
