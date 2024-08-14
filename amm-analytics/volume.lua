local dbUtils = require("db.utils")
local json = require("json")

local analytics = {}

function analytics.getDailyVolume(msg)
    local startTimestamp = tonumber(msg.Tags['Start-Timestamp'])
    local endTimestamp = tonumber(msg.Tags['End-Timestamp'])
    local ammProcessId = msg.Tags['Amm-Process-Id'] or nil

    assert(startTimestamp and endTimestamp, "Start and end timestamps are required")
    -- assert start date and end date are valid dates
    local startDate = os.date("!%Y-%m-%d", startTimestamp)
    local endDate = os.date("!%Y-%m-%d", endTimestamp)
    assert(startDate and endDate, "Start and end dates are required")

    local stmt = db:prepare([[
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
        COALESCE(SUM(volume), 0) AS daily_volume
    FROM date_range
    LEFT JOIN amm_transactions_view ON DATE(created_at_ts, 'unixepoch') = DATE(date_range.date, 'unixepoch')
    JOIN process_id ON TRUE
    WHERE
        date_range.date >= :start_timestamp AND (quote_token_process = :quote_token_process OR quote_token_process IS NULL)
        AND CASE WHEN amm_process_id IS NOT NULL THEN amm_process_id = amm_process_id ELSE TRUE END
    GROUP BY 1, 2
    ORDER BY 1 DESC;
    ]])

    if not stmt then
        error("Err: " .. db:errmsg())
    end

    stmt:bind_names({
        start_timestamp = startTimestamp,
        end_timestamp = endTimestamp,
        quote_token_process = QUOTE_TOKEN.ProcessId,
        amm_process_id = ammProcessId
    })

    local result = dbUtils.queryMany(stmt)

    ao.send({
        ['Response-For'] = 'Get-Daily-Volume',
        ['Target'] = msg.From,
        ['Data'] = json.encode(result)
    })
end

return analytics
