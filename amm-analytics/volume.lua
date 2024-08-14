local dbUtils = require("db.utils")

local analytics = {}

function analytics.getDailyVolume(msg)
    local startTimestamp = tonumber(msg.Tags['Start-Timestamp'])
    local endTimestamp = tonumber(msg.Tags['End-Timestamp'])
    local ammProcessId = msg.Tags['Amm-Process-Id'] or nil

    assert(startTimestamp and endTimestamp, "Start and end timestamps are required")
    -- assert start date and end date are valid dates
    local startDate = os.date("!*t", startTimestamp)
    local endDate = os.date("!*t", endTimestamp)
    assert(startDate and endDate, "Start and end dates are required")

    local stmt = db:prepare([[
    WITH RECURSIVE date_range(date) AS (
        SELECT DATE(:start_date, 'unixepoch')
        UNION ALL
        SELECT date(date, '+1 day')
        FROM date_range
        WHERE date < :end_date
    )
    SELECT
        date_range.date as date,
        quote_token_process,
        COALESCE(SUM(volume), 0) AS daily_volume
    FROM date_range
    LEFT JOIN amm_transactions_view ON DATE(created_at_ts, 'unixepoch') = date_range.date
    WHERE
        date_range.date >= DATE(:start_date, 'unixepoch') AND (quote_token_process = :quote_token_process OR quote_token_process IS NULL)
        CASE WHEN :amm_process_id IS NOT NULL THEN amm_process_id = :amm_process_id ELSE TRUE END
    GROUP BY 1, 2
    ORDER BY 1 DESC;
    ]])

    if not stmt then
        error("Err: " .. db:errmsg())
    end

    stmt:bind_names({
        start_date = startDate,
        end_date = endDate,
        quote_token_process = QUOTE_TOKEN.ProcessId,
        amm_process_id = ammProcessId
    })

    return dbUtils.queryMany(stmt)
end

return analytics
