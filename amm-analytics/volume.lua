local dbUtils = require("db.utils")

local analytics = {}

function analytics.getDailyVolume(msg)
    local startDate = msg['Start-Date']
    local endDate = msg['End-Date']
    local ammProcessId = msg['Amm-Process-Id'] or nil

    assert(startDate and endDate, "Start and end dates are required")
    -- assert start date and end date are valid dates
    assert(os.date("%Y-%m-%d", startDate) and os.date("%Y-%m-%d", endDate), "Start and end dates are required")

    local stmt = [[
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
    ]]

    stmt = dbUtils.bindParams(stmt, {
        start_date = startDate,
        end_date = endDate,
        quote_token_process = QUOTE_TOKEN.ProcessId,
        amm_process_id = ammProcessId
    })

    return dbUtils.queryMany(stmt)
end

return analytics
