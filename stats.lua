local sqlschema = require('sqlschema')
local stats = {}

function stats.getAggregateStats(minTimestamp, ammProcessId)
  print(1)
  local stmt, err = db:prepare[[
    SELECT 
      SUM(volume) AS total_volume,
      ROUND(SUM(CASE WHEN is_buy = 1 THEN volume ELSE 0 END)) AS buy_volume,
      ROUND(SUM(CASE WHEN is_buy = 0 THEN volume ELSE 0 END)) AS sell_volume,
      SUM(is_buy) AS buy_count,
      SUM(1 - is_buy) AS sell_count,
      COUNT(DISTINCT CASE WHEN is_buy = 1 THEN sender END) AS distinct_buyers,
      COUNT(DISTINCT CASE WHEN is_buy = 0 THEN sender END) AS distinct_sellers,
      COUNT(DISTINCT sender) AS distinct_traders
    FROM amm_transactions_view
    WHERE created_at_ts >= :min_ts
    AND amm_process = :amm;
  ]]

  if not stmt then
    error("Failed to prepare SQL statement: " .. db:errmsg())
  end

  print(stmt, err)
  stmt:bind_names({
    min_ts = minTimestamp,
    amm = ammProcessId
  })


  return sqlschema.queryOne(stmt)
end

return stats