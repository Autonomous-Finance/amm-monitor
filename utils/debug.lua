local sqlschema = require('dexi-core.sqlschema')

local debug = {}

function debug.dumpToCSV(msg)
  local stmt = db:prepare [[
    SELECT *
    FROM amm_transactions;
  ]]

  local rows = {}
  local row = stmt:step()
  while row do
    table.insert(rows, row)
    row = stmt:step()
  end

  stmt:reset()

  local csvHeader =
  "id,source,block_height,block_id,from,timestamp,is_buy,price,volume,to_token,from_token,from_quantity,to_quantity,fee,amm_process\n"
  local csvData = csvHeader

  for _, row in ipairs(rows) do
    local rowData = string.format("%s,%s,%d,%s,%s,%d,%d,%.8f,%.8f,%s,%s,%.8f,%.8f,%.8f,%s\n",
      row.id, row.source, row.block_height, row.block_id, row["from"], row["timestamp"],
      row.is_buy, row.price, row.volume, row.to_token, row.from_token, row.from_quantity,
      row.to_quantity, row.fee, row.amm_process)
    csvData = csvData .. rowData
  end

  ao.send({
    Target = msg.From,
    Data = csvData
  })
end

function debug.debugTable()
  local stmt = db:prepare [[
    SELECT * FROM amm_transactions ORDER BY created_at_ts LIMIT 100;
  ]]
  if not stmt then
    error("Failed to prepare SQL statement: " .. db:errmsg())
  end
  return sqlschema.queryMany(stmt)
end

return debug
