local dbUtils = require('db.utils')
local json = require('json')

local debug = {}


function debug.dumpToCSV(msg)
  local tableName = msg.TableName
  local orderBy = msg.OrderBy or true
  local limit = msg.Limit or 100
  local offset = msg.Offset or 0

  local stmt = db:prepare(string.format([[
    SELECT *
    FROM %s
    ORDER BY %s
    LIMIT %d
    OFFSET %d;
  ]], tableName, orderBy, limit, offset))

  -- Get column names from the database schema
  local columnNames = {}
  for i = 1, stmt:columns() do
    table.insert(columnNames, stmt:get_name(i))
  end

  -- Build CSV header
  local csvData = { table.concat(columnNames, ",") .. "\n" }

  -- Write each row to the CSV data
  for row in stmt:rows() do
    local rowData = {}
    for _, columnName in ipairs(columnNames) do
      local value = row[columnName]

      if type(value) == "string" then
        value = '"' .. value:gsub('"', '""') .. '"'
      elseif value == nil then
        value = ""
      end

      table.insert(rowData, tostring(value))
    end

    table.insert(csvData, table.concat(rowData, ",") .. "\n")
  end

  stmt:reset()

  ao.send({
    Target = msg.From,
    Data = table.concat(csvData)
  })
end

function debug.debugTransactions()
  local stmt = db:prepare [[
    SELECT * FROM amm_transactions ORDER BY created_at_ts LIMIT 100;
  ]]
  if not stmt then
    error("Failed to prepare SQL statement: " .. db:errmsg())
  end
  return dbUtils.queryMany(stmt)
end

function debug.handleGetConfig(msg)
  ao.send({
    Target = msg.From,
    ['App-Name'] = 'Dexi',
    ['Response-For'] = 'Get-Config',
    Data = json.encode({
      ['Initialized'] = tostring(Initialized),
      ['Operator'] = OPERATOR,
      ['Quote-Token'] = QUOTE_TOKEN,
      ['Payment-Token'] = {
        ProcessId = PAYMENT_TOKEN_PROCESS,
        Ticker = PAYMENT_TOKEN_TICKER
      },
      ['Supply-Updates-Provider'] = SUPPLY_UPDATES_PROVIDER,
      ['Offchain-Feed-Provider'] = OFFCHAIN_FEED_PROVIDER,
      ['Dispatch-Active'] = DISPATCH_ACTIVE,
      ['Logging-Active'] = LOGGING_ACTIVE
    })
  })
end

return debug
