local dbUtils = require('db.utils')
local json = require('json')
local utils = require(".utils")

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

  local rows = dbUtils.queryMany(stmt)
  if #rows == 0 then
    ao.send({
      Target = msg.From,
      ['Empty-Response'] = 'true',
      Data = ''
    })
    return
  end

  -- Convert rows to CSV format
  local csvData = {}
  local headers = {}
  for k, _ in pairs(rows[1]) do
    table.insert(headers, k)
  end
  table.insert(csvData, table.concat(headers, ","))

  for _, row in ipairs(rows) do
    local values = {}
    for _, header in ipairs(headers) do
      table.insert(values, row[header])
    end
    table.insert(csvData, table.concat(values, ","))
  end

  ao.send({
    Target = msg.From,
    Data = table.concat(csvData, "\n")
  })
end

function debug.getTransactionIds(msg)
  local query = [[
    SELECT id
    FROM amm_transactions
    ORDER BY created_at_ts
    LIMIT 10000;
  ]]
  local r = dbUtils.queryManyWithParams(query, {}, 'debug.getTransactionIds')
  local ids = utils.map(function(x) return x.id end, r)
  msg.reply({
    Data = json.encode(ids)
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
