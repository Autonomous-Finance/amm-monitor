-- benchmark.lua

local sqlite3 = require("lsqlite3")
local json = require("json")


db = sqlite3.open_memory()
-- Create a table
db:exec[[
CREATE TABLE IF NOT EXISTS benchmark (
  id INTEGER PRIMARY KEY,
  value TEXT
)
]]

function insertRows(numRows)
  local stmt, err = db:prepare("INSERT INTO benchmark (value) VALUES (:v)")
  for i = 1, numRows do
    stmt:bind_names({v = "row_" .. i})
    stmt:step()
    stmt:reset()
  end
  stmt:finalize()
end

Handlers.add(
  "BenchmarkInsert",
  Handlers.utils.hasMatchingTag("Action", "Benchmark-Insert"),
  function(msg)
    local numRows = tonumber(msg.Tags["Num-Rows"] or 1000000)
    print('benchmarking')
    insertRows(numRows)
    print('benchmarking done')
    print(msg.Timestamp)
    ao.send({
      Target = ao.id,
      Action = "Benchmark-Result",
      NumRows = numRows,
      StartTS = msg.Timestamp
    })
  end
)