local sqlite3 = require('lsqlite3')
local dbUtils = {}

function dbUtils.queryMany(stmt)
  local rows = {}
  -- Check if the statement was prepared successfully
  if stmt then
    for row in stmt:nrows() do
      table.insert(rows, row)
    end
    stmt:finalize()
  else
    error("Err: " .. db:errmsg())
  end
  return rows
end

function dbUtils.queryOne(stmt)
  return dbUtils.queryMany(stmt)[1]
end

function dbUtils.rawQuery(query)
  local stmt = db:prepare(query)
  if not stmt then
    error("Err: " .. db:errmsg())
  end
  return dbUtils.queryMany(stmt)
end

function dbUtils.insert(stmt)
  if stmt then
    stmt:step()
    if stmt:finalize() ~= sqlite3.OK then
      error("Failed to finalize SQL statement: " .. db:errmsg())
    end
  else
    error("Failed to prepare SQL statement: " .. db:errmsg())
  end
end

return dbUtils
