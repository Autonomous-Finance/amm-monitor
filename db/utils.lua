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

return dbUtils
