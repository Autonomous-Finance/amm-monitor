local sqlschema = require('sqlschema')
local topNConsumers = {}

function topNConsumers.dispatchMarketDataForAMM(now, ammProcessId)
  local subscribersStmt = db:prepare([[
      SELECT s.process_id, s.top_n, s.token_set
      FROM top_n_subscriptions s
      JOIN balances b ON s.owner_id = b.owner_id AND b.balance > 0
      WHERE JSON_CONTAINS(s.token_set, :ammProcessId, '$')
      ]])

  if not subscribersStmt then
    error("Err: " .. db:errmsg())
  end
  subscribersStmt:bind_names({
    now = now,
    ammProcessId = ammProcessId
  })

  local json = require("json")

  print('sending market data updates to affected subscribers')

  local marketData = sqlschema.getTopNMarketData() -- TODO send only the necessary data per each subscriber, possibly include in the subscribersStmt via SQL

  local targets = {}                               -- later log subscribers that were updated
  for row in subscribersStmt:nrows() do
    table.insert(targets, row.process_id)

    ao.send({
      ['Target'] = row.process_id,
      ['Action'] = 'TopNMarketData',
      ['Data'] = json.encode(marketData)
    })
  end
  subscribersStmt:finalize()

  print('sent market data updates to ' .. #targets .. ' subscribers')

  local message = {
    ['Target'] = ao.id,
    ['Assignments'] = targets,
    ['Action'] = 'TopNMarketData',
    ['Data'] = json.encode(marketData)
  }
  ao.send(message)

  print('Dispatched market data to all top N subscribers')
end

return topNConsumers
