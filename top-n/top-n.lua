local sqlschema = require('dexi-core.sqlschema')
local json = require('json')

local topN = {}

-- ---------------- SQL

local function queryTopNMarketData(token0)
  local orderByClause = "market_cap_rank DESC"
  local stmt = db:prepare([[
  WITH current_prices AS (
    SELECT
      amm_process,
      (SELECT price FROM amm_transactions_view WHERE amm_process = r.amm_process ORDER BY created_at_ts DESC LIMIT 1) AS current_price
    FROM amm_registry r
  )
  SELECT
    rank() OVER (ORDER BY t.total_supply * current_price DESC) AS market_cap_rank,
    r.amm_name as amm_name,
    r.amm_process as pool,
    r.amm_token1 AS token,
    t.token_name AS ticker,
    t.denominator as denomination,
    c.current_price AS current_price,
    scv.reserves_0 AS reserves_0,
    scv.reserves_1 AS reserves_1,
    scv.fee_percentage AS fee_percentage
  FROM amm_registry r
  LEFT JOIN current_prices c ON c.amm_process = r.amm_process
  LEFT JOIN token_registry t ON t.token_process = r.amm_token1
  LEFT JOIN amm_swap_params_view scv ON scv.amm_process = r.amm_process
  WHERE r.amm_token0 = :token0
  LIMIT 100
  ]], orderByClause)

  if not stmt then
    error("Err: " .. db:errmsg())
  end

  stmt:bind_names({
    token0 = token0,
  })
  return sqlschema.queryMany(stmt)
end

local function getSubscribersToAmm(now, ammProcessId)
  local subscribersStmt = db:prepare([[
      SELECT s.process_id, s.quote_token, s.top_n, s.token_set
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

  local subscribers = {}
  for row in subscribersStmt:nrows() do
    table.insert(subscribers, row.process_id)
  end
  subscribersStmt:finalize()

  return subscribers
end

-- ---------------- EXPORT

function topN.getTopNMarketData(msg)
  local quoteToken = msg.Tags['Quote-Token']
  if not quoteToken then
    error('Quote-Token is required')
  end
  if not sqlschema.isQuoteTokenAvailable(quoteToken) then
    error('Quote-Token not available: ' .. quoteToken)
  end
  ao.send({
    ['App-Name'] = 'Dexi',
    ['Payload'] = 'Top-N-Market-Data',
    Target = msg.From,
    Data = json.encode(queryTopNMarketData(quoteToken))
  })
end

function topN.dispatchMarketDataIncludingAMM(now, ammProcessId)
  local subscribers = getSubscribersToAmm(now, ammProcessId)

  local json = require("json")

  print('sending market data updates to affected subscribers')

  -- TODO regarding market data, send only the necessary data per each subscriber, possibly include in the subscribersStmt via SQL

  local marketDataPerQuoteToken = {} -- cache for the loop

  for _, subscriber in ipairs(subscribers) do
    local quoteToken = subscriber.quote_token
    marketDataPerQuoteToken[quoteToken] = marketDataPerQuoteToken[quoteToken] or
        queryTopNMarketData(quoteToken)
    local marketData = marketDataPerQuoteToken[quoteToken]

    ao.send({
      ['Target'] = subscriber.process_id,
      ['Action'] = 'TopNMarketData',
      ['Data'] = json.encode(marketData)
    })
  end

  print('sent top N market data updates to subscribers')
end

return topN
