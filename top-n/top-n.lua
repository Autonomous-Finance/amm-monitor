local sqlschema = require('dexi-core.sqlschema')
local json = require('json')

local topN = {}

-- ---------------- SQL

local sql = {}


function sql.queryTopNMarketData(quoteToken)
  local orderByClause = "market_cap_rank DESC"
  local stmt = db:prepare([[
  SELECT
    mcv.*,
    r.amm_name as amm_name,
    r.amm_process as amm_process,
    r.amm_token0 AS token0,
    r.amm_token1 AS token1,
    scv.reserves_0 AS reserves_0,
    scv.reserves_1 AS reserves_1,
    scv.fee_percentage AS fee_percentage
  FROM market_cap_view mcv
  LEFT JOIN amm_registry r ON mcv.token_process = r.amm_base_token
  LEFT JOIN token_registry t ON t.token_process = r.amm_base_token
  LEFT JOIN amm_swap_params_view scv ON scv.amm_process = r.amm_process
  WHERE r.amm_quote_token = :quoteToken
  LIMIT 100
  ]], orderByClause)

  if not stmt then
    error("Err: " .. db:errmsg())
  end

  stmt:bind_names({
    quoteToken = quoteToken,
  })
  return sqlschema.queryMany(stmt)
end

function sql.updateTopNTokenSet(specificSubscriber)
  local specificSubscriberClause = specificSubscriber
      and " AND process_id = :process_id"
      or ""
  local stmt = db:prepare [[
    UPDATE top_n_subscriptions
    SET token_set = (
      SELECT json_group_array(token_process)
      FROM (
        SELECT token_process
        FROM market_cap_view
        LIMIT top_n
      )
    )
    WHERE EXISTS (
      SELECT 1
      FROM market_cap_view
      LIMIT top_n
    ) ]] .. specificSubscriberClause .. [[;
  ]]

  if not stmt then
    error("Failed to prepare SQL statement for updating top N token sets: " .. db:errmsg())
  end

  local result, err = stmt:step()
  stmt:finalize()
  if err then
    error("Err: " .. db:errmsg())
  end
end

--[[
  Get all subscribers that have a top N subscription for the given AMM process ID.
  The subscribers must have a balance greater than 0 for the quote token of the AMM.
  The subscribers must have the AMM process ID in their token set.
  The query returns both the subscriber ID and the top N market data.
]]
function sql.getSubscribersWithMarketDataForAmm(now, ammProcessId)
  local subscribersStmt = db:prepare([[
    WITH matched_subscribers AS (
      SELECT s.process_id, s.quote_token, s.top_n, s.token_set
      FROM top_n_subscriptions s, json_each(s.token_set)
      WHERE json_each.value = :ammProcessId
      JOIN balances b ON s.owner_id = b.owner_id AND b.balance > 0
    ),
    token_list AS (
      SELECT process_id, json_each.value AS token
      FROM matched_subscribers ms, json_each(ms.token_set)
    ),
    aggregated_swap_params AS (
      SELECT
          tl.id AS subscriber_id,
          json_group_array(json_object('amm_process', spv.amm_process, 'token_0', spv.token_0, 'reserves_0', spv.reserves_0, 'token_1', spv.token_1, 'reserves_1', spv.reserves_1, 'fee_percentage', spv.fee_percentage)) AS swap_params
      FROM token_list tl
      JOIN swap_params_view spv ON tl.process_id = spv.amm_process
      GROUP BY tl.process_id
    )

    SELECT
        subs.process_id AS subscriber_id,
        subs.top_n,
        asp.swap_params
    FROM aggregated_swap_params asp
    JOIN subscribers s ON asp.subscriber_id = subs.process_id;
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

--[[
 For subscribersStmt to top N market data, update the token set
 ]]
---@param specificSubscriber string | nil if nil, token set is updated for each subscriber
function topN.updateTopNTokenSet(specificSubscriber)
  sql.updateTopNTokenSet(specificSubscriber)
end

function topN.getTopNMarketData(msg)
  local quoteToken = msg.Tags['Quote-Token']
  if not quoteToken then
    error('Quote-Token is required')
  end

  if quoteToken ~= BARK_TOKEN_PROCESS then
    error('Quote-Token must be BARK')
  end

  ao.send({
    ['App-Name'] = 'Dexi',
    ['Payload'] = 'Top-N-Market-Data',
    Target = msg.From,
    Data = json.encode(sql.getSubscribersWithMarketDataForAmm(quoteToken))
  })
end

function topN.dispatchMarketDataIncludingAMM(now, ammProcessId)
  local subscribersAndMD = sql.getSubscribersWithMarketDataForAmm(now, ammProcessId)

  print('sending market data updates to affected subscribers')

  for _, subscriberWithMD in ipairs(subscribersAndMD) do
    ao.send({
      ['Target'] = subscriberWithMD.process_id,
      ['Action'] = 'TopNMarketData',
      ['Data'] = json.encode(subscriberWithMD.swap_params)
    })
  end

  print('sent top N market data updates to subscribers')
end

return topN