local dbUtils = require('db.utils')
local json = require('json')
local responses = require('utils.responses')

local topN = {}

-- ---------------- SQL

local sql = {}


function sql.queryTopNMarketData(quoteToken, limit)
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
  FROM amm_market_cap_view mcv
  LEFT JOIN amm_registry r ON mcv.token_process = r.amm_base_token
  LEFT JOIN token_registry t ON t.token_process = r.amm_base_token
  LEFT JOIN amm_swap_params scv ON scv.amm_process = r.amm_process
  WHERE r.amm_quote_token = :quoteToken
  LIMIT :limit
  ]], orderByClause)

  if not stmt then
    error("Err: " .. db:errmsg())
  end

  stmt:bind_names({
    quoteToken = quoteToken,
    limit = limit
  })
  return dbUtils.queryMany(stmt)
end

function sql.updateTopNTokenSet(specificSubscriber)
  local specificSubscriberClause = specificSubscriber
      and " WHERE process_id = :process_id"
      or ""
  print('subscriberClause ' .. specificSubscriberClause)
  local stmtStr = [[
    WITH top_n_tokens AS (
      SELECT
          rank() over (order by market_cap DESC) market_cap_rank,
          token_process
      FROM amm_market_cap_view
      WHERE market_cap > 0
    )
    UPDATE top_n_subscriptions
    SET token_set = (
        SELECT json_group_array(token_process)
        FROM top_n_tokens
        WHERE market_cap_rank <= top_n_subscriptions.top_n
    ) ]] .. specificSubscriberClause .. [[;
  ]]
  local stmt = db:prepare(stmtStr);
  dbUtils.execute(stmt, "sql.updateTopNTokenSet")
end

--[[
  Get all subscribers that have a top N subscription for the given AMM process ID.
  The subscribers must have a balance greater than 0 for the quote token of the AMM.
  The subscribers must have the AMM process ID in their token set.
  The query returns both the subscriber ID and the top N market data.
]]
--- !!!!TODO refactor!!!!!
function sql.getActiveSubscribersWithInterestInAmm(now, ammProcessId)
  local subscribersStmt = db:prepare([[
    WITH matched_subscribers AS (
      SELECT s.process_id, s.quote_token, s.top_n, s.token_set
      FROM top_n_subscriptions s, json_each(s.token_set)
      JOIN balances b ON s.process_id = b.process_id AND CAST(b.balance AS REAL) > 0
      WHERE json_each.value = :ammProcessId
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
      JOIN amm_swap_params spv ON tl.process_id = spv.amm_process
      GROUP BY tl.process_id
    )

    SELECT
        subs.process_id AS subscriber_id,
        subs.top_n,
        asp.swap_params
    FROM aggregated_swap_params asp
    JOIN matched_subscribers s ON asp.subscriber_id = subs.process_id;
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

function sql.getTopNTokenSet(processId, quoteToken)
  local stmt = db:prepare [[
    SELECT *
    FROM top_n_subscriptions
    WHERE process_id = :process_id AND quote_token = :quote_token;
  ]]
  if not stmt then
    error("Failed to prepare SQL statement for getting top N token set: " .. db:errmsg())
  end
  stmt:bind_names({
    process_id = processId,
    quote_token = quoteToken
  })

  local row = dbUtils.queryOne(stmt)

  return row and row.token_set or nil
end

-- ---------------- EXPORT

--[[
 For subscribersStmt to top N market data, update the token set
 ]]
---@param specificSubscriber string | nil if nil, token set is updated for each subscriber
function topN.updateTopNTokenSet(specificSubscriber)
  sql.updateTopNTokenSet(specificSubscriber)
end

function topN.handleGetTopNTokenSet(msg)
  local processId = msg.Tags['Subscriber-Process-Id']
  local quoteToken = msg.Tags['Quote-Token']
  local tokenSet = topN.getTopNTokenSet(processId, quoteToken)

  if not tokenSet then
    error('No top N token set found for this subscriber and quote token')
  end

  local replyData = tokenSet
  local replyTags = { ['App-Name'] = 'Dexi' }
  responses.sendReply(msg, replyData, replyTags)
end

function topN.handleGetTopNMarketData(msg)
  local quoteToken = msg.Tags['Quote-Token']
  local n = tonumber(msg.Tags['Top-N']) or 100
  if not quoteToken then
    error('Quote-Token is required')
  end

  if quoteToken ~= QUOTE_TOKEN.ProcessId then
    error('Quote-Token must be ' .. QUOTE_TOKEN.ProcessId .. ' ( ' .. QUOTE_TOKEN.Ticker .. ' ) ')
  end

  local replyData = sql.queryTopNMarketData(quoteToken, n)
  local replyTags = { ['App-Name'] = 'Dexi' }
  responses.sendReply(msg, replyData, replyTags)
end

function topN.dispatchMarketDataIncludingAMM(now, ammProcessId)
  if not DISPATCH_ACTIVE then
    if LOGGING_ACTIVE then
      ao.send({
        Target = ao.id,
        Action = 'Log',
        Data = 'Skipping Dispatch for Top N'
      })
    end
    return
  end
  local subscribersAndMD = sql.getActiveSubscribersWithInterestInAmm(now, ammProcessId)

  print('sending market data updates to affected subscribers')

  for _, subscriberWithMD in ipairs(subscribersAndMD) do
    ao.send({
      ['Target'] = subscriberWithMD.process_id,
      ['App-Name'] = 'Dexi',
      ['Action'] = 'TopNMarketData',
      ['Data'] = json.encode(subscriberWithMD.swap_params)
    })
  end

  print('sent top N market data updates to subscribers')
end

return topN
