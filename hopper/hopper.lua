local hopper = {}
local dbUtils = require("db.utils")
local json = require("json")

-- TODO use bint!
local function get_price(pool, from_token)
  -- If from_token is token0, calculate price as token1/token0
  if pool.token0 == from_token then
    return pool.reserve1 / pool.reserve0
    -- If from_token is token1, calculate price as token0/token1
  elseif pool.token1 == from_token then
    return pool.reserve0 / pool.reserve1
  else
    return nil
  end
end

function hopper.fetch_oracle_pools()
  local pools = {}

  for row in db:nrows("SELECT process_id, price FROM oracle_prices") do
    table.insert(pools, {
      token0 = row.process_id,
      token1 = 'USD',
      reserve0 = 1,
      reserve1 = row.price
    })
  end

  return pools
end

function hopper.fetch_pools()
  local stmt = db:prepare([[
  SELECT
    amm_token0 as token0,
    amm_token1 as token1,
    CAST(reserves_0 AS NUMERIC) as reserve0,
    CAST(reserves_1 AS NUMERIC) as reserve1
  FROM amm_swap_params
  LEFT JOIN amm_registry USING (amm_process)
  WHERE coalesce(token0, '') != '' AND coalesce(token1, '') != ''
  ]])

  if not stmt then
    error("Err: " .. db:errmsg())
  end

  return dbUtils.queryMany(stmt)
end

local function build_graph(pools)
  local graph = {}
  for _, pool in ipairs(pools) do
    if not graph[pool.token0] then
      graph[pool.token0] = {}
    end
    if not graph[pool.token1] then
      graph[pool.token1] = {}
    end
    table.insert(graph[pool.token0], { token = pool.token1, pool = pool })
    table.insert(graph[pool.token1], { token = pool.token0, pool = pool })
  end
  return graph
end

local function dijkstra(graph, start_token, end_token)
  local distances = {}
  local previous = {}
  local queue = {}

  for token, _ in pairs(graph) do
    distances[token] = math.huge
    previous[token] = nil
    table.insert(queue, token)
  end
  distances[start_token] = 0

  while #queue > 0 do
    table.sort(queue, function(a, b) return distances[a] < distances[b] end)
    local current_token = table.remove(queue, 1)

    if current_token == end_token then
      break
    end

    for _, neighbor in ipairs(graph[current_token] or {}) do
      local alt = distances[current_token] + 1 -- All edges have weight 1
      if alt < distances[neighbor.token] then
        distances[neighbor.token] = alt
        previous[neighbor.token] = { token = current_token, pool = neighbor.pool }
      end
    end
  end

  -- If there's no path to the end_token, return nil
  if distances[end_token] == math.huge then
    return nil
  end

  local path = {}
  local u = end_token
  while previous[u] do
    table.insert(path, 1, previous[u])
    u = previous[u].token
  end
  return path
end

function hopper.getPrice(baseToken, quoteToken)
  local oracle_pools = hopper.fetch_oracle_pools()
  local pools = hopper.fetch_pools()
  for _, pool in ipairs(oracle_pools) do
    print('pool', json.encode(pool))
    table.insert(pools, pool)
  end

  local graph = build_graph(pools)
  local path = dijkstra(graph, baseToken, quoteToken)

  if not path then
    return nil
  end

  print('path length', #path)
  for _, step in ipairs(path) do
    print('step:', 'pool', step.pool.token0 .. '...' .. step.pool.token1, "----", 'token', step.token)
  end

  local best_price = 1

  for _, step in ipairs(path) do
    local price = get_price(step.pool, step.token)
    best_price = best_price * price
  end

  return best_price
end

function hopper.getPriceForTokenHandler(msg)
  assert(msg.Tags["Base-Token-Process"], "Base-Token-Process is required")
  assert(msg.Tags["Quote-Token-Process"], "Quote-Token-Process is required")

  local baseToken = msg.Tags["Base-Token-Process"]
  local quoteToken = msg.Tags["Quote-Token-Process"]
  local best_price = hopper.getPrice(baseToken, quoteToken)

  if ao ~= nil then
    if best_price ~= nil then
      ao.send({
        Target = msg.From,
        Action = "Hopper-Price-Update",
        ['Base-Token-Process'] = tostring(baseToken),
        ['Quote-Token-Process'] = tostring(quoteToken),
        ['Price'] = tostring(best_price)
      })
    else
      ao.send({
        Target = msg.From,
        Action = "Hopper-Price-Failure",
        ['Base-Token-Process'] = tostring(baseToken),
        ['Quote-Token-Process'] = tostring(quoteToken),
      })
    end
  else
    return { baseToken = baseToken, quoteToken = quoteToken, price = best_price }
  end
end

return hopper
