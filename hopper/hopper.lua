local sqlite3 = require("lsqlite3")
local hopper = {}

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

local function fetch_oracle_pools()
  local pools = {}

  for row in db:nrows("SELECT ticker, price FROM oracle_prices") do
    table.insert(pools, {
      token0 = row.ticker,
      token1 = 'USD',
      reserve0 = 1,
      reserve1 = row.price
    })
  end

  return pools
end

local function fetch_pools()
  local pools = {}

  for row in db:nrows("SELECT token0, token1, reserve0, reserve1 FROM pools") do
    table.insert(pools, {
      token0 = row.token0,
      token1 = row.token1,
      reserve0 = row.reserve0,
      reserve1 = row.reserve1
    })
  end

  return pools
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

function hopper.getPriceForToken(msg)
  assert(msg.Tags["Base-Token-Process"], "Base-Token-Process is required")
  assert(msg.Tags["Quote-Token-Process"], "Quote-Token-Process is required")

  local baseToken = msg.Tags["Base-Token-Process"]
  local quoteToken = msg.Tags["Quote-Token-Process"]
  -- print('baseToken', baseToken)
  -- print('quoteToken', quoteToken)

  local oracle_pools = fetch_oracle_pools()
  local pools = fetch_pools()
  for _, pool in ipairs(oracle_pools) do
    table.insert(pools, pool)
  end

  -- print('pools', pools[1].token0)
  -- print('pools', pools[1].token1)
  -- print('pools', pools[1].reserve0)
  -- print('pools', pools[1].reserve1)

  -- Calculate the best price
  local graph = build_graph(pools)
  -- print('graph', graph[pools[1].token0][1].token)
  -- print('graph', graph[pools[1].token1][1].token)

  local path = dijkstra(graph, baseToken, quoteToken)

  if not path then
    return { baseToken = baseToken, quoteToken = quoteToken, price = nil }
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

  return { baseToken = baseToken, quoteToken = quoteToken, price = best_price }
end

return hopper