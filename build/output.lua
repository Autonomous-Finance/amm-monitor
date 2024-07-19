do
  local _ENV = _ENV
  package.preload["candles"] = function(...)
    local arg = _G.arg;
    local intervals = require('dexis.intervals')
    local sqlschema = require('dexi-core.sqlschema')
    local candles = {}

    function candles.generateCandlesForXDaysInIntervalY(xDays, yInterval, endTime, ammProcessId)
      local intervalSeconds = intervals.IntervalSecondsMap[yInterval]
      if not intervalSeconds then
        error("Invalid interval specified")
        return
      end

      -- Determine the GROUP BY clause based on the interval
      local groupByClause
      if yInterval == '15m' then
        groupByClause = "strftime('%Y-%m-%d %H:%M', \"created_at_ts\" / 900 * 900, 'unixepoch')"
      elseif yInterval == '1h' then
        groupByClause = "strftime('%Y-%m-%d %H', \"created_at_ts\", 'unixepoch')"
      elseif yInterval == '4h' then
        groupByClause = "strftime('%Y-%m-%d %H', \"created_at_ts\" / 14400 * 14400, 'unixepoch')"
      elseif yInterval == '1d' then
        groupByClause = "strftime('%Y-%m-%d', \"created_at_ts\", 'unixepoch')"
      else
        error("Unsupported interval for grouping")
        return
      end

      local stmt = db:prepare(string.format([[
    SELECT
      %s AS candle_time,
      MIN(created_at_ts) AS start_timestamp,
      MAX(created_at_ts) AS end_timestamp,
      (SELECT price FROM amm_transactions WHERE created_at_ts = (SELECT MIN(created_at_ts) FROM amm_transactions_view WHERE created_at_ts >= :start_time AND created_at_ts < :end_time AND amm_process = :amm_process)) AS open,
      MAX(price) AS high,
      MIN(price) AS low,
      (SELECT price FROM amm_transactions WHERE created_at_ts = (SELECT MAX(created_at_ts) FROM amm_transactions_view WHERE created_at_ts >= :start_time AND created_at_ts < :end_time AND amm_process = :amm_process)) AS close,
      SUM(volume) / POWER(10, quote_denominator) AS volume
    FROM
      amm_transactions_view AS t1
    WHERE created_at_ts >= :start_time AND created_at_ts < :end_time AND amm_process = :amm_process
    GROUP BY
      1
    ORDER BY
      candle_time ASC
  ]], groupByClause))

      local startTime = endTime - (xDays * 24 * 3600)

      stmt:bind_names({
        start_time = startTime,
        end_time = endTime,
        amm_process = ammProcessId
      })

      local candles = sqlschema.queryMany(stmt)

      for i = 2, #candles do
        candles[i].open = candles[i - 1].close
      end

      if #candles > 0 then
        candles[1].open = 0
      end

      return candles
    end

    return candles
  end
end

do
  local _ENV = _ENV
  package.preload["indicators"] = function(...)
    local arg = _G.arg;
    local sqlschema = require('dexi-core.sqlschema')
    local indicators = {}

    function indicators.getDailyStats(ammProcessId, startDate, endDate)
      local stmt = db:prepare([[
    SELECT
      date(created_at_ts, 'unixepoch') AS date,
      MIN(price) AS low,
      MAX(price) AS high,
      (SELECT price FROM amm_transactions_view sub
       WHERE sub.amm_process = :amm_process AND date(sub.created_at_ts, 'unixepoch') = date(main.created_at_ts, 'unixepoch')
       ORDER BY sub.created_at_ts LIMIT 1) AS open,
      (SELECT price FROM amm_transactions_view sub
       WHERE sub.amm_process = :amm_process AND date(sub.created_at_ts, 'unixepoch') = date(main.created_at_ts, 'unixepoch')
       ORDER BY sub.created_at_ts DESC LIMIT 1) AS close,
      SUM(volume) AS volume
    FROM amm_transactions_view main
    WHERE amm_process = :amm_process AND date(created_at_ts, 'unixepoch') BETWEEN :start_date AND :end_date
    GROUP BY date(created_at_ts, 'unixepoch')
  ]])
      stmt:bind_names({
        amm_process = ammProcessId,
        start_date = startDate,
        end_date = endDate
      })
      return sqlschema.queryMany(stmt)
    end

    function indicators.fillMissingDates(dailyStats, startDate, endDate)
      local filledDailyStats = {}
      local currentTimestamp = os.time({
        year = startDate:sub(1, 4),
        month = startDate:sub(6, 7),
        day = startDate:sub(9,
          10)
      })
      local endTimestamp = os.time({ year = endDate:sub(1, 4), month = endDate:sub(6, 7), day = endDate:sub(9, 10) })
      local lastClose = 0

      while currentTimestamp <= endTimestamp do
        local currentDate = os.date("%Y-%m-%d", currentTimestamp)
        local found = false
        for _, row in ipairs(dailyStats) do
          if row.date == currentDate then
            filledDailyStats[#filledDailyStats + 1] = row
            lastClose = row.close
            found = true
            break
          end
        end

        if not found then
          filledDailyStats[#filledDailyStats + 1] = {
            date = currentDate,
            low = lastClose,
            high = lastClose,
            open = lastClose,
            close = lastClose,
            volume = 0
          }
        end

        currentTimestamp = currentTimestamp + 24 * 60 * 60 -- Add one day in seconds
      end

      return filledDailyStats
    end

    function indicators.calculateSMAs(dailyStats)
      local smas = {}
      for i = 1, #dailyStats do
        local sma10 = 0
        local sma20 = 0
        local sma50 = 0
        local sma100 = 0
        local sma150 = 0
        local sma200 = 0

        for j = math.max(1, i - 9), i do
          sma10 = sma10 + dailyStats[j].close
        end
        sma10 = sma10 / math.min(10, i)

        for j = math.max(1, i - 19), i do
          sma20 = sma20 + dailyStats[j].close
        end
        sma20 = sma20 / math.min(20, i)

        for j = math.max(1, i - 49), i do
          sma50 = sma50 + dailyStats[j].close
        end
        sma50 = sma50 / math.min(50, i)

        for j = math.max(1, i - 99), i do
          sma100 = sma100 + dailyStats[j].close
        end
        sma100 = sma100 / math.min(100, i)

        for j = math.max(1, i - 149), i do
          sma150 = sma150 + dailyStats[j].close
        end
        sma150 = sma150 / math.min(150, i)

        for j = math.max(1, i - 199), i do
          sma200 = sma200 + dailyStats[j].close
        end
        sma200 = sma200 / math.min(200, i)

        smas[i] = {
          sma10 = sma10,
          sma20 = sma20,
          sma50 = sma50,
          sma100 = sma100,
          sma150 = sma150,
          sma200 = sma200
        }
      end

      return smas
    end

    function indicators.calculateEMAs(dailyStats)
      local ema12 = {}
      local ema26 = {}

      for i = 1, #dailyStats do
        if i == 1 then
          ema12[i] = dailyStats[i].close
          ema26[i] = dailyStats[i].close
        else
          ema12[i] = (dailyStats[i].close - ema12[i - 1]) * 2 / 13 + ema12[i - 1]
          ema26[i] = (dailyStats[i].close - ema26[i - 1]) * 2 / 27 + ema26[i - 1]
        end
      end

      return ema12, ema26
    end

    function indicators.calculateMACD(ema12, ema26)
      local macd = {}
      local signalLine = {}
      local histogram = {}

      for i = 1, #ema12 do
        macd[i] = ema12[i] - ema26[i]

        if i == 1 then
          signalLine[i] = macd[i]
        else
          signalLine[i] = (macd[i] - signalLine[i - 1]) * 2 / 10 + signalLine[i - 1]
        end

        histogram[i] = macd[i] - signalLine[i]
      end

      return macd, signalLine, histogram
    end

    function indicators.calculateBollingerBands(dailyStats, smas)
      local upperBand = {}
      local lowerBand = {}

      for i = 1, #dailyStats do
        local sum = 0
        local count = 0

        for j = math.max(1, i - 19), i do
          sum = sum + (dailyStats[j].close - smas[i].sma20) ^ 2
          count = count + 1
        end

        local stdDev = math.sqrt(sum / count)
        upperBand[i] = smas[i].sma20 + 2 * stdDev
        lowerBand[i] = smas[i].sma20 - 2 * stdDev
      end

      return upperBand, lowerBand
    end

    function indicators.getIndicators(ammProcessId, startTimestamp, endTimestamp)
      local endDate = os.date("!%Y-%m-%d", endTimestamp)
      local startDate = os.date("!%Y-%m-%d", startTimestamp)

      local dailyStats = indicators.getDailyStats(ammProcessId, startDate, endDate)
      local filledDailyStats = indicators.fillMissingDates(dailyStats, startDate, endDate)
      local smas = indicators.calculateSMAs(filledDailyStats)
      -- local ema12, ema26 = indicators.calculateEMAs(filledDailyStats)
      -- local macd, signalLine, histogram = indicators.calculateMACD(ema12, ema26)
      -- local upperBand, lowerBand = indicators.calculateBollingerBands(filledDailyStats, smas)

      local result = {}
      for i = 1, #filledDailyStats do
        result[i] = {
          date = filledDailyStats[i].date,
          open = filledDailyStats[i].open,
          high = filledDailyStats[i].high,
          low = filledDailyStats[i].low,
          close = filledDailyStats[i].close,
          volume = filledDailyStats[i].volume,
          -- ema12 = ema12[i],
          -- ema26 = ema26[i],
          -- macd = macd[i],
          -- signalLine = signalLine[i],
          -- histogram = histogram[i],
          -- upperBand = upperBand[i],
          -- lowerBand = lowerBand[i],
          sma10 = smas[i].sma10,
          sma20 = smas[i].sma20,
          sma50 = smas[i].sma50,
          sma100 = smas[i].sma100,
          sma150 = smas[i].sma150,
          sma200 = smas[i].sma200
        }
      end

      return result
    end

    function indicators.dispatchIndicatorsMessage(ammProcessId, startTimestamp, endTimestamp)
      local subscribersStmt = db:prepare([[
      SELECT s.process_id
      FROM subscriptions s
      JOIN balances b ON s.owner_id = b.owner_id AND b.balance > 0
      WHERE amm_process_id = :amm_process_id
    ]])
      if not subscribersStmt then
        error("Err: " .. db:errmsg())
      end
      subscribersStmt:bind_names({ amm_process_id = ammProcessId })

      local processes = {}
      for row in subscribersStmt:nrows() do
        table.insert(processes, row.process_id)
      end
      subscribersStmt:finalize()

      local indicatorsResults = indicators.getIndicators(ammProcessId, startTimestamp, endTimestamp)

      local json = require("json")

      print('sending indicators to ' .. #processes .. ' processes')

      local message = {
        ['Target'] = ao.id,
        ['Assignments'] = processes,
        ['Action'] = 'IndicatorsUpdate',
        ['AMM'] = ammProcessId,
        ['Data'] = json.encode(indicatorsResults)
      }
      ao.send(message)

      -- for _, processId in ipairs(processes) do
      --   message.Target = processId
      -- ao.send(message)
      -- end
    end

    function indicators.dispatchIndicatorsForAllAMMs(now)
      local ammsStmt = db:prepare([[
      SELECT amm_process, amm_discovered_at_ts
      FROM amm_registry
    ]])
      if not ammsStmt then
        error("Err: " .. db:errmsg())
      end

      local oneWeekAgo = now - (7 * 24 * 60 * 60)

      for row in ammsStmt:nrows() do
        local ammProcessId = row.amm_process
        local discoveredAt = row.amm_discovered_at_ts

        local startTimestamp = math.max(discoveredAt, oneWeekAgo)
        indicators.dispatchIndicatorsMessage(ammProcessId, startTimestamp, now)
      end
      ammsStmt:finalize()
      print('Dispatched indicators for all AMMs')
    end

    return indicators
  end
end

do
  local _ENV = _ENV
  package.preload["intervals"] = function(...)
    local arg = _G.arg;
    local intervals = {}


    intervals.IntervalSecondsMap = {
      ["5m"] = 300,
      ["15m"] = 900,
      ["1h"] = 3600,
      ["4h"] = 14400,
      ["12h"] = 57600,
      ["6h"] = 21600,
      ["1d"] = 86400,
      ["7d"] = 86400 * 7,
      ["1M"] = 2592000
    }

    function intervals.getIntervalStart(timestamp, interval)
      timestamp = math.floor(timestamp) -- Ensure timestamp is an integer
      local date = os.date("!*t", timestamp)

      if interval == "1h" then
        date.min = 0
        date.sec = 0
      elseif interval == "15m" then
        date.min = 0
        date.sec = 0
      elseif interval == "4h" then
        date.min = 0
        date.sec = 0
        date.hour = date.hour - (date.hour % 4)
      elseif interval == "1d" then
        date.hour = 0
        date.min = 0
        date.sec = 0
      elseif interval == "1M" then
        date.day = 1
        date.hour = 0
        date.min = 0
        date.sec = 0
      else
        error("Unsupported interval: " .. interval)
      end

      return os.time(date)
    end

    return intervals
  end
end

do
  local _ENV = _ENV
  package.preload["schemas"] = function(...)
    local arg = _G.arg;
    local v = require("validation")

    local schemas = {}

    schemas.inputMessageSchema = v.is_table({
      Id = v.is_string(),
      ['Block-Height'] = v.is_number(),
      ['Block-Id'] = v.optional(v.is_string()),
      From = v.is_string(),
      Timestamp = v.optional(v.is_number()),
      Tags = v.is_table({
        ['To-Token'] = v.is_string(),
        ['From-Token'] = v.is_string(),
        ['From-Quantity'] = v.is_string(),
        ['To-Quantity'] = v.is_string(),
        ['Fee'] = v.is_string()
      }, true)
    }, true)

    return schemas;
  end
end

do
  local _ENV = _ENV
  package.preload["sqlschema"] = function(...)
    local arg = _G.arg;
    local sqlschema = {}

    sqlschema.create_table = [[
CREATE TABLE IF NOT EXISTS amm_transactions (
    id TEXT PRIMARY KEY,
    source TEXT NOT NULL CHECK (source IN ('gateway', 'message')),
    block_height INTEGER NOT NULL,
    block_id TEXT,
    sender TEXT NOT NULL,
    created_at_ts INTEGER,
    to_token TEXT NOT NULL,
    from_token TEXT NOT NULL,
    from_quantity INT NOT NULL,
    to_quantity INT NOT NULL,
    fee INT INT NULL,
    amm_process TEXT NOT NULL,
    reserves_0 TEXT NOT NULL DEFAULT "",
    reserves_1 TEXT NOT NULL DEFAULT "",
    fee_percentage TEXT NOT NULL DEFAULT ""
);
]]

    sqlschema.should_alter_table_add_reserves = function()
      local stmt = db:prepare("PRAGMA table_info(amm_transactions);")
      if not stmt then
        error("Err: " .. db:errmsg())
      end
      local hasReserves0 = false
      local hasReserves1 = false
      local hasFeePercentage = false
      for row in stmt:nrows() do
        if row.name == "reserves_0" then
          hasReserves0 = true
        elseif row.name == "reserves_1" then
          hasReserves1 = true
        elseif row.name == "fee_percentage" then
          hasFeePercentage = true
        end
      end
      stmt:reset()
      return not hasReserves0 or not hasReserves1 or not hasFeePercentage
    end

    sqlschema.alter_table_add_reserves = [[
ALTER TABLE amm_transactions
  ADD COLUMN reserves_0 TEXT NOT NULL DEFAULT "",
  ADD COLUMN reserves_1 TEXT NOT NULL DEFAULT "",
  ADD COLUMN fee_percentage TEXT NOT NULL DEFAULT "";
]]

    sqlschema.create_amm_registry_table = [[
CREATE TABLE IF NOT EXISTS amm_registry (
    amm_process TEXT PRIMARY KEY,
    amm_name TEXT NOT NULL,
    amm_token0 TEXT NOT NULL,
    amm_token1 TEXT NOT NULL,
    amm_discovered_at_ts INTEGER
);
]]

    sqlschema.create_token_registry_table = [[
CREATE TABLE IF NOT EXISTS token_registry (
    token_process TEXT PRIMARY KEY,
    token_name TEXT NOT NULL,
    denominator INT NOT NULL,
    total_supply INT NOT NULL,
    fixed_supply BOOL NOT NULL,
    token_updated_at_ts INTEGER,
    token_discovered_at_ts INTEGER
);
]]

    sqlschema.create_balances_table = [[
CREATE TABLE IF NOT EXISTS balances (
    owner_id TEXT PRIMARY KEY,
    token_id TEXT NOT NULL,
    balance INT NOT NULL
);
]]

    sqlschema.create_subscriptions_table = [[
CREATE TABLE IF NOT EXISTS subscriptions (
    process_id TEXT PRIMARY KEY,
    owner_id TEXT NOT NULL,
    amm_process_id TEXT NOT NULL
);
]]

    sqlschema.create_top_n_subscriptions_table = [[
CREATE TABLE IF NOT EXISTS top_n_subscriptions (
    process_id TEXT PRIMARY KEY,
    owner_id TEXT NOT NULL,
    quote_token_process_id TEXT NOT NULL,
    last_push_at INTEGER DEFAULT 0,
    push_interval INTEGER DEFAULT 0
);
]]

    sqlschema.create_transactions_view = [[
CREATE VIEW amm_transactions_view AS
SELECT
  id,
  source,
  block_height,
  block_id,
  sender,
  created_at_ts,
  to_token,
  from_token,
  from_quantity,
  to_quantity,
  fee,
  amm_process,
  reserves_0,
  reserves_1,
  fee_percentage,
  CASE WHEN to_token = amm_token1 THEN 1 ELSE 0 END AS is_buy,
  ROUND(CASE
    WHEN from_quantity > 0 AND to_quantity > 0 THEN
      CASE
        WHEN to_token = amm_token1 THEN
          (from_quantity * 1.0 / to_quantity) * POWER(10, ABS(t0.denominator - tq.denominator))
        ELSE
          (to_quantity * 1.0 / from_quantity) * POWER(10, ABS(t0.denominator - tq.denominator))
      END
    ELSE NULL
  END, 12) AS price,
  CASE
    WHEN to_token = amm_token1 THEN from_quantity
    ELSE to_quantity
  END AS volume,
  POWER(10, ABS(t0.denominator - tq.denominator)) AS denominator_conversion,
  t0.denominator AS quote_denominator,
  tq.denominator AS base_denominator
FROM amm_transactions
LEFT JOIN amm_registry USING (amm_process)
LEFT JOIN token_registry t0 ON t0.token_process = amm_token0
LEFT JOIN token_registry tq ON tq.token_process = amm_token1
]]

    function sqlschema.createTableIfNotExists(db)
      db:exec(sqlschema.create_table)

      if sqlschema.should_alter_table_add_reserves() then
        db:exec(sqlschema.alter_table_add_reserves)
      end

      db:exec("DROP VIEW IF EXISTS amm_transactions_view;")
      print("Err: " .. db:errmsg())

      db:exec(sqlschema.create_amm_registry_table)
      print("Err: " .. db:errmsg())

      db:exec(sqlschema.create_transactions_view)
      print("Err: " .. db:errmsg())

      db:exec(sqlschema.create_balances_table)
      print("Err: " .. db:errmsg())

      db:exec(sqlschema.create_subscriptions_table)
      print("Err: " .. db:errmsg())

      db:exec(sqlschema.create_top_n_subscriptions_table)
      print("Err: " .. db:errmsg())

      db:exec(sqlschema.create_token_registry_table)
      print("Err: " .. db:errmsg())

      sqlschema.updateAMMs()
      sqlschema.updateTokens()
    end

    function sqlschema.dropAndRecreateTableIfOwner(db)
      db:exec("DROP TABLE IF EXISTS amm_transactions;")
      sqlschema.createTableIfNotExists()
    end

    function sqlschema.queryMany(stmt)
      local rows = {}
      for row in stmt:nrows() do
        table.insert(rows, row)
      end
      stmt:reset()
      return rows
    end

    function sqlschema.queryOne(stmt)
      return sqlschema.queryMany(stmt)[1]
    end

    function sqlschema.rawQuery(query)
      local stmt = db:prepare(query)
      if not stmt then
        error("Err: " .. db:errmsg())
      end
      return sqlschema.queryMany(stmt)
    end

    function sqlschema.registerAMM(name, processId, token0, token1, discoveredAt)
      print({
        "process", processId,
        "name", name,
        "token0", token0,
        "token1", token1
      })
      local stmt = db:prepare [[
  INSERT OR REPLACE INTO amm_registry (amm_process, amm_name, amm_token0, amm_token1, amm_discovered_at_ts)
  VALUES
    (:process, :amm_name, :token0, :token1, :discovered_at)
  ]]
      if not stmt then
        error("Err: " .. db:errmsg())
      end
      stmt:bind_names({
        process = processId,
        amm_name = name,
        token0 = token0,
        token1 = token1,
        discovered_at = discoveredAt
      })
      stmt:step()
      print("Err: " .. db:errmsg())
      stmt:reset()
    end

    function sqlschema.getRegisteredAMMs()
      return sqlschema.rawQuery("SELECT * FROM amm_registry")
    end

    function sqlschema.getQuoteTokens()
      return sqlschema.rawQuery("SELECT DISTINCT amm_token0 FROM amm_registry")
    end

    function sqlschema.isQuoteTokenAvailable(token0)
      local stmt = db:prepare("SELECT COUNT(*) FROM amm_registry WHERE amm_token0 = :token0")
      if not stmt then
        error("Err: " .. db:errmsg())
      end
      stmt:bind_names({ token0 = token0 })
      local result = sqlschema.queryOne(stmt)
      return result["COUNT(*)"] > 0
    end

    function sqlschema.getOverview(now, orderBy)
      local orderByClause = "amm_discovered_at_ts DESC"

      if orderBy == "volume" then
        orderByClause = "volume DESC"
      elseif orderBy == "transactions" then
        orderByClause = "transactions DESC"
      elseif orderBy == "market_cap" then
        orderByClause = "market_cap DESC"
      end

      local stmt = db:prepare(string.format([[
  WITH stats AS (
    SELECT
      amm_process,
      COUNT(*) AS transactions,
      SUM(volume) AS volume
    FROM amm_transactions_view
    WHERE created_at_ts >= :now - 86400
    GROUP BY 1
  ), current_prices AS (
    SELECT
      amm_process,
      (SELECT price FROM amm_transactions_view WHERE amm_process = r.amm_process ORDER BY created_at_ts DESC LIMIT 1) AS current_price
    FROM amm_registry r
  )
  SELECT
    rank() OVER (ORDER BY t.total_supply * current_price DESC) AS market_cap_rank,
    r.amm_name as amm_name,
    r.amm_process as amm_process,
    r.amm_token0 AS token0,
    r.amm_token1 AS token1,
    transactions,
    volume,
    t.token_name AS token_name,
    t.total_supply AS total_supply,
    t.fixed_supply AS fixed_supply,
    t.total_supply * current_price AS market_cap,
    (SELECT price FROM amm_transactions_view WHERE amm_process = r.amm_process ORDER BY created_at_ts DESC LIMIT 1) AS current_price,
    (SELECT price FROM amm_transactions_view WHERE amm_process = r.amm_process AND created_at_ts <= :now - 300 ORDER BY created_at_ts DESC LIMIT 1) AS price_5m_ago,
    (SELECT price FROM amm_transactions_view WHERE amm_process = r.amm_process AND created_at_ts <= :now - 3600 ORDER BY created_at_ts DESC LIMIT 1) AS price_1h_ago,
    (SELECT price FROM amm_transactions_view WHERE amm_process = r.amm_process AND created_at_ts <= :now - 21600 ORDER BY created_at_ts DESC LIMIT 1) AS price_6h_ago,
    (SELECT price FROM amm_transactions_view WHERE amm_process = r.amm_process AND created_at_ts <= :now - 86400 ORDER BY created_at_ts DESC LIMIT 1) AS price_24h_ago
  FROM amm_registry r
  LEFT JOIN stats s ON s.amm_process = r.amm_process
  LEFT JOIN current_prices c ON c.amm_process = r.amm_process
  LEFT JOIN token_registry t ON t.token_process = r.amm_token1
  ORDER BY %s
  LIMIT 100
  ]], 'market_cap DESC'))

      if not stmt then
        error("Err: " .. db:errmsg())
      end

      stmt:bind_names({
        now = now
      })

      return sqlschema.queryMany(stmt)
    end

    function sqlschema.getTopNMarketData(token0)
      local orderByClause = "market_cap DESC"
      local stmt = db:prepare([[
  WITH current_prices AS (
    SELECT
      amm_process,
      (SELECT price FROM amm_transactions_view WHERE amm_process = r.amm_process ORDER BY created_at_ts DESC LIMIT 1) AS current_price
      (SELECT reserves_0 FROM amm_transactions_view WHERE amm_process = r.amm_process ORDER BY created_at_ts DESC LIMIT 1) AS current_reserves_0
      (SELECT reserves_1 FROM amm_transactions_view WHERE amm_process = r.amm_process ORDER BY created_at_ts DESC LIMIT 1) AS current_reserves_1
      (SELECT fee_percentage FROM amm_transactions_view WHERE amm_process = r.amm_process ORDER BY created_at_ts DESC LIMIT 1) AS current_fee_percentage
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
    c.reserves_0 AS reserves_0,
    c.reserves_1 AS reserves_1,
    c.fee_percentage AS fee_percentage
  FROM amm_registry r
  LEFT JOIN current_prices c ON c.amm_process = r.amm_process
  LEFT JOIN token_registry t ON t.token_process = r.amm_token1
  WHERE r.amm_token0 = :token0
  LIMIT 100
  ]], orderByClause)

      if not stmt then
        error("Err: " .. db:errmsg())
      end

      stmt:bind_names({
        token0 = token0,
        pricePrecision = PRICE_PRECISION
      })
      return sqlschema.queryMany(stmt)
    end

    function sqlschema.updateAMMs()
      sqlschema.registerAMM('TRUNK/AOCRED', 'vn5lUv8OaevTb45iI_qykad_d9MP69kuYg5mZW1zCHE',
        'Sa0iBLPNyJQrwpTTG-tWLQU-1QeUAJA73DdxGGiKoJc', 'OT9qTE2467gcozb2g8R6D6N3nQS94ENcaAIJfUzHCww', 1712737395)
      sqlschema.registerAMM('0rbit/AOCRED', '2bKo3vwB1Mo5TItmxuUQzZ11JgKauU_n2IZO1G13AIk',
        'Sa0iBLPNyJQrwpTTG-tWLQU-1QeUAJA73DdxGGiKoJc', 'BUhZLMwQ6yZHguLtJYA5lLUa9LQzLXMXRfaq9FVcPJc', 1712737395)
      sqlschema.registerAMM('BARK/AOCRED', 'U3Yy3MQ41urYMvSmzHsaA4hJEDuvIm-TgXvSm-wz-X0',
        'Sa0iBLPNyJQrwpTTG-tWLQU-1QeUAJA73DdxGGiKoJc', '8p7ApPZxC_37M06QHVejCQrKsHbcJEerd3jWNkDUWPQ', 1712737395)
      sqlschema.registerAMM('AFT/AOCRED', 'DCQJwfEQCD-OQYmfgNH4Oh6uGo9eQJbEn6WbNvtrI_k',
        'Sa0iBLPNyJQrwpTTG-tWLQU-1QeUAJA73DdxGGiKoJc', 'SpzpFLkqPGvr5ZFZPbvyAtizthmrJ13lL4VBQIBL0dg', 1712737395)
      sqlschema.registerAMM('EXP/AOCRED', 'IMcN3R14yThfHzgbYzBDuuSpzmow7zGyBHRE3Gwrtsk',
        'Sa0iBLPNyJQrwpTTG-tWLQU-1QeUAJA73DdxGGiKoJc', 'aYrCboXVSl1AXL9gPFe3tfRxRf0ZmkOXH65mKT0HHZw', 1712737395)
    end

    function sqlschema.updateTokens()
      sqlschema.registerToken('OT9qTE2467gcozb2g8R6D6N3nQS94ENcaAIJfUzHCww', 'TRUNK', 3, 34198, false, 1712737395)
      sqlschema.registerToken('8p7ApPZxC_37M06QHVejCQrKsHbcJEerd3jWNkDUWPQ', 'BARK', 3, 201047011, false, 1712737395)
      sqlschema.registerToken('SpzpFLkqPGvr5ZFZPbvyAtizthmrJ13lL4VBQIBL0dg', 'AFT', 12, 10000, false, 1712737395)
      sqlschema.registerToken('BUhZLMwQ6yZHguLtJYA5lLUa9LQzLXMXRfaq9FVcPJc', '0rbit', 12, 100109630, false, 1712737395)
      sqlschema.registerToken('aYrCboXVSl1AXL9gPFe3tfRxRf0ZmkOXH65mKT0HHZw', 'EXP', 6, 2782410, false, 1716217288)
      sqlschema.registerToken('Sa0iBLPNyJQrwpTTG-tWLQU-1QeUAJA73DdxGGiKoJc', 'AOCRED', 3, 2782410, false, 1716217288)
    end

    function sqlschema.getIndicators(timestampFrom, timestampTo, ammProcess)
      local stmt = db:prepare([[
    SELECT
      *
    FROM amm_transactions_view
    WHERE created_at_ts BETWEEN :date_from AND :date_to
      AND amm_process = :amm_process
  ]])

      if not stmt then
        error("Err: " .. db:errmsg())
      end

      stmt:bind_names({
        date_from = timestampFrom,
        date_to = timestampTo,
        amm_process = ammProcess
      })

      return sqlschema.queryMany(stmt)
    end

    function sqlschema.registerProcess(processId, ownerId, ammProcessId)
      local stmt = db:prepare [[
    INSERT INTO subscriptions (process_id, owner_id, amm_process_id)
    VALUES (:process_id, :owner_id, :amm_process_id)
    ON CONFLICT(process_id) DO UPDATE SET
    owner_id = excluded.owner_id,
    amm_process_id = excluded.amm_process_id;
  ]]
      if not stmt then
        error("Failed to prepare SQL statement for registering process: " .. db:errmsg())
      end
      stmt:bind_names({
        process_id = processId,
        owner_id = ownerId,
        amm_process_id = ammProcessId
      })
      local result, err = stmt:step()
      stmt:finalize()
      if err then
        error("Err: " .. db:errmsg())
      end
    end

    function sqlschema.registerTopNConsumer(processId, ownerId, quoteToken)
      local stmt = db:prepare [[
    INSERT INTO top_n_subscriptions (process_id, owner_id, quote_token_process_id)
    VALUES (:process_id, :owner_id, :quote_token_process_id)
    ON CONFLICT(process_id) DO UPDATE SET
    owner_id = excluded.owner_id,
    quote_token_process_id = excluded.quote_token_process_id;
  ]]
      if not stmt then
        error("Failed to prepare SQL statement for registering process: " .. db:errmsg())
      end
      stmt:bind_names({
        process_id = processId,
        owner_id = ownerId,
        quote_token_process_id = quoteToken
      })
      local result, err = stmt:step()
      stmt:finalize()
      if err then
        error("Err: " .. db:errmsg())
      end
    end

    function sqlschema.registerToken(processId, name, denominator, totalSupply, fixedSupply, updatedAt)
      local stmt = db:prepare [[
    INSERT INTO token_registry (token_process, token_name, denominator, total_supply, fixed_supply, token_updated_at_ts)
    VALUES (:process_id, :token_name, :denominator, :total_supply, :fixed_supply, :token_updated_at_ts)
    ON CONFLICT(token_process) DO UPDATE SET
    token_name = excluded.token_name,
    denominator = excluded.denominator,
    total_supply = excluded.total_supply,
    fixed_supply = excluded.fixed_supply,
    token_updated_at_ts = excluded.token_updated_at_ts;
  ]]
      if not stmt then
        error("Failed to prepare SQL statement for registering token: " .. db:errmsg())
      end
      stmt:bind_names({
        process_id = processId,
        token_name = name,
        denominator = denominator,
        total_supply = totalSupply,
        fixed_supply = fixedSupply,
        token_updated_at_ts = updatedAt
      })
      local result, err = stmt:step()
      stmt:finalize()
      if err then
        error("Err: " .. db:errmsg())
      end
    end

    function sqlschema.updateTokenSupply(processId, totalSupply, fixedSupply, updatedAt)
      local stmt = db:prepare [[
    UPDATE token_registry SET total_supply = :total_supply, fixed_supply = :fixed_supply, token_updated_at_ts = :token_updated_at_ts WHERE token_process = :token_process;
  ]]
      if not stmt then
        error("Failed to prepare SQL statement for updating token supply: " .. db:errmsg())
      end
      stmt:bind_names({
        token_process = processId,
        total_supply = totalSupply,
        fixed_supply = fixedSupply,
        token_updated_at_ts = updatedAt
      })
      local result, err = stmt:step()
      stmt:finalize()
      if err then
        error("Err: " .. db:errmsg())
      end
    end

    function sqlschema.updateBalance(ownerId, tokenId, amount, isCredit)
      local stmt = db:prepare [[
    INSERT INTO balances (owner, token_id, balance)
    VALUES (:owner_id, :token_id, :amount)
    ON CONFLICT(owner) DO UPDATE SET
      balance = CASE
        WHEN :is_credit THEN balances.balance + :amount
        ELSE balances.balance - :amount
      END
    WHERE balances.token_id = :token_id;
  ]]
      if not stmt then
        error("Failed to prepare SQL statement for updating balance: " .. db:errmsg())
      end
      stmt:bind_names({
        owner_id = ownerId,
        token_id = tokenId,
        amount = math.abs(amount), -- Ensure amount is positive
        is_credit = isCredit
      })
      local result, err = stmt:step()
      stmt:finalize()
      if err then
        error("Error updating balance: " .. db:errmsg())
      end
    end

    return sqlschema
  end
end

do
  local _ENV = _ENV
  package.preload["stats"] = function(...)
    local arg = _G.arg;
    local sqlschema = require('dexi-core.sqlschema')
    local stats = {}

    function stats.getAggregateStats(minTimestamp, ammProcessId)
      print(1)
      local stmt, err = db:prepare [[
    SELECT
      SUM(volume) AS total_volume,
      ROUND(SUM(CASE WHEN is_buy = 1 THEN volume ELSE 0 END)) AS buy_volume,
      ROUND(SUM(CASE WHEN is_buy = 0 THEN volume ELSE 0 END)) AS sell_volume,
      SUM(is_buy) AS buy_count,
      SUM(1 - is_buy) AS sell_count,
      COUNT(DISTINCT CASE WHEN is_buy = 1 THEN sender END) AS distinct_buyers,
      COUNT(DISTINCT CASE WHEN is_buy = 0 THEN sender END) AS distinct_sellers,
      COUNT(DISTINCT sender) AS distinct_traders
    FROM amm_transactions_view
    WHERE created_at_ts >= :min_ts
    AND amm_process = :amm;
  ]]

      if not stmt then
        error("Failed to prepare SQL statement: " .. db:errmsg())
      end

      print(stmt, err)
      stmt:bind_names({
        min_ts = minTimestamp,
        amm = ammProcessId
      })


      return sqlschema.queryOne(stmt)
    end

    return stats
  end
end

do
  local _ENV = _ENV
  package.preload["top-n-consumers"] = function(...)
    local arg = _G.arg;
    local sqlschema = require('dexi-core.sqlschema')
    local topNConsumers = {}

    function topNConsumers.dispatchMarketData(now)
      local subscribersStmt = db:prepare([[
    SELECT s.process_id, s.quote_token_process_id
    FROM top_n_subscriptions s
    JOIN balances b ON s.owner_id = b.owner_id AND b.balance > 0
    WHERE s.last_push_at + s.push_interval >= :now
    ]])
      if not subscribersStmt then
        error("Err: " .. db:errmsg())
      end
      subscribersStmt:bind_names({
        now = now
      })

      local json = require("json")

      print('sending market data updates to consumer processes')

      local marketDataPerQuoteToken = {} -- cache market data per quote token
      local consumers = {}               -- later log subscribers that were updated
      for row in subscribersStmt:nrows() do
        table.insert(consumers, row.process_id)
        local quoteToken = row.quote_token_process_id
        local marketData = marketDataPerQuoteToken[quoteToken]
        if not marketData then
          marketData = sqlschema.getTopNMarketData(quoteToken)
          marketDataPerQuoteToken[quoteToken] = marketData
        end
        ao.send({
          ['Target'] = row.process_id,
          ['Action'] = 'TopNMarketData',
          ['Data'] = json.encode(marketData)
        })
      end
      subscribersStmt:finalize()

      print('sent market data updates to ' .. #consumers .. ' consumer processes')

      local message = {
        ['Target'] = ao.id,
        ['Assignments'] = consumers,
        ['Action'] = 'TopNMarketData',
        ['Data'] = json.encode(marketDataPerQuoteToken)
      }
      ao.send(message)

      print('Dispatched market data to all top N consumers')
    end

    return topNConsumers
  end
end

do
  local _ENV = _ENV
  package.preload["validation"] = function(...)
    local arg = _G.arg;
    -- @file        validation.lua
    -- @author      Théo Brigitte <theo.brigitte@gmail.com>
    -- @contributor Henrique Silva <hensansi@gmail.com>
    -- @date        Thu May 28 16:05:15 2015
    --
    -- @brief       Lua schema validation library.
    --
    -- Validation is achieved by matching data against a schema.
    --
    -- A schema is a representation of the expected structure of the data. It is
    -- a combination of what we call "validators".
    -- Validators are clojures which build accurante validation function for each
    -- element of the schema.
    -- Meta-validators allow to extend the logic of the schema by providing an
    -- additional logic layer around validators.
    --  e.g. optional()
    --

    -- Import from global environment.
    local type = type
    local pairs = pairs
    local print = print
    local format = string.format
    local floor = math.floor
    local insert = table.insert
    local next = next

    -- Disable global environment.
    if _G.setfenv then
      setfenv(1, {})
    else -- Lua 5.2.
      _ENV = {}
    end

    local M = { _NAME = 'validation' }

    --- Generate error message for validators.
    --
    -- @param data mixed
    --   Value that failed validation.
    -- @param expected_type string
    --   Expected type for data
    --
    -- @return
    --   String describing the error.
    ---
    local function error_message(data, expected_type)
      if data then
        return format('is not %s.', expected_type)
      end

      return format('is missing and should be %s.', expected_type)
    end

    --- Create a readable string output from the validation errors output.
    --
    -- @param error_list table
    --   Nested table identifying where the error occured.
    --   e.g. { price = { rule_value = 'error message' } }
    -- @param parents string
    --   String of dot separated parents keys
    --
    -- @return string
    --   Message describing where the error occured. e.g. price.rule_value = "error message"
    ---
    function M.print_err(error_list, parents)
      -- Makes prefix not nil, for posterior concatenation.
      local error_output = ''
      local parents = parents or ''
      if not error_list then return false end
      -- Iterates over the list of messages.
      for key, err in pairs(error_list) do
        -- If it is a node, print it.
        if type(err) == 'string' then
          error_output = format('%s\n%s%s %s', error_output, parents, key, err)
        else
          -- If it is a table, recurse it.
          error_output = format('%s%s', error_output, M.print_err(err, format('%s%s.', parents, key)))
        end
      end

      return error_output
    end

    --- Validators.
    --
    -- A validator is a function in charge of verifying data compliance.
    --
    -- Prototype:
    -- @key
    --   Key of data being validated.
    -- @data
    --   Current data tree level. Meta-validator might need to verify other keys. e.g. assert()
    --
    -- @return
    --   true on success, false and message describing the error
    ---


    --- Generates string validator.
    --
    -- @return
    --   String validator function.
    ---
    function M.is_string()
      return function(value)
        if type(value) ~= 'string' then
          return false, error_message(value, 'a string')
        end
        return true
      end
    end

    --- Generates integer validator.
    --
    -- @return
    --   Integer validator function.
    ---
    function M.is_integer()
      return function(value)
        if type(value) ~= 'number' or value % 1 ~= 0 then
          return false, error_message(value, 'an integer')
        end
        return true
      end
    end

    --- Generates number validator.
    --
    -- @return
    --   Number validator function.
    ---
    function M.is_number()
      return function(value)
        if type(value) ~= 'number' then
          return false, error_message(value, 'a number')
        end
        return true
      end
    end

    --- Generates boolean validator.
    --
    -- @return
    --   Boolean validator function.
    ---
    function M.is_boolean()
      return function(value)
        if type(value) ~= 'boolean' then
          return false, error_message(value, 'a boolean')
        end
        return true
      end
    end

    --- Generates an array validator.
    --
    -- Validate an array by applying same validator to all elements.
    --
    -- @param validator function
    --   Function used to validate the values.
    -- @param is_object boolean (optional)
    --   When evaluted to false (default), it enforce all key to be of type number.
    --
    -- @return
    --   Array validator function.
    --   This validator return value is either true on success or false and
    --   a table holding child_validator errors.
    ---
    function M.is_array(child_validator, is_object)
      return function(value, key, data)
        local result, err = nil
        local err_array = {}

        -- Iterate the array and validate them.
        if type(value) == 'table' then
          for index in pairs(value) do
            if not is_object and type(index) ~= 'number' then
              insert(err_array, error_message(value, 'an array'))
            else
              result, err = child_validator(value[index], index, value)
              if not result then
                err_array[index] = err
              end
            end
          end
        else
          insert(err_array, error_message(value, 'an array'))
        end

        if next(err_array) == nil then
          return true
        else
          return false, err_array
        end
      end
    end

    --- Generates optional validator.
    --
    -- When data is present apply the given validator on data.
    --
    -- @param validator function
    --   Function used to validate value.
    --
    -- @return
    --   Optional validator function.
    --   This validator return true or the result from the given validator.
    ---
    function M.optional(validator)
      return function(value, key, data)
        if not value then
          return true
        else
          return validator(value, key, data)
        end
      end
    end

    --- Generates or meta validator.
    --
    -- Allow data validation using two different validators and applying
    -- or condition between results.
    --
    -- @param validator_a function
    --   Function used to validate value.
    -- @param validator_b function
    --   Function used to validate value.
    --
    -- @return
    --   Or validator function.
    --   This validator return true or the result from the given validator.
    ---
    function M.or_op(validator_a, validator_b)
      return function(value, key, data)
        if not value then
          return true
        else
          local valid, err_a = validator_a(value, key, data)
          if not valid then
            valid, err_b = validator_b(value, key, data)
          end
          if not valid then
            return valid, err_a .. " OR " .. err_b
          else
            return valid, nil
          end
        end
      end
    end

    --- Generates assert validator.
    --
    -- This function enforces the existence of key/value with the
    -- verification of the key_check.
    --
    -- @param key_check mixed
    --   Key used to check the optionality of the asserted key.
    -- @param match mixed
    --   Comparation value.
    -- @param validator function
    --   Function that validates the type of the data.
    --
    -- @return
    --   Assert validator function.
    --   This validator return true, the result from the given validator or false
    --   when the assertion fails.
    ---
    function M.assert(key_check, match, validator)
      return function(value, key, data)
        if data[key_check] == match then
          return validator(value, key, data)
        else
          return true
        end
      end
    end

    --- Generates list validator.
    --
    -- Ensure the value is contained in the given list.
    --
    -- @param list table
    --   Set of allowed values.
    -- @param value mixed
    --   Comparation value.
    -- @param validator function
    --   Function that validates the type of the data.
    --
    -- @return
    --   In list validator function.
    ---
    function M.in_list(list)
      return function(value)
        local printed_list = "["
        for _, word in pairs(list) do
          if word == value then
            return true
          end
          printed_list = printed_list .. " '" .. word .. "'"
        end

        printed_list = printed_list .. " ]"
        return false, { error_message(value, 'in list ' .. printed_list) }
      end
    end

    --- Generates table validator.
    --
    -- Validate table data by using appropriate schema.
    --
    -- @param schema table
    --   Schema used to validate the table.
    --
    -- @return
    --   Table validator function.
    --   This validator return value is either true on success or false and
    --   a nested table holding all errors.
    ---
    function M.is_table(schema, tolerant)
      return function(value)
        local result, err = nil

        if type(value) ~= 'table' then
          -- Enforce errors of childs value.
          _, err = validate_table({}, schema, tolerant)
          if not err then err = {} end
          result = false
          insert(err, error_message(value, 'a table'))
        else
          result, err = validate_table(value, schema, tolerant)
        end

        return result, err
      end
    end

    --- Validate function.
    --
    -- @param data
    --   Table containing the pairs to be validated.
    -- @param schema
    --   Schema against which the data will be validated.
    --
    -- @return
    --   String describing the error or true.
    ---
    function validate_table(data, schema, tolerant)
      -- Array of error messages.
      local errs = {}
      -- Check if the data is empty.

      -- Check if all data keys are present in the schema.
      if not tolerant then
        for key in pairs(data) do
          if schema[key] == nil then
            errs[key] = 'is not allowed.'
          end
        end
      end

      -- Iterates over the keys of the data table.
      for key in pairs(schema) do
        -- Calls a function in the table and validates it.
        local result, err = schema[key](data[key], key, data)

        -- If validation fails, print the result and return it.
        if not result then
          errs[key] = err
        end
      end

      -- Lua does not give size of table holding only string as keys.
      -- Despite the use of #table we have to manually loop over it.
      for _ in pairs(errs) do
        return false, errs
      end

      return true
    end

    return M
  end
end

local json = require("json")


local intervals = require("dexis.intervals")
local candles = require "dexi-core.candles"
local stats = require "dexi-core.stats"
local schemas = require "schemas"
local sqlite3 = require("lsqlite3")
local sqlschema = require("dexi-core.sqlschema")
local indicators = require("indicators.indicators")
local topNConsumers = require("top-n-consumers")

db = db or sqlite3.open_memory()

sqlschema.createTableIfNotExists(db)

--

OFFCHAIN_FEED_PROVIDER = 'P6i7xXWuZtuKJVJYNwEqduj0s8R_G4wZJ38TB5Knpy4'
TOKEN = ao.env.Process.Tags["Base-Token"]
AMM = ao.env.Process.Tags["Monitor-For"]

local function insertSingleMessage(msg, source, sourceAmm)
  local valid, err = schemas.inputMessageSchema(msg)
  assert(valid, 'Invalid input transaction data' .. json.encode(err))

  local stmt, err = db:prepare [[
    REPLACE INTO amm_transactions (
      id, source, block_height, block_id, sender, created_at_ts,
      to_token, from_token, from_quantity, to_quantity, fee, amm_process,
      reserves_
    ) VALUES (:id, :source, :block_height, :block_id, :sender, :created_at_ts,
              :to_token, :from_token, :from_quantity, :to_quantity, :fee, :amm_process);
  ]]

  if not stmt then
    error("Failed to prepare SQL statement: " .. db:errmsg())
  end

  stmt:bind_names({
    id = msg.Id,
    source = source,
    block_height = msg['Block-Height'],
    block_id = msg['Block-Id'] or '',
    sender = msg.recipient or '',
    created_at_ts = math.floor(msg.Timestamp / 1000),
    to_token = msg.Tags['To-Token'],
    from_token = msg.Tags['From-Token'],
    from_quantity = tonumber(msg.Tags['From-Quantity']),
    to_quantity = tonumber(msg.Tags['To-Quantity']),
    fee = tonumber(msg.Tags['Fee']),
    amm_process = sourceAmm
  })

  stmt:step()
  stmt:reset()
end


function debugTable()
  local stmt = db:prepare [[
    SELECT * FROM amm_transactions ORDER BY created_at_ts LIMIT 100;
  ]]
  if not stmt then
    error("Failed to prepare SQL statement: " .. db:errmsg())
  end
  return sqlschema.queryMany(stmt)
end

local function findPriceAroundTimestamp(targetTimestampBefore, ammProcessId)
  local stmt = db:prepare [[
    SELECT price
    FROM amm_transactions_view
    WHERE created_at_ts <= :target_timestamp_before
    AND amm_process = :amm_process_id
    ORDER BY created_at_ts DESC
    LIMIT 1;
  ]]

  if not stmt then
    error("Failed to prepare SQL statement: " .. db:errmsg())
  end

  stmt:bind_names({
    target_timestamp_before = targetTimestampBefore,
    amm_process_id = ammProcessId
  })


  local row = sqlschema.queryOne(stmt)
  local price = row and row.price or nil

  return price
end


Handlers.add(
  "GetStats",
  Handlers.utils.hasMatchingTag("Action", "Get-Stats"),
  function(msg)
    local stats = stats.getAggregateStats(0, msg.Tags.AMM)
    local now = msg.Timestamp / 1000

    local priceNow = findPriceAroundTimestamp(now, msg.Tags.AMM)
    local price24HAgo = findPriceAroundTimestamp(now - intervals.IntervalSecondsMap['1d'], msg.Tags.AMM)
    local price6HAgo = findPriceAroundTimestamp(now - intervals.IntervalSecondsMap['6h'], msg.Tags.AMM)
    local price1HAgo = findPriceAroundTimestamp(now - intervals.IntervalSecondsMap['1h'], msg.Tags.AMM)

    ao.send({
      Target = msg.From,
      ['App-Name'] = 'Dexi',
      ['Payload'] = 'Stats',
      ['AMM'] = msg.Tags.AMM,
      ['Total-Volume'] = tostring(stats.total_volume),
      ['Buy-Volume'] = tostring(stats.buy_volume),
      ['Sell-Volume'] = tostring(stats.sell_volume),
      ['Buy-Count'] = tostring(stats.buy_count),
      ['Sell-Count'] = tostring(stats.sell_count),
      ['Buyers'] = tostring(stats.distinct_buyers),
      ['Sellers'] = tostring(stats.distinct_sellers),
      ['Total-Traders'] = tostring(stats.distinct_traders),
      ['Latest-Price'] = tostring(priceNow),
      ['Price-24H-Ago'] = tostring(price24HAgo),
      ['Price-6H-Ago'] = tostring(price6HAgo),
      ['Price-1H-Ago'] = tostring(price1HAgo)
    })
  end
)


function startOfDayUTC(currentTimestamp)
  local utcDateTable = os.date("!*t", currentTimestamp)
  utcDateTable.hour = 0
  utcDateTable.min = 0
  utcDateTable.sec = 0
  return os.time(utcDateTable)
end

Handlers.add(
  "GetCandles",
  Handlers.utils.hasMatchingTag("Action", "Get-Candles"),
  function(msg)
    local days = msg.Tags.Days and tonumber(msg.Tags.Days) or 30
    local candles = candles.generateCandlesForXDaysInIntervalY(days, msg.Tags.Interval, msg.Timestamp / 1000,
      msg.Tags.AMM)
    ao.send({
      Target = msg.From,
      ['App-Name'] = 'Dexi',
      ['Payload'] = 'Candles',
      ['AMM'] = msg.Tags.AMM,
      ['Interval'] = msg.Tags.Interval or '15m',
      ['Days'] = tostring(msg.Tags.Days),
      Data = json.encode(candles)
    })
  end
)

Handlers.add(
  "UpdateLocalState",
  Handlers.utils.hasMatchingTag("Action", "Order-Confirmation-Monitor"),
  function(msg)
    local stmt = 'SELECT TRUE FROM amm_registry WHERE amm_process = :amm_process'
    local stmt = db:prepare(stmt)
    stmt:bind_names({ amm_process = msg.From })

    local row = sqlschema.queryOne(stmt)
    if row or msg.From == Owner then
      insertSingleMessage(msg, 'message', msg.From)
    end
  end
)


Handlers.add(
  "GetRegisteredAMMs",
  Handlers.utils.hasMatchingTag("Action", "Get-Registered-AMMs"),
  function(msg)
    ao.send({
      ['App-Name'] = 'Dexi',
      ['Payload'] = 'Registered-AMMs',
      Target = msg.From,
      Data = json.encode(sqlschema.getRegisteredAMMs())
    })
  end
)

Handlers.add(
  "GetOverview",
  Handlers.utils.hasMatchingTag("Action", "Get-Overview"),
  function(msg)
    local now = msg.Timestamp / 1000
    local orderBy = msg.Tags['Order-By']
    ao.send({
      ['App-Name'] = 'Dexi',
      ['Payload'] = 'Overview',
      Target = msg.From,
      Data = json.encode(sqlschema.getOverview(now, orderBy))
    })
  end
)

Handlers.add(
  "GetTopNMarketData",
  Handlers.utils.hasMatchingTag("Action", "Get-Top-N-Market-Data"),
  function(msg)
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
      Data = json.encode(sqlschema.getTopNMarketData(quoteToken))
    })
  end
)


Handlers.add(
  "ReceiveOffchainFeed", -- handler name
  Handlers.utils.hasMatchingTag("Action", "Receive-Offchain-Feed"),
  function(msg)
    if msg.From == OFFCHAIN_FEED_PROVIDER then
      local data = json.decode(msg.Data)
      for _, transaction in ipairs(data) do
        insertSingleMessage(transaction, 'gateway', transaction.Tags['AMM'])
      end
    end
  end
)


Handlers.add(
  "GetCurrentHeight",
  Handlers.utils.hasMatchingTag("Action", "Get-Current-Height"),
  function(msg)
    local stmt = db:prepare [[
      SELECT MAX(block_height) AS max_height
      FROM amm_transactions
      WHERE source = 'gateway' AND amm_process = :amm;
    ]]

    stmt:bind_names({ amm = msg.Tags.AMM })

    local row = sqlschema.queryOne(stmt)
    local gatewayHeight = row and row.max_height or 0

    stmt:reset()

    ao.send({
      Target = msg.From,
      Height = tostring(gatewayHeight)
    })
  end
)


LastTriggeredHour = -1

Handlers.add(
  "CronMinuteTick",
  Handlers.utils.hasMatchingTag("Action", "Cron-Minute-Tick"),
  function(msg)
    local now = math.floor(msg.Timestamp / 1000)
    local currentHour = math.floor(msg.Timestamp / 3600000)

    if currentHour > LastTriggeredHour then
      LastTriggeredHour = currentHour

      indicators.dispatchIndicatorsForAllAMMs(now)
      local outmsg = ao.send({
        Target = ao.id,
        Action = 'Dexi-Update-Tick',
        OK = 'true'
      })
    end

    topNConsumers.dispatchMarketData(now)
  end
)


Handlers.add(
  "RegisterProcess",
  Handlers.utils.hasMatchingTag("Action", "Register-Process"),
  function(msg)
    local processId = msg.Tags['Subscriber-Process-Id']
    local ownerId = msg.Tags['Owner-Id']
    local ammProcessId = msg.Tags['AMM-Process-Id']

    print('Registering process: ' .. processId .. ' for amm: ' .. ammProcessId .. ' with owner: ' .. ownerId)
    sqlschema.registerIndicatorSubscriber(processId, ownerId, ammProcessId)

    Send({
      Target = ao.id,
      Assignments = { ownerId, processId },
      Action = 'Dexi-Registration-Confirmation',
      AMM = ammProcessId,
      Process = processId,
      OK = 'true'
    })
  end
)

Handlers.add(
  "RegisterTopNConsumer",
  Handlers.utils.hasMatchingTag("Action", "Register-Top-N-Consumer"),
  function(msg)
    local processId = msg.Tags['Subscriber-Process-Id']
    local ownerId = msg.Tags['Owner-Id']
    local quoteToken = msg.Tags['Quote-Token']

    if not quoteToken then
      error('Quote-Token is required')
    end
    if not sqlschema.isQuoteTokenAvailable(quoteToken) then
      error('Quote-Token not available: ' .. quoteToken)
    end

    print('Registering top n consumer: ' .. processId .. ' for quote token: ' .. quoteToken .. ' with owner: ' .. ownerId)
    sqlschema.registerTopNSubscriber(processId, ownerId, quoteToken)

    Send({
      Target = ao.id,
      Assignments = { ownerId, processId },
      Action = 'Dexi-Top-N-Registration-Confirmation',
      QuoteToken = quoteToken,
      Process = processId,
      OK = 'true'
    })
  end
)

Handlers.add(
  "BatchRequestPrices",
  Handlers.utils.hasMatchingTag("Action", "Price-Batch-Request"),
  function(msg)
    local amms = json.decode(msg.Tags['AMM-List'])
    local ammPrices = {}
    for _, amm in ipairs(amms) do
      local price = findPriceAroundTimestamp(msg.Timestamp / 1000, amm)
      ammPrices[amm] = price
    end

    ao.send({
      Target = msg.From,
      Action = 'Price-Batch-Response',
      Data = json.encode(ammPrices)
    })
  end
)

Handlers.add(
  "CreditNotice",
  Handlers.utils.hasMatchingTag("Action", "Credit-Notice"),
  function(msg)
    if msg.From == 'Sa0iBLPNyJQrwpTTG-tWLQU-1QeUAJA73DdxGGiKoJc' then
      sqlschema.updateBalance(msg.Tags.Sender, msg.From, tonumber(msg.Tags.Quantity), true)
    end
  end
)



Handlers.add(
  "DumpTableToCSV",
  Handlers.utils.hasMatchingTag("Action", "Dump-Table-To-CSV"),
  function(msg)
    local stmt = db:prepare [[
      SELECT *
      FROM amm_transactions;
    ]]

    local rows = {}
    local row = stmt:step()
    while row do
      table.insert(rows, row)
      row = stmt:step()
    end

    stmt:reset()

    local csvHeader =
    "id,source,block_height,block_id,from,timestamp,is_buy,price,volume,to_token,from_token,from_quantity,to_quantity,fee,amm_process\n"
    local csvData = csvHeader

    for _, row in ipairs(rows) do
      local rowData = string.format("%s,%s,%d,%s,%s,%d,%d,%.8f,%.8f,%s,%s,%.8f,%.8f,%.8f,%s\n",
        row.id, row.source, row.block_height, row.block_id, row["from"], row["timestamp"],
        row.is_buy, row.price, row.volume, row.to_token, row.from_token, row.from_quantity,
        row.to_quantity, row.fee, row.amm_process)
      csvData = csvData .. rowData
    end

    ao.send({
      Target = msg.From,
      Data = csvData
    })
  end
)


function Trusted(msg)
  local mu = "fcoN_xJeisVsPXA-trzVAuIiqO3ydLQxM-L4XbrQKzY"
  -- return false if trusted
  if msg.Owner == mu then
    return false
  end
  if msg.From == msg.Owner then
    return false
  end
  return true
end

Handlers.prepend("qualify message",
  Trusted,
  function(msg)
    print("This Msg is not trusted!")
  end
)


-- Handlers.add(
--   "receive-data-feed",
--   Handlers.utils.hasMatchingTag("Action", "Receive-data-feed"),
--   function (msg)
--     local data = json.decode(msg.Data)
--     if data.data.transactions then
--       updateTransactions(data.data.transactions.edges)
--       print('transactions updated')
--       if #data.data.transactions.edges > 0 then
--         requestTransactions(100)
--       end
--       requestBlocks()
--     elseif data.data.blocks then
--       updateBlockTimestamps(data.data.blocks.edges)
--       print('blocks updated')
--     end
--   end
-- )
