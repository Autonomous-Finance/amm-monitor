do
    local _ENV = _ENV
    package.preload[ "candles" ] = function( ... ) local arg = _G.arg;
    local intervals = require('intervals')
    local sqlschema = require('sqlschema')
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
          SUM(volume) AS volume  
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
        candles[i].open = candles[i-1].close
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
    package.preload[ "intervals" ] = function( ... ) local arg = _G.arg;
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
        timestamp = math.floor(timestamp)  -- Ensure timestamp is an integer
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
    package.preload[ "schemas" ] = function( ... ) local arg = _G.arg;
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
    package.preload[ "sqlschema" ] = function( ... ) local arg = _G.arg;
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
        amm_process TEXT NOT NULL
    );
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
      CASE WHEN to_token = amm_token1 THEN 1 ELSE 0 END AS is_buy,
      ROUND(CASE 
        WHEN from_quantity > 0 AND to_quantity > 0 THEN
          CASE 
            WHEN to_token = amm_token1 THEN from_quantity * 1.0 / to_quantity
            ELSE to_quantity * 1.0 / from_quantity
          END
        ELSE NULL
      END, 12) AS price,
      CASE
        WHEN to_token = amm_token1 THEN from_quantity
        ELSE to_quantity
      END AS volume
    FROM amm_transactions
    LEFT JOIN amm_registry USING (amm_process)
    ]]
    
    
    function sqlschema.createTableIfNotExists(db)
        db:exec(sqlschema.create_table)
    
        db:exec("DROP VIEW IF EXISTS amm_transactions_view;")
        print("Err: " .. db:errmsg())
    
        db:exec(sqlschema.create_amm_registry_table)
        print("Err: " .. db:errmsg())
      
        db:exec(sqlschema.create_transactions_view)
        print("Err: " .. db:errmsg())
    
        sqlschema.updateAMMs()
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
      local stmt = db:prepare[[
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
    
    function sqlschema.getOverview(now, orderBy)
      local orderByClause = "amm_discovered_at_ts DESC"
      
      if orderBy == "volume" then
        orderByClause = "volume DESC"
      elseif orderBy == "transactions" then
        orderByClause = "transactions DESC"
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
      )
      SELECT
        r.amm_name as amm_name,
        r.amm_process as amm_process,
        r.amm_token0 AS token0,
        r.amm_token1 AS token1,
        transactions,
        volume,
        (SELECT price FROM amm_transactions_view WHERE amm_process = r.amm_process ORDER BY created_at_ts DESC LIMIT 1) AS current_price,
        (SELECT price FROM amm_transactions_view WHERE amm_process = r.amm_process AND created_at_ts <= :now - 300 ORDER BY created_at_ts DESC LIMIT 1) AS price_5m_ago,
        (SELECT price FROM amm_transactions_view WHERE amm_process = r.amm_process AND created_at_ts <= :now - 900 ORDER BY created_at_ts DESC LIMIT 1) AS price_15m_ago,
        (SELECT price FROM amm_transactions_view WHERE amm_process = r.amm_process AND created_at_ts <= :now - 21600 ORDER BY created_at_ts DESC LIMIT 1) AS price_6h_ago,
        (SELECT price FROM amm_transactions_view WHERE amm_process = r.amm_process AND created_at_ts <= :now - 86400 ORDER BY created_at_ts DESC LIMIT 1) AS price_24h_ago
      FROM amm_registry r
      LEFT JOIN stats s ON s.amm_process = r.amm_process
      ORDER BY %s
      LIMIT 100
      ]], orderByClause))
    
      if not stmt then
        error("Err: " .. db:errmsg())
      end
    
      stmt:bind_names({
        now = now
      })
    
      return sqlschema.queryMany(stmt)
    end
    
    function sqlschema.updateAMMs()
      sqlschema.registerAMM('TRUNK/AOCRED', 'vn5lUv8OaevTb45iI_qykad_d9MP69kuYg5mZW1zCHE', 'Sa0iBLPNyJQrwpTTG-tWLQU-1QeUAJA73DdxGGiKoJc', 'OT9qTE2467gcozb2g8R6D6N3nQS94ENcaAIJfUzHCww', 1712737395)
      sqlschema.registerAMM('0rbit/AOCRED', '2bKo3vwB1Mo5TItmxuUQzZ11JgKauU_n2IZO1G13AIk', 'Sa0iBLPNyJQrwpTTG-tWLQU-1QeUAJA73DdxGGiKoJc', 'BUhZLMwQ6yZHguLtJYA5lLUa9LQzLXMXRfaq9FVcPJc', 1712737395)
      sqlschema.registerAMM('BARK/AOCRED', 'U3Yy3MQ41urYMvSmzHsaA4hJEDuvIm-TgXvSm-wz-X0', 'Sa0iBLPNyJQrwpTTG-tWLQU-1QeUAJA73DdxGGiKoJc', '8p7ApPZxC_37M06QHVejCQrKsHbcJEerd3jWNkDUWPQ', 1712737395)
      sqlschema.registerAMM('AFT/AOCRED', 'DCQJwfEQCD-OQYmfgNH4Oh6uGo9eQJbEn6WbNvtrI_k', 'Sa0iBLPNyJQrwpTTG-tWLQU-1QeUAJA73DdxGGiKoJc', 'SpzpFLkqPGvr5ZFZPbvyAtizthmrJ13lL4VBQIBL0dg', 1712737395)
    end
    
    return sqlschema
    end
    end
    
    do
    local _ENV = _ENV
    package.preload[ "stats" ] = function( ... ) local arg = _G.arg;
    local sqlschema = require('sqlschema')
    local stats = {}
    
    function stats.getAggregateStats(minTimestamp, ammProcessId)
      print(1)
      local stmt, err = db:prepare[[
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
    package.preload[ "validation" ] = function( ... ) local arg = _G.arg;
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
          error_output = format('%s\n%s%s %s', error_output, parents ,key, err)
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
        if type(value) ~= 'number' or value%1 ~= 0 then
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
              insert(err_array, error_message(value, 'an array') )
            else
              result, err = child_validator(value[index], index, value)
              if not result then
                err_array[index] = err
              end
            end
          end
        else
          insert(err_array, error_message(value, 'an array') )
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
        if not value then return true
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
        if not value then return true
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
          insert(err, error_message(value, 'a table') )
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