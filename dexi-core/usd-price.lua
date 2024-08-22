local dbUtils = require("db.utils")
local json = require("json")

local mod = {}

function mod.getOraclePriceByProcessId(processId)
    local stmt = db:prepare [[
      SELECT *
      FROM oracle_prices
      WHERE process_id = :process_id
      ORDER BY last_update DESC
      LIMIT 1;
    ]]

    if not stmt then
        error("Failed to prepare SQL statement: " .. db:errmsg())
    end

    stmt:bind_names({
        process_id = processId
    })

    return dbUtils.queryOne(stmt)
end

function mod.getUsdPriceForToken(processId)
    local oraclePrice = mod.getOraclePriceByProcessId(processId)

    if not oraclePrice then
        return nil
    end

    return oraclePrice.price
end

function mod.updateUsdPrice(message)
    -- TODO: add whitelist check for redstone proxy
    local redstoneResponse = json.decode(message.Data)
    local arPrice = redstoneResponse['AR']['v']

    local timestamp = math.floor(redstoneResponse['AR']['t'] / 1000) -- Convert from milliseconds to seconds
    local date = os.date("%Y-%m-%d %H:%M:%S", timestamp)

    -- insert ar price as ao price
    local stmt = db:prepare [[
        INSERT OR REPLACE INTO oracle_prices (process_id, ticker, price, last_update) VALUES ('j7w28CJQHYwamMsotkhE7x0aVUggGwrBtdO5-MQ80uU', 'AO', :price, :date);
    ]]

    stmt:bind_names({ price = arPrice, date = date })
    dbUtils.execute(stmt)
end

return mod
