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
        INSERT OR REPLACE INTO oracle_prices (process_id, ticker, price, last_update, last_update_ts) VALUES ('0udHxHUaSZI4aIs4hD6rF2jRas4G_XWYnn6JwxXd0II', 'mockAO.new', :price, :date, :ts);
    ]]

    stmt:bind_names({ price = arPrice, date = date, ts = timestamp })
    dbUtils.execute(stmt)
end

return mod
