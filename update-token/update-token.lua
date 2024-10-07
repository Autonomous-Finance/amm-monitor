local hopper = require("hopper.hopper")
local dbUtils = require("db.utils")

local updateToken = {}

local tokendropRegistry = "mQES2_hwlXQS8JVSdPJvRTkp78slLCl2gpm6sW3CK9w"
local priceInUSD = 20

function updateToken.get_token_denominator(tokenProcess)
    local stmt = db:prepare [[
        SELECT
            token_process,
            token_name,
            denominator
        FROM token_registry
        WHERE token_process = :token_process
        LIMIT 1;
      ]]

    if not stmt then
        error("Failed to prepare SQL statement: " .. db:errmsg())
    end

    stmt:bind_names({
        token_process = tokenProcess
    })

    local row = dbUtils.queryOne(stmt)
    local denominator = row and row.denominator or 18

    return denominator
end

updateToken.handleGetPriceForUpdate = function(msg)
    assert(msg.Tags["Token-Process"], 'Token info data must contain a valid Token Process tag')

    local priceResponse = hopper.getPrice("USD", msg.Tags["Token-Process"])
    local totalCost = priceResponse * priceInUSD

    ao.send({
        Target = msg.From,
        Action = 'Token-Update-Price',
        ["Token-Process"] = msg.Tags["Token-Process"],
        ["Price-In-USD"] = tostring(priceInUSD),
        ["Price-In-Tokens"] = tostring(totalCost)
    })
end

updateToken.handlePayForUpdateToken = function(msg)
    assert(msg.Tags["X-Token-Process"], 'Token info data must contain a valid Token Process tag')
    assert(msg.Tags["X-Details"], 'Token info data must contain a valid Details tag')

    local denominator = updateToken.get_token_denominator(msg.Tags["X-Token-Process"])

    local priceResponse = hopper.getPrice("USD", msg.From)
    local totalCost = priceResponse * priceInUSD * 10 ^ denominator
    local quantity = tonumber(msg.Tags.Quantity)

    -- if less funds received refund the user and send back the reason
    if (quantity < totalCost) then
        -- Send back the funds
        ao.send({
            Target = msg.From,
            Action = "Transfer",
            Quantity = msg.Tags.Quantity,
            Recipient = msg.Sender
        })

        ao.send({
            Target = msg.Sender,
            Action = 'Token-Update-Failed',
            ["Token-Process"] = msg.Tags["X-Token-Process"],
            ["X-Details"] = msg.Tags["X-Details"],
            ["Reason"] = "Insufficient funds"
        })

        -- break the execution
        return false
    end

    -- if more funds received refund the difference to user
    if (quantity > totalCost) then
        -- Send back the funds
        ao.send({
            Target = msg.From,
            Action = "Transfer",
            Quantity = quantity - totalCost,
            Recipient = msg.Sender
        })
    end

    ao.send({
        Target = tokendropRegistry,
        Action = 'Update-Token-Profile',
        Sender = msg.Tags.Sender,
        ["Token-Process"] = msg.Tags["X-Token-Process"],
        Data = msg.Tags["X-Details"]
    })
end

return updateToken
