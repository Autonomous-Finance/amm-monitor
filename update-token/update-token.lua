local hopper = require("hopper.hopper")

local updateToken = {}

local tokendropRegistry = "Mrkk8xNLfy1zhx99oAJTRDAHA3j6n1TG_hIf_I03yBY"
local priceInUSD = 20

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

    local priceResponse = hopper.getPrice("USD", msg.From)
    local totalCost = priceResponse * priceInUSD -- introduce decimals
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