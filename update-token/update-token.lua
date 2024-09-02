local updateToken = {}

local tokendropRegistry = "Mrkk8xNLfy1zhx99oAJTRDAHA3j6n1TG_hIf_I03yBY"

updateToken.handlePayForUpdateToken = function(msg)
    assert(msg.Tags["X-Token-Process"], 'Token info data must contain a valid Token Process tag')
    assert(msg.Tags["X-Details"], 'Token info data must contain a valid Details tag')

    ao.send({
        Target = tokendropRegistry,
        Action = 'Update-Token-Profile',
        Sender = msg.Tags.Sender,
        ["Token-Process"] = msg.Tags["X-Token-Process"],
        Data = msg.Tags["X-Details"]
    })
end

return updateToken
