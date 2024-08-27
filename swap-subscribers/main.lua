local json = require('json')
local dbUtils = require('db.utils')

local mod = {}

function mod.getSwapSubscribers(ammProcessId)
    local stmt = db:prepare [[
      SELECT process_id FROM swap_subscriptions WHERE amm_process_id = :amm_process_id;
    ]]
    stmt:bind_names({ amm_process_id = ammProcessId })
    return dbUtils.queryMany(stmt)
end

-- todo move this somewhere else
function mod.dispatchSwapNotifications(sourceMessageId, sourceAmm)
    local subscribers = mod.getSwapSubscribers(sourceAmm)

    -- todo add balance check
    local stmt = db:prepare [[
      SELECT * FROM amm_transactions_view WHERE id = :id LIMIT 1;
    ]]
    stmt:bind_names({ id = sourceMessageId })
    local transformedSwapData = dbUtils.queryOne(stmt)
    for _, subscriber in ipairs(subscribers) do
        ao.send({
            Target = subscriber.process_id,
            Action = 'Dexi-Swap-Notification',
            Data = json.encode(transformedSwapData)
        })
    end
end

function mod.registerSwapSubscriber(processId, ammProcessId)
    local stmt = db:prepare [[
        INSERT INTO swap_subscriptions (process_id, amm_process_id) VALUES (:process_id, :amm_process_id);
    ]]
    stmt:bind_names({ process_id = processId, amm_process_id = ammProcessId })
    dbUtils.execute(stmt, 'registerSwapSubscriber')
end

function mod.unregisterSwapSubscriber(processId, ammProcessId)
    local stmt = db:prepare [[
        DELETE FROM swap_subscriptions WHERE process_id = :process_id AND amm_process_id = :amm_process_id;
    ]]
    stmt:bind_names({ process_id = processId, amm_process_id = ammProcessId })
    dbUtils.execute(stmt, 'unregisterSwapSubscriber')
end

function mod.registerSwapSubscriberHandler(msg)
    local processId = msg.Tags['Process-Id']
    local ammProcessId = msg.Tags['Amm-Process-Id']
    mod.registerSwapSubscriber(processId, ammProcessId)

    ao.send({
        Target = msg.From,
        Action = 'Swap-Subscription-Success',
        ['Amm-Process-Id'] = ammProcessId
    })
end

function mod.unregisterSwapSubscriberHandler(msg)
    local processId = msg.Tags['Process-Id']
    local ammProcessId = msg.Tags['Amm-Process-Id']
    mod.unregisterSwapSubscriber(processId, ammProcessId)

    ao.send({
        Target = msg.From,
        Action = 'Swap-Unsubscription-Success',
        ['Amm-Process-Id'] = ammProcessId
    })
end

return mod
