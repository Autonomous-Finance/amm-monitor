local json = require('json')
local dbUtils = require('db.utils')

local mod = {}

function mod.getSwapParamsSubscribers(ammProcessId)
    local stmt = db:prepare([[
      SELECT process_id FROM reserve_change_subscriptions WHERE amm_process_id = :amm_process_id;
    ]])
    stmt:bind_names({ amm_process_id = ammProcessId })
    return dbUtils.queryMany(stmt)
end

-- todo move this somewhere else
function mod.dispatchSwapParamsNotifications(sourceMessageId, sourceAmm)
    local subscribers = mod.getSwapParamsSubscribers(sourceAmm)

    -- todo add balance check
    local stmt = db:prepare [[
    SELECT * FROM amm_swap_params_changes
    LEFT JOIN amm_registry USING (amm_process)
    WHERE id = :id LIMIT 1;
    ]]
    stmt:bind_names({ id = sourceMessageId })
    local transformedSwapData = dbUtils.queryOne(stmt)
    for _, subscriber in ipairs(subscribers) do
        ao.send({
            Target = subscriber.process_id,
            Action = 'Dexi-Swap-Params-Change-Notification',
            Data = json.encode(transformedSwapData)
        })
    end
end

function mod.registerSwapParamsSubscriber(processId, ammProcessId, subscribedAtTs)
    local stmt = db:prepare [[
        INSERT INTO reserve_change_subscriptions (process_id, amm_process_id, subscribed_at_ts) VALUES (:process_id, :amm_process_id, :subscribed_at_ts);
    ]]
    stmt:bind_names({ process_id = processId, amm_process_id = ammProcessId, subscribed_at_ts = subscribedAtTs })
    dbUtils.execute(stmt, 'registerSwapParamsSubscriber')
end

function mod.unregisterSwapParamsSubscriber(processId, ammProcessId)
    local stmt = db:prepare [[
        DELETE FROM reserve_change_subscriptions WHERE process_id = :process_id AND amm_process_id = :amm_process_id;
    ]]
    stmt:bind_names({ process_id = processId, amm_process_id = ammProcessId })
    dbUtils.execute(stmt, 'unregisterSwapParamsSubscriber')
end

function mod.registerSwapParamsSubscriberHandler(msg)
    local processId = msg.Tags['Process-Id']
    local ammProcessId = msg.Tags['Amm-Process-Id']
    assert(ammProcessId, 'Amm-Process-Id is required')
    assert(processId, 'Process-Id is required')

    mod.registerSwapParamsSubscriber(processId, ammProcessId, math.floor(msg.Timestamp / 1000))

    ao.send({
        Target = msg.From,
        Action = 'Reserve-Change-Subscription-Success',
        ['Amm-Process-Id'] = ammProcessId,
        ['Process-Id'] = processId
    })
end

function mod.unregisterSwapParamsSubscriberHandler(msg)
    local processId = msg.Tags['Process-Id']
    local ammProcessId = msg.Tags['Amm-Process-Id']
    assert(ammProcessId, 'Amm-Process-Id is required')
    assert(processId, 'Process-Id is required')

    mod.unregisterSwapParamsSubscriber(processId, ammProcessId)

    ao.send({
        Target = msg.From,
        Action = 'Swap-Params-Unsubscription-Success',
        ['Amm-Process-Id'] = ammProcessId,
        ['Process-Id'] = processId
    })
end

return mod
