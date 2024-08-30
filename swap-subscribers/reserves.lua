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

function mod.getSwapParamsMessage(sourceAmm, sourceMessageId)
    local condition = 'WHERE amm_process = :source_amm ORDER BY created_at_ts DESC'
    if sourceMessageId then
        condition = 'WHERE id = :id'
    end

    local stmt = db:prepare([[
        SELECT
            amm_token0 as token0,
            amm_token1 as token1,
            reserves_0 as reserves0,
            reserves_1 as reserves1
        FROM amm_swap_params_changes
        LEFT JOIN amm_registry USING (amm_process_id)
        ]] .. condition .. [[ LIMIT 1;]])

    stmt:bind_names({ id = sourceMessageId, source_amm = sourceAmm })
    local transformedSwapData = dbUtils.queryOne(stmt)
    return transformedSwapData
end

function mod.dispatchSwapParamsNotifications(sourceMessageId, sourceAmm)
    local subscribers = mod.getSwapParamsSubscribers(sourceAmm)
    local transformedSwapData = mod.getSwapParamsMessage(sourceAmm, sourceMessageId)
    for _, subscriber in ipairs(subscribers) do
        ao.send({
            Target = subscriber.process_id,
            Action = 'Dexi-Swap-Params-Change-Notification',
            Data = json.encode(transformedSwapData),
            ['Amm-Process-Id'] = sourceAmm,
            ['Source-Message-Id'] = sourceMessageId,
            ['Token-0'] = tostring(transformedSwapData.token0),
            ['Token-1'] = tostring(transformedSwapData.token1),
            ['Reserves-0'] = tostring(transformedSwapData.reserves0),
            ['Reserves-1'] = tostring(transformedSwapData.reserves1)
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

    -- local swapParamsMessage = mod.getSwapParamsMessage(ammProcessId, nil)
    ao.send({
        Target = msg.From,
        Action = 'Reserve-Change-Subscription-Success',
        ['Amm-Process-Id'] = ammProcessId,
        ['Process-Id'] = processId,
        -- Data = swapParamsMessage.Data
    })

    ao.send({
        Target = processId,
        Action = 'Reserve-Change-Subscription-Success',
        ['Amm-Process-Id'] = ammProcessId,
        ['Process-Id'] = processId,
        -- Data = swapParamsMessage.Data
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
