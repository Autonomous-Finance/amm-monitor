local json = require('json')
local dbUtils = require('db.utils')

local mod = {}

function mod.getSwapSubscribers(ammProcessId)
    local stmt = [[
      SELECT process_id FROM swap_subscriptions WHERE amm_process_id = :amm_process_id AND expires_at_ts < :current_ts;
    ]]
    return dbUtils.queryManyWithParams(stmt, { amm_process_id = ammProcessId, current_ts = os.time() })
end

-- todo move this somewhere else
function mod.dispatchSwapNotifications(sourceMessageId, sourceAmm)
    local subscribers = mod.getSwapSubscribers(sourceAmm)

    -- todo add balance check
    local stmt = [[
      SELECT * FROM amm_transactions_view WHERE id = :id LIMIT 1;
    ]]
    local transformedSwapData = dbUtils.queryOneWithParams(stmt, { id = sourceMessageId })
    for _, subscriber in ipairs(subscribers) do
        ao.send({
            Target = subscriber.process_id,
            Action = 'Dexi-Swap-Notification',
            Data = json.encode(transformedSwapData)
        })
    end
end

function mod.registerSwapSubscriber(processId, ammProcessId, expiresAtTs)
    local stmt = db:prepare [[
        INSERT INTO swap_subscriptions (process_id, amm_process_id, expires_at_ts, subscribed_at_ts)
        VALUES (:process_id, :amm_process_id, :expires_at_ts, :subscribed_at_ts);
    ]]
    local expiresAtTs = math.max(os.time() + 60 * 60 * 24 * 3, expiresAtTs)
    stmt:bind_names({
        process_id = processId,
        amm_process_id = ammProcessId,
        expires_at_ts = expiresAtTs,
        subscribed_at_ts = os.time()
    })
    dbUtils.execute(stmt, 'registerSwapSubscriber')
end

function mod.unregisterSwapSubscriber(processId, ammProcessId)
    local stmt = db:prepare [[
        DELETE FROM swap_subscriptions WHERE process_id = :process_id AND amm_process_id = :amm_process_id;
    ]]
    stmt:bind_names({ process_id = processId, amm_process_id = ammProcessId })
    dbUtils.execute(stmt, 'unregisterSwapSubscriber')
end

function mod.unregisterAllSwapSubscribersToAmm(ammProcessId)
    local stmt = db:prepare [[
        DELETE FROM swap_subscriptions WHERE amm_process_id = :amm_process_id;
    ]]
    stmt:bind_names({ amm_process_id = ammProcessId })
    dbUtils.execute(stmt, 'unregisterAllSwapSubscribersToAmm')
end

function mod.registerSwapSubscriberHandler(msg)
    local processId = msg.Tags['Process-Id']
    local ammProcessId = msg.Tags['Amm-Process-Id']
    assert(ammProcessId, 'Amm-Process-Id is required')
    assert(processId, 'Process-Id is required')

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
    assert(ammProcessId, 'Amm-Process-Id is required')
    assert(processId, 'Process-Id is required')

    mod.unregisterSwapSubscriber(processId, ammProcessId)

    ao.send({
        Target = msg.From,
        Action = 'Swap-Unsubscription-Success',
        ['Amm-Process-Id'] = ammProcessId,
        ['Process-Id'] = processId
    })
end

return mod
