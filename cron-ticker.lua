-- cron-ticker.lua

-- Table to store balances
Balances = {}

-- Table to store subscriptions
Subscriptions = {}

-- Constants
CRON_TICK_INTERVAL = 60 -- 1 minute
DEDUCTION_AMOUNT = 0 -- Can be changed later

-- Function to update balances
function UpdateBalance(user_id, amount)
    if not Balances[user_id] then
        Balances[user_id] = amount
    else
        Balances[user_id] = Balances[user_id] + amount
    end
end

-- Function to handle cron tick
function CronTick()
    for user_id, balance in pairs(Balances) do
        new_balance = balance - DEDUCTION_AMOUNT
        Balances[user_id] = new_balance

        if new_balance > 0 then
            for _, subscription in ipairs(Subscriptions[user_id]) do
                DispatchMessage(subscription, "Cron tick for user " .. user_id)
            end
        end
    end
end

-- Function to subscribe to cron ticks
function Subscribe(user_id, process_id)
    if not Subscriptions[user_id] then
        Subscriptions[user_id] = {}
    end
    table.insert(Subscriptions[user_id], process_id)
end

-- Function to dispatch message (placeholder)
function DispatchMessage(process_id, message)
    print("Dispatching message to " .. process_id .. ": " .. message)
end