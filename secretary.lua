LLAMA_TOKEN_PROCESS = "LLAMA_TOKEN_PROCESS"
HOURLY_EMISSION_LIMIT = 1000000

MESSAGES_TO_SEND = {
    {
        originalMessageId = '1',
        sender = 'wallet',
        amount = 100,
        content = "I want a grant for xyz",
    }
}

LLAMAS = {
    ['process_id'] = {
        busyWithMessage = '1',
        submittedTimestamp = 1231332,
    },
    ['process_id2'] = {
        busyWithMessage = nil,
        submittedTimestamp = nil,
    }
}

EMISSIONS = {}


local function removeMessageAndResetLlama(messageId)
    for i, message in ipairs(MESSAGES_TO_SEND) do
        if message.originalMessageId == messageId then
            table.remove(MESSAGES_TO_SEND, i)
            break
        end
    end
    
    for llamaId, llama in pairs(LLAMAS) do
        if llama.busyWithMessage == messageId then
            llama.busyWithMessage = nil
            llama.submittedTimestamp = nil
            break
        end
    end
end



local function processCreditNotice(msg)
    local messageId = msg.Id
    local sender = msg.From
    local amount = msg.Quantity
    local content = msg.Data
    table.insert(MESSAGES_TO_SEND, {
        originalMessageId = messageId,
        sender = sender,
        amount = amount,
        content = content
    })
end

local function calculateEmissions(grade, currentTime)
    local totalEmissions = 0

    local adjustment = 1
    local latestEmission = EMISSIONS[#EMISSIONS]
    if latestEmission + 3600 > currentTime then
        for i, emission in ipairs(EMISSIONS) do
            if currentTime - emission.timestamp <= 3600 then
                totalEmissions = totalEmissions + emission.amount
            end
        end
        adjustment = HOURLY_EMISSION_LIMIT / math.max(HOURLY_EMISSION_LIMIT/100, totalEmissions) -- 10k
    end
    
    
    return 100 * grade * adjustment
end

local function sendLlamaToken(amount, recipient, currentTime)
    ao.send({
        Target = LLAMA_TOKEN_PROCESS,
        Action = "Transfer",
        Recipient = recipient,
        Quantity = amount
    })
    table.insert(EMISSIONS, {
        amount = amount,
        recipient = recipient,
        timestamp = currentTime
    })
end


local function clearExpiredLlamas(currentTime)
    for llamaId, llama in pairs(LLAMAS) do
        if llama.busyWithMessage and currentTime - llama.submittedTimestamp >= 60 then
            llama.busyWithMessage = nil
            llama.submittedTimestamp = nil
        end
    end
end

local function dispatchHighestPriorityMessage(currentTime)
    clearExpiredLlamas(currentTime)
    
    table.sort(MESSAGES_TO_SEND, function(a, b) return a.amount > b.amount end)
    local highestPriorityMessage = table.remove(MESSAGES_TO_SEND, 1)
    if highestPriorityMessage then
        local messageId = highestPriorityMessage.originalMessageId
        local llamaFound = false
        
        for llamaId, llama in pairs(LLAMAS) do
            if llama.busyWithMessage == messageId then
                -- Llama is already busy with this message, abort
                table.insert(MESSAGES_TO_SEND, 1, highestPriorityMessage)
                return
            elseif not llama.busyWithMessage then
                -- Found a non-busy llama, send the message
                llama.busyWithMessage = messageId
                llama.submittedTimestamp = currentTime
                ao.send({
                    Target = llamaId,
                    Action = "Llama-Grade-Request",
                    Data = highestPriorityMessage.content
                })
                llamaFound = true
                break
            end
        end
        
        if not llamaFound then
            table.insert(MESSAGES_TO_SEND, 1, highestPriorityMessage)
        end
    end
end



Handlers.add(
    "CreditNoticeHandler",
    Handlers.utils.hasMatchingTag("Action", "Credit-Notice"),
    function (msg)
        processCreditNotice(msg)
        dispatchHighestPriorityMessage()
    end
)


Handlers.add(
    "LlamaResponseHandler",
    Handlers.utils.hasMatchingTag("Action", "Llama-Response"),
    function (msg)
        local originalMessage = msg.Data
        local grade = tonumber(msg.Grade)
        local recipient = msg['Origial-Sender']
        local originalMessageId = msg['Original-Message-Id']
    
    
        local tokenAmount = calculateEmissions(grade)
        sendLlamaToken(tokenAmount, recipient)
        removeMessageAndResetLlama(originalMessageId)

        ao.send({
            Target = 'GatherTown',
            Action = "Send DM",
            Data = 'You have been granted ' .. tokenAmount .. ' Llama tokens.'
        })
    end
)

