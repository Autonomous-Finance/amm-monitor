local utils = require ".utils"
local json = require "json"

local function newmodule(selfId, env)
  local ao = {}
  ao.id = selfId
  ao.env = env

  local _my = {}

  --[[
    if message is for the process we're testing, handle according to globally defined handlers
    otherwise, use simplified mock handling with dedicated module representing the target process

    @param rawMsg table with key-value pairs representing
    {
      Target = string, -- process id
      From = string, -- process id or wallet id; if not provided, defaults to self
      Data = string, -- message data
      Tags = table, -- key-value pairs representing message tags
      TagName1 = TagValue1, -- tag key-value pair of strings
      TagName2 = TagValue2, -- tag key-value pair of strings
    }
  ]]
  function ao.send(rawMsg)
    if _G.IsInUnitTest then return end

    local msg = _my.formatMsg(rawMsg)

    if _G.AllMessages then
      table.insert(_G.AllMessages, msg)
    end

    if msg.Target == _G.Owner then
      _G.LastMsgToOwner = msg
      return
    end

    if msg.Target == _G.User then
      _G.LastMsgToUser = msg
      return
    end

    if msg.Assignments and utils.includes(_G.User, msg.Assignments) then
      _G.LastMsgToUser = msg
      return
    end

    if msg.Tags.Action == 'LogInfo' then
      table.insert(_G.AgentLogInfos, msg.Data)
    end

    if msg.Tags.Action == 'LogError' then
      table.insert(_G.AgentLogErrors, msg.Data)
    end

    if msg.Target == _G.MainProcessId then
      _G.Handlers.evaluate(msg, _my.env)
    else
      local targetProcess = _G.Processes[msg.Target]
      if targetProcess then
        targetProcess.handle(msg)
      else
        printVerb(2)('⚠️ !!! No handler found for target: ' .. msg.Target)
        printVerb(2)('Message: ' .. json.encode(msg))
      end
    end
  end

  function ao.spawn(msg)
    printVerb(2)('Message: ' .. json.encode(msg))
  end

  -- INTERNAL

  _my.env = {
    Process = {
      Id = '9876',
      Tags = {
        {
          name = 'Data-Protocol',
          value = 'ao'
        },
        {
          name = 'Variant',
          value = 'ao.TN.1'
        },
        {
          name = 'Type',
          value = 'Process'
        }
      }
    },
    Module = {
      Id = '4567',
      Tags = {
        {
          name = 'Data-Protocol',
          value = 'ao'
        },
        {
          name = 'Variant',
          value = 'ao.TN.1'
        },
        {
          name = 'Type',
          value = 'Module'
        }
      }
    }
  }

  _my.createMsg = function()
    return {
      Id = '1234',
      Target = 'AOS',
      Owner = "fcoN_xJeisVsPXA-trzVAuIiqO3ydLQxM-L4XbrQKzY",
      From = 'OWNER',
      Data = '1984',
      Tags = {},
      ['Block-Height'] = '1',
      Timestamp = _G.VirtualTime or os.time(),
      Module = '4567'
    }
  end

  _my.formatMsg = function(msg)
    local formattedMsg = _my.createMsg()
    formattedMsg.From = msg.From or ao.id
    formattedMsg.Data = msg.Data or nil
    formattedMsg.Tags = msg.Tags or formattedMsg.Tags
    formattedMsg.Timestamp = msg.Timestamp or formattedMsg.Timestamp

    for k, v in pairs(msg) do
      if not formattedMsg[k] then
        formattedMsg.Tags[k] = v
      end

      formattedMsg[k] = v
    end

    return formattedMsg
  end

  return ao
end

return newmodule
