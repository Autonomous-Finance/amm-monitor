local json = require "json"

local mod = {}

function mod.sendReply(msg, data, tags)
  msg.reply({
    Action = msg.Tags.Action .. "-Response",
    Tags = tags,
    Data = json.encode(data)
  })
end

return mod
