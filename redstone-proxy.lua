local json = require("json")

DEXI_PROCESS = 'snR3flTItDdCtCqHrJBWTV2vg0kj0onq2Wydlu-crhc'
DEXI_PROCESS_V2 = 'UPnzIDH1HnxPOPgUvVyccPAwUwCcB1JjEOTR89LYTzM'

Handlers.add("Cron-Tick", Handlers.utils.hasMatchingTag("Action", "Cron-Tick"), function(message)
    ao.send({
        Target = '4fVi8P-xSRWxZ0EE0EpltDe8WJJvcD9QyFXMqfk-1UQ', -- the Oracle Storage Process id
        Action = "Request-Latest-Data",                         -- 'Request-Latest-Data' can be used for backwards compatibility
        Tickers = json.encode({ "AR" })                         -- required prices
    })
end)


Handlers.add("Receive-RedStone-Prices", Handlers.utils.hasMatchingTag("Action", "Receive-RedStone-Prices"),
    function(message)
        ao.send({
            Target = DEXI_PROCESS,
            Action = "Receive-RedStone-Prices",
            Data = message.Data
        })
        ao.send({
            Target = DEXI_PROCESS_V2,
            Action = "Receive-RedStone-Prices",
            Data = message.Data
        })
    end)
