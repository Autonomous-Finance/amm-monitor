local volume = require("amm-analytics.volume")
local poolOverview = require("amm-analytics.pool-overview")
local poolPnl = require("amm-analytics.pool-token-pnl")

local analytics = {
    getDailyVolume = volume.getDailyVolume,
    getPoolOverview = poolOverview.getPoolOverview,
    getPoolPnlHistoryForUser = poolPnl.getPoolPnlHistoryForUser
}

return analytics
