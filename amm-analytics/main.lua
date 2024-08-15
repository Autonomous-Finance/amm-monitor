local volume = require("amm-analytics.volume")
local poolOverview = require("amm-analytics.pool-overview")

local analytics = {
    getDailyVolume = volume.getDailyVolume,
    getPoolOverview = poolOverview.getPoolOverview
}

return analytics
