local volume = require("amm-analytics.volume")
local poolOverview = require("amm-analytics.pool-overview")
local poolPnl = require("amm-analytics.pool-token-pnl")
local pairFinder = require("amm-analytics.pair-finder")
local lockedTokens = require("amm-analytics.locked-tokens")

local analytics = {
    getDailyVolume = volume.getDailyVolume,
    getPoolOverview = poolOverview.getPoolOverview,
    getPoolPnlHistoryForUser = poolPnl.getPoolPnlHistoryForUser,
    findBestPairsForToken = pairFinder.findBestPairsForToken,
    getOneYearLockedShare = lockedTokens.getOneYearLockedShare,
    getAggregateLockedTokens = lockedTokens.getAggregateLockedTokens,
    getCurrentTvl = poolPnl.getCurrentTvl
}

return analytics
