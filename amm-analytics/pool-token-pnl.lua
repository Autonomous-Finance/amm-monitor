local dbUtils = require("db.utils")
local json = require("json")
local hopper = require("hopper.hopper")

local analytics = {}

function analytics.getCurrentTvl(ammProcess)
    local stmt = db:prepare([[
        SELECT
            reserves_0,
            reserves_1,
            t0.denominator as demoninator0,
            t1.denominator as demoninator1,
            amm_token0 as token0,
            amm_token1 as token1
        FROM amm_transactions_view
        JOIN token_registry t0 ON t0.token_process = amm_token0
        JOIN token_registry t1 ON t1.token_process = amm_token1
        WHERE amm_process = :amm_process
        ORDER BY created_at_ts DESC
    ]])

    if not stmt then
        error("Err: " .. db:errmsg())
    end

    stmt:bind_names({ amm_process = ammProcess })

    local result = dbUtils.queryOne(stmt)

    if not result then
        return nil
    end

    local reserves_0 = result.reserves_0
    local reserves_1 = result.reserves_1
    local denominator0 = result.demoninator0
    local denominator1 = result.demoninator1

    local value0 = reserves_0 / 10 ^ denominator0
    local value1 = reserves_1 / 10 ^ denominator1

    local price0 = hopper.getPrice(result.token0, 'USD')
    local price1 = hopper.getPrice(result.token1, 'USD')

    local value0 = value0 * price0
    local value1 = value1 * price1

    return value0 + value1
end

function analytics.getPoolVolume(ammProcess, since)
    local stmt = db:prepare([[
        SELECT
            SUM(volume_usd) as volume_usd
        FROM amm_transactions_view
        WHERE amm_process = :amm_process AND created_at_ts > :since
    ]])

    if not stmt then
        error("Err: " .. db:errmsg())
    end

    stmt:bind_names({ amm_process = ammProcess, since = since })

    local result = dbUtils.queryOne(stmt)
    return result.volume_usd
end

function analytics.getHistoricalTvlForPool(ammProcess, since)
    local stmt = db:prepare([[
        select
            amm_process,
            date(created_at_ts, 'unixepoch') as dt,
            max(
                reserves_0 / pow(10, token0_denominator) * token0_usd_price
                + reserves_1 / pow(10, token1_denominator) * token1_usd_price
            ) as tvl,
            sum(volume_usd) * lp_fee_percentage as total_fees,
            sum(volume_usd) as volume_usd
        from amm_transactions_view
        where amm_process = :amm_process and created_at_ts > :since
        group by amm_process, date(created_at_ts, 'unixepoch')
    ]])
    if not stmt then
        error("Err: " .. db:errmsg())
    end

    stmt:bind_names({ amm_process = ammProcess, since = since })

    return dbUtils.queryMany(stmt)
end

function analytics.getHistricalProfitForPools(ammProcesses, since)
    -- get pools for user with initial tvl
    -- for each pool get historical tvl with forward fill since last change multiplied by user share
end

function analytics.forwardFillTvlForPool(ammProcess, since, userShare)
    local poolTvl = analytics.getHistoricalTvlForPool(ammProcess, since)
    local lastTvl = poolTvl[1].tvl

    for _, tvl in ipairs(poolTvl) do
        if tvl.tvl == nil then
            tvl.tvl = lastTvl
            if userShare then
                tvl.tvl_user = tvl.tvl * userShare
            end
        end
    end

    return poolTvl
end

function analytics.getPoolFees(ammProcess, since)
    local stmt = db:prepare([[
        SELECT
            SUM(volume_usd) as volume_usd
        FROM amm_transactions_view
        WHERE amm_process = :amm_process AND created_at_ts > :since
    ]])

    if not stmt then
        error("Err: " .. db:errmsg())
    end

    stmt:bind_names({ amm_process = ammProcess, since = since })

    local result = dbUtils.queryOne(stmt)
    return result.volume_usd
end

function analytics.calculatePnlForUserAndAmm(user, currentTimestamp)
    local pools = analytics.getPoolTokensForUser(user)

    for _, pool in ipairs(pools) do
        pool.current_tvl = analytics.getCurrentTvl(pool.amm_process)
        pool.initial_user_tvl = analytics.getInitalTvlForUserAndAmm(pool.amm_process) * pool.user_share
        pool.current_user_tvl = pool.current_tvl * pool.user_share
        pool.total_volume = analytics.getPoolVolume(pool.amm_process, 0)
        pool.volume24h = analytics.getPoolVolume(pool.amm_process, currentTimestamp - 24 * 60 * 60)
        pool.volume24hAgo = analytics.getPoolVolume(pool.amm_process, currentTimestamp - 48 * 60 * 60)
        pool.user_fees = analytics.getPoolFees(pool.amm_process, pool.last_change_ts) * pool.user_share
        if pool.current_tvl then
            pool.total_apy = pool.user_fees / pool.current_tvl
            pool.pnl = pool.current_tvl - pool.initial_tvl
        end
        pool.historical_pnl = analytics.getHistoricalPnlForPool(pool.amm_process, pool.last_change_ts - 7 * 24 * 60 * 60,
            pool.user_share)
    end

    return pools
end

function analytics.getInitalTvlForUserAndAmm(ammProcess)
    local stmt = db:prepare([[
        select tvl_in_usd from reserve_changes where amm_process = :amm_process
    ]])

    if not stmt then
        error("Err: " .. db:errmsg())
    end

    stmt:bind_names({
        amm_process = ammProcess,
    })

    local result = dbUtils.queryOne(stmt)
    return result.tvl_in_usd
end

function analytics.getPoolTokensForUser(user)
    local stmt = db:prepare([[
    with pool_token_balances as (
        select
            amm_process,
            recipient,
            sum(transfer_quantity) as user_total_tokens,
            max(created_at_ts) as last_change_ts
        from reserve_changes
        where recipient = :recipient
        group by amm_process, recipient
    ), latest_pool_token_balances as (
        SELECT
            amm_process,
            SUM(CAST(delta_pool_tokens AS NUMERIC)) AS total_pool_tokens
        FROM reserve_changes
        where recipient = :recipient
        GROUP BY amm_process
    )
    select *, user_total_tokens / CAST(total_pool_tokens AS NUMERIC) as user_share
    from pool_token_balances
        left join latest_pool_token_balances using (amm_process)
    ]])

    if not stmt then
        error("Err: " .. db:errmsg())
    end

    stmt:bind_names({ recipient = user })


    return dbUtils.queryMany(stmt)
end

function analytics.getPoolPnlHistoryForUser(msg)
    assert(msg.Tags.User, "User is required")
    local result = analytics.calculatePnlForUserAndAmm(msg.Tags.User, math.floor(msg.Timestamp))

    ao.send({
        ['Response-For'] = 'Get-Pool-Pnl-History',
        ['Target'] = msg.From,
        ['Data'] = json.encode(result)
    })
end

return analytics
