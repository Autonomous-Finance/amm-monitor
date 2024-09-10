local dbUtils = require("db.utils")
local json = require("json")
local hopper = require("hopper.hopper")

local analytics = {}

-- replace with amm_swap_params instead of transactions_view
function analytics.getCurrentTvl(ammProcess)
    local stmt = db:prepare([[
        SELECT
            reserves_0,
            reserves_1,
            t0.denominator as demoninator0,
            t1.denominator as demoninator1,
            amm_token0 as token0,
            amm_token1 as token1
        FROM amm_registry
        JOIN amm_swap_params USING (amm_process)
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

function analytics.getPoolVolume(ammProcess, since, till)
    if not till then
        till = 99999999999
    end

    local stmt = db:prepare([[
        SELECT
            SUM(volume_usd) as volume_usd
        FROM amm_transactions_view
        WHERE amm_process = :amm_process AND created_at_ts > :since AND created_at_ts < :till
    ]])

    if not stmt then
        error("Err: " .. db:errmsg())
    end

    stmt:bind_names({ amm_process = ammProcess, since = since, till = till })

    local result = dbUtils.queryOne(stmt)
    return result.volume_usd
end

function analytics.getTvlAtDate(ammProcess, date)
    local stmt = [[
        with reserves as (
            select
                reserves_0,
                reserves_1,
                token0_denominator,
                token1_denominator,
                token0_usd_price,
                token1_usd_price
            from amm_transactions_view
            where amm_process = :amm_process and created_at_ts < :date
            order by created_at_ts desc
            limit 1
        )
        select
            amm_process,
            date(created_at_ts, 'unixepoch') as dt,
            max(
                reserves_0 / pow(10, token0_denominator) * token0_usd_price
                + reserves_1 / pow(10, token1_denominator) * token1_usd_price
            ) as tvl
        from reserves
        order by created_at_ts
    ]]

    return dbUtils.queryManyWithParams(stmt, { amm_process = ammProcess, date = date })
end

function analytics.getHistoricalTvlForPool(ammProcess, since)
    local currentTime = math.floor(os.time() / 1000)
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
        order by created_at_ts
    ]])
    if not stmt then
        error("Err: " .. db:errmsg())
    end

    stmt:bind_names({ amm_process = ammProcess, since = since })

    local result = dbUtils.queryMany(stmt)

    -- Add current TVL
    local currentTvl = analytics.getCurrentTvl(ammProcess)
    local currentDate = os.date("!%Y-%m-%d", currentTime)

    for i = #result, 1, -1 do
        if result[i].dt == currentDate then
            -- Replace the last entry for the current date with the current TVL
            result[i].tvl = currentTvl
            break
        end
    end

    -- always add latest
    table.insert(result, {
        amm_process = ammProcess,
        dt = currentDate,
        tvl = currentTvl,
        total_fees = 0,
        volume_usd = 0
    })

    return result
end

function analytics.getForwardFilledTvlForPool(ammProcess, since, userShare)
    local poolTvl = analytics.getHistoricalTvlForPool(ammProcess, since)
    local lastUserTvl = poolTvl[1].tvl * userShare
    poolTvl[1].tvl_user = lastUserTvl
    poolTvl[1].pnl_user = 0
    poolTvl[1].pnl_percent = 0

    for i = 2, #poolTvl do
        local tvl = poolTvl[i]
        local userTvl = tvl.tvl * userShare
        tvl.tvl_user = userTvl
        local pnlUser = userTvl - lastUserTvl
        tvl.pnl_user = pnlUser
        lastUserTvl = userTvl
        tvl.pnl_percent = pnlUser / lastUserTvl
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

function analytics.groupHistoricalPnlByDay(historicalPnlByDay, historicalPnl)
    -- Group the data by date and sum the pnl_user values
    for _, entry in ipairs(historicalPnl) do
        local date = entry.dt
        local pnl = entry.pnl_user

        if historicalPnlByDay[date] then
            historicalPnlByDay[date] = historicalPnlByDay[date] + pnl
        else
            historicalPnlByDay[date] = pnl
        end
    end
end

function analytics.calculatePnlForUserAndAmm(user)
    local currentTimestamp = math.floor(os.time() / 1000)
    local pools = analytics.getPoolTokensForUser(user)
    local historicalPnlByDay = {}

    for _, pool in ipairs(pools) do
        pool.current_tvl = analytics.getCurrentTvl(pool.amm_process) or 0
        pool.initial_user_tvl = analytics.getInitalTvlForUserAndAmm(pool.amm_process, user) * pool.user_share
        pool.current_user_tvl = pool.current_tvl * pool.user_share
        pool.total_volume = analytics.getPoolVolume(pool.amm_process, 0)
        pool.volume24h = analytics.getPoolVolume(pool.amm_process, currentTimestamp - 24 * 60 * 60)
        pool.volume24hAgo = analytics.getPoolVolume(pool.amm_process, currentTimestamp - 48 * 60 * 60,
            currentTimestamp - 24 * 60 * 60)
        pool.user_fees = analytics.getPoolFees(pool.amm_process, pool.last_change_ts) * pool.user_share
        if pool.current_tvl then
            pool.total_apy = pool.current_user_tvl / pool.initial_user_tvl
            pool.pnl = pool.current_user_tvl - pool.initial_user_tvl
        end
        pool.historical_pnl = analytics.getForwardFilledTvlForPool(pool.amm_process,
            pool.last_change_ts - 7 * 24 * 60 * 60,
            pool.user_share)

        local thirty_days_ago = currentTimestamp - 30 * 24 * 60 * 60
        if pool.last_change_ts < thirty_days_ago then
            -- todo pick this from transactions_view
            pool.tvl_user_30d_ago = analytics.getTvlAtDate(pool.amm_process, thirty_days_ago).tvl * pool.user_share
        else
            pool.tvl_user_30d_ago = pool.initial_user_tvl
        end
        pool.pnl_30d_ago = pool.current_user_tvl - pool.tvl_user_30d_ago
        pool.pnl_30d_percentage = pool.pnl_30d_ago / pool.tvl_user_30d_ago
        analytics.groupHistoricalPnlByDay(historicalPnlByDay, pool.historical_pnl)
    end



    return {
        pools = pools,
        total_historical_pnl_by_day = historicalPnlByDay
    }
end

function analytics.getInitalTvlForUserAndAmm(ammProcess, user)
    local stmt = db:prepare([[
        select tvl_in_usd from reserve_changes where amm_process = :amm_process and recipient = :recipient order by created_at_ts limit 1
    ]])

    if not stmt then
        error("Err: " .. db:errmsg())
    end

    stmt:bind_names({
        amm_process = ammProcess,
        recipient = user
    })

    local result = dbUtils.queryOne(stmt)
    return result.tvl_in_usd
end

function analytics.getLastPoolTokenAmount(ammProcess)
    local stmt = [[
        select
            total_pool_tokens
        from reserve_changes
        where amm_process = :amm_process
        order by created_at_ts desc
        limit 1
    ]]

    local result = dbUtils.queryOneWithParams(stmt, { amm_process = ammProcess })
    if result then
        return result.total_pool_tokens
    else
        return nil
    end
end

function analytics.getPoolTokensForUser(user)
    local stmt = db:prepare([[
        SELECT
            amm_process,
            recipient,
            SUM(transfer_quantity) AS user_total_tokens,
            MAX(created_at_ts) AS last_change_ts
        FROM reserve_changes
        WHERE recipient = :recipient
        GROUP BY amm_process, recipient
    ]])

    if not stmt then
        error("Err: " .. db:errmsg())
    end

    stmt:bind_names({ recipient = user })

    local result = dbUtils.queryMany(stmt)

    -- Add total_pool_tokens and user_share to each pool
    for i, pool in ipairs(result) do
        local total_pool_tokens = analytics.getLastPoolTokenAmount(pool.amm_process)
        if total_pool_tokens then
            pool.total_pool_tokens = total_pool_tokens
            pool.user_share = pool.user_total_tokens / total_pool_tokens
        else
            -- Handle case when total_pool_tokens is nil
            pool.total_pool_tokens = nil
            pool.user_share = nil
        end
    end

    return result
end

function analytics.getPoolPnlHistoryForUser(msg)
    assert(msg.Tags.User, "User is required")
    local result = analytics.calculatePnlForUserAndAmm(msg.Tags.User)

    ao.send({
        ['Response-For'] = 'Get-Pool-Pnl-History',
        ['Target'] = msg.From,
        ['Data'] = json.encode(result)
    })
end

return analytics
