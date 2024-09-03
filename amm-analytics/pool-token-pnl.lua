local dbUtils = require("db.utils")
local json = require("json")
local hopper = require("hopper.hopper")

local analytics = {}

function analytics.getCurrentTvl(ammProcess)
    local stmt = db:prepare([[
        SELECT
            reserves0,
            reserves1,
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

    local reserves0 = result.reserves0
    local reserves1 = result.reserves1
    local denominator0 = result.denominator0
    local denominator1 = result.denominator1

    local value0 = reserves0 / 10 ^ denominator0
    local value1 = reserves1 / 10 ^ denominator1

    local price0 = hopper.getPrice(result.token0, 'USD')
    local price1 = hopper.getPrice(result.token1, 'USD')

    local value0 = value0 * price0
    local value1 = value1 * price1

    return value0 + value1
end

function calculatePnlForUserAndAmm(user)
    local pools = getPoolTokensForUser(user)

    for _, pool in ipairs(pools) do
        local initialTvl = getInitalTvlForUserAndAmm(pool.amm_process) * pool.user_share
        local currentTvl = analytics.getCurrentTvl(pool.amm_process) * pool.user_share
        local pnl = currentTvl - initialTvl
        pool.pnl = pnl
    end

    return pools
end

function getInitalTvlForUserAndAmm(ammProcess)
    local stmt = db:prepare([[
        select tvl_in_usd from reserve_changes where amm_process = :amm_process
    ]])

    if not stmt then
        error("Err: " .. db:errmsg())
    end

    stmt:bind_names({
        amm_process = ammProcess,
    })

    return dbUtils.queryOne(stmt)
end

function getPoolTokensForUser(user)
    local stmt = db:prepare([[
    with pool_token_balances as (
        select
            amm_process,
            recipient,
            sum(transfer_quantity) as user_total_tokens
        from reserve_changes
        group by amm_process, recipient
        where recipient = :recipient
    ), latest_pool_token_balances as (
        SELECT
            amm_process,
            SUM(CAST(delta_pool_tokens AS NUMERIC)) AS total_pool_tokens
        FROM reserve_changes
        GROUP BY amm_process
        where recipient = :recipient
    )
    select *, user_total_tokens / CAST(total_pool_tokens AS NUMERIC) as user_share
    from pool_token_balances
        left join latest_pool_token_balances using (amm_process)
    ]])

    stmt:bind_names({ recipient = user })

    if not stmt then
        error("Err: " .. db:errmsg())
    end

    return dbUtils.queryMany(stmt)
end

function analytics.getPoolPnlHistoryForUser(msg)
    assert(msg.Tags.User, "User is required")
    local result = calculatePnlForUserAndAmm(msg.Tags.User)

    ao.send({
        ['Response-For'] = 'Get-Pool-Pnl-History',
        ['Target'] = msg.From,
        ['Data'] = json.encode(result)
    })
end

return analytics
