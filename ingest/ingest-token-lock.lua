local json = require('json')
local dbUtils = require('db.utils')
local lookups = require('dexi-core.lookups')
local bint = require('.bint')(256)

local ingestTokenLock = {}

-- SQL functions

local function insertOrUpdateLockedTokens(entry)
    local stmt = db:prepare [[
    INSERT INTO locked_tokens (
      id, locked_by, locked_token, initial_locked_value, current_locked_value,
      locked_period, locked_until, locked_at_ts
    ) VALUES (
      :id, :locked_by, :locked_token, :initial_locked_value, :current_locked_value,
      :locked_period, :locked_until, :locked_at_ts
    )
    ON CONFLICT(id) DO UPDATE SET
      current_locked_value = :current_locked_value
    WHERE current_locked_value > 0;
  ]]

    if not stmt then
        error("Failed to prepare SQL statement: " .. db:errmsg())
    end

    stmt:bind_names(entry)
    dbUtils.execute(stmt, "insertOrUpdateLockedTokens")
end

local function getLockedTokenEntry(id)
    local stmt = db:prepare [[
    SELECT * FROM locked_tokens
    WHERE id = :id;
  ]]
    local row = dbUtils.queryOneWithParams(stmt, { id = id })

    return row
end

-- Handler functions

function ingestTokenLock.handleLockNotification(msg)
    assert(msg.Tags["Id"], "Missing Id tag")
    assert(msg.Tags["Locked-By"], "Missing Locked-By tag")
    assert(msg.Tags["Locked-Token"], "Missing Locked-Token tag")
    assert(msg.Tags["Locked-Value"], "Missing Locked-Value tag")
    assert(msg.Tags["Locked-Period"], "Missing Locked-Period tag")
    assert(msg.Tags["Locked-Until"], "Missing Locked-Until tag")
    assert(msg.Tags["Current-Timestamp"], "Missing Current-Timestamp tag")

    local lockedToken = msg.Tags["Locked-Token"]

    -- Check if the locked token is a registered AMM
    if not lookups.ammInfo(lockedToken) then
        print('Ignoring lock notification for non-AMM token: ' .. lockedToken)
        return
    end

    local entry = {
        id = msg.Tags["Id"],
        locked_by = msg.Tags["Locked-By"],
        locked_token = lockedToken,
        initial_locked_value = msg.Tags["Locked-Value"],
        current_locked_value = msg.Tags["Locked-Value"],
        locked_period = tonumber(msg.Tags["Locked-Period"]),
        locked_until = tonumber(msg.Tags["Locked-Until"]),
        locked_at_ts = tonumber(msg.Tags["Current-Timestamp"])
    }

    print('Recording locked tokens ' .. json.encode(entry))
    insertOrUpdateLockedTokens(entry)
end

function ingestTokenLock.handleClaimNotification(msg)
    assert(msg.Tags["Id"], "Missing Id tag")
    assert(msg.Tags["Claimed-By"], "Missing Claimed-By tag")
    assert(msg.Tags["Claimed-Token"], "Missing Claimed-Token tag")
    assert(msg.Tags["Claimed-Quantity"], "Missing Claimed-Quantity tag")

    if not lookups.ammInfo(msg.tags['Claimed-Token']) then
        print('Ignoring claim notification for non-AMM token: ' .. msg.tags['Claimed-Token'])
        return
    end

    local lockedEntry = getLockedTokenEntry(msg.Tags["Id"])

    if lockedEntry then
        local newLockedValue = bint(lockedEntry.current_locked_value) - bint(msg.Tags["Claimed-Quantity"])

        if newLockedValue > 0 then
            local updatedEntry = {
                id = lockedEntry.id,
                locked_by = lockedEntry.locked_by,
                locked_token = lockedEntry.locked_token,
                initial_locked_value = lockedEntry.initial_locked_value,
                current_locked_value = tostring(newLockedValue),
                locked_period = lockedEntry.locked_period,
                locked_until = lockedEntry.locked_until,
                locked_at_ts = lockedEntry.locked_at_ts
            }

            print('Updating locked tokens ' .. json.encode(updatedEntry))
            insertOrUpdateLockedTokens(updatedEntry)
        else
            -- Delete the record if nothing is locked
            local stmt = db:prepare [[
                DELETE FROM locked_tokens
                WHERE id = :id;
            ]]

            stmt:bind_names({ id = lockedEntry.id })
            dbUtils.execute(stmt, "deleteLockedTokens")
            print('Deleting locked tokens record for ' .. lockedEntry.id)
        end
    else
        print('No locked tokens found for ID: ' .. msg.Tags["Id"])
    end
end

return ingestTokenLock
