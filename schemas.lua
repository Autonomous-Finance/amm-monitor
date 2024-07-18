local v = require("validation")

local schemas = {}

schemas.inputMessageSchema = v.is_table({
    Id = v.is_string(),
    ['Block-Height'] = v.is_number(),
    ['Block-Id'] = v.optional(v.is_string()),
    From = v.is_string(),
    Timestamp = v.optional(v.is_number()),
    Tags = v.is_table({
        ['To-Token'] = v.is_string(),
        ['From-Token'] = v.is_string(),
        ['From-Quantity'] = v.is_string(),
        ['To-Quantity'] = v.is_string(),
        ['Fee'] = v.is_string()
    }, true)
}, true)

schemas.swapParamsSchema = function(reserves_0, reserves_1, fee_percentage)
    local isString0, err0 = v.is_bint_string()(reserves_0)
    local isString1, err1 = v.is_bint_string()(reserves_1)
    local isStringFee, errFee = v.is_float_string()(fee_percentage)

    local areAllValid = isString0 and isString1 and isStringFee

    if not areAllValid then
        return false, err0 or err1 or errFee
    end

    return true
end

return schemas;
