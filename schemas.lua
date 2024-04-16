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

return schemas;