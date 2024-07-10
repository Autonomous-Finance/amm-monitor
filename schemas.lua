local v = require("validation")

local schemas = {}

schemas.ammInputMessageSchema = v.is_table({
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
        ['Fee'] = v.is_string(),
        ['Reserve-Base'] = v.is_string(),
        ['Reserve-Quote'] = v.is_string(),
    }, true)
}, true)

schemas.dexOrderMessageSchema = v.is_table({
    Id = v.is_string(),
    ['Block-Height'] = v.is_number(),
    ['Block-Id'] = v.optional(v.is_string()),
    From = v.is_string(),
    Timestamp = v.optional(v.is_number()),
    Tags = v.is_table({
        ['Order-Type'] = v.is_string(),
        ['Order-Status'] = v.is_string,
        ['Order-Side'] = v.is_string(),
        ['Original-Quantity'] = v.is_string(),
        ['Executed-Quantity'] = v.is_string(),
        ['Price'] = v.is_string(),
        ['Wallet'] = v.is_string(),
    }, true)
}, true)

schemas.dexTradeMessageSchema = v.is_table({
    Id = v.is_string(),
    ['Block-Height'] = v.is_number(),
    ['Block-Id'] = v.optional(v.is_string()),
    From = v.is_string(),
    Timestamp = v.optional(v.is_number()),
    Tags = v.is_table({
        ['Original-Quantity'] = v.is_string(),
        ['Executed-Quantity'] = v.is_string,
        ['Price'] = v.is_string(),
        ['Maker-Fees'] = v.is_string(),
        ['Taker-Fees'] = v.is_string(),
        ['Is-Buyer-Taker'] = v.is_string(),
        ['Order-Id'] = v.is_string(),
        ['Match-With'] = v.is_string(),
    }, true)
}, true)

return schemas;
