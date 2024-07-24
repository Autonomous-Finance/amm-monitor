local sqlschema = require('db.sqlschema')
local dexiCore = require('dexi-core.dexi-core')

local dbSeed = {}

function dbSeed.createMissingTables()
  db:exec(sqlschema.create_transactions_table)

  db:exec(sqlschema.create_amm_swap_params_changes_table)

  db:exec(sqlschema.create_amm_swap_params_table)
  print("Err: " .. db:errmsg())

  db:exec(sqlschema.create_token_supply_changes_table)
  print("Err: " .. db:errmsg())

  db:exec(sqlschema.create_amm_registry_table)
  print("Err: " .. db:errmsg())

  db:exec("DROP VIEW IF EXISTS amm_transactions_view;")
  print("Err: " .. db:errmsg())

  db:exec(sqlschema.create_transactions_view)
  print("Err: " .. db:errmsg())

  db:exec("DROP VIEW IF EXISTS amm_market_cap_view;")
  print("Err: " .. db:errmsg())

  db:exec(sqlschema.create_market_cap_view)
  print("Err: " .. db:errmsg())

  db:exec(sqlschema.create_balances_table)
  print("Err: " .. db:errmsg())

  db:exec(sqlschema.create_indicator_subscriptions_table)
  print("Err: " .. db:errmsg())

  db:exec(sqlschema.create_top_n_subscriptions_table)
  print("Err: " .. db:errmsg())

  db:exec(sqlschema.create_token_registry_table)
  print("Err: " .. db:errmsg())
end

local function seedAMMs()
  dexiCore.registerAMM('TRUNK/AOCRED', 'vn5lUv8OaevTb45iI_qykad_d9MP69kuYg5mZW1zCHE',
    'Sa0iBLPNyJQrwpTTG-tWLQU-1QeUAJA73DdxGGiKoJc', 'OT9qTE2467gcozb2g8R6D6N3nQS94ENcaAIJfUzHCww', 1712737395)
  dexiCore.registerAMM('0rbit/AOCRED', '2bKo3vwB1Mo5TItmxuUQzZ11JgKauU_n2IZO1G13AIk',
    'Sa0iBLPNyJQrwpTTG-tWLQU-1QeUAJA73DdxGGiKoJc', 'BUhZLMwQ6yZHguLtJYA5lLUa9LQzLXMXRfaq9FVcPJc', 1712737395)
  dexiCore.registerAMM('BARK/AOCRED', 'U3Yy3MQ41urYMvSmzHsaA4hJEDuvIm-TgXvSm-wz-X0',
    'Sa0iBLPNyJQrwpTTG-tWLQU-1QeUAJA73DdxGGiKoJc', '8p7ApPZxC_37M06QHVejCQrKsHbcJEerd3jWNkDUWPQ', 1712737395)
  dexiCore.registerAMM('AFT/AOCRED', 'DCQJwfEQCD-OQYmfgNH4Oh6uGo9eQJbEn6WbNvtrI_k',
    'Sa0iBLPNyJQrwpTTG-tWLQU-1QeUAJA73DdxGGiKoJc', 'SpzpFLkqPGvr5ZFZPbvyAtizthmrJ13lL4VBQIBL0dg', 1712737395)
  dexiCore.registerAMM('EXP/AOCRED', 'IMcN3R14yThfHzgbYzBDuuSpzmow7zGyBHRE3Gwrtsk',
    'Sa0iBLPNyJQrwpTTG-tWLQU-1QeUAJA73DdxGGiKoJc', 'aYrCboXVSl1AXL9gPFe3tfRxRf0ZmkOXH65mKT0HHZw', 1712737395)
end

local function seedTokens()
  dexiCore.registerToken('OT9qTE2467gcozb2g8R6D6N3nQS94ENcaAIJfUzHCww', 'TRUNK', 3, 34198, false, 1712737395)
  dexiCore.registerToken('8p7ApPZxC_37M06QHVejCQrKsHbcJEerd3jWNkDUWPQ', 'BARK', 3, 201047011, false, 1712737395)
  dexiCore.registerToken('SpzpFLkqPGvr5ZFZPbvyAtizthmrJ13lL4VBQIBL0dg', 'AFT', 12, 10000, false, 1712737395)
  dexiCore.registerToken('BUhZLMwQ6yZHguLtJYA5lLUa9LQzLXMXRfaq9FVcPJc', '0rbit', 12, 100109630, false, 1712737395)
  dexiCore.registerToken('aYrCboXVSl1AXL9gPFe3tfRxRf0ZmkOXH65mKT0HHZw', 'EXP', 6, 2782410, false, 1716217288)
  dexiCore.registerToken('Sa0iBLPNyJQrwpTTG-tWLQU-1QeUAJA73DdxGGiKoJc', 'AOCRED', 3, 2782410, false, 1716217288)
end

function dbSeed.seed()
  seedAMMs()
  seedTokens()
end

function dbSeed.handleResetDBState(msg)
  if msg.From ~= Owner then
    error('Only the owner can reset-and-seed the database')
  end

  db:exec("DROP TABLE IF EXISTS amm_transactions;")
  dbSeed.createTableIfNotExists()
  dbSeed.seed()
end

return dbSeed
