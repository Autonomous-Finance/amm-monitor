local sqlschema = require('db.sqlschema')
local dexiCore = require('dexi-core.dexi-core')
local dbUtils = require('db.utils')

local dbSeed = {}

function dbSeed.createMissingTables()
  db:exec(sqlschema.create_transactions_table)

  db:exec(sqlschema.create_amm_swap_params_changes_table)

  db:exec(sqlschema.create_amm_swap_params_table)
  print("create_amm_swap_params_table: " .. db:errmsg() == 'not an error' and '✅' or db:errmsg())

  db:exec(sqlschema.create_token_supply_changes_table)
  print("create_token_supply_changes_table: " .. db:errmsg() == 'not an error' and '✅' or db:errmsg())

  db:exec(sqlschema.create_amm_registry_table)
  print("create_amm_registry_table: " .. (db:errmsg() == 'not an error' and '✅' or db:errmsg()))

  db:exec("DROP VIEW IF EXISTS amm_transactions_view;")
  print("DROP amm_transactions_view : " .. (db:errmsg() == 'not an error' and '✅' or db:errmsg()))

  db:exec(sqlschema.create_transactions_view)
  print("create_transactions_view: " .. (db:errmsg() == 'not an error' and '✅' or db:errmsg()))

  db:exec("DROP VIEW IF EXISTS amm_market_cap_view;")
  print("DROP amm_market_cap_view " .. (db:errmsg() == 'not an error' and '✅' or db:errmsg()))

  db:exec(sqlschema.create_market_cap_view)
  print("create_market_cap_view: " .. (db:errmsg() == 'not an error' and '✅' or db:errmsg()))

  db:exec(sqlschema.create_balances_table)
  print("create_balances_table: " .. (db:errmsg() == 'not an error' and '✅' or db:errmsg()))

  db:exec(sqlschema.create_indicator_subscriptions_table)
  print("create_indicator_subscriptions_table: " .. (db:errmsg() == 'not an error' and '✅' or db:errmsg()))

  db:exec(sqlschema.create_top_n_subscriptions_table)
  print("create_top_n_subscriptions_table: " .. (db:errmsg() == 'not an error' and '✅' or db:errmsg()))

  db:exec(sqlschema.create_swap_subscription_table)
  print("create_swap_subscription_table: " .. (db:errmsg() == 'not an error' and '✅' or db:errmsg()))

  db:exec(sqlschema.create_reserve_change_subscription_table)
  print("create_reserve_change_subscription_table: " .. (db:errmsg() == 'not an error' and '✅' or db:errmsg()))

  db:exec(sqlschema.create_token_registry_table)
  print("create_token_registry_table: " .. (db:errmsg() == 'not an error' and '✅' or db:errmsg()))

  db:exec(sqlschema.create_oracle_prices_table)
  print("create_oracle_prices_table: " .. (db:errmsg() == 'not an error' and '✅' or db:errmsg()))

  db:exec(sqlschema.create_reserve_change_table)
  print("create_reserve_change_table: " .. (db:errmsg() == 'not an error' and '✅' or db:errmsg()))

  db:exec(sqlschema.create_agents_table)
  print("create_agents_table: " .. (db:errmsg() == 'not an error' and '✅' or db:errmsg()))

  db:exec(sqlschema.create_locked_tokens_table)
  print("create_locked_tokens_table: " .. (db:errmsg() == 'not an error' and '✅' or db:errmsg()))

  db:exec(sqlschema.create_community_approved_tokens_table)
  print("create_community_approved_tokens_table: " .. (db:errmsg() == 'not an error' and '✅' or db:errmsg()))
end

local function seedOraclePrices()
  local stmt = db:prepare([[
      INSERT OR REPLACE INTO oracle_prices (process_id, ticker, price, last_update, last_update_ts) VALUES ('0udHxHUaSZI4aIs4hD6rF2jRas4G_XWYnn6JwxXd0II', 'mockAO.new', 1.0, '2024-09-23', 1727085221);
  ]])
  dbUtils.execute(stmt)
end


local function seedCommunityApprovedTokens()
  local stmt = db:prepare([[
      INSERT OR REPLACE INTO community_approved_tokens (id, ticker, approved_at_ts) VALUES ('0udHxHUaSZI4aIs4hD6rF2jRas4G_XWYnn6JwxXd0II', 'mockAO', 1727085221);
  ]])
  dbUtils.execute(stmt)
end

function dbSeed.seed()
  seedOraclePrices()
  seedCommunityApprovedTokens()
end

function dbSeed.handleResetDBState(msg)
  if msg.From ~= Owner and msg.From ~= ao.id then
    error('Only the owner and the process itself can reset-and-seed the database')
  end

  db:exec("DELETE FROM amm_transactions")
  db:exec("DELETE FROM amm_swap_params_changes")
  db:exec("DELETE FROM token_supply_changes")
  db:exec("DELETE FROM amm_registry")
  db:exec("DELETE FROM amm_swap_params")
  db:exec("DELETE FROM token_registry")
  db:exec("DELETE FROM oracle_prices")
  db:exec("DELETE FROM balances")
  db:exec("DELETE FROM indicator_subscriptions")
  db:exec("DELETE FROM top_n_subscriptions")
  db:exec("DELETE FROM swap_subscriptions")
  db:exec("DELETE FROM reserve_change_subscriptions")
  db:exec("DELETE FROM reserve_changes")
  dbSeed.createMissingTables()
  dbSeed.seed()

  AmmSubscriptions = {}
end

return dbSeed
