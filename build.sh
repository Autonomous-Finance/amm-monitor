/opt/homebrew/bin/luacheck process.lua schemas.lua sqlschema.lua intervals.lua candles.lua stats.lua validation.lua indicators.lua
/opt/homebrew/bin/amalg.lua -s process.lua -o build/output.lua sqlschema intervals schemas validation candles stats indicators
npx aoform apply