#!/bin/bash

if [[ "$(uname)" == "Linux" ]]; then
    BIN_PATH="$HOME/.luarocks/bin"
else
    BIN_PATH="/opt/homebrew/bin"
fi

$BIN_PATH/luacheck process.lua \
    validation/validation.lua validation/validation-schemas.lua \
    dexi-core/dexi-core.lua dexi-core/sqlschema.lua \
    dexi-core/intervals.lua dexi-core/candles.lua dexi-core/stats.lua dexi-core/overview.lua dexi-core/price-around.lua \
    indicators/indicators.lua indicators/calc.lua \
    top-n/top-n.lua
    


$BIN_PATH/amalg.lua -s process.lua -o build/output.lua \
    validation-schemas validation \
    sqlschema \
    dexi-core.dexi-core dexi-core.sqlschema \
    dexi-core.intervals dexi-core.candles dexi-core.stats dexi-core.overview dexi-core.price-around\
    indicators.indicators indicators.calc \
    top-n/top-n

npx aoform apply
