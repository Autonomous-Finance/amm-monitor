#!/bin/bash

if [[ "$(uname)" == "Linux" ]]; then
    BIN_PATH="$HOME/.luarocks/bin"
else
    BIN_PATH="/opt/homebrew/bin"
fi

$BIN_PATH/luacheck process.lua schemas.lua sqlschema.lua intervals.lua candles.lua stats.lua validation.lua indicators.lua top-n-consumers.lua
$BIN_PATH/amalg.lua -s process.lua -o build/output.lua sqlschema intervals schemas validation candles stats indicators top-n-consumers
npx aoform apply
