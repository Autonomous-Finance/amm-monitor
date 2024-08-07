#!/bin/bash

if [[ "$(uname)" == "Linux" ]]; then
    BIN_PATH="$HOME/.luarocks/bin"
else
    BIN_PATH="/opt/homebrew/bin"
fi

# Recreate build directories
rm -rf ./build

mkdir -p ./build

$BIN_PATH/luacheck process.lua \
    validation/validation.lua validation/validation-schemas.lua \
    db/sqlschema.lua db/seed.lua db/utils.lua \
    subscriptions/subscriptions.lua \
    integrate-amm.integrate-amm \
    dexi-core/dexi-core.lua \
    dexi-core/intervals.lua dexi-core/candles.lua dexi-core/stats.lua dexi-core/overview.lua dexi-core/price-around.lua \
    ingest/ingest.lua \
    indicators/indicators.lua indicators/calc.lua \
    top-n/top-n.lua \
    utils/debug.lua \
    ownable/ownable.lua \
    ops/config-ops.lua ops/emergency.lua ops/initialize.lua
    
$BIN_PATH/amalg.lua -s process.lua -o build/output.lua \
    validation.validation validation.validation-schemas \
    db.sqlschema db.seed db.utils \
    subscriptions.subscriptions \
    integrate-amm.integrate-amm \
    dexi-core.dexi-core \
    dexi-core.intervals dexi-core.candles dexi-core.stats dexi-core.overview dexi-core.price-around\
    ingest.ingest \
    indicators.indicators indicators.calc \
    top-n.top-n \
    utils.debug \
    ownable.ownable \
    ops.config-ops ops.emergency ops.initialize