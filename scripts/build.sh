#!/bin/bash

# Recreate build directories
rm -rf ./build

mkdir -p ./build


amalg.lua -s process.lua -o build/output.lua \
    validation.validation validation.validation-schemas \
    db.sqlschema db.seed db.utils \
    subscriptions.subscriptions \
    integrate-amm.integrate-amm \
    dexi-core.dexi-core \
    dexi-core.intervals dexi-core.candles dexi-core.stats dexi-core.overview dexi-core.price-around dexi-core.usd-price \
    dexi-core.lookups \
    ingest.ingest \
    indicators.indicators indicators.calc \
    top-n.top-n \
    utils.debug utils.responses \
    ownable.ownable \
    ops.config-ops ops.emergency ops.initialize \
    amm-analytics.main amm-analytics.volume amm-analytics.pool-overview amm-analytics.pool-token-pnl amm-analytics.pair-finder \
    swap-subscribers.main swap-subscribers.reserves \
    hopper.hopper \
    update-token.update-token \
    ingest.ingest-token-lock \
    amm-analytics.locked-tokens
