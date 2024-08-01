#!/bin/bash

export WALLET_JSON="$(cat ~/.aos.json)"

npx aoform apply -f processes.dev.yaml