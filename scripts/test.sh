#!/bin/bash

export LUA_PATH="$PWD/src/?.lua;$PWD/?.lua;$PWD/packages/?.lua;;"

# Run tests
echo "Running tests ..."
busted test --pattern "_test"
