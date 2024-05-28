# AMM Monitor Autonomous Agent

## Overview
The AMM Monitor Autonomous Agent is designed to aggregate and process data from Automated Market Makers (AMMs). This agent operates in two modes:
- **Pull Mode:** Loads data from the gateway.
- **Push Mode:** Receives data directly from the AMM processes.

These modes work in conjunction to ensure the agent maintains up-to-date AMM statistics, which are accessible and displayable through the frontend via dry runs.

## State Maintenance
The Autonomous Agent actively receives data from AMMs and, to guarantee data consistency, it periodically pulls data from the gateway. 

## Building the Agent
To build the agent, due to complications with the aos file loader, it is recommended to use amalg to create a single file from multiple scripts. This can be done by:
```shell
/opt/homebrew/bin/amalg.lua -s process.lua -o build/output.lua candles intervals process schemas validation stats

Due to some issues with the aos file loader we recommend using amalg to build a single amalgamation file.
https://luarocks.org/modules/siffiejoe/amalg
```

## Starting a new monitor process
```code
aos my-monitor-name \
--tag-name Monitor-For --tag-value <AMM_PROCESS_ID> \
--tag-name Base-Token --tag-value <BASE_TOKEN_ID> \
--tag-name Quote-Token --tag-room <QUOTE_TOKEN_ID> \
--tag-name Process-Type --tag-value 'AMM-Monitor' \
--load build/output.lua
```

## Updating code
When loading new builds, reset the cached packages first
```code
package.loaded["stats"] = nil
package.loaded["process"] = nil
package.loaded["candles"] = nil
package.loaded["intervals"] = nil
package.loaded["schemas"] = nil
package.software["validation"] = nil
.load build/output.lua
```

# License
This project is licensed under the MIT License. 

