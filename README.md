# AMM Monitor

This process serves as aggregation process for AMM data.
It has to modes of operation:
- pull, where it loads data from the gateway
- push, where it receives messages from the AMM process directly

Both these modes operate in tandem to maintain current statistics of the AMM.
These can then be queried and displayed on the frontend (via dry-run).

## State maintenance
The process is happy to receive messages directly from the AMM. However to ensure consistency it will periodically pull data from the gateway.
Currently pulling is implemented offchain due to ongoing issues with 0rbit.


## Building
Due to some issues with the aos file loader we recommend using amalg to build a single amalgamation file.
https://luarocks.org/modules/siffiejoe/amalg

To build on osx do:
`/opt/homebrew/bin/amalg.lua -s process.lua -o build/output.lua candles intervals process schemas validation stats`


## Starting a new monitor process
```
aos my-monitor-name \
--tag-name Monitor-For --tag-value <AMM_PROCESS_ID> \
--tag-name Base-Token --tag-value <BASE_TOKEN_ID> \
--tag-name Quote-Token --tag-value <QUOTE_TOKEN_ID> \
--tag-name Process-Type --tag-value 'AMM-Monitor' \
--load build/output.lua
```



## Updating code
When loading new builds, reset the cached packages first
```
package.loaded["stats"] = nil
package.loaded["process"] = nil
package.loaded["candles"] = nil
package.loaded["intervals"] = nil
package.loaded["schemas"] = nil
package.loaded["validation"] = nil
.load build/output.lua
```
