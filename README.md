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
`bash build.sh`


## Provisioning
Provision via aoform (https://github.com/Autonomous-Finance/aoform)
`npx aoform apply`
