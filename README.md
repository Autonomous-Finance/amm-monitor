# AMM Monitor Autonomous Agent

## Overview
The AMM Monitor Autonomous Agent is designed to aggregate and process data from Automated Market Makers (AMMs). This agent operates in two modes:
- **Pull Mode:** Loads data from the gateway.
- **Push Mode:** Receives data directly from the AMM processes.
These modes work in conjunction to ensure the agent maintains up-to-date AMM statistics, which are accessible and displayable through the frontend via dry runs.


## State Maintenance
The Autonomous Agent actively receives data from AMMs and, to guarantee data consistency, it periodically pulls data from the gateway. 

# Handlers

## Get-Candles
The handler expects input tags for the number of days (Days), the interval (Interval), and the AMM identifier (AMM). The handler outputs a JSON-encoded response containing candlestick data (open, high, low, close, volume) for the specified AMM over the given time period and interval.

## Get-Overview
The handler returns a JSON-encoded response containing the current state of the monitored AMMs. This includes the current price, 24h volume, and 24h change. It accepts an Order-By tag to sort the AMMs by `volume, transactions or date`.

## Register-Process
The 'Register-Process' handler is used to register a process to monitor a specific AMM. The handler expects input tags for the AMM identifier (AMM-Process-Id), an Owner-Id and the process identifier (Subscriber-Process-Id).
Once registered the owner (wallet with Owner-Id) has to send 1 AOCred to the Dexi process to activate the subscription.

## Get-Top-N-Market-Data
The handler returns a JSON-encoded response containing specific market data related to the monitored AMMs. It is designed for consumption by token index investors. The market data contains latest prices of monitored AMMs that have the quote token as specified in the `Quote-Token` tag. Results are sorted by the market cap of the base token in descending order. 

The latest price of an AMM is the price of the last trade that took place, and may not be representative at all for trades that involve volumes of significantly different magnitude than that specific trade. Therefore, the market data also includes the latest reserves of the AMM, as well as the current swap fee. This allows consumers of this data to very accurately predict an expected swap output for a trade with a specific input amount, without the need to send a dedicated message to the AMM process.

This data can be obtained not only by calling the `Get-Top-N-Market-Data` handler, but also by subscribing to the data (see next section)

## Register-Top-N-Consumer
Here is how a process like a token index fund agent would subscribe itself to DEXI in order to receive Top-N-Market-Data.
```lua
ao.send({
    Target = DEXI_PROCESS_ID,
    Action = 'Register-Top-N-Consumer',
    ['Subscriber-Process-Id'] = ao.id,
    ['Owner-Id'] = Owner,
    ['Quote-Token'] = 'abc_TokenProcessId_xyz'
})
```
As with subscriptions for regular AMM data, once registered the owner (wallet with `Owner-Id`) has to send 1 AOCred to the Dexi process to activate the subscription.


## Get-Stats
The Get-Stats Action can be used to retreive statistics about a specific AMM. The handler expects input tags for the AMM identifier (AMM).
The response message looks like this:
```
ao.send({
    Target = msg.From, 
    ['App-Name'] = 'Dexi',
    ['Payload'] = 'Stats',
    ['AMM'] = msg.Tags.AMM,
    ['Total-Volume'] = tostring(stats.total_volume),
    ['Buy-Volume'] = tostring(stats.buy_volume),
    ['Sell-Volume'] = tostring(stats.sell_volume),
    ['Buy-Count'] = tostring(stats.buy_count),
    ['Sell-Count'] = tostring(stats.sell_count),
    ['Buyers'] = tostring(stats.distinct_buyers),
    ['Sellers'] = tostring(stats.distinct_sellers),
    ['Total-Traders'] = tostring(stats.distinct_traders),
    ['Latest-Price'] = tostring(priceNow),
    ['Price-24H-Ago'] = tostring(price24HAgo),
    ['Price-6H-Ago'] = tostring(price6HAgo),
    ['Price-1H-Ago'] = tostring(price1HAgo)
})
```


## Data Feeds [WIP]

### Top N Market Data

Processes subscribe to be notified with market data related to their "top N tokens".

Subscribers are notified immediately as soon as a relevant change occurs. Relevant changes are

- a new transaction has occurred involving one of the tokens in the token set of that subscriber
- a liquidity addition / removal has occurred involving one of the tokens in the token set of that subscriber

For this, DEXI computes & tracks for each subscriber what their top N token set is. In order to track this data efficiently, DEXI keeps updating the token set on relevant events, which are:

- subscriber process was registered
- market caps have changed
  - total supply of a token has changed
  - a trade has occurred

We don't treat liquidity provision as an event that would change market cap, since this affects neither the price nor the total supply of any token.

In the current implementation Top N Market Data works under the **assumption that all monitored pools share the same quote token**, in which the market cap is actually expressed.


## Building the Agent
To build the agent, due to complications with the aos file loader, it is recommended to use amalg to create a single file from multiple scripts. This can be done by something like:

```bash
/opt/homebrew/bin/luacheck [file1] [file2] ...

/opt/homebrew/bin/amalg.lua -s process.lua -o build/output.lua [module1] [module2] ...

npx aoform apply
```
See `./build.sh` for details

In our experience amalagamations work more reliably than the aos loader (for now) and it also has the nice property of beign able to deploy with just one command.

To build & deploy on OSX do:
`bash build.sh`


## TODO

- use subscribable package
- ? remove isTrusted check (not needed any longer)
- move .lua files into a src/