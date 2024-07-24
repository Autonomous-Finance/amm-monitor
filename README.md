# DEXI AMM Monitor Autonomous Agent

## Overview
The DEXI AMM Monitor Autonomous Agent is designed to aggregate and process data from Automated Market Makers (AMMs). This agent operates in two modes:
- **Pull Mode:** Loads data from the gateway.
- **Push Mode:** Receives data directly from the AMM processes.
These modes work in conjunction to ensure the agent maintains up-to-date AMM statistics, which are accessible and displayable through the frontend via dry runs.


## State Maintenance
The Autonomous Agent actively receives data from AMMs and, to guarantee data consistency, it periodically pulls data from the gateway. 

## DEXI Core

### Register-Process
The 'Register-Process' handler is used to register a process to monitor a specific AMM. The handler expects input tags for the AMM identifier (AMM-Process-Id), an Owner-Id and the process identifier (Subscriber-Process-Id).
Once registered the owner (wallet with Owner-Id) has to send 1 AOCred to the Dexi process to activate the subscription.

### Get-Candles
The handler expects input tags for the number of days (Days), the interval (Interval), and the AMM identifier (AMM). The handler outputs a JSON-encoded response containing candlestick data (open, high, low, close, volume) for the specified AMM over the given time period and interval.

### Get-Overview
The handler returns a JSON-encoded response containing the current state of the monitored AMMs. This includes the current price, 24h volume, and 24h change. It accepts an Order-By tag to sort the AMMs by `volume, transactions or date`.

### Get-Stats
The Get-Stats Action can be used to retreive statistics about a specific AMM. The handler expects input tags for the AMM identifier (AMM).
The response message looks like this:
```lua
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

### GetRegisteredAMMs

This handler can be used to obtain a list of the AMMs currently monitored by DEXI

[TODO]

### BatchRequestPrices

[TODO]

### GetCurrentHeight

[TODO]

### SubscribeIndicators

This handler can be used to subscribe for various indicators (SMA, EMA, MACD, Bollinger Bands).

[TODO]

## Top N Market Data

DEXI can provide **market data** on the tokens that it tracks, catering to the needs of agents which require **live data on top coins, as ranked by market cap**.

The market data includes current swap parameters of the amms that are associated with these tokens:
- token reserves 
- current swap fee

This makes the top N market data usable by agents which are interested in up-to-date **accurate DEX prices**. An agent with this top N market data from DEXI would itself be able to perform the swap calculation by knowning the latest pool reserves and fee. This allows an agent to skip the step of asking the AMM for an expected swap output, which leads to:
- more accurate estimations of the net value of its holdings
- swifter execution of swaps on specific signals (**1-step swap** instead of 2-steps)

DEXI provides the *top N market data* both
- on a pull basis (`Action = "Get-Top-N-Market-Data"`)
- on a subscription basis (agent subscribes to receives updates in real time, configuring `n` and the quote token)

### How it works

Processes subscribe to be notified with market data related to their "top N tokens".

Subscribers are notified as soon as DEXI learns about relevant changes. **Relevant changes** are

- a new transaction has occurred involving one of the tokens in the token set of that subscriber
- a liquidity addition / removal has occurred involving one of the tokens in the token set of that subscriber
- the total supply of a token has changed, such that the market cap ranking has changed, such that the top N token set of the subscriber has changed (not yet, but in v2)

For this, DEXI computes & **tracks for each subscriber which tokens exactly their top N set contains**. In order to do this efficiently, DEXI keeps updating the token set on relevant events, which are:

- subscriber process was registered
- market caps have changed
  - total supply of a token has changed
  - a trade has occurred

We don't treat liquidity provision as an event that would change market cap, since this affects neither the price nor the total supply of any token.

### Top N Market Data Limitations

The current implementation (v1) is based only on *BRK* token pairs. 

1. *BRK* is the quote token that market cap is measured in.
2. *BRK* is the only available quote token for top N market data subscriptions
3. DEXI don't handle potential changes to the token set of a user, once that token set is determined. They can occur though, as soon as market cap rank changes affect the top N tokens of that subscriber.

### Handlers

#### Get-Top-N-Market-Data
The handler returns a JSON-encoded response containing specific market data related to the monitored AMMs. It is designed for consumption by token index investors. The market data contains latest prices of monitored AMMs that have the quote token as specified in the `Quote-Token` tag. Results are sorted by the market cap of the base token in descending order. 

The latest price of an AMM is the price of the last trade that took place, and may not be representative at all for trades that involve volumes of significantly different magnitude than that specific trade. Therefore, the market data also includes the latest reserves of the AMM, as well as the current swap fee. This allows consumers of this data to very accurately predict an expected swap output for a trade with a specific input amount, without the need to send a dedicated message to the AMM process.

This data can be obtained not only by calling the `Get-Top-N-Market-Data` handler, but also by subscribing to the data (see next section)

#### Subscribe-Top-N
Here is how a process like a token index fund agent would subscribe itself to DEXI in order to receive Top-N-Market-Data.
```lua
ao.send({
    Target = DEXI_PROCESS_ID,
    Action = 'Subscribe-Top-N',
    ['Subscriber-Process-Id'] = ao.id,
    ['Owner-Id'] = Owner,
    ['Quote-Token'] = 'abc_TokenProcessId_xyz'
})
```
As with subscriptions for regular AMM data, once registered the owner (wallet with `Owner-Id`) has to send 1 AOCred to the Dexi process to activate the subscription.




## Building & Deploying DEXI

To build & deploy do:
`npm run build`

In order to build DEXI, it is recommended to use amalg to create a single file from multiple scripts. 
This is due to complications with the aos file loader. 

Amalgamation can be done by something like:

```bash
/opt/homebrew/bin/luacheck [file1] [file2] ...

/opt/homebrew/bin/amalg.lua -s process.lua -o build/output.lua [module1] [module2] ...
```
See `./build.sh` for details

In our experience amalagamations work more reliably than the aos loader (for now) and it also has the nice property of beign able to deploy with just one command.

After amalgamation, the `build.sh` script uses [aoform](https://github.com/Autonomous-Finance/aoform) to deploy DEXI, according to the configuration in `processes.yaml`. aoform manages a history of deployments (see `state.yaml`), such that you can have it do either:
- deploy a new process
- perform an Eval on the last deployed process

```bash
npx aoform apply
```

## TODO

- use subscribable package
- use squishy and remap package paths
- move .lua files into a src/