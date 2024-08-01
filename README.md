# Dexi AMM Monitor Autonomous Agent

## Overview

The Dexi AMM Monitor Autonomous Agent is designed to **aggregate and process data** from Automated Market Makers (AMMs).

With the aggregated data, Dexi 

- provides stats **on request**
- offers data feeds **on a subscription basis**

### Dexi: Consumer and Producer

In order to do its work, Dexi is both a **subscription consumer** and a **subscription provider**:

#### Dexi subscribes to AMMs

> AMMs ----- [ swaps             ] -------- - - - - >      Dexi   
>            [ liquidity changes ]                      (aggregate)
>            [ fee changes       ] 

#### Agents subscribe to DEXI

>    Dexi      ------ [ market indicators                     ] - - - - > Agents
> (aggregate)         [ top n tokens by m_cap (w/ price data) ]



## Aggregation

As an aggregator, Dexi passively receives data from the AMM processes. 

In principle, Dexi can also operate in a **Pull Mode**, meaning that it loads AMM data from the gateway. This helps ensure data consistency, when used to backfill periodically via an external system (not on AO). This feature was successfully used on the testnet in the initial Dexi release but is **disabled in the v1 release**.

### How Dexi subscribes to AMMs

1. AMMs require the capability to provide such subscriptions. [Bark](https://github.com/Autonomous-Finance/bark-amm) AMMs have it by default. Typically, subscribing to an AMM involves a **payment**.
2. DEXI needs to subscribe to each registered AMM. This step is typically triggered by an **AMM creator** who is interested in having their AMM integrated with DEXI. The payment tokens would typically be provided by that AMM creator. See details [below](#registering-an-amm-on-dexi)

## Dexi-Provided Subscriptions

A process can be subscribed to Dexi in order to receive data feeds. Currently there are 2 topics available

- Market Indicators
  - parameter: AMM pool
  - returns: trading candles, volume, indicators (sma, ema, macd, bollinger bands) for specified AMM pool
- Top N Market Data
  - paramter: N
  - returns: market data (AMM pool reserves & fees) for the top N tokens ranked by market cap

### Subscription pattern

An active subscription involves 2 parts

1. establishing the subscription ( a process registers as a subscriber to a given topic )
2. activating it via a payment

#### Payments
Payments are a means to prevent abuse of the service.

**Anyone can pay** for a subscriber.

In v1, Dexi **tracks payments per subscriber**, as opposed to per subscription. Furthermore, it requires **one-time payments**, so it's not related to the duration of the subscription or actual data provided.

#### Canceling a subscription

In v1, Dexi subscriptions run until canceled. Only the subscriber process can cancel.

#### No Duplicate Subscriptions

Dexi v1 supports **no duplicate subscriptions**. It's taking into consideration the nature of the parameters - "duplication" is defined in a way that suits the expected needs of subscribers.

Specifically:
- for indicators, the combination (subscriber_id, amm_id) must be unique
- for top n market data, the combination (subscriber_id, quote_token) must be unique

**Example Scenario 1**

A process has a subscription to "indicators" for TRUNK-BARK and a subscription to "indicators" for EXP-BARK. This works. 

However, it cannot have one more "indicators" subscription for TRUNK-BARK. 

**Example Scenario 2**

A process has a subscription to "top n market data" for the **top 5** tokens by market cap, with market cap expressed in **BARK**. Furthermore, it has a subscription to "top n market data" for the **top 5**, with market cap expressed in **wAR**. Having these 2 at the same time works. 

However, the process cannot have one more "top n market data" subscription for the **top 3** tokens, with market cap expressed in **BARK** or **wAR**. In this case, the difference in the `N` parameter doesn't matter. 

## Dexi Data - Core

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

## Dexi Data - Market Indicators

DEXI provides a few indicators related to the monitored AMMs, along with the associated candles and volume data

- SMA
- EMA (not in v1)
- MACD (not in v1)
- Bollinger Bands (not in v1)

### Handlers

#### Get-Indicators

This handler can be used to request a single instance of indicators data.

```lua
ao.send({
  Target = <DEXI_PROCESS>,      -- the Dexi process ID
  Action = 'Get-Indicators',
  AMM = <AMM_PROCESS>           -- AMM of the token pair of interest
})
```

#### Subscribe-Indicators

This handler can be used to subscribe to indicators data related to a specific AMM. 

A process sending a 'Susbcribe-Indicators' message registers as a subscriber. 

The handler expects an input tag for the AMM identifier (AMM-Process-Id).

After registration a payment is necessary in order to activate the subscription. 
Anyone can perform the payment. 1 DEXI must be sent to the Dexi process, while forward-tagging the subscriber process id.

Here is an example of how a process would subscribe and also perform the payment

```lua
-- register as a subscriber

ao.send({
    Target = <DEXI_PROCESS>,
    Action = 'Subscribe-Indicators',
    ['AMM'] = <AMM_PROCESS>,                    -- AMM of the token pair of interest
})

-- ...
-- after confirmation, activate subscription by making a payment

ao.send({
  Target = <DEXI_TOKEN_PROCESS>,
  Action = 'Transfer',
  Recipient = <DEXI_PROCESS>,
  Quantity = "1",
  ['X-Subscriber-Process-Id'] = ao.id,         -- Process that is being subscribed. In this case, the sender of the message
})
```

## Dexi Data - Top N Market Data

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

```lua
ao.send({
  Target = <DEXI_PROCESS>,                -- the Dexi process ID
  Action = 'Get-Top-N-Market-Data',
  ["Top-N"] = <number_of_tokens>          -- Given the list of tokens sorted descending by market cap, how many tokens to include
  ["Quote-Token"] = <quote_token>         -- Quote Token by which the market cap is determined (currently only BARK is supported)
})
```

#### Get-Top-N-Token-Set
The handler returns a JSON-encoded response containing the tokens of interest for a specific top n market data subscription. 

```lua
ao.send({
  Target = <DEXI_PROCESS>,                -- the Dexi process ID
  Action = 'Get-Top-N-Market-Data',
  ["Subscriber-Process-Id"] = <id>        -- The subscriber process ID
  ["Quote-Token"] = <quote_token>         -- Quote Token by which the market cap is determined in this subscription
})
```

#### Subscribe-Top-N
Here is how a process like a token index fund agent would subscribe to Dexi in order to receive Top-N-Market-Data

```lua
-- register as a subscriber

ao.send({
    Target = <DEXI_PROCESS>,
    Action = 'Subscribe-Top-N',
    ['Top-N'] = '5',                            -- Will always send for top 5 tokens in market cap ranking
    ['Quote-Token'] = 'abc_TokenProcessId_xyz'  -- Quote Token by which the market cap is determined (currently only BARK is supported)
})

-- ...
-- after confirmation, activate subscription by making a payment

ao.send({
  Target = <DEXI_TOKEN_PROCESS>,
  Action = 'Transfer',
  Recipient = <DEXI_PROCESS>,
  Quantity = "1",
  ['X-Subscriber-Process-Id'] = ao.id,         -- Process that is being subscribed. In this case, the sender of the message
})
```

## Registering an AMM on Dexi

In order for an AMM to be registered on Dexi, a payment must be made to Dexi (the aggregator) in DEXI tokens.

Upon receiving the payment, Dexi itself subscribes to the AMM for the relevant data.

Only AMMs that support subscriptions can be effectively registered on Dexi.

Anyone can register an AMM, but typically this would be the creator of the AMM. 

```lua
-- the Dexi token process (https://github.com/Autonomous-Finance/dexi-token/)
DEXI_TOKEN_PROCESS = '123-TODO-DEXI-Token-Process-Id'
-- the Dexi process in this repo
DEXI_PROCESS = '123-TODO-DEXI-Aggregator-Process-Id' 
-- the AMM that should be registered on Dexi
AMM_PROCESS = '123-Amm-Process-Id'

ao.send({
  Target = DEXI_TOKEN_PROCESS,
  Action = 'Transfer',
  Recipient = DEXI_PROCESS,
  Quantity = '1',
  ["X-AMM-Process"] = AMM_PROCESS
})
```

The registration is a multi-step process. As Dexi performs these steps, it sends updates to the entity which made the initial payment.
These updates are tagged
```lua
{
  Action = "Dexi-AMM-Registration-Confirmation",
  AMM = ammProcessId,
  Status = currentStatus
}
```

Typically the process is completed within a few seconds.

The **current status** can have one of the values
1. `received-request--initializing`
2. `initialized--subscribing`
3. `subscribed--paying`
4. `paid--complete`

Dexi has a message handler `{Action = "Get-AMM-Registration-Status"}` for AMM registration initiators to check the progress of an AMM registration. 


## Building & Deploying Dexi

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

## Ownable

## Ownable

This process implements basic ownable functionality, as provided by this [package](https://github.com/Autonomous-Finance/aos-packages/tree/main/packages/ownable).

Most importantly, this prepares the process for having its ownership renounced at the appropriate moment after release. That moment will be determined based on the operational experience, when the team is confident that functionality is reliable.

## TODO


- REORGANIZE
  - use squishy and remap package paths
  - move .lua files into a src/

- thorough VALIDATIONS
  
- graceful error handling
  
- TESTS  - - -  unit tests (no deployments required)
  - seed with 5 amms, 5 tokens
    - check correct top N 
    - check mcap ranking given total supplies
    - check correct reserves given amm data initialization
  - simulate amm swaps => ingest TXs (with reserves updates)
    - check reserves have updated correctly
  - simulate a few subscriptions, with some of them for indicators, some for top N 
    - check top N token sets correclty determined
    - perform new swap
    - check if attempts to dispatch occur for both topN and indicators
    - check if topN payload is as expected
  - simulate huge swap to increase price of the coin with smallest mcap => bring to top of the ranking
    - check if token is at top of the ranking
    - check if top N token sets are correclty updated
    - check if top N updates are dispatched correctly