# Dexi Backend

Dexi is a decentralized application built on the AO blockchain, designed to enhance DeFi data analytics. It integrates SQLite for sophisticated on-chain data processing.

Dexi processes transaction data from AMMs, utilizing AO's scalability and code execution capabilities to perform complex data operations directly on the blockchain.

# Handlers

## Get-Candles
The handler expects input tags for the number of days (Days), the interval (Interval), and the AMM identifier (AMM). The handler outputs a JSON-encoded response containing candlestick data (open, high, low, close, volume) for the specified AMM over the given time period and interval.

## Get-Overview
The handler returns a JSON-encoded response containing the current state of the monitored AMMs. This includes the current price, 24h volume, and 24h change. It accepts an Order-By tag to sort the AMMs by `volume, transactions or date`.

## Register-Process
The 'Register-Process' handler is used to register a process to monitor a specific AMM. The handler expects input tags for the AMM identifier (AMM-Process-Id), an Owner-Id and the process identifier (Subscriber-Process-Id).
Once registered the owner (wallet with Owner-Id) has to send 1 AOCred to the Dexi process to activate the subscription.

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



## Building & Deploying
While AOS comes with a built in loader that can resolve modules, we opt for using Lua Amalg. This gives us an amalgamation file that can be easily Eval'd in one go with AOconnect. We use this in conjunction with aoform, a lightweight utility to deploy AO processes. You can check it out here:
https://github.com/Autonomous-Finance/aoform

Deployment (build.sh):
```
/opt/homebrew/bin/luacheck process.lua schemas.lua sqlschema.lua intervals.lua candles.lua stats.lua validation.lua indicators.lua

/opt/homebrew/bin/amalg.lua -s process.lua -o build/output.lua sqlschema intervals schemas validation candles stats indicators

npx aoform apply
```
In our experience amalagamations work more reliably than the aos loader (for now) and it also has the nice property of beign able to deploy with just one command.


To build & deploy on OSX do:
`bash build.sh`
