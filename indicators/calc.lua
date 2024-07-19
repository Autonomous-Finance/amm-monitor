local calc = {}

function calc.calculateSMAs(dailyStats)
  local smas = {}
  for i = 1, #dailyStats do
    local sma10 = 0
    local sma20 = 0
    local sma50 = 0
    local sma100 = 0
    local sma150 = 0
    local sma200 = 0

    for j = math.max(1, i - 9), i do
      sma10 = sma10 + dailyStats[j].close
    end
    sma10 = sma10 / math.min(10, i)

    for j = math.max(1, i - 19), i do
      sma20 = sma20 + dailyStats[j].close
    end
    sma20 = sma20 / math.min(20, i)

    for j = math.max(1, i - 49), i do
      sma50 = sma50 + dailyStats[j].close
    end
    sma50 = sma50 / math.min(50, i)

    for j = math.max(1, i - 99), i do
      sma100 = sma100 + dailyStats[j].close
    end
    sma100 = sma100 / math.min(100, i)

    for j = math.max(1, i - 149), i do
      sma150 = sma150 + dailyStats[j].close
    end
    sma150 = sma150 / math.min(150, i)

    for j = math.max(1, i - 199), i do
      sma200 = sma200 + dailyStats[j].close
    end
    sma200 = sma200 / math.min(200, i)

    smas[i] = {
      sma10 = sma10,
      sma20 = sma20,
      sma50 = sma50,
      sma100 = sma100,
      sma150 = sma150,
      sma200 = sma200
    }
  end

  return smas
end

function calc.calculateEMAs(dailyStats)
  local ema12 = {}
  local ema26 = {}

  for i = 1, #dailyStats do
    if i == 1 then
      ema12[i] = dailyStats[i].close
      ema26[i] = dailyStats[i].close
    else
      ema12[i] = (dailyStats[i].close - ema12[i - 1]) * 2 / 13 + ema12[i - 1]
      ema26[i] = (dailyStats[i].close - ema26[i - 1]) * 2 / 27 + ema26[i - 1]
    end
  end

  return ema12, ema26
end

function calc.calculateMACD(ema12, ema26)
  local macd = {}
  local signalLine = {}
  local histogram = {}

  for i = 1, #ema12 do
    macd[i] = ema12[i] - ema26[i]

    if i == 1 then
      signalLine[i] = macd[i]
    else
      signalLine[i] = (macd[i] - signalLine[i - 1]) * 2 / 10 + signalLine[i - 1]
    end

    histogram[i] = macd[i] - signalLine[i]
  end

  return macd, signalLine, histogram
end

function calc.calculateBollingerBands(dailyStats, smas)
  local upperBand = {}
  local lowerBand = {}

  for i = 1, #dailyStats do
    local sum = 0
    local count = 0

    for j = math.max(1, i - 19), i do
      sum = sum + (dailyStats[j].close - smas[i].sma20) ^ 2
      count = count + 1
    end

    local stdDev = math.sqrt(sum / count)
    upperBand[i] = smas[i].sma20 + 2 * stdDev
    lowerBand[i] = smas[i].sma20 - 2 * stdDev
  end

  return upperBand, lowerBand
end

return calc
