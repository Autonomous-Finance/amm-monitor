local intervals = {}


intervals.IntervalSecondsMap = {
  ["5m"] = 300,
  ["15m"] = 900,
  ["1h"] = 3600,
  ["4h"] = 14400,
  ["12h"] = 57600,
  ["6h"] = 21600,
  ["1d"] = 86400,
  ["7d"] = 86400 * 7,
  ["1M"] = 2592000
}

function intervals.getIntervalStart(timestamp, interval)
  timestamp = math.floor(timestamp)   -- Ensure timestamp is an integer
  local date = os.date("!*t", timestamp)

  if interval == "1h" then
    date.min = 0
    date.sec = 0
  elseif interval == "15m" then
    date.min = 0
    date.sec = 0
  elseif interval == "4h" then
    date.min = 0
    date.sec = 0
    date.hour = date.hour - (date.hour % 4)
  elseif interval == "1d" then
    date.hour = 0
    date.min = 0
    date.sec = 0
  elseif interval == "1M" then
    date.day = 1
    date.hour = 0
    date.min = 0
    date.sec = 0
  else
    error("Unsupported interval: " .. interval)
  end

  return os.time(date)
end

return intervals
