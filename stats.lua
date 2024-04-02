local stats = {}

function stats.getAggregateStats(minTimestamp)
    local minTimestamp = minTimestamp or 0
  
    local totalVolume = 0
    local buyVolume = 0
    local sellVolume = 0
    local buyCount = 0
    local sellCount = 0
    local buyers = {}
    local sellers = {}
    local latestPrice = nil
    local latestTimestamp = 0
  
  
    for _, transaction in ipairs(Transactions) do
      -- Only consider transactions with a block height greater than the specified minHeight
      if transaction['Block-Height'] and transaction['Block-Height'] > minTimestamp then
        -- Update the latest price if this transaction's block height is the highest so far
        if transaction.Price and transaction['Block-Height'] > latestTimestamp then
          latestPrice = transaction.Price
          latestTimestamp = transaction['Block-Height']
        end
  
        -- Add to the total volume if the transaction has a volume
        if transaction.Volume then
          totalVolume = totalVolume + transaction.Volume
  
          -- Accumulate buy or sell volume
          if transaction.IsBuy then
            buyVolume = buyVolume + transaction.Volume
            buyers[transaction['From']] = true
          else
            sellVolume = sellVolume + transaction.Volume
            sellers[transaction['From']] = true
          end
        end
  
        -- Count buys and sells based on the 'IsBuy' flag
        if transaction.IsBuy then
          buyCount = buyCount + 1
        else
          sellCount = sellCount + 1
        end
      end
    end
  
    -- Count the number of distinct buyers, sellers, and total traders
    local distinctBuyers = 0
    for _ in pairs(buyers) do distinctBuyers = distinctBuyers + 1 end
  
    local distinctSellers = 0
    for _ in pairs(sellers) do distinctSellers = distinctSellers + 1 end
  
    -- Combine buyers and sellers to count distinct traders
    local traders = {}
    for address, _ in pairs(buyers) do traders[address] = true end
    for address, _ in pairs(sellers) do traders[address] = true end
  
    local distinctTraders = 0
    for _ in pairs(traders) do distinctTraders = distinctTraders + 1 end
  
    return totalVolume, buyVolume, sellVolume, buyCount, sellCount, distinctBuyers, distinctSellers, distinctTraders, latestPrice
end

return stats