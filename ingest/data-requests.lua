-- currently unused

local _0RBIT = "WSXUI2JjYUldJ7CKq9wE1MGwXs-ldzlUlHOQszwQe0s"


function simpleTemplate(template, data)
  return (template:gsub('($%b{})', function(w) return data[w:sub(3, -2)] or w end))
end

function requestTransactions(first)
  -- Ensure minHeight is set to one more than the current max height in Transactions to fetch new transactions
  local minHeight = 1
  if #Transactions > 0 then
    minHeight = Transactions[#Transactions].height
  end

  local maxHeight = 2147483647 -- max int 32
  first = first or 100

  local sortParam = 'HEIGHT_ASC' --sortAscending and 'HEIGHT_ASC' or 'HEIGHT_DESC'

  local queryTemplate = [[
      query {
          transactions(tags: [{name: "Action", values: ["Order-Confirmation"]}, {name: "From-Process", values: ["${AMM}"]}], sort: ${sortParam}, first: ${first}, block: {min: ${minHeight}, max: ${maxHeight}}) {
            edges {
                node {
                id
                owner {
                  address
                }
                block {
                  height
                  id
                }
                tags {
                  name
                  value
                }
              }
            }
          }
        }
    ]]

  local templatedQuery = simpleTemplate(queryTemplate, {
    first = tonumber(first),
    sortParam = sortParam,
    minHeight = minHeight,
    maxHeight = maxHeight,
    AMM = AMM
  })

  local message = {
    query = templatedQuery,
    variables = {}
  }


  ao.send({
    Target = _0RBIT,
    Action = "Post-Real-Data",
    Url = "https://arweave.net/graphql",
    Body = json.encode(message)
  })
  return templatedQuery
end

function requestBlocks()
  local blockIdsSet = {}
  local blockIds = {}

  -- Collect unique block IDs from transactions where blockTimestamp is nil
  for _, transaction in ipairs(Transactions) do
    if transaction.blockId and transaction.blockTimestamp == nil and not blockIdsSet[transaction.blockId] then
      blockIdsSet[transaction.blockId] = true -- Mark this ID as collected
      table.insert(blockIds, '"' .. transaction.blockId .. '"')
      -- Stop collecting once we have 100 unique block IDs
      if #blockIds >= 100 then
        break
      end
    end
  end

  -- If there are no block IDs to request, return early
  if #blockIds == 0 then
    return
  end

  local queryTemplate = [[
      query {
        blocks(ids: [${blockIds}]) {
          edges {
            node {
              id
              height
              timestamp
            }
          }
        }
      }
    ]]

  local templatedQuery = simpleTemplate(queryTemplate, {
    blockIds = table.concat(blockIds, ', ')
  })

  local message = {
    query = templatedQuery,
    variables = {}
  }

  ao.send({
    Target = _0RBIT,
    Action = "Post-Real-Data",
    Url = "https://arweave.net/graphql",
    Body = json.encode(message)
  })
end

function updateTransactions(data)
  local dataToInsert = {}
  for _, edge in ipairs(data) do
    local node = edge.node
    local nodeFlattened = {
      Id = node.id,
      ['Block-Height'] = node.block.height,
      From = node.owner.address,
      ['Block-Id'] = node.block.id,
      Timestamp = nil,
      Tags = node.Tags
    }
    table.insert(dataToInsert, nodeFlattened)
  end
  insertManyTransactions(dataToInsert)
end

function updateBlockTimestamps(blockData)
  for _, edge in ipairs(blockData) do
    local blockId = edge.node.id
    local blockTimestamp = edge.node.timestamp

    -- Iterate over Transactions to find matching blockId and update blockTimestamp
    for _, transaction in ipairs(Transactions) do
      if transaction.blockId == blockId then
        transaction.blockTimestamp = blockTimestamp
      end
    end
  end
end
