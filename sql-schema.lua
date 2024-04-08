local sqlschema = {}

sqlschema.create_table = [[
CREATE TABLE amm_transactions (
    id TEXT PRIMARY KEY,
    source TEXT NOT NULL CHECK (source IN ('gateway', 'message')),
    block_height INTEGER NOT NULL,
    block_id TEXT,
    "from" TEXT NOT NULL,
    "timestamp" INTEGER,
    is_buy INTEGER NOT NULL CHECK (is_buy IN (0, 1)),
    price REAL NOT NULL,
    volume REAL NOT NULL,
    to_token TEXT NOT NULL,
    from_token TEXT NOT NULL,
    from_quantity REAL NOT NULL,
    to_quantity REAL NOT NULL,
    fee REAL NOT NULL,
    amm_process TEXT NOT NULL
);
]]

return sqlschema