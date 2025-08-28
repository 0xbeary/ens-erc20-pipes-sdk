CREATE TABLE IF NOT EXISTS sync_status
(
    id                  String,
    block_number        UInt64,
    block_hash          FixedString(66),
    timestamp           UInt64,
    updated_at          DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (id);

CREATE TABLE IF NOT EXISTS ens_events
(
    id                  UInt64 DEFAULT rowNumberInAllBlocks(),  -- Auto-incrementing ID (ClickHouse way)
    event_type          LowCardinality(String),                 -- Matches PostgreSQL, optimized for ClickHouse
    log_index           UInt32,
    transaction_index   UInt32,
    transaction_hash    FixedString(66),                        -- Matches PostgreSQL VARCHAR(66)
    contract_address    FixedString(42),                        -- Renamed from 'address' (reserved keyword)
    block_hash          FixedString(66),                        -- Missing field from PostgreSQL
    block_number        UInt64,
    block_timestamp     DateTime CODEC (DoubleDelta, ZSTD),     -- Matches PostgreSQL naming
    created_at          DateTime DEFAULT now(),                 -- Matches PostgreSQL
    
    -- Event-specific fields (denormalized for better ClickHouse performance)
    from_address        Nullable(FixedString(42)),
    to_address          Nullable(FixedString(42)),
    amount              Nullable(String),
    delegator           Nullable(FixedString(42)),
    from_delegate       Nullable(FixedString(42)),
    to_delegate         Nullable(FixedString(42)),
    delegate            Nullable(FixedString(42)),
    previous_balance    Nullable(String),
    new_balance         Nullable(String)
)
ENGINE = MergeTree
ORDER BY (block_timestamp, block_number, transaction_hash, log_index);
