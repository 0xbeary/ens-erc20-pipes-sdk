-- ENS Transfer events table
CREATE TABLE IF NOT EXISTS ens_transfers
(
    transaction_hash    FixedString(66),
    log_index          UInt32,
    transaction_index  UInt32,
    contract_address   FixedString(42),
    block_hash         FixedString(66),
    block_number       UInt64,
    block_timestamp    DateTime CODEC (DoubleDelta, ZSTD),
    created_at         DateTime DEFAULT now(),
    
    -- Transfer-specific fields
    from_address       FixedString(42),
    to_address         FixedString(42),
    amount             String
)
ENGINE = MergeTree
ORDER BY (block_timestamp, block_number, transaction_hash, log_index);

-- ENS DelegateChanged events table
CREATE TABLE IF NOT EXISTS ens_delegate_changed
(
    transaction_hash    FixedString(66),
    log_index          UInt32,
    transaction_index  UInt32,
    contract_address   FixedString(42),
    block_hash         FixedString(66),
    block_number       UInt64,
    block_timestamp    DateTime CODEC (DoubleDelta, ZSTD),
    created_at         DateTime DEFAULT now(),
    
    -- DelegateChanged-specific fields
    delegator          FixedString(42),
    from_delegate      FixedString(42),
    to_delegate        FixedString(42)
)
ENGINE = MergeTree
ORDER BY (block_timestamp, block_number, transaction_hash, log_index);

-- ENS DelegateVotesChanged events table
CREATE TABLE IF NOT EXISTS ens_delegate_votes_changed
(
    transaction_hash    FixedString(66),
    log_index          UInt32,
    transaction_index  UInt32,
    contract_address   FixedString(42),
    block_hash         FixedString(66),
    block_number       UInt64,
    block_timestamp    DateTime CODEC (DoubleDelta, ZSTD),
    created_at         DateTime DEFAULT now(),
    
    -- DelegateVotesChanged-specific fields
    delegate           FixedString(42),
    previous_balance   String,
    new_balance        String
)
ENGINE = MergeTree
ORDER BY (block_timestamp, block_number, transaction_hash, log_index);

-- Target table for the materialized view
CREATE TABLE IF NOT EXISTS ens_all_events
(
    event_type         LowCardinality(String),
    transaction_hash   FixedString(66),
    log_index         UInt32,
    transaction_index UInt32,
    contract_address  FixedString(42),
    block_hash        FixedString(66),
    block_number      UInt64,
    block_timestamp   DateTime,
    created_at        DateTime,
    from_address      String,
    to_address        String,
    amount            String,
    delegator         String,
    from_delegate     String,
    to_delegate       String,
    delegate          String,
    previous_balance  String,
    new_balance       String
)
ENGINE = MergeTree
ORDER BY (block_timestamp, block_number, transaction_hash, log_index);

-- Materialized view to aggregate all events (for backwards compatibility)
CREATE MATERIALIZED VIEW IF NOT EXISTS ens_all_events_mv TO ens_all_events AS
SELECT 
    'Transfer' as event_type,
    transaction_hash,
    log_index,
    transaction_index,
    contract_address,
    block_hash,
    block_number,
    block_timestamp,
    created_at,
    from_address,
    to_address,
    amount,
    '' as delegator,
    '' as from_delegate,
    '' as to_delegate,
    '' as delegate,
    '' as previous_balance,
    '' as new_balance
FROM ens_transfers
UNION ALL
SELECT 
    'DelegateChanged' as event_type,
    transaction_hash,
    log_index,
    transaction_index,
    contract_address,
    block_hash,
    block_number,
    block_timestamp,
    created_at,
    '' as from_address,
    '' as to_address,
    '' as amount,
    delegator,
    from_delegate,
    to_delegate,
    '' as delegate,
    '' as previous_balance,
    '' as new_balance
FROM ens_delegate_changed
UNION ALL
SELECT 
    'DelegateVotesChanged' as event_type,
    transaction_hash,
    log_index,
    transaction_index,
    contract_address,
    block_hash,
    block_number,
    block_timestamp,
    created_at,
    '' as from_address,
    '' as to_address,
    '' as amount,
    '' as delegator,
    '' as from_delegate,
    '' as to_delegate,
    delegate,
    previous_balance,
    new_balance
FROM ens_delegate_votes_changed;
