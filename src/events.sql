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

-- Current Token Balances Materialized View (calculated directly from transfers)
CREATE MATERIALIZED VIEW IF NOT EXISTS current_token_balances
ENGINE = MergeTree
ORDER BY address
AS
WITH balance_changes AS (
    -- Credits (receiving tokens)
    SELECT 
        to_address as address,
        block_number,
        block_timestamp,
        log_index,
        toDecimal256(amount, 0) as amount_change
    FROM ens_transfers
    WHERE to_address != '0x0000000000000000000000000000000000000000'
    
    UNION ALL
    
    -- Debits (sending tokens)
    SELECT 
        from_address as address,
        block_number,
        block_timestamp,
        log_index,
        -toDecimal256(amount, 0) as amount_change
    FROM ens_transfers
    WHERE from_address != '0x0000000000000000000000000000000000000000'
),
running_balances AS (
    SELECT 
        address,
        block_number,
        log_index,
        sum(amount_change) OVER (
            PARTITION BY address 
            ORDER BY block_number, log_index 
            ROWS UNBOUNDED PRECEDING
        ) as running_balance
    FROM balance_changes
),
latest_balances AS (
    SELECT 
        address,
        block_number,
        running_balance,
        row_number() OVER (PARTITION BY address ORDER BY block_number DESC, log_index DESC) as rn
    FROM running_balances
)
SELECT 
    address,
    block_number as latest_block_number,
    toString(running_balance) as current_balance
FROM latest_balances
WHERE rn = 1 AND running_balance > 0;

-- Current Delegate Power Materialized View (directly from delegate votes changed events)
CREATE MATERIALIZED VIEW IF NOT EXISTS current_delegate_power
ENGINE = MergeTree
ORDER BY delegate_address
AS
WITH latest_votes AS (
    SELECT 
        delegate,
        new_balance,
        block_number,
        block_timestamp,
        log_index,
        row_number() OVER (PARTITION BY delegate ORDER BY block_number DESC, log_index DESC) as rn
    FROM ens_delegate_votes_changed
)
SELECT 
    delegate as delegate_address,
    new_balance as voting_power,
    block_number,
    block_timestamp,
    log_index,
    now() as last_refreshed
FROM latest_votes
WHERE rn = 1;

-- Current Delegations Materialized View (directly from delegate changed events)
CREATE MATERIALIZED VIEW IF NOT EXISTS current_delegations
ENGINE = MergeTree
ORDER BY delegator
AS
WITH ranked_delegations AS (
    SELECT 
        delegator,
        to_delegate as delegate,
        from_delegate as prior_delegate,
        block_number,
        block_timestamp,
        row_number() OVER (PARTITION BY delegator ORDER BY block_number DESC, log_index DESC) as rn
    FROM ens_delegate_changed
)
SELECT 
    rd.delegator,
    coalesce(ctb.current_balance, '0') as delegator_balance,
    rd.delegate,
    rd.prior_delegate,
    rd.block_timestamp as delegated_timestamp
FROM ranked_delegations rd
LEFT JOIN current_token_balances ctb ON ctb.address = rd.delegator
WHERE rd.rn = 1;

-- Top 100 Delegates Materialized View
CREATE MATERIALIZED VIEW IF NOT EXISTS top_100_delegates
ENGINE = MergeTree
ORDER BY rank
AS
WITH delegate_stats AS (
    SELECT 
        delegate as delegate_address,
        sum(toDecimal256(delegator_balance, 0)) as voting_power,
        count(DISTINCT delegator) as delegations,
        countIf(toDecimal256(delegator_balance, 0) >= 1000000000000000000) as non_zero_delegations
    FROM current_delegations
    WHERE lower(delegate) != '0x0000000000000000000000000000000000000000'
    GROUP BY delegate
),
ranked_delegates AS (
    SELECT 
        delegate_address,
        voting_power,
        delegations,
        non_zero_delegations,
        row_number() OVER (ORDER BY voting_power DESC) as rank
    FROM delegate_stats
    ORDER BY voting_power DESC
    LIMIT 100
),
delegate_power_30d AS (
    SELECT 
        delegate as delegate_address,
        argMax(new_balance, block_timestamp) as voting_power_30d_ago
    FROM ens_delegate_votes_changed
    WHERE block_timestamp <= (now() - INTERVAL 30 DAY)
    GROUP BY delegate
)
SELECT 
    rd.rank,
    rd.delegate_address,
    toString(rd.voting_power) as voting_power,
    coalesce(dp30.voting_power_30d_ago, '0') as voting_power_30d_ago,
    rd.delegations,
    rd.non_zero_delegations,
    toString(rd.voting_power - toDecimal256(coalesce(dp30.voting_power_30d_ago, '0'), 0)) as power_change_30d
FROM ranked_delegates rd
LEFT JOIN delegate_power_30d dp30 ON dp30.delegate_address = rd.delegate_address
ORDER BY rd.voting_power DESC;
