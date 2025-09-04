CREATE TABLE IF NOT EXISTS ens_evt_approval 
(
    block_number                UInt32 CODEC (DoubleDelta, ZSTD),
    transaction_hash            FixedString(66),
    contract                    LowCardinality(FixedString(42)),
    log_index                   UInt16,
    timestamp                   DateTime CODEC (DoubleDelta, ZSTD),
    owner                       LowCardinality(FixedString(42)),
    spender                     LowCardinality(FixedString(42)),
    value                       UInt256,
    sign                        Int8 DEFAULT 1
)
ENGINE = CollapsingMergeTree(sign)
PARTITION BY toYYYYMM(timestamp)
ORDER BY (block_number, timestamp);

CREATE TABLE IF NOT EXISTS ens_evt_claim 
(
    block_number                UInt32 CODEC (DoubleDelta, ZSTD),
    transaction_hash            FixedString(66),
    contract                    LowCardinality(FixedString(42)),
    log_index                   UInt16,
    timestamp                   DateTime CODEC (DoubleDelta, ZSTD),
    claimant                    LowCardinality(FixedString(42)),
    amount                      UInt256,
    sign                        Int8 DEFAULT 1
)
ENGINE = CollapsingMergeTree(sign)
PARTITION BY toYYYYMM(timestamp)
ORDER BY (block_number, timestamp);

CREATE TABLE IF NOT EXISTS ens_evt_delegate_changed 
(
    block_number                UInt32 CODEC (DoubleDelta, ZSTD),
    transaction_hash            FixedString(66),
    contract                    LowCardinality(FixedString(42)),
    log_index                   UInt16,
    timestamp                   DateTime CODEC (DoubleDelta, ZSTD),
    delegator                   LowCardinality(FixedString(42)),
    from_delegate               LowCardinality(FixedString(42)),
    to_delegate                 LowCardinality(FixedString(42)),
    sign                        Int8 DEFAULT 1
)
ENGINE = CollapsingMergeTree(sign)
PARTITION BY toYYYYMM(timestamp)
ORDER BY (block_number, timestamp);

CREATE TABLE IF NOT EXISTS ens_evt_delegate_votes_changed 
(
    block_number                UInt32 CODEC (DoubleDelta, ZSTD),
    transaction_hash            FixedString(66),
    contract                    LowCardinality(FixedString(42)),
    log_index                   UInt16,
    timestamp                   DateTime CODEC (DoubleDelta, ZSTD),
    delegate                    LowCardinality(FixedString(42)),
    previous_balance            UInt256,
    new_balance                 UInt256,
    sign                        Int8 DEFAULT 1
)
ENGINE = CollapsingMergeTree(sign)
PARTITION BY toYYYYMM(timestamp)
ORDER BY (block_number, timestamp);

CREATE TABLE IF NOT EXISTS ens_evt_merkle_root_changed 
(
    block_number                UInt32 CODEC (DoubleDelta, ZSTD),
    transaction_hash            FixedString(66),
    contract                    LowCardinality(FixedString(42)),
    log_index                   UInt16,
    timestamp                   DateTime CODEC (DoubleDelta, ZSTD),
    merkle_root                 FixedString(32),
    sign                        Int8 DEFAULT 1
)
ENGINE = CollapsingMergeTree(sign)
PARTITION BY toYYYYMM(timestamp)
ORDER BY (block_number, timestamp);

CREATE TABLE IF NOT EXISTS ens_evt_ownership_transferred 
(
    block_number                UInt32 CODEC (DoubleDelta, ZSTD),
    transaction_hash            FixedString(66),
    contract                    LowCardinality(FixedString(42)),
    log_index                   UInt16,
    timestamp                   DateTime CODEC (DoubleDelta, ZSTD),
    previous_owner              LowCardinality(FixedString(42)),
    new_owner                   LowCardinality(FixedString(42)),
    sign                        Int8 DEFAULT 1
)
ENGINE = CollapsingMergeTree(sign)
PARTITION BY toYYYYMM(timestamp)
ORDER BY (block_number, timestamp);

CREATE TABLE IF NOT EXISTS ens_evt_transfer (
    block_number                UInt32 CODEC (DoubleDelta, ZSTD),
    transaction_hash            FixedString(66),
    contract                    LowCardinality(FixedString(42)),
    log_index                   UInt16,
    timestamp                   DateTime CODEC (DoubleDelta, ZSTD),
    from                        LowCardinality(FixedString(42)),
    to                          LowCardinality(FixedString(42)),
    value                       UInt256,
    sign                        Int8 DEFAULT 1
)
ENGINE = CollapsingMergeTree(sign)
PARTITION BY toYYYYMM(timestamp)
ORDER BY (block_number, timestamp);

-- Current Token Balances (calculated from transfers)
CREATE TABLE IF NOT EXISTS current_token_balances
(
    address LowCardinality(FixedString(42)),
    balance_change Decimal(76, 0),
    latest_block_number UInt32
)
ENGINE = SummingMergeTree
ORDER BY address;

CREATE MATERIALIZED VIEW IF NOT EXISTS current_token_balances_mv TO current_token_balances AS
WITH balance_changes AS (
    -- Credits (receiving tokens)
    SELECT 
        to as address,
        block_number,
        timestamp,
        log_index,
        toDecimal256(value, 0) as amount_change
    FROM ens_evt_transfer
    WHERE to != '0x0000000000000000000000000000000000000000'
      AND sign = 1
    
    UNION ALL
    
    -- Debits (sending tokens)
    SELECT 
        from as address,
        block_number,
        timestamp,
        log_index,
        -toDecimal256(value, 0) as amount_change
    FROM ens_evt_transfer
    WHERE from != '0x0000000000000000000000000000000000000000'
      AND sign = 1
)
SELECT 
    address,
    sum(amount_change) as balance_change,
    argMax(block_number, timestamp) as latest_block_number
FROM balance_changes
GROUP BY address;

-- Current Delegate Power Materialized View
CREATE TABLE IF NOT EXISTS current_delegate_power
(
    delegate_address LowCardinality(FixedString(42)),
    voting_power UInt256,
    block_number UInt32,
    timestamp DateTime,
    log_index UInt16,
    last_refreshed DateTime
)
ENGINE = ReplacingMergeTree(timestamp)
ORDER BY delegate_address;

CREATE MATERIALIZED VIEW IF NOT EXISTS current_delegate_power_mv
ENGINE = ReplacingMergeTree(timestamp)
ORDER BY delegate_address
AS
SELECT 
    delegate as delegate_address,
    new_balance as voting_power,
    block_number,
    timestamp,
    log_index,
    now() as last_refreshed
FROM ens_evt_delegate_votes_changed
WHERE sign = 1;

-- Current Delegations Materialized View
CREATE TABLE IF NOT EXISTS current_delegations
(
    delegator LowCardinality(FixedString(42)),
    delegator_balance Decimal(76, 0),
    delegate LowCardinality(FixedString(42)),
    prior_delegate LowCardinality(FixedString(42)),
    delegated_timestamp DateTime
)
ENGINE = ReplacingMergeTree(delegated_timestamp)
ORDER BY delegator;

CREATE MATERIALIZED VIEW IF NOT EXISTS current_delegations_mv
ENGINE = ReplacingMergeTree(delegated_timestamp)
ORDER BY delegator
AS
WITH ranked_delegations AS (
    SELECT 
        delegator,
        to_delegate as delegate,
        from_delegate as prior_delegate,
        block_number,
        timestamp,
        row_number() OVER (PARTITION BY delegator ORDER BY block_number DESC, log_index DESC) as rn
    FROM ens_evt_delegate_changed
    WHERE sign = 1
)
SELECT 
    rd.delegator,
    coalesce(ctb.balance_change, 0) as delegator_balance,
    rd.delegate,
    rd.prior_delegate,
    rd.timestamp as delegated_timestamp
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
        sum(delegator_balance) as voting_power,
        count(DISTINCT delegator) as delegations,
        countIf(delegator_balance >= 1000000000000000000) as non_zero_delegations
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
        argMax(new_balance, timestamp) as voting_power_30d_ago
    FROM ens_evt_delegate_votes_changed
    WHERE timestamp <= (now() - INTERVAL 30 DAY)
      AND sign = 1
    GROUP BY delegate
)
SELECT 
    rd.rank,
    rd.delegate_address,
    rd.voting_power,
    coalesce(dp30.voting_power_30d_ago, 0) as voting_power_30d_ago,
    rd.delegations,
    rd.non_zero_delegations,
    rd.voting_power - coalesce(dp30.voting_power_30d_ago, 0) as power_change_30d
FROM ranked_delegates rd
LEFT JOIN delegate_power_30d dp30 ON dp30.delegate_address = rd.delegate_address
ORDER BY rd.voting_power DESC;
WHERE sign = 1
UNION ALL
SELECT 
    'MerkleRootChanged' as event_type,
    transaction_hash,
    log_index,
    contract,
    block_number,
    timestamp,
    '' as from_address,
    '' as to_address,
    0 as amount,
    '' as owner,
    '' as spender,
    '' as claimant,
    '' as delegator,
    '' as from_delegate,
    '' as to_delegate,
    '' as delegate,
    0 as previous_balance,
    0 as new_balance,
    merkle_root,
    '' as previous_owner,
    '' as new_owner
FROM ens_evt_merkle_root_changed
WHERE sign = 1
UNION ALL
SELECT 
    'OwnershipTransferred' as event_type,
    transaction_hash,
    log_index,
    contract,
    block_number,
    timestamp,
    '' as from_address,
    '' as to_address,
    0 as amount,
    '' as owner,
    '' as spender,
    '' as claimant,
    '' as delegator,
    '' as from_delegate,
    '' as to_delegate,
    '' as delegate,
    0 as previous_balance,
    0 as new_balance,
    '' as merkle_root,
    previous_owner,
    new_owner
FROM ens_evt_ownership_transferred
WHERE sign = 1;

-- Current Token Balances (calculated from transfers)
CREATE TABLE IF NOT EXISTS current_token_balances
(
    address LowCardinality(FixedString(42)),
    balance_change Decimal(76, 0),
    latest_block_number UInt32
)
ENGINE = SummingMergeTree
ORDER BY address;

CREATE MATERIALIZED VIEW IF NOT EXISTS current_token_balances_mv TO current_token_balances AS
WITH balance_changes AS (
    -- Credits (receiving tokens)
    SELECT 
        to as address,
        block_number,
        timestamp,
        log_index,
        toDecimal256(value, 0) as amount_change
    FROM ens_evt_transfer
    WHERE to != '0x0000000000000000000000000000000000000000'
      AND sign = 1
    
    UNION ALL
    
    -- Debits (sending tokens)
    SELECT 
        from as address,
        block_number,
        timestamp,
        log_index,
        -toDecimal256(value, 0) as amount_change
    FROM ens_evt_transfer
    WHERE from != '0x0000000000000000000000000000000000000000'
      AND sign = 1
)
SELECT 
    address,
    sum(amount_change) as balance_change,
    argMax(block_number, timestamp) as latest_block_number
FROM balance_changes
GROUP BY address;

-- Current Delegate Power Materialized View
CREATE TABLE IF NOT EXISTS current_delegate_power
(
    delegate_address LowCardinality(FixedString(42)),
    voting_power UInt256,
    block_number UInt32,
    timestamp DateTime,
    log_index UInt16,
    last_refreshed DateTime
)
ENGINE = ReplacingMergeTree(timestamp)
ORDER BY delegate_address;

CREATE MATERIALIZED VIEW IF NOT EXISTS current_delegate_power_mv TO current_delegate_power AS
SELECT 
    delegate as delegate_address,
    new_balance as voting_power,
    block_number,
    timestamp,
    log_index,
    now() as last_refreshed
FROM ens_evt_delegate_votes_changed
WHERE sign = 1;

-- Current Delegations Materialized View
CREATE TABLE IF NOT EXISTS current_delegations
(
    delegator LowCardinality(FixedString(42)),
    delegator_balance Decimal(76, 0),
    delegate LowCardinality(FixedString(42)),
    prior_delegate LowCardinality(FixedString(42)),
    delegated_timestamp DateTime
)
ENGINE = ReplacingMergeTree(delegated_timestamp)
ORDER BY delegator;

CREATE MATERIALIZED VIEW IF NOT EXISTS current_delegations_mv TO current_delegations AS
WITH ranked_delegations AS (
    SELECT 
        delegator,
        to_delegate as delegate,
        from_delegate as prior_delegate,
        block_number,
        timestamp,
        row_number() OVER (PARTITION BY delegator ORDER BY block_number DESC, log_index DESC) as rn
    FROM ens_evt_delegate_changed
    WHERE sign = 1
)
SELECT 
    rd.delegator,
    coalesce(ctb.balance_change, 0) as delegator_balance,
    rd.delegate,
    rd.prior_delegate,
    rd.timestamp as delegated_timestamp
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
        sum(delegator_balance) as voting_power,
        count(DISTINCT delegator) as delegations,
        countIf(delegator_balance >= 1000000000000000000) as non_zero_delegations
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
        argMax(new_balance, timestamp) as voting_power_30d_ago
    FROM ens_evt_delegate_votes_changed
    WHERE timestamp <= (now() - INTERVAL 30 DAY)
      AND sign = 1
    GROUP BY delegate
)
SELECT 
    rd.rank,
    rd.delegate_address,
    rd.voting_power,
    coalesce(dp30.voting_power_30d_ago, 0) as voting_power_30d_ago,
    rd.delegations,
    rd.non_zero_delegations,
    rd.voting_power - coalesce(dp30.voting_power_30d_ago, 0) as power_change_30d
FROM ranked_delegates rd
LEFT JOIN delegate_power_30d dp30 ON dp30.delegate_address = rd.delegate_address
ORDER BY rd.voting_power DESC;

