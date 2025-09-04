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

-- All Events Unified View
CREATE TABLE IF NOT EXISTS ens_all_events
(
    event_type LowCardinality(String),
    block_number UInt32 CODEC (DoubleDelta, ZSTD),
    transaction_hash FixedString(66),
    contract LowCardinality(FixedString(42)),
    log_index UInt16,
    timestamp DateTime CODEC (DoubleDelta, ZSTD),
    from_address String,
    to_address String,
    value UInt256,
    owner String,
    spender String,
    claimant String,
    amount UInt256,
    delegator String,
    from_delegate String,
    to_delegate String,
    delegate String,
    previous_balance UInt256,
    new_balance UInt256,
    merkle_root String,
    previous_owner String,
    new_owner String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(timestamp)
ORDER BY (block_number, timestamp, log_index);

CREATE MATERIALIZED VIEW IF NOT EXISTS ens_all_events_mv TO ens_all_events AS
SELECT 
    'Transfer' as event_type,
    block_number,
    transaction_hash,
    contract,
    log_index,
    timestamp,
    from as from_address,
    to as to_address,
    value,
    '' as owner,
    '' as spender,
    '' as claimant,
    0 as amount,
    '' as delegator,
    '' as from_delegate,
    '' as to_delegate,
    '' as delegate,
    0 as previous_balance,
    0 as new_balance,
    '' as merkle_root,
    '' as previous_owner,
    '' as new_owner
FROM ens_evt_transfer
WHERE sign = 1

UNION ALL

SELECT 
    'Approval' as event_type,
    block_number,
    transaction_hash,
    contract,
    log_index,
    timestamp,
    '' as from_address,
    '' as to_address,
    value,
    owner,
    spender,
    '' as claimant,
    0 as amount,
    '' as delegator,
    '' as from_delegate,
    '' as to_delegate,
    '' as delegate,
    0 as previous_balance,
    0 as new_balance,
    '' as merkle_root,
    '' as previous_owner,
    '' as new_owner
FROM ens_evt_approval
WHERE sign = 1

UNION ALL

SELECT 
    'Claim' as event_type,
    block_number,
    transaction_hash,
    contract,
    log_index,
    timestamp,
    '' as from_address,
    '' as to_address,
    0 as value,
    '' as owner,
    '' as spender,
    claimant,
    amount,
    '' as delegator,
    '' as from_delegate,
    '' as to_delegate,
    '' as delegate,
    0 as previous_balance,
    0 as new_balance,
    '' as merkle_root,
    '' as previous_owner,
    '' as new_owner
FROM ens_evt_claim
WHERE sign = 1

UNION ALL

SELECT 
    'DelegateChanged' as event_type,
    block_number,
    transaction_hash,
    contract,
    log_index,
    timestamp,
    '' as from_address,
    '' as to_address,
    0 as value,
    '' as owner,
    '' as spender,
    '' as claimant,
    0 as amount,
    delegator,
    from_delegate,
    to_delegate,
    '' as delegate,
    0 as previous_balance,
    0 as new_balance,
    '' as merkle_root,
    '' as previous_owner,
    '' as new_owner
FROM ens_evt_delegate_changed
WHERE sign = 1

UNION ALL

SELECT 
    'DelegateVotesChanged' as event_type,
    block_number,
    transaction_hash,
    contract,
    log_index,
    timestamp,
    '' as from_address,
    '' as to_address,
    0 as value,
    '' as owner,
    '' as spender,
    '' as claimant,
    0 as amount,
    '' as delegator,
    '' as from_delegate,
    '' as to_delegate,
    delegate,
    previous_balance,
    new_balance,
    '' as merkle_root,
    '' as previous_owner,
    '' as new_owner
FROM ens_evt_delegate_votes_changed
WHERE sign = 1

UNION ALL

SELECT 
    'MerkleRootChanged' as event_type,
    block_number,
    transaction_hash,
    contract,
    log_index,
    timestamp,
    '' as from_address,
    '' as to_address,
    0 as value,
    '' as owner,
    '' as spender,
    '' as claimant,
    0 as amount,
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
    block_number,
    transaction_hash,
    contract,
    log_index,
    timestamp,
    '' as from_address,
    '' as to_address,
    0 as value,
    '' as owner,
    '' as spender,
    '' as claimant,
    0 as amount,
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

