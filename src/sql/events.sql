CREATE TABLE IF NOT EXISTS ens_evt_approval 
(
    block_number                UInt32 CODEC (DoubleDelta, ZSTD),
    transaction_hash            String,
    contract                    LowCardinality(String),
    log_index                   UInt16,
    timestamp                   DateTime CODEC (DoubleDelta, ZSTD),
    owner                       LowCardinality(String),
    spender                     LowCardinality(String),
    value                       UInt256,
    sign                        Int8 DEFAULT 1
)
ENGINE = CollapsingMergeTree(sign)
PARTITION BY toYYYYMM(timestamp)
ORDER BY (block_number, timestamp);

CREATE TABLE IF NOT EXISTS ens_evt_claim 
(
    block_number                UInt32 CODEC (DoubleDelta, ZSTD),
    transaction_hash            String,
    contract                    LowCardinality(String),
    log_index                   UInt16,
    timestamp                   DateTime CODEC (DoubleDelta, ZSTD),
    claimant                    LowCardinality(String),
    amount                      UInt256,
    sign                        Int8 DEFAULT 1
)
ENGINE = CollapsingMergeTree(sign)
PARTITION BY toYYYYMM(timestamp)
ORDER BY (block_number, timestamp);

CREATE TABLE IF NOT EXISTS ens_evt_delegate_changed 
(
    block_number                UInt32 CODEC (DoubleDelta, ZSTD),
    transaction_hash            String,
    contract                    LowCardinality(String),
    log_index                   UInt16,
    timestamp                   DateTime CODEC (DoubleDelta, ZSTD),
    delegator                   LowCardinality(String),
    from_delegate               LowCardinality(String),
    to_delegate                 LowCardinality(String),
    sign                        Int8 DEFAULT 1
)
ENGINE = CollapsingMergeTree(sign)
PARTITION BY toYYYYMM(timestamp)
ORDER BY (block_number, timestamp);

CREATE TABLE IF NOT EXISTS ens_evt_delegate_votes_changed 
(
    block_number                UInt32 CODEC (DoubleDelta, ZSTD),
    transaction_hash            String,
    contract                    LowCardinality(String),
    log_index                   UInt16,
    timestamp                   DateTime CODEC (DoubleDelta, ZSTD),
    delegate                    LowCardinality(String),
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
    transaction_hash            String,
    contract                    LowCardinality(String),
    log_index                   UInt16,
    timestamp                   DateTime CODEC (DoubleDelta, ZSTD),
    merkle_root                 String,
    sign                        Int8 DEFAULT 1
)
ENGINE = CollapsingMergeTree(sign)
PARTITION BY toYYYYMM(timestamp)
ORDER BY (block_number, timestamp);

CREATE TABLE IF NOT EXISTS ens_evt_ownership_transferred 
(
    block_number                UInt32 CODEC (DoubleDelta, ZSTD),
    transaction_hash            String,
    contract                    LowCardinality(String),
    log_index                   UInt16,
    timestamp                   DateTime CODEC (DoubleDelta, ZSTD),
    previous_owner              LowCardinality(String),
    new_owner                   LowCardinality(String),
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
