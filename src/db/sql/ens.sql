CREATE TABLE IF NOT EXISTS ens_evt_approval (
    block_number UInt32 CODEC (DoubleDelta, ZSTD),
    transaction_hash FixedString(66),
    contract LowCardinality(FixedString(42)),
    log_index UInt16,
    timestamp DateTime CODEC (DoubleDelta, ZSTD),
    owner LowCardinality(FixedString(42)),
	spender LowCardinality(FixedString(42)),
	value UInt256,
    sign Int8 DEFAULT 1
)
ENGINE = CollapsingMergeTree(sign)
PARTITION BY toYYYYMM(timestamp)
ORDER BY (block_number, timestamp);

CREATE TABLE IF NOT EXISTS ens_evt_claim (
    block_number UInt32 CODEC (DoubleDelta, ZSTD),
    transaction_hash FixedString(66),
    contract LowCardinality(FixedString(42)),
    log_index UInt16,
    timestamp DateTime CODEC (DoubleDelta, ZSTD),
    claimant LowCardinality(FixedString(42)),
	amount UInt256,
    sign Int8 DEFAULT 1
)
ENGINE = CollapsingMergeTree(sign)
PARTITION BY toYYYYMM(timestamp)
ORDER BY (block_number, timestamp);

CREATE TABLE IF NOT EXISTS ens_evt_delegate_changed (
    block_number UInt32 CODEC (DoubleDelta, ZSTD),
    transaction_hash FixedString(66),
    contract LowCardinality(FixedString(42)),
    log_index UInt16,
    timestamp DateTime CODEC (DoubleDelta, ZSTD),
    delegator LowCardinality(FixedString(42)),
	from_delegate LowCardinality(FixedString(42)),
	to_delegate LowCardinality(FixedString(42)),
    sign Int8 DEFAULT 1
)
ENGINE = CollapsingMergeTree(sign)
PARTITION BY toYYYYMM(timestamp)
ORDER BY (block_number, timestamp);

CREATE TABLE IF NOT EXISTS ens_evt_delegate_votes_changed (
    block_number UInt32 CODEC (DoubleDelta, ZSTD),
    transaction_hash FixedString(66),
    contract LowCardinality(FixedString(42)),
    log_index UInt16,
    timestamp DateTime CODEC (DoubleDelta, ZSTD),
    delegate LowCardinality(FixedString(42)),
	previous_balance UInt256,
	new_balance UInt256,
    sign Int8 DEFAULT 1
)
ENGINE = CollapsingMergeTree(sign)
PARTITION BY toYYYYMM(timestamp)
ORDER BY (block_number, timestamp);
-- FixedString(66)
CREATE TABLE IF NOT EXISTS ens_evt_merkle_root_changed (
    block_number UInt32 CODEC (DoubleDelta, ZSTD),
    transaction_hash FixedString(66),
    contract LowCardinality(FixedString(42)),
    log_index UInt16,
    timestamp DateTime CODEC (DoubleDelta, ZSTD),
    merkle_root FixedString(66),
    sign Int8 DEFAULT 1
)
ENGINE = CollapsingMergeTree(sign)
PARTITION BY toYYYYMM(timestamp)
ORDER BY (block_number, timestamp);

CREATE TABLE IF NOT EXISTS ens_evt_ownership_transferred (
    block_number UInt32 CODEC (DoubleDelta, ZSTD),
    transaction_hash FixedString(66),
    contract LowCardinality(FixedString(42)),
    log_index UInt16,
    timestamp DateTime CODEC (DoubleDelta, ZSTD),
    previous_owner LowCardinality(FixedString(42)),
	new_owner LowCardinality(FixedString(42)),
    sign Int8 DEFAULT 1
)
ENGINE = CollapsingMergeTree(sign)
PARTITION BY toYYYYMM(timestamp)
ORDER BY (block_number, timestamp);

CREATE TABLE IF NOT EXISTS ens_evt_transfer (
    block_number UInt32 CODEC (DoubleDelta, ZSTD),
    transaction_hash FixedString(66),
    contract LowCardinality(FixedString(42)),
    log_index UInt16,
    timestamp DateTime CODEC (DoubleDelta, ZSTD),
    from LowCardinality(FixedString(42)),
	to LowCardinality(FixedString(42)),
	value UInt256,
    sign Int8 DEFAULT 1
)
ENGINE = CollapsingMergeTree(sign)
PARTITION BY toYYYYMM(timestamp)
ORDER BY (block_number, timestamp);

