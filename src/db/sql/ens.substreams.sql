-- Substreams ENS Event Tables
-- These are separate from the pipes-based tables to avoid conflicts

CREATE TABLE IF NOT EXISTS ens_substreams_evt_approval (
    block_number UInt64,
    block_hash String,
    transaction_hash String,
    log_index UInt32,
    transaction_index UInt32,
    address String,
    timestamp UInt64,
    owner String,
    approved String,
    tokenId String
) ENGINE = ReplacingMergeTree()
ORDER BY (block_number, transaction_hash, log_index);

CREATE TABLE IF NOT EXISTS ens_substreams_evt_claim (
    block_number UInt64,
    block_hash String,
    transaction_hash String,
    log_index UInt32,
    transaction_index UInt32,
    address String,
    timestamp UInt64,
    claimant String,
    amount String
) ENGINE = ReplacingMergeTree()
ORDER BY (block_number, transaction_hash, log_index);

CREATE TABLE IF NOT EXISTS ens_substreams_evt_delegate_changed (
    block_number UInt64,
    block_hash String,
    transaction_hash String,
    log_index UInt32,
    transaction_index UInt32,
    address String,
    timestamp UInt64,
    delegator String,
    fromDelegate String,
    toDelegate String
) ENGINE = ReplacingMergeTree()
ORDER BY (block_number, transaction_hash, log_index);

CREATE TABLE IF NOT EXISTS ens_substreams_evt_delegate_votes_changed (
    block_number UInt64,
    block_hash String,
    transaction_hash String,
    log_index UInt32,
    transaction_index UInt32,
    address String,
    timestamp UInt64,
    delegate String,
    previousBalance String,
    newBalance String
) ENGINE = ReplacingMergeTree()
ORDER BY (block_number, transaction_hash, log_index);

CREATE TABLE IF NOT EXISTS ens_substreams_evt_merkle_root_changed (
    block_number UInt64,
    block_hash String,
    transaction_hash String,
    log_index UInt32,
    transaction_index UInt32,
    address String,
    timestamp UInt64,
    merkleRoot String
) ENGINE = ReplacingMergeTree()
ORDER BY (block_number, transaction_hash, log_index);

CREATE TABLE IF NOT EXISTS ens_substreams_evt_ownership_transferred (
    block_number UInt64,
    block_hash String,
    transaction_hash String,
    log_index UInt32,
    transaction_index UInt32,
    address String,
    timestamp UInt64,
    previousOwner String,
    newOwner String
) ENGINE = ReplacingMergeTree()
ORDER BY (block_number, transaction_hash, log_index);

CREATE TABLE IF NOT EXISTS ens_substreams_evt_transfer (
    block_number UInt64,
    block_hash String,
    transaction_hash String,
    log_index UInt32,
    transaction_index UInt32,
    address String,
    timestamp UInt64,
    from String,
    to String,
    tokenId String
) ENGINE = ReplacingMergeTree()
ORDER BY (block_number, transaction_hash, log_index);
