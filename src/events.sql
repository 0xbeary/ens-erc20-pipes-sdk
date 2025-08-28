CREATE TABLE IF NOT EXISTS ens_events
(
    `block_number` UInt64,
    `transaction_hash` String,
    `transaction_index` UInt32,
    `log_index` UInt32,
    `token` String,
    `timestamp` UInt32,
    `event_type` String,
    `from` Nullable(String),
    `to` Nullable(String),
    `amount` Nullable(String),
    `delegator` Nullable(String),
    `from_delegate` Nullable(String),
    `to_delegate` Nullable(String),
    `delegate` Nullable(String),
    `previous_balance` Nullable(String),
    `new_balance` Nullable(String)
)
ENGINE = MergeTree
ORDER BY (timestamp, block_number, transaction_hash, log_index);
