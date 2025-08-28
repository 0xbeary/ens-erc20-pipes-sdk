CREATE TABLE IF NOT EXISTS events
(
    event_type        String,
    args              String,
    log_index         UInt32,
    transaction_index UInt32,
    transaction_hash  String,
    address           String,
    block_hash        String,
    block_number      UInt64,
    block_timestamp   DateTime,
    created_at        DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (block_number, event_type, transaction_hash, log_index);
