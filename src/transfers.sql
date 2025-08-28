CREATE TABLE IF NOT EXISTS transfers_raw
(
  timestamp           DateTime CODEC (DoubleDelta, ZSTD),
  pre_balance         UInt256
) ENGINE = MergeTree()
ORDER BY (timestamp);

CREATE MATERIALIZED VIEW IF NOT EXISTS active_balance_stats
ENGINE = AggregatingMergeTree()
ORDER BY (timestamp)
AS
SELECT
  toStartOfHour(timestamp) as timestamp,
  avgState(toFloat64(pre_balance) / 1e6) AS avg_active_wallet_balance
FROM transfers_raw
GROUP BY timestamp;



