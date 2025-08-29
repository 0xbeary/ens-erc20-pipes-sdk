# ens-erc20-indexer-pipes-sdk

A small EVM data pipe powered by Pipes SDK. Gets the balances of USDT wallets that moved the token and computes hourly averages. Can be used to track the relative dominance of large vs small players chainwide.

See the [Soldexer pipes overview](https://docs.soldexer.dev/pipes/overview) as well as `src/main.ts` and `src/transfers.sql` for details.

This software is in early beta. Interfaces of the components involved can change without notice.

## Running the pipe

```bash
# Install dependencies
pnpm install

# Start ClickHouse
docker compose up -d

# Start the indexer (src/index.ts)
pnpm start
# or alternatively: pnpm exec ts-node src/index.ts
```
You can now visit the local [ClickHouse console](http://localhost:8123/play) and observe the data that the pipe produces:
```sql
select * from transfers_raw;
```
```sql
-- see https://clickhouse.com/docs/materialized-view/incremental-materialized-view
select timestamp, avgMerge(avg_active_wallet_balance) from active_balance_stats group by timestamp;
```

## Related repositories

| Name | Description |
|------|-------------|
| [soldexer](https://github.com/subsquid-labs/soldexer) | The original Soldexer repo. Includes three pipes with some real-world utility. |
| [solana-ingest](https://github.com/subsquid/squid-sdk/tree/master/solana/solana-ingest) | Extracts Solana data and uploads compressed chunks to S3. |
| [solana-data-service](https://github.com/subsquid/squid-sdk/tree/solana-data-service/solana/solana-data-service) | Streams live Solana blocks to Portals via RPC. |
| [sqd-portal](https://github.com/subsquid/sqd-portal) | Handles incoming queries and routes to workers or hotblocks. |
| [worker-rs](https://github.com/subsquid/worker-rs) | Decentralized worker that queries and serves data chunks from S3. |
