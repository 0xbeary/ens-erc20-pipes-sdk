# ens-erc20-indexer-pipes-sdk

This pipe demonstrates generating a full local event stream from events from any contract:

All you need to do is to:

1. Create a an abi file
2. generate tables
3. generate a stream

Then you can:

4. add other data sources / streams for rich data experiences
5. create materialised views for real time data 

### Steps to create indexer

Typescript codegen from the contract ABI:
`npx @subsquid/evm-typegen src/packages/streams/src/streams/evm/contracts 0xC18360217D8F7Ab5e7c516566761Ea12Ce7F9D72 --etherscan-api-key $ETHERSCAN_API`

Create a new stream from the desired events
`src/streams/ens.stream.ts`

Generate clickhouse tables for the raw events
`npx events-to-table generate --from address --contract 0xC18360217D8F7Ab5e7c516566761Ea12Ce7F9D72 --etherscan $ETHERSCAN_API --output src/db/sql/ens.sql --table-prefix ens_evt`

Forward the events from the stream to the desired table
`src/indexers/ens.indexer.ts`


## Running the indexer

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
