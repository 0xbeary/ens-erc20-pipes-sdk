/* simple-pipe - a small demo showcasing the pipes architecture
 *
 * Gets the balances of USDT wallets that moved the token
 * and computes hourly averages. Can be used to track the
 * relative dominance of large vs small players chainwide.
 *
 * This executable only fetches the raw data required.
 * See src/transfers.sql for definition of the materialized
 * view that aggregates it into hourly averages.
 */

import path from 'node:path';
import { PortalAbstractStream } from '@sqd-pipes/core';
import { createClickhouseClient, ensureTables } from './clickhouse';
import { ClickhouseState } from '@sqd-pipes/core';
import { createLogger } from './logger';

type TransferPreBalance = {
  timestamp: Date;
  pre_balance: bigint;
};

// A stream with just the data we need
class TransferPreBalancesStream extends PortalAbstractStream<
  TransferPreBalance,
  { token: string; }
> {
  async stream(): Promise<ReadableStream<TransferPreBalance[]>> {
    // First, we request a stream of raw data from SQD network
    // Structure of the getStream argument mirrors that of
    // the raw Soldexer API JSON request, see
    // https://docs.soldexer.dev/api-reference/endpoint/post-stream
    const source = await this.getStream({
      type: 'solana',
      fields: {
        block: {
          // Although we're only using timestamp in the final data,
          // we also need slot number and block hash for progress tracking
          number: true,
          hash: true,
          timestamp: true,
        },
        tokenBalance: {
          preAmount: true,
        },
      },
      tokenBalances: [
        {
          preMint: [ this.options.args.token ]
        }
      ]
    });

    // Transforming the raw data stream into a stream
    // of convenient TransferPreBalance objects. If we needed
    // to do any decoding we would do it here
    const stream = source.pipeThrough(
      new TransformStream({
        transform: ({ blocks }, controller) => {
          const res = blocks.flatMap((block: any) => {
            if (!block.tokenBalances) return [];

            const blockTimestamp = new Date(block.header.timestamp * 1000);

            return block.tokenBalances.map((tb) => ({
              timestamp: blockTimestamp,
              pre_balance: tb.preAmount,
            }));
          });

          controller.enqueue(res);
        },
      }),
    );

    return stream;
  }
}

const TRACKED_TOKEN = 'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB'; // USDT

async function main() {
  const clickhouse = createClickhouseClient();

  const ds = new TransferPreBalancesStream({
    portal: 'https://portal.sqd.dev/datasets/solana-mainnet',
    blockRange: {
      from: 317617480, // Jan 31 2025
      // Check out
      // https://portal.sqd.dev/datasets/solana-mainnet/metadata
      // for up-to-date info on the earliest available block
    },
    args: {
      token: TRACKED_TOKEN,
    },
    // We use the indexer state to track the last block processed
    // and resume from there
    state: new ClickhouseState(clickhouse, {
      table: 'solana_sync_status',
      id: 'transfers',
    }),
    logger: createLogger('transfers'),
  });

  // Ensure that ClickHouse has the necessary table and
  // the materialized view
  await ensureTables(clickhouse, path.join(__dirname, 'transfers.sql'));

  for await (const transfers of await ds.stream()) {
    await clickhouse.insert({
      table: 'transfers_raw',
      values: transfers,
      format: 'JSONEachRow',
    });

    await ds.ack();
  }
}

void main();
