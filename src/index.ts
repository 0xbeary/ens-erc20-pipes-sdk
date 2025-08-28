import * as path from 'node:path';
import { createClickhouseClient, ensureTables, toUnixTime } from './utils/clickhouse';
import { createLogger } from './utils/logger';
import { getConfig } from '../config';
import { ClickhouseState } from '@sqd-pipes/core';
import { EnsIndexerStream } from './streams/ens.stream';

const config = getConfig();

const logger = createLogger('ens-indexer').child({ network: config.network });
logger.debug('indexer started');

async function main() {
  const clickhouse = await createClickhouseClient();
  await ensureTables(clickhouse, __dirname);

  const ds = new EnsIndexerStream({
    portal: config.portal.url,
    blockRange: {
      from: config.blockFrom,
    },
    args: {
      clickhouseClient: clickhouse,
      contracts: [config.contractAddress],
    },
    logger,
    state: new ClickhouseState(clickhouse, {
      table: `sync_status`,
      database: 'default',
      id: `ens-indexer`,
      onRollback: async ({ state, latest }) => {
        logger.info(`ROLLBACK called. latest.timestamp: ${latest.timestamp}`);
        if (!latest.timestamp) {
          return; // fresh table
        }
        await state.removeAllRows({
          table: `ens_events`,
          where: `timestamp > ${latest.timestamp}`,
        });
        logger.info(`ROLLBACK finished. Rows are removed`);
      },
    }),
  });
  await ds.initialize();

  for await (const events of await ds.stream()) {
    if (events.length === 0) {
      await ds.ack();
      continue;
    }
    
    await clickhouse.insert({
      table: `ens_events`,
      values: events.map((e) => {
        const base = {
            block_number: e.block.number,
            transaction_hash: e.transaction.hash,
            transaction_index: e.transaction.index,
            log_index: e.transaction.logIndex,
            token: e.token_address,
            timestamp: toUnixTime(e.timestamp),
            event_type: e.type,
        }

        if (e.type === 'Transfer') {
            return {
                ...base,
                from: e.from,
                to: e.to,
                amount: e.amount.toString(),
            }
        }
        if (e.type === 'DelegateChanged') {
            return {
                ...base,
                delegator: e.delegator,
                from_delegate: e.fromDelegate,
                to_delegate: e.toDelegate,
            }
        }
        if (e.type === 'DelegateVotesChanged') {
            return {
                ...base,
                delegate: e.delegate,
                previous_balance: e.previousBalance.toString(),
                new_balance: e.newBalance.toString(),
            }
        }
        return base;
      }),
      format: 'JSONEachRow',
    });
    
    await ds.ack();
  }
}

void main();
