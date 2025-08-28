import * as path from 'node:path';
import { createClickhouseClient, ensureTables } from './utils/clickhouse';
import { createLogger } from './utils/logger';
import { getConfig } from '../config';
import { ClickhouseState } from '@sqd-pipes/core';
import { EnsIndexerStream } from './streams/ens.stream';

// Load configuration settings
const config = getConfig();

// Setup logger with network context
const logger = createLogger('ens-indexer').child({ network: config.network });
logger.debug('indexer started');

async function main() {
  // Initialize ClickHouse client and ensure tables exist
  const clickhouse = await createClickhouseClient();
  await ensureTables(clickhouse, __dirname);

  // Setup the ENS event stream
  const ds = new EnsIndexerStream({
    portal: config.portal.url,
    blockRange: {
      from: config.blockFrom, // Start from configured block
    },
    args: {
      clickhouseClient: clickhouse,
      contracts: [config.contractAddress], // ENS token contract
    },
    logger,
    // Track indexing progress and handle blockchain reorgs
    state: new ClickhouseState(clickhouse, {
      table: `sync_status`,
      database: 'default',
      id: `ens-indexer`,
      // Handle blockchain reorganizations
      onRollback: async ({ state, latest }) => {
        logger.info(`ROLLBACK called. latest.timestamp: ${latest.timestamp}`);
        if (!latest.timestamp) {
          return; // fresh table
        }
        // Remove events from rolled-back blocks
        await state.removeAllRows({
          table: `ens_events`,
          where: `block_timestamp > ${latest.timestamp}`,
        });
        logger.info(`ROLLBACK finished. Rows are removed`);
      },
    }),
  });
  await ds.initialize();

  // Process events in batches
  for await (const events of await ds.stream()) {
    // Skip empty batches
    if (events.length === 0) {
      await ds.ack();
      continue;
    }
    
    // Get block information for logging
    const firstEvent = events[0];
    const blockNumber = firstEvent.block.number;
    const blockHash = firstEvent.block.hash;
    
    // Insert events into ClickHouse
    await clickhouse.insert({
      table: `ens_events`,
      values: events.map((e) => {
        // Common fields for all event types
        const base = {
            event_type: e.type,
            log_index: e.transaction.logIndex,
            transaction_index: e.transaction.index,
            transaction_hash: e.transaction.hash,
            contract_address: e.token_address,
            block_hash: e.block.hash,
            block_number: e.block.number,
            block_timestamp: e.timestamp, // Timestamp from the stream
        }

        // Add event-specific fields based on type
        if (e.type === 'Transfer') {
            return {
                ...base,
                from_address: e.from,
                to_address: e.to,
                amount: e.amount.toString(), // Convert bigint to string
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
                previous_balance: e.previousBalance.toString(), // Convert bigint to string
                new_balance: e.newBalance.toString(), // Convert bigint to string
            }
        }
        return base;
      }),
      format: 'JSONEachRow',
    });
    
    // Log processing progress with block information
    logger.info(`Processed ${events.length} events from block ${blockNumber} (${blockHash.slice(0, 10)}...)`);
    
    // Acknowledge successful processing
    await ds.ack();
  }
}

void main();
