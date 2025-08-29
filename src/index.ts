import * as path from 'node:path';
import { createClickhouseClient, ensureTables, toUnixTime } from './utils/clickhouse';
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
  const clickhouse = createClickhouseClient();
  await ensureTables(clickhouse, __dirname);

  // Setup the ENS event stream
  const ds = new EnsIndexerStream({
    portal: config.portal.url,
    blockRange: {
      from: config.blockFrom, // Start from configured block (only used on first run, state manager handles resumption)
    },
    args: {
      contracts: [config.contractAddress], // ENS token contract
    },
    logger,
    // Track indexing progress and handle blockchain reorgs
    state: new ClickhouseState(clickhouse, {
      table: `evm_sync_status`,
      database: process.env.CLICKHOUSE_DB || 'default',
      id: `ens-indexer`,
    }),
  });

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
    
    // Group events by type for efficient batch insertion
    const transferEvents = events.filter(e => e.type === 'Transfer');
    const delegateChangedEvents = events.filter(e => e.type === 'DelegateChanged');
    const delegateVotesChangedEvents = events.filter(e => e.type === 'DelegateVotesChanged');
    
    // Insert Transfer events
    if (transferEvents.length > 0) {
      await clickhouse.insert({
        table: 'ens_transfers',
        values: transferEvents.map((e) => ({
          transaction_hash: e.transaction.hash,
          log_index: e.transaction.logIndex,
          transaction_index: e.transaction.index,
          contract_address: e.token_address,
          block_hash: e.block.hash,
          block_number: e.block.number,
          block_timestamp: toUnixTime(e.timestamp),
          from_address: e.from,
          to_address: e.to,
          amount: e.amount.toString(),
        })),
        format: 'JSONEachRow',
      });
    }
    
    // Insert DelegateChanged events
    if (delegateChangedEvents.length > 0) {
      await clickhouse.insert({
        table: 'ens_delegate_changed',
        values: delegateChangedEvents.map((e) => ({
          transaction_hash: e.transaction.hash,
          log_index: e.transaction.logIndex,
          transaction_index: e.transaction.index,
          contract_address: e.token_address,
          block_hash: e.block.hash,
          block_number: e.block.number,
          block_timestamp: toUnixTime(e.timestamp),
          delegator: e.delegator,
          from_delegate: e.fromDelegate,
          to_delegate: e.toDelegate,
        })),
        format: 'JSONEachRow',
      });
    }
    
    // Insert DelegateVotesChanged events
    if (delegateVotesChangedEvents.length > 0) {
      await clickhouse.insert({
        table: 'ens_delegate_votes_changed',
        values: delegateVotesChangedEvents.map((e) => ({
          transaction_hash: e.transaction.hash,
          log_index: e.transaction.logIndex,
          transaction_index: e.transaction.index,
          contract_address: e.token_address,
          block_hash: e.block.hash,
          block_number: e.block.number,
          block_timestamp: toUnixTime(e.timestamp),
          delegate: e.delegate,
          previous_balance: e.previousBalance.toString(),
          new_balance: e.newBalance.toString(),
        })),
        format: 'JSONEachRow',
      });
    }
    
    // Log processing progress with block information
    logger.info(`Processed ${events.length} events - block ${blockNumber} (${blockHash.slice(0, 10)}...)`);
    
    // Acknowledge successful processing
    await ds.ack();
  }
}

void main();
