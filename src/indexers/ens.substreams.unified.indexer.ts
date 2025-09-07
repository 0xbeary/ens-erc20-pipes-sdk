import { ClickHouseClient } from '@clickhouse/client';
import { ClickhouseState } from '@sqd-pipes/core';
import { SubstreamsEnsEventStreamFactory } from '../substreams/ens.stream';
import { DatabaseBatch, ensureTables, getDefaultRollback, logger } from '../utils';

const TABLE_PREFIX = 'ens_substreams_evt';

const APPROVAL_TABLE = `${TABLE_PREFIX}_approval`;
const CLAIM_TABLE = `${TABLE_PREFIX}_claim`;
const DELEGATE_CHANGED_TABLE = `${TABLE_PREFIX}_delegate_changed`;
const DELEGATE_VOTES_CHANGED_TABLE = `${TABLE_PREFIX}_delegate_votes_changed`;
const MERKLE_ROOT_CHANGED_TABLE = `${TABLE_PREFIX}_merkle_root_changed`;
const OWNERSHIP_TRANSFERRED_TABLE = `${TABLE_PREFIX}_ownership_transferred`;
const TRANSFER_TABLE = `${TABLE_PREFIX}_transfer`;

export const substreamsTableNames: string[] = [
  APPROVAL_TABLE,
  CLAIM_TABLE,
  DELEGATE_CHANGED_TABLE,
  DELEGATE_VOTES_CHANGED_TABLE,
  MERKLE_ROOT_CHANGED_TABLE,
  OWNERSHIP_TRANSFERRED_TABLE,
  TRANSFER_TABLE,
];

export type Network = 'base-mainnet' | 'ethereum-mainnet';

/**
 * ENS Event Indexer using Substreams
 * This provides the same interface as the pipes-based indexer but uses Substreams internally
 */
export async function indexEnsEventsSubstreams(
  client: ClickHouseClient,
  contracts: string[],
  datasetHeight: string | number,
  network: Network = 'ethereum-mainnet',
) {
  // Ensure Substreams-specific tables exist
  await ensureTables(client, './src/db/sql/ens.substreams.sql');

  // Create the Substreams-based event stream with the same interface as pipes
  const SubstreamsEnsEvents = SubstreamsEnsEventStreamFactory({
    // Portal is not used in Substreams but kept for API compatibility
    portal: `https://mainnet.eth.streamingfast.io`,
    blockRange: {
      from: typeof datasetHeight === 'string' ? parseInt(datasetHeight) : datasetHeight,
    },
    state: {
      id: 'ens_substreams_events',
      onRollback: async ({ state, latest }) => {
        if (!latest.timestamp) {
          return; // fresh table
        }

        try {
          // Use the same rollback logic but adapted for Substreams
          for (const tableName of substreamsTableNames) {
            await client.command({
              query: `ALTER TABLE ${tableName} DELETE WHERE timestamp > ${latest.timestamp}`
            });
          }
        } catch (err) {
          logger.error(`onRollback err: ${err}`);
          throw err;
        }
      },
    },
  }, contracts);

  const ensEvents = SubstreamsEnsEvents(client);
  const stream = await ensEvents.stream();
  const dbBatch = new DatabaseBatch(client);

  logger.info(`Starting Substreams ENS indexer from block ${datasetHeight}`);

  // Process the stream with the same pattern as the pipes-based indexer
  const reader = stream.getReader();
  
  try {
    while (true) {
      const { done, value: blocks } = await reader.read();
      
      if (done) {
        logger.info('Substreams stream completed');
        break;
      }

      if (blocks && blocks.length > 0) {
        await Promise.all(
          blocks.flatMap((block) => [
            dbBatch.insert(block.Approval, APPROVAL_TABLE),
            dbBatch.insert(block.Claim, CLAIM_TABLE),
            dbBatch.insert(block.DelegateChanged, DELEGATE_CHANGED_TABLE),
            dbBatch.insert(block.DelegateVotesChanged, DELEGATE_VOTES_CHANGED_TABLE),
            dbBatch.insert(block.MerkleRootChanged, MERKLE_ROOT_CHANGED_TABLE),
            dbBatch.insert(block.OwnershipTransferred, OWNERSHIP_TRANSFERRED_TABLE),
            dbBatch.insert(block.Transfer, TRANSFER_TABLE),
          ]),
        );

        // Acknowledge processing (for API compatibility)
        await ensEvents.ack();

        logger.info(`Processed ${blocks.length} blocks via Substreams`);
      }
    }
  } catch (error) {
    logger.error(`Substreams indexer error: ${error}`);
    throw error;
  } finally {
    reader.releaseLock();
  }
}
