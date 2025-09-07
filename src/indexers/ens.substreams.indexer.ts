import { ClickHouseClient } from '@clickhouse/client';
import type { BlockScopedData, BlockUndoSignal, ModulesProgress } from '@substreams/core/proto';
import type { IMessageTypeRegistry } from "@bufbuild/protobuf";
import { ClickhouseCursor } from '../substreams/cursor';
import { startSubstreams } from '../substreams/main';
import { Handlers } from '../substreams/types';
import { DatabaseBatch, ensureTables, logger } from '../utils';
import { tableNames } from './ens.indexer';

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

export async function indexEnsEventsSubstreams(
  client: ClickHouseClient,
  contracts: string[] = [],
) {
  // Ensure tables exist (use the Substreams-specific SQL schema)
  await ensureTables(client, './src/db/sql/ens.substreams.sql');
  
  const cursor = new ClickhouseCursor(client, 'ens_substreams');
  const dbBatch = new DatabaseBatch(client);

  const blockScopeDataHandler = async (response: BlockScopedData, registry: IMessageTypeRegistry) => {
    const output = response.output?.mapOutput;
    const cursor_value = response.cursor;
    const blockNumber = Number(response.clock?.number || 0);

    if (output !== undefined) {
      try {
        const message = output.unpack(registry);
        if (message === undefined) {
          throw new Error(`Failed to unpack output of type ${output.typeUrl}`);
        }

        // Convert output to JSON for processing
        const outputAsJson = output.toJson({typeRegistry: registry});
        logger.info(`Processing block ${blockNumber} with events`);

        // Process events based on the structure of your Substreams output
        // This will depend on your specific Substreams module output format
        await processSubstreamsEvents(outputAsJson, dbBatch, blockNumber);

        // Cursor writing MUST happen after you have successfully processed the message
        await cursor.writeCursor(cursor_value, blockNumber);
        
      } catch (error) {
        logger.error(`Error processing block ${blockNumber}: ${error}`);
        throw error;
      }
    }
  };

  /*
      Handle BlockUndoSignal messages.
      You will receive this message after a fork has happened in the blockchain.
      Because of the fork, you have probably read incorrect blocks, so you must rewind.
  */
  const blockUndoSignalHandler = async (response: BlockUndoSignal) => {
    const lastValidBlock = response.lastValidBlock;
    const lastValidCursor = response.lastValidCursor;

    logger.warn(`Blockchain undo detected, returning to valid block #${lastValidBlock?.number} (${lastValidBlock?.id})`);

    try {
      // Rollback data in ClickHouse - remove all rows with block_number > lastValidBlock.number
      if (lastValidBlock?.number) {
        await rollbackSubstreamsData(client, Number(lastValidBlock.number));
      }

      // Update cursor to the last valid position
      await cursor.rollbackToCursor(lastValidCursor);
      
    } catch (error) {
      logger.error(`Error handling blockchain undo: ${error}`);
      throw error;
    }
  };

  const progressHandler = (message: ModulesProgress) => {
    // Log progress periodically
    logger.debug(`Progress update received`);
  };

  const handlers = new Handlers(blockScopeDataHandler, blockUndoSignalHandler, progressHandler);

  // Start the Substreams indexer
  await startSubstreams(handlers, client, 'ens_substreams');
}

async function processSubstreamsEvents(events: any, dbBatch: DatabaseBatch, blockNumber: number) {
  // This function will need to be adapted based on your specific Substreams output format
  // For now, I'm assuming a structure similar to your pipes-based events
  
  if (events.approval && Array.isArray(events.approval)) {
    await dbBatch.insert(events.approval.map(e => ({...e, block_number: blockNumber})), APPROVAL_TABLE);
  }
  
  if (events.claim && Array.isArray(events.claim)) {
    await dbBatch.insert(events.claim.map(e => ({...e, block_number: blockNumber})), CLAIM_TABLE);
  }
  
  if (events.delegateChanged && Array.isArray(events.delegateChanged)) {
    await dbBatch.insert(events.delegateChanged.map(e => ({...e, block_number: blockNumber})), DELEGATE_CHANGED_TABLE);
  }
  
  if (events.delegateVotesChanged && Array.isArray(events.delegateVotesChanged)) {
    await dbBatch.insert(events.delegateVotesChanged.map(e => ({...e, block_number: blockNumber})), DELEGATE_VOTES_CHANGED_TABLE);
  }
  
  if (events.merkleRootChanged && Array.isArray(events.merkleRootChanged)) {
    await dbBatch.insert(events.merkleRootChanged.map(e => ({...e, block_number: blockNumber})), MERKLE_ROOT_CHANGED_TABLE);
  }
  
  if (events.ownershipTransferred && Array.isArray(events.ownershipTransferred)) {
    await dbBatch.insert(events.ownershipTransferred.map(e => ({...e, block_number: blockNumber})), OWNERSHIP_TRANSFERRED_TABLE);
  }
  
  if (events.transfer && Array.isArray(events.transfer)) {
    await dbBatch.insert(events.transfer.map(e => ({...e, block_number: blockNumber})), TRANSFER_TABLE);
  }
}

async function rollbackSubstreamsData(client: ClickHouseClient, lastValidBlock: number) {
  logger.info(`Rolling back Substreams data to block ${lastValidBlock}`);
  
  for (const tableName of substreamsTableNames) {
    try {
      await client.command({
        query: `ALTER TABLE ${tableName} DELETE WHERE block_number > ${lastValidBlock}`
      });
      logger.debug(`Rolled back table ${tableName} to block ${lastValidBlock}`);
    } catch (error) {
      logger.error(`Error rolling back table ${tableName}: ${error}`);
      throw error;
    }
  }
}
