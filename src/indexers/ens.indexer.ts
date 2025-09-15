import { ClickHouseClient } from '@clickhouse/client'
import { ClickhouseState } from '@sqd-pipes/core'

import { DatabaseBatch, ensureTables, getDefaultRollback, logger, debugDatabase } from '../utils'
import { EnsEventStream } from '../streams/ens.stream'

const TABLE_PREFIX = 'ens_evt'

const APPROVAL_TABLE = `${TABLE_PREFIX}_approval`
const CLAIM_TABLE = `${TABLE_PREFIX}_claim`
const DELEGATE_CHANGED_TABLE = `${TABLE_PREFIX}_delegate_changed`
const DELEGATE_VOTES_CHANGED_TABLE = `${TABLE_PREFIX}_delegate_votes_changed`
const MERKLE_ROOT_CHANGED_TABLE = `${TABLE_PREFIX}_merkle_root_changed`
const OWNERSHIP_TRANSFERRED_TABLE = `${TABLE_PREFIX}_ownership_transferred`
const TRANSFER_TABLE = `${TABLE_PREFIX}_transfer`

export const tableNames: string[] = [
  APPROVAL_TABLE,
  CLAIM_TABLE,
  DELEGATE_CHANGED_TABLE,
  DELEGATE_VOTES_CHANGED_TABLE,
  MERKLE_ROOT_CHANGED_TABLE,
  OWNERSHIP_TRANSFERRED_TABLE,
  TRANSFER_TABLE,
]

export type Network = 'base-mainnet' | 'ethereum-mainnet'

export async function indexEnsEvents(
  client: ClickHouseClient,
  portalUrl: string,
  contracts: string[],
  datasetHeight: string | number,
  network: Network = 'ethereum-mainnet',
) {
  await ensureTables(client, './src/db/sql/ens.sql')
  
  logger.info('Checking database state...')
  await debugDatabase(client)

  const ensEvents = EnsEventStream({
    portal: `${portalUrl}/datasets/${network}`,
    blockRange: {
      from: datasetHeight,
    },
    state: new ClickhouseState(client, {
      table: 'evm_sync_status',
      id: 'ens_events',
      database: process.env.CLICKHOUSE_DB,
      onRollback: getDefaultRollback(tableNames),
    }),
    logger,
  }, contracts)

  const stream = await ensEvents.stream()
  const dbBatch = new DatabaseBatch(client)

  for await (const blocks of stream) {
    logger.info(`Processing ${blocks.length} blocks`)
    
    for (const block of blocks) {
      const totalEvents = (block.Approval?.length || 0) + (block.Claim?.length || 0) + 
                         (block.DelegateChanged?.length || 0) + (block.DelegateVotesChanged?.length || 0) + 
                         (block.MerkleRootChanged?.length || 0) + (block.OwnershipTransferred?.length || 0) + 
                         (block.Transfer?.length || 0)
                         
      logger.info(`Block events: Approval=${block.Approval?.length || 0}, Claim=${block.Claim?.length || 0}, DelegateChanged=${block.DelegateChanged?.length || 0}, DelegateVotesChanged=${block.DelegateVotesChanged?.length || 0}, MerkleRootChanged=${block.MerkleRootChanged?.length || 0}, OwnershipTransferred=${block.OwnershipTransferred?.length || 0}, Transfer=${block.Transfer?.length || 0} (Total: ${totalEvents})`)
      
      if (block.Approval && block.Approval.length > 0) {
        logger.info(`Found ${block.Approval.length} Approval events - sample: ${JSON.stringify(block.Approval[0], null, 2)}`)
      }
      if (block.Transfer && block.Transfer.length > 0) {
        logger.info(`Found ${block.Transfer.length} Transfer events - sample: ${JSON.stringify(block.Transfer[0], null, 2)}`)
      }
    }

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
    )

    await ensEvents.ack()
  }
}
