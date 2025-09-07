import { logger, createClickhouseClient } from './utils'
import { indexEnsEvents } from './indexers/ens.indexer'
import { indexEnsEventsSubstreams } from './indexers/ens.substreams.unified.indexer'
import { getConfig } from '../config'

// Main execution
async function main() {
  const config = getConfig()
  const client = createClickhouseClient()
  
  // Check which indexer type to use based on environment variable
  const indexerType = process.env.INDEXER_TYPE || 'substreams'; // 'pipes' or 'substreams'
  
  logger.info(`Starting ENS indexer on ${config.network} using ${indexerType} architecture`)
  logger.info(`Portal URL: ${config.portal.url}`)
  logger.info(`Starting from block: ${config.blockFrom}`)
  
  try {
    if (indexerType === 'substreams') {
      logger.info('Using Substreams indexer architecture with unified interface')
      await indexEnsEventsSubstreams(
        client,
        [...config.contractAddress],
        config.blockFrom,
        config.network === 'mainnet' ? 'ethereum-mainnet' : 'base-mainnet'
      )
    } else {
      logger.info('Using Pipes indexer architecture')
      await indexEnsEvents(
        client,
        config.portal.url,
        [...config.contractAddress],
        config.blockFrom,
        config.network === 'mainnet' ? 'ethereum-mainnet' : 'base-mainnet'
      )
    }
  } catch (error) {
    logger.error({ error }, 'Indexer failed')
    process.exit(1)
  }
}

// Run the main function if this file is executed directly
if (require.main === module) {
  main().catch((error) => {
    console.error('Fatal error:', error)
    process.exit(1)
  })
}