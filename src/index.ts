import { logger, createClickhouseClient } from './utils'
import { indexEnsEvents } from './indexers/ens.indexer'
import { getConfig } from '../config'

// Main execution
async function main() {
  const config = getConfig()
  const client = createClickhouseClient()
  
  logger.info(`Starting ENS indexer on ${config.network}`)
  logger.info(`Portal URL: ${config.portal.url}`)
  logger.info(`Starting from block: ${config.blockFrom}`)
  
  try {
    await indexEnsEvents(
      client,
      config.portal.url,
      config.blockFrom,
      config.network === 'mainnet' ? 'ethereum-mainnet' : 'base-mainnet'
    )
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