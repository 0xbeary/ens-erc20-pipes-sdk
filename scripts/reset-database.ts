import 'dotenv/config'
import { createClickhouseClient, logger } from '../src/utils'

async function resetDatabase() {
  const client = createClickhouseClient()
  
  try {
    logger.info('Fetching all tables...')
    
    // Get all tables
    const tablesResult = await client.query({
      query: 'SHOW TABLES',
      format: 'JSONEachRow',
    })
    const tables = await tablesResult.json() as Array<{ name: string }>
    
    if (tables.length === 0) {
      logger.info('No tables found in database')
      return
    }
    
    logger.info(`Found ${tables.length} tables: ${tables.map(t => t.name).join(', ')}`)
    
    // Drop each table
    for (const table of tables) {
      logger.info(`Dropping table: ${table.name}`)
      await client.command({
        query: `DROP TABLE IF EXISTS ${table.name}`
      })
    }
    
    logger.info('All tables dropped successfully')
    
    // Verify no tables remain
    const remainingTablesResult = await client.query({
      query: 'SHOW TABLES',
      format: 'JSONEachRow',
    })
    const remainingTables = await remainingTablesResult.json() as Array<{ name: string }>
    
    if (remainingTables.length === 0) {
      logger.info('Database reset complete - all tables removed')
    } else {
      logger.warn(`Some tables may still exist: ${remainingTables.map(t => t.name).join(', ')}`)
    }
    
  } catch (error) {
    logger.error(`Failed to reset database: ${error}`)
    throw error
  } finally {
    await client.close()
  }
}

// Run if called directly
if (require.main === module) {
  resetDatabase().catch((error) => {
    console.error('Reset failed:', error)
    process.exit(1)
  })
}
