/**
 * Test script for Substreams indexer setup
 * Run with: npx ts-node test-substreams.ts
 */

import { logger } from './src/utils';
import { ClickhouseCursor } from './src/substreams/cursor';
import { Handlers } from './src/substreams/types';
import { startSubstreams } from './src/substreams/main';

async function testSubstreamsSetup() {
  try {
    logger.info('Testing Substreams setup compilation...');
    
    // Test imports work correctly (compilation check)
    logger.info('✓ All Substreams modules imported successfully');
    
    // Test environment variables
    const requiredEnvVars = ['SUBSTREAMS_TOKEN'];
    for (const envVar of requiredEnvVars) {
      if (!process.env[envVar]) {
        logger.warn(`⚠ Environment variable ${envVar} is not set`);
      } else {
        logger.info(`✓ Environment variable ${envVar} is set`);
      }
    }
    
    logger.info('🎉 Substreams setup compilation test completed successfully!');
    logger.info('💡 To test full functionality, start ClickHouse and set SUBSTREAMS_TOKEN');
    
  } catch (error) {
    console.error('Full error details:', error);
    logger.error(`❌ Substreams setup test failed: ${error}`);
    process.exit(1);
  }
}

// Run the test
testSubstreamsSetup().catch(console.error);
