import 'dotenv/config';

export function getConfig() {
  return {
    network: process.env.NETWORK || 'mainnet',
    dbPath: process.env.DB_PATH || './db',
    blockFrom: parseInt(process.env.BLOCK_FROM || '0'),
    contractAddress: process.env.CONTRACT_ADDRESS || '0xC18360217D8F7Ab5e7c516566761Ea12Ce7F9D72',
    portal: {
      url: process.env.PORTAL_URL || 'https://portal.sqd.dev/datasets/ethereum-mainnet',
    },
  };
}
