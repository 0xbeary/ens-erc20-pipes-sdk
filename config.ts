import 'dotenv/config';

export function getConfig() {
  return {
    network: 'mainnet',
    blockFrom: parseInt(process.env.BLOCK_FROM || '0'),
    contractAddress: '0xC18360217D8F7Ab5e7c516566761Ea12Ce7F9D72',
    portal: {
      url: 'https://portal.sqd.dev/datasets/ethereum-mainnet',
    },
  };
}
