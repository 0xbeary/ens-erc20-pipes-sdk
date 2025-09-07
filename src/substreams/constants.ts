export const TOKEN: string = process.env.SUBSTREAMS_TOKEN || '';
export const ENDPOINT = process.env.SUBSTREAMS_ENDPOINT || "https://mainnet.eth.streamingfast.io";
export const SPKG = process.env.SUBSTREAMS_SPKG || "https://spkg.io/v1/files/ethereum-common-v0.3.1.spkg";
export const MODULE = process.env.SUBSTREAMS_MODULE || "all_events";
export const START_BLOCK = process.env.SUBSTREAMS_START_BLOCK ? parseInt(process.env.SUBSTREAMS_START_BLOCK) : 23314199;
export const STOP_BLOCK = process.env.SUBSTREAMS_STOP_BLOCK ? parseInt(process.env.SUBSTREAMS_STOP_BLOCK) : undefined;
