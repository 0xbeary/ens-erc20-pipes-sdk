# Substreams Architecture

This project now supports two indexer architectures:

1. **Pipes Architecture** (original) - Uses Subsquid's pipes for streaming blockchain data
2. **Substreams Architecture** (new) - Uses StreamingFast's Substreams for real-time blockchain data processing

## Substreams Features

- **Real-time streaming**: Direct connection to Substreams for real-time blockchain data
- **Automatic rollback handling**: Built-in support for blockchain reorganizations
- **ClickHouse integration**: Seamless integration with your existing ClickHouse database
- **Cursor management**: Persistent cursor storage in ClickHouse for reliable resumption
- **Error handling**: Automatic retry logic for transient errors

## Configuration

### Environment Variables

Copy `.env.substreams.example` to `.env` and configure:

```bash
# Required: Your Substreams API token
SUBSTREAMS_TOKEN=your_substreams_token_here

# Optional: Substreams endpoint (defaults to mainnet)
SUBSTREAMS_ENDPOINT=https://mainnet.eth.streamingfast.io

# Optional: Substreams package URL
SUBSTREAMS_SPKG=https://spkg.io/v1/files/ethereum-common-v0.3.1.spkg

# Optional: Output module name
SUBSTREAMS_MODULE=all_events

# Optional: Start block (defaults to 23314199)
SUBSTREAMS_START_BLOCK=23314199

# Optional: Stop block (leave empty for continuous)
SUBSTREAMS_STOP_BLOCK=

# Required: Choose indexer type
INDEXER_TYPE=substreams
```

### Running the Substreams Indexer

```bash
# Set the indexer type to substreams
export INDEXER_TYPE=substreams

# Run the indexer
npm start
```

## Architecture Overview

### Components

1. **Substreams Core** (`src/substreams/`)
   - `main.ts` - Main entry point for Substreams
   - `types.ts` - TypeScript types and handlers
   - `cursor.ts` - ClickHouse-based cursor management
   - `handlers.ts` - Message handling logic
   - `error.ts` - Error handling and retry logic
   - `constants.ts` - Configuration constants

2. **ENS Substreams Indexer** (`src/indexers/ens.substreams.indexer.ts`)
   - Event processing logic
   - ClickHouse data insertion
   - Rollback handling

3. **Database Schema** (`src/db/sql/ens.substreams.sql`)
   - Separate tables for Substreams data
   - Uses `ens_substreams_evt_*` prefix to avoid conflicts

### State Management

The Substreams indexer maintains state through:

1. **Cursor Storage**: Cursors are stored in ClickHouse table `substreams_cursor`
2. **Block Number Tracking**: Each cursor includes the associated block number
3. **Rollback Support**: Automatic rollback of data when blockchain reorganizations occur

### Rollback Handling

When a `BlockUndoSignal` is received:

1. The system identifies the last valid block
2. All data with `block_number > last_valid_block` is deleted from ClickHouse
3. The cursor is reset to the last valid position
4. Streaming resumes from the rollback point

### Table Structure

Substreams tables use the prefix `ens_substreams_evt_` to distinguish from pipes tables:

- `ens_substreams_evt_approval`
- `ens_substreams_evt_claim`
- `ens_substreams_evt_delegate_changed`
- `ens_substreams_evt_delegate_votes_changed`
- `ens_substreams_evt_merkle_root_changed`
- `ens_substreams_evt_ownership_transferred`
- `ens_substreams_evt_transfer`

## Switching Between Architectures

You can switch between pipes and Substreams by changing the `INDEXER_TYPE` environment variable:

```bash
# Use pipes architecture
export INDEXER_TYPE=pipes

# Use Substreams architecture
export INDEXER_TYPE=substreams
```

Both architectures can run simultaneously by using different table prefixes, allowing for comparison and migration testing.

## Error Handling

The Substreams indexer includes comprehensive error handling:

- **Retryable errors**: Network timeouts, temporary service unavailability
- **Fatal errors**: Authentication failures, invalid arguments
- **Cursor errors**: Database connection issues during cursor operations

The system automatically retries retryable errors with exponential backoff.

## Monitoring

The indexer provides detailed logging for:

- Block processing progress
- Cursor updates
- Error conditions
- Rollback events
- Database operations

Use the logs to monitor indexer health and performance.
