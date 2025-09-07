# Substreams Indexer Implementation Summary

## ✅ What's Been Added

### 1. Core Substreams Infrastructure
- **`src/substreams/types.ts`** - TypeScript types and handler definitions
- **`src/substreams/main.ts`** - Main Substreams entry point with connection management
- **`src/substreams/cursor.ts`** - ClickHouse-based cursor management for reliable resumption
- **`src/substreams/handlers.ts`** - Message handling logic for different response types
- **`src/substreams/error.ts`** - Error handling and retry logic
- **`src/substreams/constants.ts`** - Configuration constants from environment variables

### 2. ENS-Specific Substreams Indexer
- **`src/indexers/ens.substreams.indexer.ts`** - ENS event processing with Substreams
- **`src/db/sql/ens.substreams.sql`** - Separate database schema for Substreams data

### 3. Rollback & State Management
- **Cursor Storage**: Persistent cursor storage in ClickHouse table `substreams_cursor`
- **Block Tracking**: Each cursor includes associated block number for rollback support
- **Automatic Rollbacks**: Handles `BlockUndoSignal` messages by:
  - Deleting data with `block_number > last_valid_block`
  - Resetting cursor to last valid position
  - Resuming from rollback point

### 4. Integration with Existing System
- **`src/index.ts`** - Updated to support both pipes and Substreams via `INDEXER_TYPE` env var
- **Dual Architecture Support**: Both indexers can run simultaneously with different table prefixes
- **Shared Utilities**: Reuses existing ClickHouse, logging, and database utilities

### 5. Configuration & Documentation
- **`.env.substreams.example`** - Example environment configuration
- **`SUBSTREAMS.md`** - Comprehensive documentation
- **`test-substreams.ts`** - Setup validation script

## 🏗️ Architecture Overview

### Data Flow
```
Substreams API → Main Handler → Event Processor → ClickHouse
                     ↓
              Cursor Management → ClickHouse (substreams_cursor table)
                     ↓
              Rollback Handler → Delete invalid data + Reset cursor
```

### Key Features

1. **Real-time Streaming**: Direct connection to Substreams for live blockchain data
2. **Reliable State Management**: ClickHouse-based cursor storage with block tracking
3. **Automatic Rollback Handling**: Built-in support for blockchain reorganizations
4. **Error Recovery**: Automatic retry logic for transient errors
5. **Dual Architecture**: Can run alongside existing pipes-based indexer

### Table Structure

**Substreams Tables** (prefix: `ens_substreams_evt_`):
- `ens_substreams_evt_approval`
- `ens_substreams_evt_claim`
- `ens_substreams_evt_delegate_changed`
- `ens_substreams_evt_delegate_votes_changed`
- `ens_substreams_evt_merkle_root_changed`
- `ens_substreams_evt_ownership_transferred`
- `ens_substreams_evt_transfer`

**State Management Table**:
- `substreams_cursor` - Stores cursors with block numbers for resumption

## 🚀 How to Use

### 1. Install Dependencies
```bash
pnpm install
```

### 2. Configure Environment
```bash
cp .env.substreams.example .env
# Edit .env with your SUBSTREAMS_TOKEN and other settings
export INDEXER_TYPE=substreams
```

### 3. Run the Indexer
```bash
npm start
```

### 4. Switch Between Architectures
```bash
# Use Substreams
export INDEXER_TYPE=substreams

# Use Pipes (original)
export INDEXER_TYPE=pipes
```

## 🔧 Key Integration Points

### ClickHouse State Management
The Substreams indexer integrates seamlessly with your existing ClickHouse setup:

1. **Shared Connection**: Uses the same ClickHouse client configuration
2. **Separate Tables**: Uses `ens_substreams_evt_*` prefix to avoid conflicts
3. **State Persistence**: Cursor stored in dedicated `substreams_cursor` table
4. **Rollback Support**: Leverages ClickHouse's `ALTER TABLE DELETE` for efficient rollbacks

### Error Handling & Resilience
- **Retryable Errors**: Network issues, temporary service unavailability
- **Fatal Errors**: Authentication failures, invalid configurations
- **Automatic Retry**: Built-in exponential backoff for transient errors
- **State Recovery**: Resumes from last committed cursor after any interruption

### Monitoring & Observability
- **Detailed Logging**: Block processing, cursor updates, errors, rollbacks
- **Progress Tracking**: Real-time visibility into indexing progress
- **Health Monitoring**: Connection status, error rates, processing metrics

## 🔄 Rollback Handling Details

When blockchain reorganizations occur:

1. **Detection**: `BlockUndoSignal` message received from Substreams
2. **Data Cleanup**: All rows with `block_number > last_valid_block` are deleted
3. **Cursor Reset**: Cursor updated to last valid position
4. **Resumption**: Streaming continues from the rollback point
5. **Consistency**: Ensures data integrity across reorganizations

This provides robust handling of blockchain forks while maintaining data consistency in ClickHouse.

## ✅ Testing

Run the setup validation:
```bash
npm run test-substreams
```

This validates:
- Module compilation
- Import resolution
- Environment configuration

For full testing, ensure ClickHouse is running and `SUBSTREAMS_TOKEN` is set.

## 📚 Next Steps

1. **Configure your Substreams token** in environment variables
2. **Start ClickHouse** for data storage
3. **Run the indexer** with `INDEXER_TYPE=substreams`
4. **Monitor logs** for processing progress
5. **Compare data** between pipes and Substreams architectures if needed

The implementation provides a production-ready Substreams indexer that plays nicely with your existing ClickHouse state management and handles rollbacks properly!
