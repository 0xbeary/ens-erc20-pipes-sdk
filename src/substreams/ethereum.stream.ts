import { PortalAbstractStream, ClickhouseState } from '@sqd-pipes/core';
import { startSubstreams } from './main';
import { Handlers } from './types';
import { ClickhouseCursor } from './cursor';
import type { BlockScopedData, BlockUndoSignal, ModulesProgress } from '@substreams/core/proto';
import type { IMessageTypeRegistry } from "@bufbuild/protobuf";
import { ClickHouseClient } from '@clickhouse/client';
import { logger } from '../utils/logger';
import { DatabaseBatch } from '../utils/database-batch';

type EthereumEventBlock = {
  blockNumber: number;
  timestamp: Date;
  transactionHash: string;
  events: any[];
  // ENS-specific event fields to match existing structure
  Approval?: any[];
  Claim?: any[];
  DelegateChanged?: any[];
  DelegateVotesChanged?: any[];
  MerkleRootChanged?: any[];
  OwnershipTransferred?: any[];
  Transfer?: any[];
};

type SubstreamsArgs = {
  startBlock?: number;
  stopBlock?: number;
  outputModule: string;
  contracts?: string[];
};

export class SubstreamsEthereumStream extends PortalAbstractStream<
  EthereumEventBlock,
  SubstreamsArgs
> {
  private streamController?: ReadableStreamDefaultController<EthereumEventBlock[]>;
  private eventBuffer: EthereumEventBlock[] = [];
  private isStreaming = false;
  private client: ClickHouseClient;
  private cursor: ClickhouseCursor;
  private stateOptions: any;

  constructor(options: any, client: ClickHouseClient, contracts: string[] = []) {
    super(options);
    this.client = client;
    this.cursor = new ClickhouseCursor(client, options.state?.id || 'substreams_default');
    this.stateOptions = options.state;
  }

  async stream(): Promise<ReadableStream<EthereumEventBlock[]>> {
    return new ReadableStream({
      start: (controller) => {
        this.streamController = controller;
        this.startSubstreamsConnection();
      },
      cancel: () => {
        this.isStreaming = false;
      }
    });
  }

  private async startSubstreamsConnection() {
    this.isStreaming = true;

    const handlers = new Handlers(
      // BlockScopedData handler
      async (response: BlockScopedData, registry: IMessageTypeRegistry) => {
        if (!this.isStreaming) return;

        try {
          const output = response.output?.mapOutput;
          const cursor_value = response.cursor;
          const blockNumber = Number(response.clock?.number || 0);
          const timestamp = response.clock?.timestamp ? 
            new Date(Number(response.clock.timestamp.seconds) * 1000) : 
            new Date();

          if (output !== undefined) {
            const message = output.unpack(registry);
            if (message === undefined) {
              throw new Error(`Failed to unpack output of type ${output.typeUrl}`);
            }

            const outputAsJson = output.toJson({typeRegistry: registry});
            const events = this.parseSubstreamsOutput(outputAsJson);

            const blockData: EthereumEventBlock = {
              blockNumber,
              timestamp,
              transactionHash: '', // Will be filled from events if available
              events,
              // Parse events into ENS-specific categories to match existing structure
              ...this.categorizeEvents(events)
            };

            this.eventBuffer.push(blockData);

            // Batch and emit events - smaller batches for responsiveness
            if (this.eventBuffer.length >= 1) { // Process each block immediately for better performance
              this.flushBuffer();
            }

            // Store cursor for resumption - critical for Substreams
            if (cursor_value) {
              await this.cursor.writeCursor(cursor_value, blockNumber);
            }

          }
        } catch (error) {
          logger.error(`Error processing block scoped data: ${error}`);
          // Don't rethrow - let the stream continue
        }
      },

      // BlockUndoSignal handler
      async (response: BlockUndoSignal) => {
        const lastValidBlock = response.lastValidBlock;
        const lastValidCursor = response.lastValidCursor;

        logger.warn(`Blockchain reorganization detected: reverting to block ${lastValidBlock?.number}`);
        
        try {
          // Handle rollback using existing ClickhouseState pattern
          if (this.stateOptions && this.stateOptions.onRollback && lastValidBlock?.number) {
            // Use current timestamp as fallback since BlockRef doesn't have timestamp
            const rollbackTimestamp = Date.now();
            
            await this.stateOptions.onRollback({
              state: this.stateOptions,
              latest: { timestamp: rollbackTimestamp }
            });
          }

          // Store the rollback cursor
          if (lastValidCursor) {
            await this.cursor.rollbackToCursor(lastValidCursor);
          }

        } catch (error) {
          logger.error(`Error handling rollback: ${error}`);
          throw error;
        }
      },

      // ModulesProgress handler
      (response: ModulesProgress) => {
        // Log progress occasionally
        logger.debug('Substreams progress update received');
      }
    );

    try {
      await startSubstreams(handlers, this.client, this.stateOptions?.id || 'substreams_default');
    } catch (error) {
      logger.error(`Substreams connection error: ${error}`);
      if (this.streamController && this.isStreaming) {
        this.streamController.error(error);
      }
    }
  }

  private parseSubstreamsOutput(output: any): any[] {
    const events: any[] = [];
    
    if (output && typeof output === 'object') {
      // Handle different output structures based on your Substreams module
      if (Array.isArray(output)) {
        events.push(...output);
      } else if (output.events && Array.isArray(output.events)) {
        events.push(...output.events);
      } else {
        // If it's a single event object
        events.push(output);
      }
    }

    return events;
  }

  private categorizeEvents(events: any[]): Partial<EthereumEventBlock> {
    const categorized: Partial<EthereumEventBlock> = {
      Approval: [],
      Claim: [],
      DelegateChanged: [],
      DelegateVotesChanged: [],
      MerkleRootChanged: [],
      OwnershipTransferred: [],
      Transfer: []
    };

    for (const event of events) {
      // Categorize events based on event name or signature
      const eventName = event.name || event.event || event.type;
      
      switch (eventName) {
        case 'Approval':
          categorized.Approval!.push(event);
          break;
        case 'Claim':
          categorized.Claim!.push(event);
          break;
        case 'DelegateChanged':
          categorized.DelegateChanged!.push(event);
          break;
        case 'DelegateVotesChanged':
          categorized.DelegateVotesChanged!.push(event);
          break;
        case 'MerkleRootChanged':
          categorized.MerkleRootChanged!.push(event);
          break;
        case 'OwnershipTransferred':
          categorized.OwnershipTransferred!.push(event);
          break;
        case 'Transfer':
          categorized.Transfer!.push(event);
          break;
        default:
          // Handle unknown events or add to a general events array
          break;
      }
    }

    return categorized;
  }

  private flushBuffer() {
    if (this.eventBuffer.length > 0 && this.streamController && this.isStreaming) {
      this.streamController.enqueue([...this.eventBuffer]);
      this.eventBuffer = [];
    }
  }

  // Implement ack() method to match PortalAbstractStream interface
  async ack(): Promise<void> {
    // In Substreams, the cursor is automatically managed through the handlers
    // This method exists for API compatibility with the pipes pattern
    return Promise.resolve();
  }
}

// Enhanced ClickhouseState that works with Substreams cursors
export class SubstreamsClickhouseState extends ClickhouseState {
  private cursor: ClickhouseCursor;

  constructor(
    client: ClickHouseClient,
    options: {
      table: string;
      id: string;
      database?: string;
      onRollback?: (params: { state: any; latest: { timestamp?: number } }) => Promise<void>;
    }
  ) {
    super(client, options);
    this.cursor = new ClickhouseCursor(client, options.id);
  }

  // Override to use Substreams cursor instead of block numbers
  async get(): Promise<string | null> {
    return await this.cursor.getCursor();
  }

  async set(cursor: string): Promise<void> {
    // Extract block number from cursor if possible, otherwise use current timestamp
    const blockNumber = Date.now(); // Fallback - in real implementation you'd extract from cursor
    await this.cursor.writeCursor(cursor, blockNumber);
    
    // Note: ClickhouseState doesn't have a set method in this version
    // The cursor is already stored in our custom cursor management
  }
}
