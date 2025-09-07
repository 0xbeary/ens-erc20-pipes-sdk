import { ClickHouseClient } from '@clickhouse/client';
import { startSubstreams } from './main';
import { Handlers } from './types';
import { ClickhouseCursor } from './cursor';
import type { BlockScopedData, BlockUndoSignal, ModulesProgress } from '@substreams/core/proto';
import type { IMessageTypeRegistry } from "@bufbuild/protobuf";
import { logger } from '../utils/logger';
import { getDefaultRollback } from '../utils/rollback';

type EnsEventBlock = {
  blockNumber: number;
  timestamp: Date;
  transactionHash: string;
  // ENS-specific event fields to match existing structure
  Approval: any[];
  Claim: any[];
  DelegateChanged: any[];
  DelegateVotesChanged: any[];
  MerkleRootChanged: any[];
  OwnershipTransferred: any[];
  Transfer: any[];
};

type SubstreamsOptions = {
  state: {
    id: string;
    onRollback?: (params: { state: any; latest: { timestamp?: number } }) => Promise<void>;
  };
  startBlock?: number;
  stopBlock?: number;
  outputModule?: string;
  contracts?: string[];
};

/**
 * Substreams-based ENS Event Stream that provides the same interface as the pipes-based version
 * This allows for drop-in replacement while using Substreams under the hood
 */
export class SubstreamsEnsEventStream {
  private client: ClickHouseClient;
  private options: SubstreamsOptions;
  private cursor: ClickhouseCursor;
  private streamController?: ReadableStreamDefaultController<EnsEventBlock[]>;
  private eventBuffer: EnsEventBlock[] = [];
  private isStreaming = false;

  constructor(options: SubstreamsOptions, client: ClickHouseClient, contracts: string[] = []) {
    this.client = client;
    this.options = options;
    this.cursor = new ClickhouseCursor(client, options.state.id);
  }

  async stream(): Promise<ReadableStream<EnsEventBlock[]>> {
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
            
            // Parse and categorize events to match existing ENS indexer structure
            const blockData: EnsEventBlock = {
              blockNumber,
              timestamp,
              transactionHash: this.extractTransactionHash(outputAsJson),
              ...this.categorizeEnsEvents(outputAsJson)
            };

            this.eventBuffer.push(blockData);

            // Process each block immediately for better performance
            if (this.eventBuffer.length >= 1) {
              this.flushBuffer();
            }

            // Store cursor for resumption
            if (cursor_value) {
              await this.cursor.writeCursor(cursor_value, blockNumber);
            }
          }
        } catch (error) {
          logger.error(`Error processing block scoped data: ${error}`);
          // Continue processing other blocks
        }
      },

      // BlockUndoSignal handler
      async (response: BlockUndoSignal) => {
        const lastValidBlock = response.lastValidBlock;
        const lastValidCursor = response.lastValidCursor;

        logger.warn(`Blockchain reorganization detected: reverting to block ${lastValidBlock?.number}`);
        
        try {
          // Handle rollback using the same pattern as pipes
          if (this.options.state.onRollback && lastValidBlock?.number) {
            // Use block number to calculate approximate timestamp for rollback
            const rollbackTimestamp = Date.now() - (1000 * 12 * 100); // Approximate 100 blocks ago
            
            await this.options.state.onRollback({
              state: this.options.state,
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
        logger.debug('Substreams progress update received');
      }
    );

    try {
      await startSubstreams(handlers, this.client, this.options.state.id);
    } catch (error) {
      logger.error(`Substreams connection error: ${error}`);
      if (this.streamController && this.isStreaming) {
        this.streamController.error(error);
      }
    }
  }

  private extractTransactionHash(output: any): string {
    // Extract transaction hash from Substreams output
    // This depends on your specific Substreams module structure
    if (output && output.transaction_hash) {
      return output.transaction_hash;
    }
    if (output && Array.isArray(output) && output[0] && output[0].transaction_hash) {
      return output[0].transaction_hash;
    }
    return '';
  }

  private categorizeEnsEvents(output: any): Omit<EnsEventBlock, 'blockNumber' | 'timestamp' | 'transactionHash'> {
    const events = {
      Approval: [] as any[],
      Claim: [] as any[],
      DelegateChanged: [] as any[],
      DelegateVotesChanged: [] as any[],
      MerkleRootChanged: [] as any[],
      OwnershipTransferred: [] as any[],
      Transfer: [] as any[]
    };

    // Parse events based on your Substreams output structure
    if (output && typeof output === 'object') {
      // Handle different possible structures
      const eventsArray = output.events || [output];
      
      for (const event of eventsArray) {
        if (!event) continue;
        
        const eventName = event.name || event.event || event.type;
        const eventData = event.data || event;
        
        switch (eventName) {
          case 'Approval':
            events.Approval.push(eventData);
            break;
          case 'Claim':
            events.Claim.push(eventData);
            break;
          case 'DelegateChanged':
            events.DelegateChanged.push(eventData);
            break;
          case 'DelegateVotesChanged':
            events.DelegateVotesChanged.push(eventData);
            break;
          case 'MerkleRootChanged':
            events.MerkleRootChanged.push(eventData);
            break;
          case 'OwnershipTransferred':
            events.OwnershipTransferred.push(eventData);
            break;
          case 'Transfer':
            events.Transfer.push(eventData);
            break;
        }
      }
    }

    return events;
  }

  private flushBuffer() {
    if (this.eventBuffer.length > 0 && this.streamController && this.isStreaming) {
      this.streamController.enqueue([...this.eventBuffer]);
      this.eventBuffer = [];
    }
  }

  // Implement ack() method for API compatibility
  async ack(): Promise<void> {
    // In Substreams, cursor management is handled automatically
    return Promise.resolve();
  }
}

/**
 * Factory function that creates a Substreams-based ENS event stream
 * This provides the same interface as your existing pipes-based EnsEventStream
 */
export function SubstreamsEnsEventStreamFactory(
  options: {
    portal?: string; // Not used in Substreams but kept for API compatibility
    blockRange: { from: number; to?: number };
    state: {
      id: string;
      onRollback?: (params: { state: any; latest: { timestamp?: number } }) => Promise<void>;
    };
  },
  contracts: string[]
) {
  return (client: ClickHouseClient) => {
    return new SubstreamsEnsEventStream(
      {
        state: options.state,
        startBlock: options.blockRange.from,
        stopBlock: options.blockRange.to,
        contracts
      },
      client,
      contracts
    );
  };
}
