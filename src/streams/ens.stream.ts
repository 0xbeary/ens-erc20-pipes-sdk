import { BlockRef, PortalAbstractStream } from '@sqd-pipes/core';
import { NodeClickHouseClient } from '@clickhouse/client/dist/client';
import { events as abi_events } from '../abi/ens.abi';

// Define types for the events we are interested in
export type Transfer = {
  from: string;
  to: string;
  amount: bigint;
};

export type DelegateChanged = {
  delegator: string;
  fromDelegate: string;
  toDelegate: string;
};

export type DelegateVotesChanged = {
  delegate: string;
  previousBalance: bigint;
  newBalance: bigint;
};

// Union type for all ENS events with common fields
export type EnsEvent = (
  | ({ type: 'Transfer' } & Transfer)
  | ({ type: 'DelegateChanged' } & DelegateChanged)
  | ({ type: 'DelegateVotesChanged' } & DelegateVotesChanged)
) & {
  token_address: string;
  block: BlockRef;
  transaction: {
    hash: string;
    index: number;
    logIndex: number;
  };
  timestamp: Date;
};

type Args = {
  clickhouseClient: NodeClickHouseClient;
  contracts?: string[];
};

export class EnsIndexerStream extends PortalAbstractStream<EnsEvent, Args> {
  async initialize() {
    // Initialize any internal state if needed
    // For now, this is empty but the method needs to exist
  }

  async stream(): Promise<ReadableStream<EnsEvent[]>> {
    // Request raw blockchain data from SQD Portal
    const source = await this.getStream({
      type: 'evm',
      fields: {
        block: {
          number: true,
          hash: true,
          timestamp: true,
        },
        transaction: {
          from: true,
          to: true,
          hash: true,
        },
        log: {
          address: true,
          topics: true,
          data: true,
          transactionHash: true,
          logIndex: true,
          transactionIndex: true,
        },
      },
      logs: [
        {
          address: this.options.args?.contracts,
          // Filter for the three ENS events we care about
          topic0: [
            abi_events.Transfer.topic,
            abi_events.DelegateChanged.topic,
            abi_events.DelegateVotesChanged.topic,
          ],
        },
      ],
    });

    // Transform raw blockchain data into structured events
    return source.pipeThrough(
      new TransformStream({
        transform: async ({ blocks }, controller) => {
          const allEvents = blocks
            // Only process blocks that have logs
            .filter((block: any) => block.logs)
            // Flatten: blocks → logs → events
            .flatMap((block: any) => 
              block.logs
                // Transform each log into an ENS event
                .map((log: any) => {
                  // Common fields for all event types
                  const common = {
                    token_address: log.address,
                    block: block.header,
                    transaction: {
                      hash: log.transactionHash,
                      index: log.transactionIndex,
                      logIndex: log.logIndex,
                    },
                    // Convert Unix timestamp (seconds) to JavaScript Date
                    timestamp: new Date(block.header.timestamp * 1000),
                  };

                  try {
                    // Decode the specific event type based on topic0
                    if (abi_events.Transfer.is(log)) {
                      const decoded = abi_events.Transfer.decode(log);
                      return {
                        ...common,
                        type: 'Transfer',
                        from: decoded.from,
                        to: decoded.to,
                        amount: decoded.value,
                      };
                    } else if (abi_events.DelegateChanged.is(log)) {
                      const decoded = abi_events.DelegateChanged.decode(log);
                      return {
                        ...common,
                        type: 'DelegateChanged',
                        delegator: decoded.delegator,
                        fromDelegate: decoded.fromDelegate,
                        toDelegate: decoded.toDelegate,
                      };
                    } else if (abi_events.DelegateVotesChanged.is(log)) {
                      const decoded = abi_events.DelegateVotesChanged.decode(log);
                      return {
                        ...common,
                        type: 'DelegateVotesChanged',
                        delegate: decoded.delegate,
                        previousBalance: decoded.previousBalance,
                        newBalance: decoded.newBalance,
                      };
                    }
                    return null; // Unknown event type
                  } catch (e) {
                    this.logger.error(`Error decoding event: ${e}`);
                    return null; // Failed to decode
                  }
                })
                // Remove failed/null events
                .filter((event: EnsEvent | null) => event !== null)
            );

          // Only emit batches that contain events
          if (allEvents.length > 0) {
            this.logger.info(`Processed ${allEvents.length} events`);
            controller.enqueue(allEvents);
          }
        },
      }),
    );
  }
}
