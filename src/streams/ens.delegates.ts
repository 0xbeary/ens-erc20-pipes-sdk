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
          topic0: [
            abi_events.Transfer.topic,
            abi_events.DelegateChanged.topic,
            abi_events.DelegateVotesChanged.topic,
          ],
        },
      ],
    });

    return source.pipeThrough(
      new TransformStream({
        transform: async ({ blocks }, controller) => {
          const allEvents: EnsEvent[] = [];
          for (const b of blocks) {
            const block: any = b;

            if (!block.logs) {
              continue;
            }

            for (const l of block.logs) {
              const common = {
                token_address: l.address,
                block: block.header,
                transaction: {
                  hash: l.transactionHash,
                  index: l.transactionIndex,
                  logIndex: l.logIndex,
                },
                timestamp: new Date(block.header.timestamp * 1000),
              };
              try {
                if (abi_events.Transfer.is(l)) {
                  const decoded = abi_events.Transfer.decode(l);
                  allEvents.push({
                    ...common,
                    type: 'Transfer',
                    from: decoded.from,
                    to: decoded.to,
                    amount: decoded.value,
                  });
                } else if (abi_events.DelegateChanged.is(l)) {
                  const decoded = abi_events.DelegateChanged.decode(l);
                  allEvents.push({
                    ...common,
                    type: 'DelegateChanged',
                    delegator: decoded.delegator,
                    fromDelegate: decoded.fromDelegate,
                    toDelegate: decoded.toDelegate,
                  });
                } else if (abi_events.DelegateVotesChanged.is(l)) {
                  const decoded = abi_events.DelegateVotesChanged.decode(l);
                  allEvents.push({
                    ...common,
                    type: 'DelegateVotesChanged',
                    delegate: decoded.delegate,
                    previousBalance: decoded.previousBalance,
                    newBalance: decoded.newBalance,
                  });
                }
              } catch (e) {
                this.logger.error(`Error decoding event: ${e}`);
              }
            }
          }
          if (allEvents.length > 0) {
            this.logger.info(`Processed ${allEvents.length} events`);
            controller.enqueue(allEvents);
          }
        },
      }),
    );
  }
}
