import { PortalAbstractStream } from '@sqd-pipes/core'
import type { AbiEvent } from '@subsquid/evm-abi'

type EvenResponseMetadata = {
  contract: string
  blockNumber: number
  transactionHash: string
  timestamp: Date
  logIndex: number
}

export type Events = Record<string, AbiEvent<any>>

type EventArgs<T extends Events> = {
  readonly [K in keyof T]: T[K] extends AbiEvent<any> ? T[K] : never
}

type EventResponse<T extends Events> = {
  [K in keyof T]: (ReturnType<T[K]['decode']> & EvenResponseMetadata)[]
}

type Args<T extends Events> = {
  contracts?: string[]
  events: EventArgs<T>
}

export class EvmDecodedEventStream<T extends Events> extends PortalAbstractStream<
  EventResponse<T>,
  Args<T>
> {
  async stream(): Promise<ReadableStream<EventResponse<T>[]>> {
    const { contracts, events } = this.options.args
    const eventTopics = Object.values(events).map((event) => event.topic)
    const stream = await this.getStream({
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
          sighash: true,
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
          address: contracts,
          topic0: eventTopics,
          transaction: true,
        },
      ],
    })

    return stream.pipeThrough(
      new TransformStream({
        transform: ({ blocks }, controller) => {
          const result = blocks.map((block) => {
            const eventResponse = {} as EventResponse<T>

            for (const eventName in events) {
              ;(eventResponse[eventName as keyof T] as ReturnType<T[keyof T]['decode']>[]) = []
            }

            if (block.logs) {
              for (const log of block.logs) {
                for (const eventName in events) {
                  const eventAbi = events[eventName]
                  const topic0 = log.topics[0]

                  if (topic0 === eventAbi.topic) {
                    try {
                      const decoded = eventAbi.decode(log)
                      const eventArray = eventResponse[eventName as keyof T] as ReturnType<
                        typeof eventAbi.decode
                      >[]

                      eventArray.push({
                        ...decoded,
                        contract: log.address,
                        blockNumber: block.header.number,
                        transactionHash: log.transactionHash,
                        timestamp: new Date(block.header.timestamp * 1000),
                        logIndex: log.logIndex,
                      })
                    } catch (error) {
                      const typedError = error as unknown as Error
                      this.logger.warn(`Failed to decode log for event ${eventName}: ${typedError.message}`)
                    }
                    break
                  }
                }
              }
            }

            return eventResponse
          })

          controller.enqueue(result)
        },
      }),
    )
  }
}