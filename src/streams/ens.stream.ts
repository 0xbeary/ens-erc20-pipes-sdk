import { events } from '../abi/ens.abi'
import { type Events, EvmDecodedEventStream } from '../utils/decoding/events.stream'

type ParamsWithoutArgs<T extends Events> = Omit<
  ConstructorParameters<typeof EvmDecodedEventStream<T>>[0],
  'args'
>

export const EnsEventStream = (
  params: ParamsWithoutArgs<typeof events>,
  contracts: string[]
) =>
  new EvmDecodedEventStream({
    ...params,
    args: {
      contracts,
      events,
    },
  })
