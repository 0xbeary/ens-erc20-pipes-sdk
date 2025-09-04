import { events } from '../abi/ens.abi'
import { type Events, EvmDecodedEventStream } from '../utils/decoding/events.stream'

type ParamsWithoutArgs<T extends Events> = Omit<
  ConstructorParameters<typeof EvmDecodedEventStream<T>>[0],
  'args'
>

export const EnsEventStream = (params: ParamsWithoutArgs<typeof events>) =>
  new EvmDecodedEventStream({
    ...params,
    args: {
      contracts: ['0xC18360217D8F7Ab5e7c516566761Ea12Ce7F9D72'],
      events,
    },
  })
  