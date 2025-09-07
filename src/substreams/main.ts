import {
    createRequest,
    streamBlocks,
    createAuthInterceptor,
    createRegistry,
    fetchSubstream,
} from '@substreams/core';
import type {Package} from '@substreams/core/proto';
import type { Transport, Interceptor } from "@connectrpc/connect";
import { createConnectTransport } from "@connectrpc/connect-web";
import type { IMessageTypeRegistry } from "@bufbuild/protobuf";
import { ClickhouseCursor } from "./cursor";
import { isErrorRetryable } from "./error";
import { handleResponseMessage } from "./handlers";
import { Handlers } from "./types";
import { ENDPOINT, MODULE, SPKG, START_BLOCK, STOP_BLOCK, TOKEN } from './constants';
import { ClickHouseClient } from '@clickhouse/client';
import { logger } from '../utils/logger';

/*
    Entrypoint of the Substreams application.
    Because of the long-running connection, Substreams will disconnect from time to time.
    The application MUST handle disconnections and commit the provided cursor to avoid missing information.
*/
export const startSubstreams = async (handlers: Handlers, client: ClickHouseClient, cursorId: string = 'default') => {
    const pkg: Package = await fetchPackage()
    const registry: IMessageTypeRegistry = createRegistry(pkg);
    const authInterceptor: Interceptor = createAuthInterceptor(TOKEN);
    const cursor = new ClickhouseCursor(client, cursorId);

    if (!TOKEN || TOKEN === "" || TOKEN === "<SUBSTREAMS-TOKEN>") {
        throw new Error("You must set the 'SUBSTREAMS_TOKEN' environment variable, please read the README for further details");
    }

    const transport = createConnectTransport({
        baseUrl: ENDPOINT,
        interceptors: [authInterceptor],
        useBinaryFormat: true,
        jsonOptions: {
            typeRegistry: registry,
        },
    });

    // The infinite loop handles disconnections. Every time a disconnection error is thrown, the loop will automatically reconnect
    // and start consuming from the latest committed cursor.
    while (true) {
        try {
            await stream(pkg, registry, transport, handlers, cursor);

            // Break out of the loop when the stream is finished
            break;
        } catch (e) {
            if (!isErrorRetryable(e)) {
              logger.error(`A fatal error occurred: ${e}`)
              throw e
            }

            logger.warn(`A retryable error occurred (${e}), retrying after backoff`)
            logger.error(e)
            // Add backoff - wait 5 seconds before retrying
            await new Promise(resolve => setTimeout(resolve, 5000));
        }
    }
}

const fetchPackage = async () => {
    return await fetchSubstream(SPKG)
}

const stream = async (
    pkg: Package, 
    registry: IMessageTypeRegistry, 
    transport: Transport, 
    handlers: Handlers, 
    cursor: ClickhouseCursor
) => {
    const startCursor = await cursor.getCursor();
    
    const request = createRequest({
        substreamPackage: pkg,
        outputModule: MODULE,
        productionMode: true,
        startBlockNum: START_BLOCK,
        stopBlockNum: STOP_BLOCK,
        startCursor: startCursor ?? undefined
    });

    logger.info(`Starting Substreams from block ${START_BLOCK}${startCursor ? ` with cursor ${startCursor.substring(0, 20)}...` : ' (no cursor)'}`);

    // Stream the blocks
    for await (const response of streamBlocks(transport, request)) {
        /*
            Decode the response and handle the message.
            There are different types of response messages that you can receive. You can read more about the response message in the docs:
            https://substreams.streamingfast.io/documentation/consume/reliability-guarantees#the-response-format
        */
        handleResponseMessage(response.message, registry, handlers);
    }
}
