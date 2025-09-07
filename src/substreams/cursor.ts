import { ClickHouseClient } from '@clickhouse/client';

const CURSOR_TABLE = "substreams_cursor";

export interface CursorData {
    id: string;
    cursor: string;
    block_number: number;
    timestamp: number;
}

export class ClickhouseCursor {
    private client: ClickHouseClient;
    private id: string;

    constructor(client: ClickHouseClient, id: string = "default") {
        this.client = client;
        this.id = id;
    }

    async ensureTable(): Promise<void> {
        const createTableQuery = `
            CREATE TABLE IF NOT EXISTS ${CURSOR_TABLE} (
                id String,
                cursor String,
                block_number UInt64,
                timestamp UInt64
            ) ENGINE = ReplacingMergeTree(timestamp)
            ORDER BY id
        `;
        
        await this.client.command({ query: createTableQuery });
    }

    async getCursor(): Promise<string | null> {
        try {
            await this.ensureTable();
            
            const result = await this.client.query({
                query: `SELECT cursor FROM ${CURSOR_TABLE} WHERE id = {id:String} ORDER BY timestamp DESC LIMIT 1`,
                query_params: { id: this.id }
            });
            
            const rows = await result.json<CursorData>();
            return rows.data.length > 0 ? rows.data[0].cursor : null;
        } catch (error) {
            console.error("Error getting cursor:", error);
            return null;
        }
    }

    async writeCursor(cursor: string, blockNumber: number): Promise<void> {
        try {
            await this.ensureTable();
            
            await this.client.insert({
                table: CURSOR_TABLE,
                values: [{
                    id: this.id,
                    cursor,
                    block_number: blockNumber,
                    timestamp: Date.now()
                }],
                format: 'JSONEachRow'
            });
        } catch (error) {
            console.error("Error writing cursor:", error);
            throw new Error("COULD_NOT_COMMIT_CURSOR");
        }
    }

    async rollbackToCursor(cursor: string): Promise<void> {
        try {
            await this.ensureTable();
            
            // Get the block number for this cursor
            const result = await this.client.query({
                query: `SELECT block_number FROM ${CURSOR_TABLE} WHERE id = {id:String} AND cursor = {cursor:String} LIMIT 1`,
                query_params: { id: this.id, cursor }
            });
            
            const rows = await result.json<CursorData>();
            if (rows.data.length === 0) {
                throw new Error(`Cursor not found: ${cursor}`);
            }
            
            // Update cursor to the rollback point
            await this.writeCursor(cursor, rows.data[0].block_number);
        } catch (error) {
            console.error("Error rolling back cursor:", error);
            throw new Error("COULD_NOT_READ_CURSOR");
        }
    }
}
