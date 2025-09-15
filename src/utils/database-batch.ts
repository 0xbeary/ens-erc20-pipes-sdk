import { ClickHouseClient } from '@clickhouse/client'
import { toSnakeCase } from './string-utils'

// TODO: make this global
// @ts-ignore
BigInt.prototype.toJSON = function () {
  return this.toString()
}

export class DatabaseBatch {
  constructor(private client: ClickHouseClient) {}

  async insert(values: object[], table: string) {
    if (!values.length) {
      return
    }

    console.log(`Inserting ${values.length} rows into ${table}`)

    const parsedValues = values.map((value) =>
      Object.entries(value).reduce<Record<string, any>>((acc, [key, value]) => {
        acc[toSnakeCase(key)] = value
        return acc
      }, {}),
    )

    try {
      await this.client.insert({
        table,
        values: parsedValues,
        format: 'JSONEachRow',
      })
      console.log(`Successfully inserted ${values.length} rows into ${table}`)
    } catch (error) {
      console.error(`Failed to insert data into ${table}:`, error)
      throw error
    }
  }

  async insertMultiple(batches: { values: object[]; table: string }[]) {
    console.log(`Inserting into ${batches.length} tables`)
    await Promise.all(batches.map(({ values, table }) => this.insert(values, table)))
  }
}
