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
    if (!values.length) return

    const parsedValues = values.map((value) =>
      Object.entries(value).reduce<Record<string, any>>((acc, [key, value]) => {
        acc[toSnakeCase(key)] = value
        return acc
      }, {}),
    )

    await this.client.insert({
      table,
      values: parsedValues,
      format: 'JSONEachRow',
    })
  }

  async insertMultiple(batches: { values: object[]; table: string }[]) {
    await Promise.all(batches.map(({ values, table }) => this.insert(values, table)))
  }
}
