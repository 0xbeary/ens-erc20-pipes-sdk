import { inspect } from 'node:util'
import { Options } from '@sqd-pipes/core'
import { logger } from './logger'

export function getDefaultRollback(tablesToRollack: string | string[]): Options['onRollback'] {
  return async ({ state, latest }) => {
    if (!latest.timestamp) {
      return // fresh table
    }

    try {
      await state.removeAllRows({
        table: tablesToRollack,
        where: `timestamp > ${latest.timestamp}`,
      })
    } catch (err) {
      logger.error(`onRollback err: ${inspect(err)}`)
      throw err
    }
  }
}