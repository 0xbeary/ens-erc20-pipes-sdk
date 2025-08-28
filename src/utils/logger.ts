import * as process from 'node:process';
import { pino } from 'pino';

export function createLogger(ns: string) {
  return pino({
    level: process.env.LOG_LEVEL || 'info',
    messageKey: 'message',
    transport: {
      target: 'pino-pretty',
      options: {
        messageKey: 'message',
        singleLine: true,
      },
    },

    base: { ns: ns },
  });
}
