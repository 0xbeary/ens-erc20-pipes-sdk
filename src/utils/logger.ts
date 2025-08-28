import * as process from 'node:process';
import { pino } from 'pino';

/**
 * Shared logger instance for consistent logging across the application
 */
export const logger = pino({
  level: process.env.LOG_LEVEL || 'info',
  messageKey: 'message',
  transport: {
    target: 'pino-pretty',
    options: {
      colorize: true,
      messageKey: 'message',
      singleLine: false,
    },
  },
  base: {},
});

/**
 * Creates a child logger with additional context
 */
export function createChildLogger(context: Record<string, unknown>) {
  return logger.child(context);
}

/**
 * Creates a logger with namespace (for backward compatibility)
 */
export function createLogger(ns: string) {
  return logger.child({ ns });
}
