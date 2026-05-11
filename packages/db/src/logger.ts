import pino from 'pino';

export const logger = pino({
  name: 'finlake-db',
  level: process.env.LOG_LEVEL ?? 'info',
});
