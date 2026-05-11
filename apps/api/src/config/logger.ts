import pino from 'pino';
import { loadEnv } from './env.js';

const env = loadEnv();

export const logger = pino({
  name: 'finlake-api',
  level: env.LOG_LEVEL,
});
