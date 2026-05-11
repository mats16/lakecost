import { loadEnv } from './config/env.js';
import { logger } from './config/logger.js';
import { createDatabaseClient } from '@finlake/db';
import { buildApp } from './app.js';

async function main() {
  const env = loadEnv();
  const db = await createDatabaseClient(env);
  if (env.MIGRATE_ON_BOOT) {
    await db.migrate();
  }
  const app = await buildApp({ env, db });
  const port = env.DATABRICKS_APP_PORT ?? env.PORT;
  app.listen(port, '0.0.0.0', () => {
    logger.info({ port, backend: db.backend, nodeEnv: env.NODE_ENV }, 'finlake api listening');
  });
}

main().catch((err) => {
  console.error('Fatal startup error:', err);
  process.exit(1);
});
