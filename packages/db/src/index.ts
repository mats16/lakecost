import type { Env } from '@finlake/shared';
import { logger } from './logger.js';
import type { DatabaseClient } from './DatabaseClient.js';
import { SqliteClient } from './SqliteClient.js';
import { LakebaseClient } from './LakebaseClient.js';
import { resolveSqlitePath } from './paths.js';

export type { DatabaseClient } from './DatabaseClient.js';
export type { Repositories } from './repositories/index.js';
export { settingsToRecord } from './repositories/index.js';

export async function createDatabaseClient(env: Env): Promise<DatabaseClient> {
  switch (env.DB_BACKEND) {
    case 'sqlite': {
      const sqlitePath = resolveSqlitePath(env);
      logger.info({ sqlitePath }, 'DB_BACKEND=sqlite, using SQLite');
      return await SqliteClient.create({ sqlitePath });
    }

    case 'lakebase': {
      logger.info('DB_BACKEND=lakebase, using Lakebase (no fallback)');
      return await LakebaseClient.create(env);
    }

    case 'auto':
    default: {
      if (env.LAKEBASE_INSTANCE_NAME || env.PGHOST) {
        try {
          const client = await LakebaseClient.create(env);
          await client.healthCheck();
          logger.info('DB_BACKEND=auto, Lakebase initialized');
          return client;
        } catch (err) {
          logger.warn({ err }, 'Lakebase init failed, falling back to SQLite');
        }
      }
      const sqlitePath = resolveSqlitePath(env);
      logger.info({ sqlitePath }, 'DB_BACKEND=auto, using SQLite fallback');
      return await SqliteClient.create({ sqlitePath });
    }
  }
}
