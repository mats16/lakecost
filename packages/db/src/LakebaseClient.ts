import type { Env } from '@finlake/shared';
import type { DatabaseClient } from './DatabaseClient.js';
import type { Repositories } from './repositories/index.js';
import { logger } from './logger.js';

/**
 * Lakebase-backed DatabaseClient.
 *
 * Phase 1b: this is intentionally a stub that throws. The full implementation
 * wires `@databricks/lakebase`'s `createLakebasePool()` (returns a `pg.Pool`)
 * into Drizzle (`drizzle-orm/node-postgres`) and reuses the same Repositories
 * interface as SqliteClient.
 *
 * It is gated behind DB_BACKEND=lakebase or auto with PGHOST/LAKEBASE_INSTANCE_NAME
 * present, so SQLite-only deployments never touch this file.
 */
export class LakebaseClient implements DatabaseClient {
  readonly backend = 'lakebase' as const;

  private constructor(public readonly repos: Repositories) {}

  static async create(_env: Env): Promise<LakebaseClient> {
    logger.error('LakebaseClient is not yet implemented (Phase 1b follow-up)');
    throw new Error(
      'LakebaseClient is not yet implemented. Set DB_BACKEND=sqlite for now or wait for Phase 1b.',
    );
  }

  async healthCheck(): Promise<{ ok: true; backend: 'lakebase' }> {
    throw new Error('LakebaseClient.healthCheck not implemented');
  }

  async migrate(): Promise<void> {
    throw new Error('LakebaseClient.migrate not implemented');
  }

  async close(): Promise<void> {
    /* noop */
  }
}
