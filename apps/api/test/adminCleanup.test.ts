import assert from 'node:assert/strict';
import test from 'node:test';
import type { DatabaseClient } from '@finlake/db';
import {
  AdminCleanupRequestSchema,
  CATALOG_SETTING_KEY,
  CATALOG_USER_GROUP_SETTING_KEY,
  GENIE_SPACE_SETTING_KEY,
  LAKEFLOW_PIPELINE_SETTING_KEYS,
  MEDALLION_SCHEMA_SETTING_KEYS,
  PRICING_NOTEBOOK_WORKSPACE_PATH_SETTING_KEY,
  type Env,
} from '@finlake/shared';
import {
  ADMIN_CLEANUP_SETTING_KEYS,
  cleanupFinLakeResources,
  parseAdminCleanupJobId,
} from '../src/services/adminCleanup.js';
import {
  LEGACY_SHARED_PIPELINE_SETTING_KEYS,
  SHARED_PIPELINE_SETTING_KEYS,
} from '../src/services/dataSourceSetup.js';

test('AdminCleanupRequestSchema defaults deleteCatalog to false', () => {
  const parsed = AdminCleanupRequestSchema.parse({});
  assert.equal(parsed.deleteCatalog, false);
});

test('ADMIN_CLEANUP_SETTING_KEYS includes catalog, schema, resource, and legacy keys', () => {
  assert.deepEqual(
    new Set(ADMIN_CLEANUP_SETTING_KEYS),
    new Set([
      CATALOG_SETTING_KEY,
      CATALOG_USER_GROUP_SETTING_KEY,
      MEDALLION_SCHEMA_SETTING_KEYS.bronze,
      MEDALLION_SCHEMA_SETTING_KEYS.silver,
      MEDALLION_SCHEMA_SETTING_KEYS.gold,
      LAKEFLOW_PIPELINE_SETTING_KEYS.pipelineId,
      LAKEFLOW_PIPELINE_SETTING_KEYS.jobId,
      SHARED_PIPELINE_SETTING_KEYS.workspaceRoot,
      PRICING_NOTEBOOK_WORKSPACE_PATH_SETTING_KEY,
      LEGACY_SHARED_PIPELINE_SETTING_KEYS.pipelineId,
      LEGACY_SHARED_PIPELINE_SETTING_KEYS.jobId,
      GENIE_SPACE_SETTING_KEY,
    ]),
  );
});

test('parseAdminCleanupJobId skips invalid or missing job ids', () => {
  assert.equal(parseAdminCleanupJobId(undefined), null);
  assert.equal(parseAdminCleanupJobId(''), null);
  assert.equal(parseAdminCleanupJobId('abc'), null);
  assert.equal(parseAdminCleanupJobId('-1'), null);
  assert.equal(parseAdminCleanupJobId('42'), 42);
});

test('cleanup response preserves resource failures while database cleanup completes', async () => {
  const db = fakeDb({
    [LAKEFLOW_PIPELINE_SETTING_KEYS.jobId]: '123',
    [LAKEFLOW_PIPELINE_SETTING_KEYS.pipelineId]: 'pipeline-1',
    [SHARED_PIPELINE_SETTING_KEYS.workspaceRoot]: '/Workspace/Shared/finlake/data_sources/shared',
  });
  const response = await cleanupFinLakeResources(db, {} as Env, { deleteCatalog: false });

  assert.equal(response.resources.find((r) => r.resourceType === 'job')?.status, 'failed');
  assert.equal(response.resources.find((r) => r.resourceType === 'pipeline')?.status, 'failed');
  assert.equal(response.resources.find((r) => r.resourceType === 'workspace')?.status, 'failed');
  assert.equal(response.database.status, 'deleted');
  assert.equal(response.database.deletedSettings, ADMIN_CLEANUP_SETTING_KEYS.length);
  assert.equal(response.database.deletedDataSources, 2);
  assert.equal(response.database.deletedPricingData, 4);
  assert.equal(response.database.deletedCachedAggregations, 3);
  assert.equal(response.database.deletedSetupState, 1);
});

function fakeDb(settings: Record<string, string>): DatabaseClient {
  return {
    backend: 'sqlite',
    repos: {
      appSettings: {
        list: async () =>
          Object.entries(settings).map(([key, value]) => ({
            key,
            value,
            updatedAt: new Date(0).toISOString(),
          })),
        deleteMany: async () => ADMIN_CLEANUP_SETTING_KEYS.length,
        get: async () => null,
        upsert: async (key: string, value: string) => ({
          key,
          value,
          updatedAt: new Date(0).toISOString(),
        }),
        delete: async () => {},
      },
      dataSources: {
        clear: async () => 2,
        list: async () => [],
        get: async () => null,
        create: async () => {
          throw new Error('not implemented');
        },
        update: async () => {
          throw new Error('not implemented');
        },
        delete: async () => {},
      },
      pricingData: {
        get: async () => null,
        getBySlug: async () => null,
        getByNotebookId: async () => null,
        upsert: async (input) => ({ ...input, updatedAt: new Date(0).toISOString() }),
        clear: async () => 4,
      },
      cachedAggregations: {
        clear: async () => 3,
        get: async () => null,
        set: async () => {},
        prune: async () => 0,
      },
      setupState: {
        clear: async () => 1,
        get: async () => null,
        upsert: async (value) => value,
        recordCheck: async () => {},
      },
      budgets: {
        list: async () => [],
        create: async () => {
          throw new Error('not implemented');
        },
        update: async () => null,
        delete: async () => {},
      },
      userPreferences: {
        get: async () => null,
        upsert: async (value) => value,
      },
    },
    healthCheck: async () => ({ ok: true, backend: 'sqlite' }),
    migrate: async () => {},
    close: async () => {},
  };
}
