import { settingsToRecord, type DatabaseClient } from '@finlake/db';
import { z } from 'zod';
import {
  CATALOG_SETTING_KEY,
  CATALOG_USER_GROUP_SETTING_KEY,
  GENIE_SPACE_SETTING_KEY,
  LAKEFLOW_PIPELINE_SETTING_KEYS,
  MEDALLION_SCHEMA_SETTING_KEYS,
  quoteIdent,
  type AdminCleanupDatabaseResult,
  type AdminCleanupResponse,
  type AdminCleanupResourceResult,
  type Env,
} from '@finlake/shared';
import { logger } from '../config/logger.js';
import { buildAppWorkspaceClient, buildUserExecutor } from './statementExecution.js';
import {
  LEGACY_SHARED_PIPELINE_SETTING_KEYS,
  SHARED_PIPELINE_SETTING_KEYS,
} from './dataSourceSetup.js';
import { deleteFinLakeGenieSpace, GenieServiceError } from './genie.js';

export const ADMIN_CLEANUP_SETTING_KEYS = [
  CATALOG_SETTING_KEY,
  CATALOG_USER_GROUP_SETTING_KEY,
  MEDALLION_SCHEMA_SETTING_KEYS.bronze,
  MEDALLION_SCHEMA_SETTING_KEYS.silver,
  MEDALLION_SCHEMA_SETTING_KEYS.gold,
  LAKEFLOW_PIPELINE_SETTING_KEYS.pipelineId,
  LAKEFLOW_PIPELINE_SETTING_KEYS.jobId,
  SHARED_PIPELINE_SETTING_KEYS.workspaceRoot,
  LEGACY_SHARED_PIPELINE_SETTING_KEYS.pipelineId,
  LEGACY_SHARED_PIPELINE_SETTING_KEYS.jobId,
  GENIE_SPACE_SETTING_KEY,
] as const;

export async function cleanupFinLakeResources(
  db: DatabaseClient,
  env: Env,
  opts: { deleteCatalog: boolean; userToken?: string },
): Promise<AdminCleanupResponse> {
  const settings = settingsToRecord(await db.repos.appSettings.list());
  const resources: AdminCleanupResourceResult[] = [];
  const wc = buildAppWorkspaceClient(env);
  const jobId =
    settings[LAKEFLOW_PIPELINE_SETTING_KEYS.jobId] ??
    settings[LEGACY_SHARED_PIPELINE_SETTING_KEYS.jobId];
  const pipelineId =
    settings[LAKEFLOW_PIPELINE_SETTING_KEYS.pipelineId] ??
    settings[LEGACY_SHARED_PIPELINE_SETTING_KEYS.pipelineId];

  resources.push(
    ...(await Promise.all([
      deleteJobResource(wc, jobId),
      deletePipelineResource(wc, pipelineId),
      deleteWorkspaceResource(wc, settings[SHARED_PIPELINE_SETTING_KEYS.workspaceRoot]),
      deleteGenieResource(db, env, settings[GENIE_SPACE_SETTING_KEY]),
      deleteCatalogResource(env, settings[CATALOG_SETTING_KEY], {
        deleteCatalog: opts.deleteCatalog,
        userToken: opts.userToken,
      }),
    ])),
  );

  const database = await cleanupDatabase(db);
  return { resources, database };
}

export function parseAdminCleanupJobId(raw: string | undefined): number | null {
  const value = raw?.trim();
  if (!value) return null;
  const parsed = Number(value);
  return Number.isSafeInteger(parsed) && parsed > 0 ? parsed : null;
}

async function deleteJobResource(
  wc: ReturnType<typeof buildAppWorkspaceClient>,
  rawJobId: string | undefined,
): Promise<AdminCleanupResourceResult> {
  const jobId = parseAdminCleanupJobId(rawJobId);
  if (!rawJobId?.trim()) return skipped('job', null, 'No saved Lakeflow job id.');
  if (jobId === null) return skipped('job', rawJobId, 'Saved Lakeflow job id is invalid.');
  if (!wc)
    return failed('job', String(jobId), 'Databricks app workspace client is not configured.');
  try {
    await wc.jobs.delete({ job_id: jobId });
    return deleted('job', String(jobId), 'Lakeflow job deleted.');
  } catch (err) {
    return failed('job', String(jobId), messageOf(err));
  }
}

async function deletePipelineResource(
  wc: ReturnType<typeof buildAppWorkspaceClient>,
  pipelineId: string | undefined,
): Promise<AdminCleanupResourceResult> {
  const id = pipelineId?.trim();
  if (!id) return skipped('pipeline', null, 'No saved Lakeflow pipeline id.');
  if (!wc) return failed('pipeline', id, 'Databricks app workspace client is not configured.');
  try {
    await wc.pipelines.delete({ pipeline_id: id });
    return deleted('pipeline', id, 'Lakeflow pipeline deleted.');
  } catch (err) {
    return failed('pipeline', id, messageOf(err));
  }
}

async function deleteWorkspaceResource(
  wc: ReturnType<typeof buildAppWorkspaceClient>,
  workspaceRoot: string | undefined,
): Promise<AdminCleanupResourceResult> {
  const path = workspaceRoot?.trim();
  if (!path) return skipped('workspace', null, 'No saved pipeline workspace root.');
  if (!wc) return failed('workspace', path, 'Databricks app workspace client is not configured.');
  try {
    await wc.workspace.delete({ path, recursive: true });
    return deleted('workspace', path, 'Pipeline workspace files deleted.');
  } catch (err) {
    return failed('workspace', path, messageOf(err));
  }
}

async function deleteGenieResource(
  db: DatabaseClient,
  env: Env,
  spaceId: string | undefined,
): Promise<AdminCleanupResourceResult> {
  const id = spaceId?.trim();
  if (!id) return skipped('genie_space', null, 'No saved Genie Space id.');
  try {
    await deleteFinLakeGenieSpace(env, db);
    return deleted('genie_space', id, 'Genie Space deleted.');
  } catch (err) {
    if (err instanceof GenieServiceError) {
      return failed('genie_space', id, err.message);
    }
    return failed('genie_space', id, messageOf(err));
  }
}

async function deleteCatalogResource(
  env: Env,
  catalog: string | undefined,
  opts: { deleteCatalog: boolean; userToken?: string },
): Promise<AdminCleanupResourceResult> {
  const name = catalog?.trim();
  if (!opts.deleteCatalog) {
    return skipped('catalog', name || null, 'Physical catalog deletion was not requested.');
  }
  if (!name) return skipped('catalog', null, 'No saved catalog name.');
  const executor = buildUserExecutor(env, opts.userToken);
  if (!executor) {
    return failed(
      'catalog',
      name,
      'OBO access token, DATABRICKS_HOST, and SQL_WAREHOUSE_ID are required to drop the catalog.',
    );
  }
  try {
    await executor.run(`DROP CATALOG IF EXISTS ${quoteIdent(name)} CASCADE`, [], z.unknown());
    return deleted('catalog', name, 'Physical catalog dropped.');
  } catch (err) {
    return failed('catalog', name, messageOf(err));
  }
}

async function cleanupDatabase(db: DatabaseClient): Promise<AdminCleanupDatabaseResult> {
  const result: AdminCleanupDatabaseResult = {
    status: 'deleted',
    message: null,
    deletedSettings: 0,
    deletedDataSources: 0,
    deletedCachedAggregations: 0,
    deletedSetupState: 0,
  };
  const failures: string[] = [];

  await Promise.all([
    countStep('app_settings', failures, async () => {
      result.deletedSettings = await db.repos.appSettings.deleteMany(ADMIN_CLEANUP_SETTING_KEYS);
    }),
    countStep('data_sources', failures, async () => {
      result.deletedDataSources = await db.repos.dataSources.clear();
    }),
    countStep('cached_aggregations', failures, async () => {
      result.deletedCachedAggregations = await db.repos.cachedAggregations.clear();
    }),
    countStep('setup_state', failures, async () => {
      result.deletedSetupState = await db.repos.setupState.clear();
    }),
  ]);

  if (failures.length > 0) {
    result.status = 'failed';
    result.message = failures.join(' ');
    logger.warn({ failures }, 'Admin database cleanup completed with failures');
  }
  return result;
}

async function countStep(
  label: string,
  failures: string[],
  fn: () => Promise<void>,
): Promise<void> {
  try {
    await fn();
  } catch (err) {
    failures.push(`${label}: ${messageOf(err)}`);
  }
}

function deleted(
  resourceType: AdminCleanupResourceResult['resourceType'],
  resourceId: string | null,
  message: string,
): AdminCleanupResourceResult {
  return { resourceType, resourceId, status: 'deleted', message };
}

function skipped(
  resourceType: AdminCleanupResourceResult['resourceType'],
  resourceId: string | null,
  message: string,
): AdminCleanupResourceResult {
  return { resourceType, resourceId, status: 'skipped', message };
}

function failed(
  resourceType: AdminCleanupResourceResult['resourceType'],
  resourceId: string | null,
  message: string,
): AdminCleanupResourceResult {
  return { resourceType, resourceId, status: 'failed', message };
}

function messageOf(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}
