import type { DatabaseClient } from '@lakecost/db';
import {
  ACCOUNT_PRICES_DEFAULT,
  CATALOG_SETTING_KEY,
  DATABRICKS_FOCUS_VERSION,
  FOCUS_REFRESH_CRON_DEFAULT,
  FOCUS_REFRESH_TIMEZONE_DEFAULT,
  FOCUS_VIEW_SCHEMA_DEFAULT,
  focusViewFqn,
  tableLeafName,
  unquotedFqn,
  validateAccountPricesTable,
  type DataSourceSetupBody,
  type DataSourceSetupResult,
  type Env,
} from '@lakecost/shared';
import { z } from 'zod';
import { buildUserExecutor, buildUserWorkspaceClient } from './statementExecution.js';
import {
  deletePipelineSchedule,
  upsertPipelineSchedule,
  type PipelineScheduleParams,
} from './databricksJobs.js';
import { DataSourceSetupError } from './dataSourceErrors.js';
import {
  buildFocusPipelineConfiguration,
  buildFocusPipelineSql,
} from './databricksFocusTransformPipelineSql.js';

interface FocusConfig {
  accountPricesTable: string;
  cronExpression: string;
  timezoneId: string;
  legacyPipelineId: string | null;
  workspacePath: string | null;
}

export function readFocusConfig(config: Record<string, unknown>): FocusConfig {
  const get = (k: string): string | null => {
    const v = config[k];
    return typeof v === 'string' && v.trim().length > 0 ? v.trim() : null;
  };
  return {
    accountPricesTable: get('accountPricesTable') ?? ACCOUNT_PRICES_DEFAULT,
    cronExpression: get('cronExpression') ?? FOCUS_REFRESH_CRON_DEFAULT,
    timezoneId: get('timezoneId') ?? FOCUS_REFRESH_TIMEZONE_DEFAULT,
    legacyPipelineId: get('pipelineId'),
    workspacePath: get('workspacePath'),
  };
}

export function workspacePathFor(appName: string, dataSourceId: number): string {
  return `/Workspace/Shared/${appName}/data_sources/${dataSourceId}/databricksFocusTransformPipeline.sql`;
}

/**
 * Identifier slot inside the resource label — `focus` for Databricks system
 * tables, the AWS account id for AWS CUR, etc. Falls back to `id<row-id>` so
 * the name is always unique even before the per-provider config is filled in.
 */
function resourceSlug(source: {
  id: number;
  providerName: string;
  config: Record<string, unknown>;
}): string {
  const fromConfig = (k: string): string | null => {
    const v = source.config[k];
    return typeof v === 'string' && v.trim().length > 0 ? v.trim() : null;
  };
  switch (source.providerName) {
    case 'Databricks':
      return 'focus';
    case 'AWS':
      return fromConfig('awsAccountId') ?? String(source.id);
    case 'Azure':
      return fromConfig('subscriptionId') ?? String(source.id);
    default:
      return String(source.id);
  }
}

export function resourceLabelBase(source: {
  id: number;
  providerName: string;
  config: Record<string, unknown>;
}): string {
  return `finops-${source.providerName.toLowerCase()}-${resourceSlug(source)}`;
}

/**
 * Provision a Lakeflow Declarative Pipeline that materializes the FOCUS view,
 * plus a Databricks Job that triggers the pipeline on cron. On success the
 * data source row's `job_id`, `pipeline_id`, and `config.workspacePath` are
 * updated.
 */
export async function setupFocusDataSource(
  env: Env,
  db: DatabaseClient,
  userToken: string | undefined,
  dataSourceId: number,
  body: DataSourceSetupBody,
): Promise<DataSourceSetupResult> {
  if (!userToken) {
    throw new DataSourceSetupError(
      'Missing OBO access token. Run behind Databricks Apps or `databricks apps run-local`.',
      401,
    );
  }
  if (!env.DATABRICKS_HOST) {
    throw new DataSourceSetupError('DATABRICKS_HOST must be configured.', 400);
  }
  if (!env.DATABRICKS_APP_NAME) {
    throw new DataSourceSetupError('DATABRICKS_APP_NAME must be configured.', 400);
  }
  const [source, catalogSetting] = await Promise.all([
    db.repos.dataSources.get(dataSourceId),
    db.repos.appSettings.get(CATALOG_SETTING_KEY),
  ]);
  if (!source) throw new DataSourceSetupError('Data source not found', 404);
  if (source.providerName !== 'Databricks') {
    throw new DataSourceSetupError(
      `Setup is only supported for providerName='Databricks' (got '${source.providerName}')`,
      400,
    );
  }

  const catalog = (catalogSetting?.value ?? '').trim();
  if (!catalog) {
    throw new DataSourceSetupError(
      'Main catalog not configured. Set catalog_name in Configure → Admin first.',
      400,
    );
  }

  const existing = readFocusConfig(source.config);
  const tableName = body.tableName ?? tableLeafName(source.tableName);
  const accountPricesRaw = body.accountPricesTable ?? existing.accountPricesTable;
  const cronExpression = (body.cronExpression ?? existing.cronExpression).trim();
  const timezoneId = (body.timezoneId ?? existing.timezoneId).trim();

  let accountPricesTable: string;
  try {
    accountPricesTable = validateAccountPricesTable(accountPricesRaw);
  } catch (err) {
    throw new DataSourceSetupError((err as Error).message, 400);
  }

  const target = { catalog, schema: FOCUS_VIEW_SCHEMA_DEFAULT, table: tableName };
  let pipelineSql: string;
  let fqn: string;
  try {
    pipelineSql = buildFocusPipelineSql({ catalog, table: tableName, accountPricesTable });
    fqn = focusViewFqn(target);
  } catch (err) {
    throw new DataSourceSetupError(`Invalid view target: ${(err as Error).message}`, 400);
  }

  const wc = buildUserWorkspaceClient(env, userToken);
  if (!wc) throw new DataSourceSetupError('Failed to build Databricks workspace client', 500);

  const workspacePath = workspacePathFor(env.DATABRICKS_APP_NAME, dataSourceId);

  const labelBase = resourceLabelBase(source);
  const scheduleParams: PipelineScheduleParams = {
    pipelineName: `${labelBase}-pipeline`,
    jobName: `${labelBase}-job`,
    pipelineSql,
    workspacePath,
    catalog,
    schema: FOCUS_VIEW_SCHEMA_DEFAULT,
    configuration: buildFocusPipelineConfiguration(tableName, accountPricesTable),
    cronExpression,
    timezoneId,
  };

  await assertCanReadUsageTable(env, userToken);

  let result;
  try {
    result = await upsertPipelineSchedule(wc, scheduleParams, {
      jobId: source.jobId,
      pipelineId: source.pipelineId ?? existing.legacyPipelineId,
    });
  } catch (err) {
    throw new DataSourceSetupError(
      `Failed to provision pipeline/job for ${fqn}: ${(err as Error).message}`,
      500,
    );
  }

  const updated = await db.repos.dataSources.update(dataSourceId, {
    tableName: unquotedFqn(catalog, FOCUS_VIEW_SCHEMA_DEFAULT, tableName),
    jobId: result.jobId,
    pipelineId: result.pipelineId,
    focusVersion: DATABRICKS_FOCUS_VERSION,
    enabled: true,
    config: {
      ...source.config,
      accountPricesTable,
      cronExpression,
      timezoneId,
      workspacePath: result.workspacePath,
    },
  });

  return {
    dataSourceId: updated.id,
    jobId: result.jobId,
    pipelineId: result.pipelineId,
    fqn,
    cronExpression,
    timezoneId,
    createdView: false,
  };
}

async function assertCanReadUsageTable(env: Env, userToken: string): Promise<void> {
  if (!env.SQL_WAREHOUSE_ID) {
    throw new DataSourceSetupError(
      [
        'SQL_WAREHOUSE_ID must be configured to verify system.billing.usage access',
        'before creating the FOCUS pipeline/job.',
      ].join(' '),
      400,
    );
  }
  const executor = buildUserExecutor(env, userToken);
  if (!executor) {
    throw new DataSourceSetupError(
      'Failed to build Databricks SQL executor for system.billing.usage access check.',
      500,
    );
  }
  try {
    await executor.run(
      'SELECT 1 AS ok FROM system.billing.usage LIMIT 1',
      [],
      z.object({ ok: z.number() }),
    );
  } catch (err) {
    throw new DataSourceSetupError(
      [
        'Cannot read system.billing.usage with the current user.',
        'Grant USE CATALOG on system, USE SCHEMA on system.billing, and SELECT on',
        'system.billing.usage before creating the FOCUS pipeline/job.',
        (err as Error).message,
      ].join(' '),
      400,
    );
  }
}

/**
 * Best-effort cleanup of the Databricks-side pipeline+job for a data source.
 * Returns `true` if cleanup ran (or was unnecessary), `false` if skipped due
 * to missing credentials — callers may want to warn the user.
 */
export async function teardownFocusDataSource(
  env: Env,
  userToken: string | undefined,
  source: { jobId: number | null; pipelineId: string | null; config: Record<string, unknown> },
): Promise<{ skippedTeardown: boolean }> {
  const hasRemoteResources =
    source.jobId !== null ||
    source.pipelineId !== null ||
    (typeof source.config.pipelineId === 'string' && source.config.pipelineId.length > 0) ||
    (typeof source.config.workspacePath === 'string' && source.config.workspacePath.length > 0);

  if (!userToken || !env.DATABRICKS_HOST) {
    return { skippedTeardown: hasRemoteResources };
  }
  const pipelineId =
    source.pipelineId ??
    (typeof source.config.pipelineId === 'string' && source.config.pipelineId.length > 0
      ? source.config.pipelineId
      : null);
  const workspacePath =
    typeof source.config.workspacePath === 'string' && source.config.workspacePath.length > 0
      ? source.config.workspacePath
      : null;
  if (source.jobId === null && pipelineId === null && workspacePath === null) {
    return { skippedTeardown: false };
  }
  const wc = buildUserWorkspaceClient(env, userToken);
  if (!wc) return { skippedTeardown: true };
  await deletePipelineSchedule(wc, {
    jobId: source.jobId,
    pipelineId,
    workspacePath,
  });
  return { skippedTeardown: false };
}

export async function runDataSourceJob(
  env: Env,
  db: DatabaseClient,
  userToken: string | undefined,
  dataSourceId: number,
): Promise<{ dataSourceId: number; jobId: number; runId: number }> {
  if (!userToken) {
    throw new DataSourceSetupError(
      'Missing OBO access token. Run behind Databricks Apps or `databricks apps run-local`.',
      401,
    );
  }
  if (!env.DATABRICKS_HOST) {
    throw new DataSourceSetupError('DATABRICKS_HOST must be configured.', 400);
  }

  const source = await db.repos.dataSources.get(dataSourceId);
  if (!source) throw new DataSourceSetupError('Data source not found', 404);
  if (source.jobId === null) {
    throw new DataSourceSetupError('No Databricks job has been created for this data source.', 400);
  }

  const wc = buildUserWorkspaceClient(env, userToken);
  if (!wc) throw new DataSourceSetupError('Failed to build Databricks workspace client', 500);

  let run;
  try {
    run = await wc.jobs.runNow({ job_id: source.jobId });
  } catch (err) {
    throw new DataSourceSetupError(
      `Failed to run job #${source.jobId}: ${(err as Error).message}`,
      500,
    );
  }
  if (typeof run.run_id !== 'number') {
    throw new DataSourceSetupError(`Databricks Jobs API returned no run_id`, 500);
  }

  return { dataSourceId: source.id, jobId: source.jobId, runId: run.run_id };
}
