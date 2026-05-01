import { settingsToRecord, type DatabaseClient } from '@lakecost/db';
import {
  ACCOUNT_PRICES_DEFAULT,
  AWS_FOCUS_VERSION,
  CATALOG_SETTING_KEY,
  DATABRICKS_FOCUS_VERSION,
  FOCUS_REFRESH_CRON_DEFAULT,
  FOCUS_REFRESH_TIMEZONE_DEFAULT,
  focusViewFqn,
  medallionSchemaNamesFromSettings,
  normalizeS3Prefix,
  s3BucketFromUrl,
  s3ExportPath,
  tableLeafName,
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
  buildAwsFocusPipelineConfiguration,
  buildAwsFocusPipelineSql,
} from './awsFocusTransformPipelineSql.js';
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

interface AwsFocusConfig {
  awsAccountId: string | null;
  s3Bucket: string | null;
  s3Prefix: string | null;
  exportName: string | null;
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

function readAwsFocusConfig(config: Record<string, unknown>): AwsFocusConfig {
  const get = (k: string): string | null => {
    const v = config[k];
    return typeof v === 'string' && v.trim().length > 0 ? v.trim() : null;
  };
  const externalLocationUrl = get('externalLocationUrl');
  return {
    awsAccountId: get('awsAccountId'),
    s3Bucket:
      get('s3Bucket') ?? (externalLocationUrl ? s3BucketFromUrl(externalLocationUrl) : null),
    s3Prefix: get('s3Prefix'),
    exportName: get('exportName'),
    cronExpression: get('cronExpression') ?? FOCUS_REFRESH_CRON_DEFAULT,
    timezoneId: get('timezoneId') ?? FOCUS_REFRESH_TIMEZONE_DEFAULT,
    legacyPipelineId: get('pipelineId'),
    workspacePath: get('workspacePath'),
  };
}

export function workspacePathFor(
  appName: string,
  dataSourceId: number,
  filename = 'databricksFocusTransformPipeline.sql',
): string {
  return `/Workspace/Shared/${appName}/data_sources/${dataSourceId}/${filename}`;
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
  if (source.providerName === 'Databricks') return 'focus';
  if (isAwsProvider(source.providerName)) return fromConfig('awsAccountId') ?? String(source.id);
  if (source.providerName === 'Azure') return fromConfig('subscriptionId') ?? String(source.id);
  return String(source.id);
}

export function resourceLabelBase(source: {
  id: number;
  providerName: string;
  config: Record<string, unknown>;
}): string {
  const providerSlug = isAwsProvider(source.providerName)
    ? 'aws'
    : source.providerName.toLowerCase();
  return `finops-${providerSlug}-${resourceSlug(source)}`;
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
  const [source, settingsRows] = await Promise.all([
    db.repos.dataSources.get(dataSourceId),
    db.repos.appSettings.list(),
  ]);
  if (!source) throw new DataSourceSetupError('Data source not found', 404);
  if (source.providerName !== 'Databricks' && !isAwsProvider(source.providerName)) {
    throw new DataSourceSetupError(
      `Setup is only supported for Databricks and AWS data sources (got '${source.providerName}')`,
      400,
    );
  }

  const appSettings = settingsToRecord(settingsRows);
  const catalog = (appSettings[CATALOG_SETTING_KEY] ?? '').trim();
  const medallionSchemas = medallionSchemaNamesFromSettings(appSettings);
  if (!catalog) {
    throw new DataSourceSetupError(
      'Main catalog not configured. Set catalog_name in Configure → Catalog first.',
      400,
    );
  }

  const tableName = body.tableName ?? tableLeafName(source.tableName);

  const target = { catalog, schema: medallionSchemas.silver, table: tableName };
  let pipelineSql: string;
  let fqn: string;
  let configuration: Record<string, string> | undefined;
  let cronExpression: string;
  let timezoneId: string;
  let workspacePath: string;
  let pipelineId: string | null;
  let nextConfig: Record<string, unknown>;
  let focusVersion: string;
  try {
    fqn = focusViewFqn(target);
    if (source.providerName === 'Databricks') {
      const existing = readFocusConfig(source.config);
      const accountPricesRaw = body.accountPricesTable ?? existing.accountPricesTable;
      const accountPricesTable = validateAccountPricesTable(accountPricesRaw);
      pipelineSql = buildFocusPipelineSql({
        catalog,
        table: tableName,
        goldSchema: medallionSchemas.gold,
        accountPricesTable,
      });
      configuration = buildFocusPipelineConfiguration(
        tableName,
        accountPricesTable,
        medallionSchemas.gold,
      );
      cronExpression = (body.cronExpression ?? existing.cronExpression).trim();
      timezoneId = (body.timezoneId ?? existing.timezoneId).trim();
      workspacePath = workspacePathFor(env.DATABRICKS_APP_NAME, dataSourceId);
      pipelineId = source.pipelineId ?? existing.legacyPipelineId;
      focusVersion = DATABRICKS_FOCUS_VERSION;
      nextConfig = {
        ...source.config,
        accountPricesTable,
        cronExpression,
        timezoneId,
        targetSchema: medallionSchemas.silver,
        goldSchema: medallionSchemas.gold,
      };
    } else {
      const existing = readAwsFocusConfig(source.config);
      const awsSource = readAwsFocusSource(existing);
      configuration = buildAwsFocusPipelineConfiguration(
        tableName,
        awsSource.s3Bucket,
        awsSource.s3Prefix,
        awsSource.exportName,
        medallionSchemas.gold,
      );
      pipelineSql = buildAwsFocusPipelineSql();
      cronExpression = (body.cronExpression ?? existing.cronExpression).trim();
      timezoneId = (body.timezoneId ?? existing.timezoneId).trim();
      workspacePath = workspacePathFor(
        env.DATABRICKS_APP_NAME,
        dataSourceId,
        'awsFocusTransformPipeline.sql',
      );
      pipelineId = source.pipelineId ?? existing.legacyPipelineId;
      focusVersion = AWS_FOCUS_VERSION;
      nextConfig = {
        ...source.config,
        cronExpression,
        timezoneId,
        sourcePath: s3ExportDataPath(awsSource),
        targetSchema: medallionSchemas.silver,
        goldSchema: medallionSchemas.gold,
      };
    }
  } catch (err) {
    throw new DataSourceSetupError(`Invalid view target: ${(err as Error).message}`, 400);
  }

  const wc = buildUserWorkspaceClient(env, userToken);
  if (!wc) throw new DataSourceSetupError('Failed to build Databricks workspace client', 500);

  const labelBase = resourceLabelBase(source);
  const scheduleParams: PipelineScheduleParams = {
    pipelineName: `${labelBase}-pipeline`,
    jobName: `${labelBase}-job`,
    pipelineSql,
    workspacePath,
    catalog,
    schema: medallionSchemas.silver,
    configuration,
    cronExpression,
    timezoneId,
  };

  if (source.providerName === 'Databricks') {
    await assertCanReadUsageTable(env, userToken);
  }

  let result;
  try {
    result = await upsertPipelineSchedule(wc, scheduleParams, {
      jobId: source.jobId,
      pipelineId,
    });
  } catch (err) {
    throw new DataSourceSetupError(
      `Failed to provision pipeline/job for ${fqn}: ${(err as Error).message}`,
      500,
    );
  }

  const updated = await db.repos.dataSources.update(dataSourceId, {
    tableName,
    jobId: result.jobId,
    pipelineId: result.pipelineId,
    focusVersion,
    enabled: true,
    config: {
      ...nextConfig,
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

function isAwsProvider(providerName: string): boolean {
  return providerName === 'AWS' || providerName === 'Amazon Web Services';
}

function readAwsFocusSource(config: AwsFocusConfig): {
  s3Bucket: string;
  s3Prefix: string;
  exportName: string;
} {
  if (!config.s3Bucket) {
    throw new Error('S3 bucket is not configured. Save the AWS source before creating the job.');
  }
  const s3Prefix = normalizeS3Prefix(config.s3Prefix ?? '');
  if (!s3Prefix) {
    throw new Error('S3 prefix is not configured. Save the AWS source before creating the job.');
  }
  if (!config.exportName) {
    throw new Error('Export name is not configured. Save the AWS source before creating the job.');
  }
  return { s3Bucket: config.s3Bucket, s3Prefix, exportName: config.exportName };
}

function s3ExportDataPath({
  s3Bucket,
  s3Prefix,
  exportName,
}: {
  s3Bucket: string;
  s3Prefix: string;
  exportName: string;
}): string {
  return `${s3ExportPath(s3Bucket, s3Prefix, exportName)}/data`;
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
