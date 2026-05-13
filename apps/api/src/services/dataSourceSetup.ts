import { settingsToRecord, type DatabaseClient } from '@finlake/db';
import {
  ACCOUNT_PRICES_DEFAULT,
  AWS_FOCUS_VERSION,
  CATALOG_SETTING_KEY,
  DATABRICKS_FOCUS_VERSION,
  FOCUS_REFRESH_CRON_DEFAULT,
  FOCUS_REFRESH_TIMEZONE_DEFAULT,
  GOLD_USAGE_TABLES,
  LAKEFLOW_PIPELINE_SETTING_KEYS,
  focusSourceTables,
  focusViewFqn,
  medallionSchemaNamesFromSettings,
  normalizeS3Prefix,
  quoteIdent,
  s3BucketFromUrl,
  s3ExportPath,
  tableLeafName,
  validateAccountPricesTable,
  type DataSource,
  type DataSourceSetupBody,
  type DataSourceSetupResult,
  type Env,
} from '@finlake/shared';
import { z } from 'zod';
import {
  buildAppExecutor,
  buildAppWorkspaceClient,
  buildUserExecutor,
} from './statementExecution.js';
import {
  upsertPipelineSchedule,
  type PipelineScheduleParams,
  type PipelineSourceFile,
} from './databricksJobs.js';
import { DataSourceSetupError } from './dataSourceErrors.js';
import {
  awsUsageTableName,
  buildAwsFocusSilverPipelineSql,
} from './awsFocusTransformPipelineSql.js';
import { buildFocusSilverPipelineSql } from './databricksFocusTransformPipelineSql.js';
import { grantStatements } from './focusPermissions.js';

interface FocusConfig {
  accountPricesTable: string;
}

interface AwsFocusConfig {
  awsAccountId: string | null;
  s3Bucket: string | null;
  s3Prefix: string | null;
  exportName: string | null;
}

export const SHARED_PIPELINE_SETTING_KEYS = {
  jobId: LAKEFLOW_PIPELINE_SETTING_KEYS.jobId,
  pipelineId: LAKEFLOW_PIPELINE_SETTING_KEYS.pipelineId,
  workspaceRoot: 'focus_pipeline_workspace_root',
} as const;

export const LEGACY_SHARED_PIPELINE_SETTING_KEYS = {
  jobId: 'focus_pipeline_job_id',
  pipelineId: 'focus_pipeline_id',
} as const;

const SHARED_PIPELINE_FILENAME_GOLD = 'gold_usage.sql';
const FOCUS_12_BILLING_COLUMNS = [
  { name: 'AvailabilityZone', type: 'STRING' },
  { name: 'BilledCost', type: 'DOUBLE' },
  { name: 'BillingAccountId', type: 'STRING' },
  { name: 'BillingAccountName', type: 'STRING' },
  { name: 'BillingAccountType', type: 'STRING' },
  { name: 'BillingCurrency', type: 'STRING' },
  { name: 'BillingPeriodEnd', type: 'TIMESTAMP' },
  { name: 'BillingPeriodStart', type: 'TIMESTAMP' },
  { name: 'CapacityReservationId', type: 'STRING' },
  { name: 'CapacityReservationStatus', type: 'STRING' },
  { name: 'ChargeCategory', type: 'STRING' },
  { name: 'ChargeClass', type: 'STRING' },
  { name: 'ChargeDescription', type: 'STRING' },
  { name: 'ChargeFrequency', type: 'STRING' },
  { name: 'ChargePeriodEnd', type: 'TIMESTAMP' },
  { name: 'ChargePeriodStart', type: 'TIMESTAMP' },
  { name: 'CommitmentDiscountCategory', type: 'STRING' },
  { name: 'CommitmentDiscountId', type: 'STRING' },
  { name: 'CommitmentDiscountName', type: 'STRING' },
  { name: 'CommitmentDiscountQuantity', type: 'DOUBLE' },
  { name: 'CommitmentDiscountStatus', type: 'STRING' },
  { name: 'CommitmentDiscountType', type: 'STRING' },
  { name: 'CommitmentDiscountUnit', type: 'STRING' },
  { name: 'ConsumedQuantity', type: 'DOUBLE' },
  { name: 'ConsumedUnit', type: 'STRING' },
  { name: 'ContractedCost', type: 'DOUBLE' },
  { name: 'ContractedUnitPrice', type: 'DOUBLE' },
  { name: 'EffectiveCost', type: 'DOUBLE' },
  { name: 'InvoiceId', type: 'STRING' },
  { name: 'InvoiceIssuerName', type: 'STRING' },
  { name: 'ListCost', type: 'DOUBLE' },
  { name: 'ListUnitPrice', type: 'DOUBLE' },
  { name: 'PricingCategory', type: 'STRING' },
  { name: 'PricingCurrency', type: 'STRING' },
  { name: 'PricingCurrencyContractedUnitPrice', type: 'DOUBLE' },
  { name: 'PricingCurrencyEffectiveCost', type: 'DOUBLE' },
  { name: 'PricingCurrencyListUnitPrice', type: 'DOUBLE' },
  { name: 'PricingQuantity', type: 'DOUBLE' },
  { name: 'PricingUnit', type: 'STRING' },
  { name: 'ProviderName', type: 'STRING' },
  { name: 'PublisherName', type: 'STRING' },
  { name: 'RegionId', type: 'STRING' },
  { name: 'RegionName', type: 'STRING' },
  { name: 'ResourceId', type: 'STRING' },
  { name: 'ResourceName', type: 'STRING' },
  { name: 'ResourceType', type: 'STRING' },
  { name: 'ServiceCategory', type: 'STRING' },
  { name: 'ServiceName', type: 'STRING' },
  { name: 'ServiceSubcategory', type: 'STRING' },
  { name: 'SkuId', type: 'STRING' },
  { name: 'SkuMeter', type: 'STRING' },
  { name: 'SkuPriceDetails', type: 'MAP<STRING, STRING>' },
  { name: 'SkuPriceId', type: 'STRING' },
  { name: 'SubAccountId', type: 'STRING' },
  { name: 'SubAccountName', type: 'STRING' },
  { name: 'SubAccountType', type: 'STRING' },
  { name: 'Tags', type: 'MAP<STRING, STRING>' },
] as const;
const AWS_EXTENSION_COLUMNS = [
  { name: 'x_Discounts', type: 'MAP<STRING, DOUBLE>' },
  { name: 'x_Operation', type: 'STRING' },
  { name: 'x_ServiceCode', type: 'STRING' },
] as const;
const DATABRICKS_EXTENSION_COLUMNS = [
  { name: 'x_Serverless', type: 'BOOLEAN' },
  { name: 'x_Photon', type: 'BOOLEAN' },
] as const;
const USAGE_DETAIL_COLUMNS = [
  ...FOCUS_12_BILLING_COLUMNS,
  ...AWS_EXTENSION_COLUMNS,
  ...DATABRICKS_EXTENSION_COLUMNS,
] as const;
const USAGE_DIMENSION_COLUMNS = [
  'ChargePeriodStart',
  'BillingPeriodStart',
  'BillingAccountId',
  'BillingAccountName',
  'BillingCurrency',
  'SubAccountId',
  'SubAccountName',
  'SubAccountType',
  'ProviderName',
  'ServiceCategory',
  'ServiceSubcategory',
  'ServiceName',
  'SkuId',
  'SkuMeter',
] as const;
const USAGE_RESOURCE_COLUMNS = ['ResourceType', 'ResourceId', 'ResourceName', 'Tags'] as const;
const USAGE_COST_COLUMNS = ['ListCost', 'BilledCost', 'ContractedCost', 'EffectiveCost'] as const;
const DAILY_USAGE_SOURCE_COLUMNS = [...USAGE_DIMENSION_COLUMNS, ...USAGE_COST_COLUMNS] as const;
const MONTHLY_USAGE_SOURCE_COLUMNS = [
  ...USAGE_DIMENSION_COLUMNS,
  ...USAGE_RESOURCE_COLUMNS,
  ...USAGE_COST_COLUMNS,
] as const;

export function readFocusConfig(config: Record<string, unknown>): FocusConfig {
  const get = (k: string): string | null => {
    const v = config[k];
    return typeof v === 'string' && v.trim().length > 0 ? v.trim() : null;
  };
  return {
    accountPricesTable: get('accountPricesTable') ?? ACCOUNT_PRICES_DEFAULT,
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
 * Register or update one data source, then regenerate the shared FinLake
 * Lakeflow pipeline/job that materializes all enabled sources.
 */
export async function setupFocusDataSource(
  env: Env,
  db: DatabaseClient,
  userToken: string | undefined,
  dataSourceId: number,
  body: DataSourceSetupBody,
): Promise<DataSourceSetupResult> {
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
  if (source.providerName === 'Databricks' && !userToken) {
    throw new DataSourceSetupError(
      'Missing OBO access token. Run behind Databricks Apps or `databricks apps run-local`.',
      401,
    );
  }

  const appSettings = settingsToRecord(settingsRows);
  const catalog = (appSettings[CATALOG_SETTING_KEY] ?? '').trim();
  const medallionSchemas = medallionSchemaNamesFromSettings(appSettings);
  if (!catalog) {
    throw new DataSourceSetupError(
      'Main catalog not configured. Set catalog_name in Catalog first.',
      400,
    );
  }

  let fqn: string;
  let nextConfig: Record<string, unknown>;
  let focusVersion: string;
  let tableName: string;
  let databricksAccountPricesTable: string | null = null;
  try {
    if (source.providerName === 'Databricks') {
      const existing = readFocusConfig(source.config);
      tableName = body.tableName ?? tableLeafName(source.tableName);
      const accountPricesRaw = body.accountPricesTable ?? existing.accountPricesTable;
      const accountPricesTable = validateAccountPricesTable(accountPricesRaw);
      databricksAccountPricesTable = accountPricesTable;
      focusVersion = DATABRICKS_FOCUS_VERSION;
      nextConfig = {
        ...source.config,
        accountPricesTable,
        targetSchema: medallionSchemas.silver,
        goldSchema: medallionSchemas.gold,
      };
    } else {
      const existing = readAwsFocusConfig(source.config);
      const awsSource = readAwsFocusSource(existing);
      tableName = awsUsageTableName(awsSource.awsAccountId);
      focusVersion = AWS_FOCUS_VERSION;
      nextConfig = {
        ...source.config,
        awsAccountId: awsSource.awsAccountId,
        sourcePath: s3ExportDataPath(awsSource),
        targetSchema: medallionSchemas.silver,
        goldSchema: medallionSchemas.gold,
      };
    }
    fqn = focusViewFqn({ catalog, schema: medallionSchemas.silver, table: tableName });
  } catch (err) {
    throw new DataSourceSetupError(`Invalid view target: ${(err as Error).message}`, 400);
  }

  const wc = buildAppWorkspaceClient(env);
  if (!wc) {
    throw new DataSourceSetupError(
      'Failed to build Databricks app service principal workspace client. Check DATABRICKS_CLIENT_ID and DATABRICKS_CLIENT_SECRET.',
      500,
    );
  }

  if (source.providerName === 'Databricks') {
    await assertCanReadUsageTable(env, userToken as string);
    await grantAppSystemTableAccess(env, userToken as string, databricksAccountPricesTable);
    await assertAppCanReadSystemTables(env, databricksAccountPricesTable);
  }

  const candidateSource: DataSource = {
    ...source,
    tableName,
    focusVersion,
    enabled: true,
    config: nextConfig,
  };
  const allSources = await db.repos.dataSources.list();
  const candidateSources = allSources.map((row) =>
    row.id === dataSourceId ? candidateSource : row,
  ) as DataSource[];
  let result;
  try {
    result = await syncSharedFocusPipeline(env, db, candidateSources, {
      catalog,
      silverSchema: medallionSchemas.silver,
      goldSchema: medallionSchemas.gold,
    });
  } catch (err) {
    throw new DataSourceSetupError(
      `Failed to provision shared pipeline/job for ${fqn}: ${(err as Error).message}`,
      500,
      'lakeflowJob',
    );
  }

  const updated = await db.repos.dataSources.update(dataSourceId, {
    tableName,
    focusVersion,
    enabled: true,
    config: nextConfig,
  });

  return {
    dataSourceId: updated.id,
    jobId: result.jobId,
    pipelineId: result.pipelineId,
    fqn,
    goldFqn: focusViewFqn({
      catalog,
      schema: medallionSchemas.gold,
      table: GOLD_USAGE_TABLES.daily,
    }),
    cronExpression: result.cronExpression,
    timezoneId: result.timezoneId,
    createdView: false,
  };
}

function isAwsProvider(providerName: string): boolean {
  return providerName === 'AWS' || providerName === 'Amazon Web Services';
}

function readAwsFocusSource(config: AwsFocusConfig): {
  awsAccountId: string;
  s3Bucket: string;
  s3Prefix: string;
  exportName: string;
} {
  if (!config.awsAccountId || !/^\d{12}$/.test(config.awsAccountId)) {
    throw new Error('AWS billing account id must be configured as a 12 digit account id.');
  }
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
  return {
    awsAccountId: config.awsAccountId,
    s3Bucket: config.s3Bucket,
    s3Prefix,
    exportName: config.exportName,
  };
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
        'Grant USE CATALOG, USE SCHEMA, and SELECT on the system catalog',
        'before creating the FOCUS pipeline/job.',
        (err as Error).message,
      ].join(' '),
      400,
      'systemGrants',
    );
  }
}

async function grantAppSystemTableAccess(
  env: Env,
  userToken: string,
  accountPricesTable: string | null,
): Promise<void> {
  if (!env.SQL_WAREHOUSE_ID) {
    throw new DataSourceSetupError(
      [
        'SQL_WAREHOUSE_ID must be configured to grant app service principal',
        'system table access before creating the shared FOCUS pipeline/job.',
      ].join(' '),
      400,
    );
  }
  const sp = (env.DATABRICKS_CLIENT_ID ?? '').trim();
  if (!sp) {
    throw new DataSourceSetupError(
      'DATABRICKS_CLIENT_ID must be configured before granting system table access.',
      400,
    );
  }
  const executor = buildUserExecutor(env, userToken);
  if (!executor) {
    throw new DataSourceSetupError(
      'OBO access token + DATABRICKS_HOST + SQL_WAREHOUSE_ID required to grant system table access.',
      400,
    );
  }
  const statements = grantStatements(
    'grant',
    focusSourceTables(accountPricesTable ?? ACCOUNT_PRICES_DEFAULT),
    sp,
  );
  for (const stmt of statements) {
    try {
      await executor.run(stmt.sql, [], z.unknown());
    } catch (err) {
      throw new DataSourceSetupError(
        `Failed to grant ${stmt.label} to the app service principal before creating the shared FOCUS pipeline/job: ${(err as Error).message}`,
        400,
        'systemGrants',
      );
    }
  }
}

async function assertAppCanReadSystemTables(
  env: Env,
  accountPricesTable: string | null,
): Promise<void> {
  if (!env.SQL_WAREHOUSE_ID) {
    throw new DataSourceSetupError(
      [
        'SQL_WAREHOUSE_ID must be configured to verify app service principal',
        'system table access before creating the shared FOCUS pipeline/job.',
      ].join(' '),
      400,
    );
  }
  const executor = buildAppExecutor(env);
  if (!executor) {
    throw new DataSourceSetupError(
      'Failed to build Databricks SQL executor for app service principal system.billing.usage access check.',
      500,
    );
  }
  try {
    for (const table of focusSourceTables(accountPricesTable ?? ACCOUNT_PRICES_DEFAULT)) {
      await executor.run(
        `SELECT 1 AS ok FROM ${focusViewFqn(table)} LIMIT 1`,
        [],
        z.object({ ok: z.number() }),
      );
    }
  } catch (err) {
    throw new DataSourceSetupError(
      [
        'Cannot read required system tables with the app service principal after granting access.',
        'Grant USE CATALOG, USE SCHEMA, and SELECT on the required catalogs to the app service principal',
        'before creating the shared FOCUS pipeline/job.',
        (err as Error).message,
      ].join(' '),
      400,
      'systemGrants',
    );
  }
}

export async function runDataSourceJob(
  env: Env,
  db: DatabaseClient,
  _userToken: string | undefined,
  dataSourceId: number,
): Promise<{ dataSourceId: number; jobId: number; runId: number }> {
  if (!env.DATABRICKS_HOST) {
    throw new DataSourceSetupError('DATABRICKS_HOST must be configured.', 400);
  }

  const source = await db.repos.dataSources.get(dataSourceId);
  if (!source) throw new DataSourceSetupError('Data source not found', 404);
  const appSettings = settingsToRecord(await db.repos.appSettings.list());
  const jobId = sharedJobIdSetting(appSettings);
  if (jobId === null) {
    throw new DataSourceSetupError('No shared Databricks job has been created.', 400);
  }
  const wc = buildAppWorkspaceClient(env);
  if (!wc) {
    throw new DataSourceSetupError(
      'Failed to build Databricks app service principal workspace client',
      500,
    );
  }

  let run;
  try {
    run = await wc.jobs.runNow({ job_id: jobId });
  } catch (err) {
    throw new DataSourceSetupError(`Failed to run job #${jobId}: ${(err as Error).message}`, 500);
  }
  if (typeof run.run_id !== 'number') {
    throw new DataSourceSetupError(`Databricks Jobs API returned no run_id`, 500);
  }

  return { dataSourceId: source.id, jobId, runId: run.run_id };
}

export async function runSharedFocusJob(
  env: Env,
  db: DatabaseClient,
): Promise<{ jobId: number; runId: number }> {
  if (!env.DATABRICKS_HOST) {
    throw new DataSourceSetupError('DATABRICKS_HOST must be configured.', 400);
  }

  const appSettings = settingsToRecord(await db.repos.appSettings.list());
  const jobId = sharedJobIdSetting(appSettings);
  if (jobId === null) {
    throw new DataSourceSetupError('No shared Databricks job has been created.', 400);
  }
  const wc = buildAppWorkspaceClient(env);
  if (!wc) {
    throw new DataSourceSetupError(
      'Failed to build Databricks app service principal workspace client',
      500,
    );
  }

  let run;
  try {
    run = await wc.jobs.runNow({ job_id: jobId });
  } catch (err) {
    throw new DataSourceSetupError(`Failed to run job #${jobId}: ${(err as Error).message}`, 500);
  }
  if (typeof run.run_id !== 'number') {
    throw new DataSourceSetupError(`Databricks Jobs API returned no run_id`, 500);
  }

  return { jobId, runId: run.run_id };
}

export async function syncSharedFocusPipeline(
  env: Env,
  db: DatabaseClient,
  sourcesOverride?: DataSource[],
  opts?: {
    catalog?: string;
    silverSchema?: string;
    goldSchema?: string;
    cronExpression?: string;
    timezoneId?: string;
  },
): Promise<{
  jobId: number;
  pipelineId: string;
  workspacePaths: string[];
  cronExpression: string;
  timezoneId: string;
}> {
  if (!env.DATABRICKS_APP_NAME) {
    throw new DataSourceSetupError('DATABRICKS_APP_NAME must be configured.', 400);
  }
  const [settingsRows, allSources] = await Promise.all([
    db.repos.appSettings.list(),
    sourcesOverride ? Promise.resolve(sourcesOverride) : db.repos.dataSources.list(),
  ]);
  const appSettings = settingsToRecord(settingsRows);
  const catalog = opts?.catalog ?? (appSettings[CATALOG_SETTING_KEY] ?? '').trim();
  const medallionSchemas = medallionSchemaNamesFromSettings(appSettings);
  const silverSchema = opts?.silverSchema ?? medallionSchemas.silver;
  const goldSchema = opts?.goldSchema ?? medallionSchemas.gold;
  if (!catalog) {
    throw new DataSourceSetupError(
      'Main catalog not configured. Set catalog_name in Catalog first.',
      400,
    );
  }

  const enabledSources = allSources.filter((source) => source.enabled);
  if (enabledSources.length === 0) {
    throw new DataSourceSetupError(
      'No enabled data sources are available for the shared pipeline.',
      400,
    );
  }

  const wc = buildAppWorkspaceClient(env);
  if (!wc) {
    throw new DataSourceSetupError(
      'Failed to build Databricks app service principal workspace client. Check DATABRICKS_CLIENT_ID and DATABRICKS_CLIENT_SECRET.',
      500,
    );
  }
  const existingJobId = sharedJobIdSetting(appSettings);
  const existingSchedule =
    opts?.cronExpression && opts?.timezoneId
      ? null
      : await readExistingJobSchedule(wc, existingJobId);
  const cronExpression =
    opts?.cronExpression ?? existingSchedule?.cronExpression ?? FOCUS_REFRESH_CRON_DEFAULT;
  const timezoneId =
    opts?.timezoneId ?? existingSchedule?.timezoneId ?? FOCUS_REFRESH_TIMEZONE_DEFAULT;

  const workspaceRoot =
    appSettings[SHARED_PIPELINE_SETTING_KEYS.workspaceRoot] ??
    sharedPipelineWorkspaceRoot(env.DATABRICKS_APP_NAME);
  const sourceFiles = enabledSources.map((source) => sourcePipelineFile(workspaceRoot, source));
  const goldFile: PipelineSourceFile = {
    workspacePath: `${workspaceRoot}/${SHARED_PIPELINE_FILENAME_GOLD}`,
    pipelineSql: buildUsageGoldSql({
      catalog,
      silverSchema,
      goldSchema,
      sources: sourceFiles.map((file) => ({
        tableName: file.tableName,
        providerName: file.providerName,
      })),
    }),
  };
  const params: PipelineScheduleParams = {
    pipelineName: 'finops-focus-shared-pipeline',
    jobName: 'finops-focus-shared-job',
    files: [...sourceFiles, goldFile],
    catalog,
    schema: silverSchema,
    cronExpression,
    timezoneId,
    servicePrincipalId: env.DATABRICKS_CLIENT_ID,
    environmentTag: env.NODE_ENV,
  };
  const result = await upsertPipelineSchedule(wc, params, {
    jobId: existingJobId,
    pipelineId: sharedPipelineIdSetting(appSettings),
  });

  await Promise.all([
    db.repos.appSettings.upsert(SHARED_PIPELINE_SETTING_KEYS.jobId, String(result.jobId)),
    db.repos.appSettings.upsert(SHARED_PIPELINE_SETTING_KEYS.pipelineId, result.pipelineId),
    db.repos.appSettings.upsert(SHARED_PIPELINE_SETTING_KEYS.workspaceRoot, workspaceRoot),
    db.repos.appSettings.delete(LEGACY_SHARED_PIPELINE_SETTING_KEYS.jobId),
    db.repos.appSettings.delete(LEGACY_SHARED_PIPELINE_SETTING_KEYS.pipelineId),
  ]);

  return { ...result, cronExpression, timezoneId };
}

async function readExistingJobSchedule(
  wc: ReturnType<typeof buildAppWorkspaceClient> & {},
  jobId: number | null,
): Promise<{ cronExpression: string; timezoneId: string } | null> {
  if (jobId === null) return null;
  try {
    const job = await wc.jobs.get({ job_id: jobId });
    const cronExpression = job.settings?.schedule?.quartz_cron_expression?.trim();
    const timezoneId = job.settings?.schedule?.timezone_id?.trim();
    return cronExpression && timezoneId ? { cronExpression, timezoneId } : null;
  } catch {
    return null;
  }
}

function sharedPipelineWorkspaceRoot(appName: string): string {
  return `/Workspace/Shared/${appName}/data_sources/shared`;
}

function sharedJobIdSetting(settings: Record<string, string>): number | null {
  return numberSetting(
    settings[SHARED_PIPELINE_SETTING_KEYS.jobId] ??
      settings[LEGACY_SHARED_PIPELINE_SETTING_KEYS.jobId],
  );
}

function sharedPipelineIdSetting(settings: Record<string, string>): string | null {
  return stringSetting(
    settings[SHARED_PIPELINE_SETTING_KEYS.pipelineId] ??
      settings[LEGACY_SHARED_PIPELINE_SETTING_KEYS.pipelineId],
  );
}

function sourcePipelineFile(
  workspaceRoot: string,
  source: DataSource,
): PipelineSourceFile & { tableName: string; providerName: string } {
  if (source.providerName === 'Databricks') {
    const config = readFocusConfig(source.config);
    const tableName = tableLeafName(source.tableName);
    return {
      tableName,
      providerName: source.providerName,
      workspacePath: `${workspaceRoot}/databricks_${source.id}.sql`,
      pipelineSql: buildFocusSilverPipelineSql({
        table: tableName,
        accountPricesTable: config.accountPricesTable,
      }),
    };
  }
  if (!isAwsProvider(source.providerName)) {
    throw new Error(`Unsupported shared pipeline provider "${source.providerName}"`);
  }
  const awsSource = readAwsFocusSource(readAwsFocusConfig(source.config));
  const tableName = awsUsageTableName(awsSource.awsAccountId);
  return {
    tableName,
    providerName: source.providerName,
    workspacePath: `${workspaceRoot}/aws_${awsSource.awsAccountId}.sql`,
    pipelineSql: buildAwsFocusSilverPipelineSql({
      tableName,
      s3Bucket: awsSource.s3Bucket,
      s3Prefix: awsSource.s3Prefix,
      exportName: awsSource.exportName,
    }),
  };
}

export function buildUsageGoldSql({
  catalog,
  silverSchema,
  goldSchema,
  sources,
}: {
  catalog: string;
  silverSchema: string;
  goldSchema: string;
  sources: Array<{ tableName: string; providerName: string }>;
}): string {
  const usageDetailUnionSql = sources
    .map(
      (source) =>
        `SELECT
    ${USAGE_DETAIL_COLUMNS.map((column) => usageDetailColumnExpression(source, column)).join(',\n    ')}
  FROM ${quoteIdent(catalog)}.${quoteIdent(silverSchema)}.${quoteIdent(source.tableName)}`,
    )
    .join('\n  UNION ALL\n  ');
  return /* sql */ `CREATE VIEW ${quoteIdent('usage')}
COMMENT 'FOCUS 1.2 compatible usage details managed by FinLake'
AS
WITH focus_rows AS (
  ${usageDetailUnionSql}
)
SELECT
  ${USAGE_DETAIL_COLUMNS.map((column) => quoteIdent(column.name)).join(',\n  ')}
FROM focus_rows;

CREATE OR REFRESH MATERIALIZED VIEW ${quoteIdent(goldSchema)}.${quoteIdent(GOLD_USAGE_TABLES.daily)}
COMMENT 'FOCUS daily usage rollup managed by FinLake'
AS
WITH focus_rows AS (
  SELECT
    ${DAILY_USAGE_SOURCE_COLUMNS.map(quoteIdent).join(',\n    ')}
  FROM ${quoteIdent(silverSchema)}.${quoteIdent('usage')}
)
SELECT
  CAST(ChargePeriodStart AS DATE) AS x_ChargeDate,
  CAST(DATE_TRUNC('MONTH', BillingPeriodStart) AS DATE) AS x_BillingMonth,
  BillingAccountId,
  BillingAccountName,
  BillingCurrency,
  SubAccountId,
  SubAccountName,
  SubAccountType,
  ProviderName,
  ServiceCategory,
  ServiceSubcategory,
  ServiceName,
  SkuId,
  SkuMeter,
  CAST(SUM(COALESCE(ListCost, 0)) AS DECIMAL(30, 15)) AS ListCost,
  CAST(SUM(COALESCE(BilledCost, 0)) AS DECIMAL(30, 15)) AS BilledCost,
  CAST(SUM(COALESCE(ContractedCost, 0)) AS DECIMAL(30, 15)) AS ContractedCost,
  CAST(SUM(COALESCE(EffectiveCost, 0)) AS DECIMAL(30, 15)) AS EffectiveCost
FROM focus_rows
GROUP BY
  CAST(ChargePeriodStart AS DATE),
  CAST(DATE_TRUNC('MONTH', BillingPeriodStart) AS DATE),
  BillingAccountId,
  BillingAccountName,
  BillingCurrency,
  SubAccountId,
  SubAccountName,
  SubAccountType,
  ProviderName,
  ServiceCategory,
  ServiceSubcategory,
  ServiceName,
  SkuId,
  SkuMeter;

CREATE OR REFRESH MATERIALIZED VIEW ${quoteIdent(goldSchema)}.${quoteIdent(GOLD_USAGE_TABLES.monthly)}
COMMENT 'FOCUS monthly usage rollup aggregated at the resource level (ResourceType, ResourceId, ResourceName) with latest Tags per resource. Managed by FinLake.'
AS
WITH focus_rows AS (
  SELECT
    ${MONTHLY_USAGE_SOURCE_COLUMNS.map(quoteIdent).join(',\n    ')}
  FROM ${quoteIdent(silverSchema)}.${quoteIdent('usage')}
)
SELECT
  CAST(DATE_TRUNC('MONTH', BillingPeriodStart) AS DATE) AS x_BillingMonth,
  BillingAccountId,
  BillingAccountName,
  BillingCurrency,
  SubAccountId,
  SubAccountName,
  SubAccountType,
  ProviderName,
  ServiceCategory,
  ServiceSubcategory,
  ServiceName,
  SkuId,
  SkuMeter,
  ResourceType,
  ResourceId,
  ResourceName,
  MAX_BY(Tags, ChargePeriodStart) AS Tags,
  CAST(SUM(COALESCE(ListCost, 0)) AS DECIMAL(30, 15)) AS ListCost,
  CAST(SUM(COALESCE(BilledCost, 0)) AS DECIMAL(30, 15)) AS BilledCost,
  CAST(SUM(COALESCE(ContractedCost, 0)) AS DECIMAL(30, 15)) AS ContractedCost,
  CAST(SUM(COALESCE(EffectiveCost, 0)) AS DECIMAL(30, 15)) AS EffectiveCost
FROM focus_rows
GROUP BY
  CAST(DATE_TRUNC('MONTH', BillingPeriodStart) AS DATE),
  BillingAccountId,
  BillingAccountName,
  BillingCurrency,
  SubAccountId,
  SubAccountName,
  SubAccountType,
  ProviderName,
  ServiceCategory,
  ServiceSubcategory,
  ServiceName,
  SkuId,
  SkuMeter,
  ResourceType,
  ResourceId,
  ResourceName;`;
}

function usageDetailColumnExpression(
  source: { providerName: string },
  column: (typeof USAGE_DETAIL_COLUMNS)[number],
): string {
  if (sourceHasUsageDetailColumn(source, column.name)) {
    return quoteIdent(column.name);
  }
  return `CAST(NULL AS ${column.type}) AS ${quoteIdent(column.name)}`;
}

function sourceHasUsageDetailColumn(source: { providerName: string }, columnName: string): boolean {
  if (FOCUS_12_BILLING_COLUMNS.some((column) => column.name === columnName)) return true;
  if (isAwsProvider(source.providerName)) {
    return AWS_EXTENSION_COLUMNS.some((column) => column.name === columnName);
  }
  if (source.providerName === 'Databricks') {
    return DATABRICKS_EXTENSION_COLUMNS.some((column) => column.name === columnName);
  }
  return false;
}

function stringSetting(value: string | undefined): string | null {
  return value && value.trim().length > 0 ? value.trim() : null;
}

function numberSetting(value: string | undefined): number | null {
  if (!value) return null;
  const n = Number(value);
  return Number.isSafeInteger(n) && n > 0 ? n : null;
}
