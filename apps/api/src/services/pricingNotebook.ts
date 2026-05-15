import { readFile } from 'node:fs/promises';
import { settingsToRecord, type DatabaseClient } from '@finlake/db';
import {
  DATABRICKS_ACCOUNT_PRICES_TABLE_DEFAULT,
  DATABRICKS_LIST_PRICES_TABLE_DEFAULT,
  AWS_EC2_PRICING_TABLE_DEFAULT,
  AWS_RDS_PRICING_TABLE_DEFAULT,
  DOWNLOADS_VOLUME_DEFAULT,
  PRICING_SCHEMA_DEFAULT,
  catalogFromSettings,
  isActivePricingRunStatus,
  medallionSchemaNamesFromSettings,
  quoteIdent,
  unquotedFqn,
  type AwsPricingId,
  type DatabricksPricingId,
  type Env,
  type PricingData,
  type PricingId,
  type PricingNotebookDeleteResult,
  type PricingNotebookListResponse,
  type PricingNotebookState,
} from '@finlake/shared';
import { getDatabricksRunSnapshot } from './databricksRunStatus.js';
import { DataSourceSetupError } from './dataSourceErrors.js';
import { uploadPipelineFile } from './databricksJobs.js';
import {
  buildAppWorkspaceClient,
  buildUserExecutor,
  type StatementExecutor,
  type WorkspaceClient,
} from './statementExecution.js';
import { z } from 'zod';

const AWS_PRICING_NOTEBOOK_NAME = 'pricing_ingest_aws.ipynb';
const DATABRICKS_PRICING_NOTEBOOK_NAME = 'pricing_ingest_databricks.ipynb';
const AWS_PROVIDER = 'AWS';
const DATABRICKS_PROVIDER = 'Databricks';

interface BasePricingService {
  service: string;
  id: PricingId;
  provider: string;
  tableName: string;
  rawTableName: string;
  notebookName: string;
  source: string;
}

interface AwsPricingService extends BasePricingService {
  id: AwsPricingId;
  provider: typeof AWS_PROVIDER;
  kind: 'aws';
  priceListFile: string;
}

interface DatabricksPricingService extends BasePricingService {
  id: DatabricksPricingId;
  provider: typeof DATABRICKS_PROVIDER;
  kind: 'databricks';
}

export type PricingService = AwsPricingService | DatabricksPricingService;

function defineAwsPricingService(
  service: string,
  id: AwsPricingId,
  tableName: string,
): AwsPricingService {
  return {
    service,
    id,
    provider: AWS_PROVIDER,
    tableName,
    rawTableName: `pricing_${id}`,
    notebookName: AWS_PRICING_NOTEBOOK_NAME,
    kind: 'aws',
    priceListFile: `pricing_${id}.csv`,
    source: `https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/${service}/current/index.csv`,
  };
}

const PRICING_SERVICES: readonly PricingService[] = [
  defineAwsPricingService('AmazonEC2', 'aws_ec2', AWS_EC2_PRICING_TABLE_DEFAULT),
  defineAwsPricingService('AmazonRDS', 'aws_rds', AWS_RDS_PRICING_TABLE_DEFAULT),
  defineDatabricksPricingService(
    'List Prices',
    'databricks_list_prices',
    DATABRICKS_LIST_PRICES_TABLE_DEFAULT,
    'system.billing.list_prices',
  ),
  defineDatabricksPricingService(
    'Account Prices',
    'databricks_account_prices',
    DATABRICKS_ACCOUNT_PRICES_TABLE_DEFAULT,
    'system.billing.account_prices',
  ),
];

function defineDatabricksPricingService(
  service: string,
  id: DatabricksPricingId,
  tableName: string,
  source: string,
): DatabricksPricingService {
  return {
    id,
    provider: DATABRICKS_PROVIDER,
    service,
    tableName,
    rawTableName: `pricing_${id}`,
    notebookName: DATABRICKS_PRICING_NOTEBOOK_NAME,
    source,
    kind: 'databricks',
  };
}

const cachedNotebookContent = new Map<string, string>();
async function readPricingNotebook(notebookName: string): Promise<string> {
  const cached = cachedNotebookContent.get(notebookName);
  if (cached) return cached;
  const content = await readFile(new URL(`../notebooks/${notebookName}`, import.meta.url), 'utf8');
  cachedNotebookContent.set(notebookName, content);
  return content;
}

export function pricingNotebookWorkspacePath(
  appName: string,
  notebookName = AWS_PRICING_NOTEBOOK_NAME,
): string {
  return `/Workspace/Shared/${appName}/pricing/${notebookName}`;
}

export function pricingServiceById(id: string): PricingService {
  const service = PRICING_SERVICES.find((candidate) => candidate.id === id);
  if (!service) {
    throw new DataSourceSetupError(`Unsupported pricing service id: ${id}`, 400);
  }
  return service;
}

export function awsPricingServiceById(id: string): AwsPricingService {
  const service = pricingServiceById(id);
  if (service.kind !== 'aws') {
    throw new DataSourceSetupError(`Unsupported AWS pricing service id: ${id}`, 400);
  }
  return service;
}

function pricingDownloadFilePath(
  catalog: string,
  bronzeSchema: string,
  service: AwsPricingService,
): string {
  return `/Volumes/${catalog}/${bronzeSchema}/${DOWNLOADS_VOLUME_DEFAULT}/${service.priceListFile}`;
}

function pricingRawTableFqn(
  catalog: string,
  bronzeSchema: string,
  service: PricingService,
): string {
  return unquotedFqn(catalog, bronzeSchema, service.rawTableName);
}

export async function uploadPricingNotebook(
  wc: WorkspaceClient,
  workspacePath: string,
  notebookName = AWS_PRICING_NOTEBOOK_NAME,
): Promise<string> {
  const content = await readPricingNotebook(notebookName);
  await uploadPipelineFile(wc, workspacePath, content, { format: 'JUPYTER', language: 'PYTHON' });
  return workspacePath;
}

export async function pricingNotebookState(
  db: DatabaseClient,
  env: Env,
  deps: PricingNotebookDeps = {},
): Promise<PricingNotebookListResponse> {
  const pricingRows = await Promise.all(
    PRICING_SERVICES.map((service) => db.repos.pricingData.get(service.provider, service.service)),
  );
  const resolvedRows = await Promise.all(
    pricingRows.map((row) => syncActivePricingRun(env, db, row ?? null, deps)),
  );
  return {
    items: PRICING_SERVICES.map((service, index) =>
      stateFromPricingData(resolvedRows[index] ?? null, service),
    ),
  };
}

export async function pricingNotebookStateById(
  db: DatabaseClient,
  env: Env,
  id: string,
  deps: PricingNotebookDeps = {},
): Promise<PricingNotebookState> {
  const service = pricingServiceById(id);
  const pricingRow = await db.repos.pricingData.get(service.provider, service.service);
  const resolvedRow = await syncActivePricingRun(env, db, pricingRow, deps);
  return stateFromPricingData(resolvedRow, service);
}

interface PricingNotebookDeps {
  workspaceClient?: WorkspaceClient;
  uploadNotebook?: (
    wc: WorkspaceClient,
    workspacePath: string,
    notebookName?: string,
  ) => Promise<string>;
}

export async function deletePricingNotebookData(
  env: Env,
  db: DatabaseClient,
  userToken: string | undefined,
  id: string,
  deps: { executor?: StatementExecutor } = {},
): Promise<PricingNotebookDeleteResult> {
  const service = pricingServiceById(id);
  const pricingData = await db.repos.pricingData.getById(service.id);
  if (!pricingData) {
    throw new DataSourceSetupError(`Pricing data is not configured for ${id}.`, 404);
  }

  if (pricingData.table) {
    if (!userToken && !deps.executor) {
      throw new DataSourceSetupError('OBO access token required', 401);
    }
    const executor = deps.executor ?? buildUserExecutor(env, userToken);
    if (!executor) {
      throw new DataSourceSetupError(
        'OBO access token + DATABRICKS_HOST + SQL_WAREHOUSE_ID required to drop pricing table.',
        400,
      );
    }
    await executor.run(
      `DROP TABLE IF EXISTS ${quoteThreePartTable(pricingData.table)}`,
      [],
      z.unknown(),
    );
  }

  const deletedPricingData = await db.repos.pricingData.deleteById(id);
  return {
    id: service.id,
    table: pricingData.table,
    droppedTable: Boolean(pricingData.table),
    deletedPricingData,
  };
}

async function upsertPricingNotebook(
  env: Env,
  db: DatabaseClient,
  id: string,
  deps: PricingNotebookDeps,
): Promise<PricingData> {
  const service = pricingServiceById(id);
  const [settings, current] = await Promise.all([
    db.repos.appSettings.list().then(settingsToRecord),
    db.repos.pricingData.get(service.provider, service.service),
  ]);
  const catalog = catalogFromSettings(settings);
  if (!catalog) {
    throw new DataSourceSetupError('Main catalog is not configured.', 400);
  }
  const medallion = medallionSchemaNamesFromSettings(settings);
  const targetTable =
    current?.table ?? unquotedFqn(catalog, PRICING_SCHEMA_DEFAULT, service.tableName);
  const rawDataPath =
    current?.rawDataPath ??
    (service.kind === 'aws' ? pricingDownloadFilePath(catalog, medallion.bronze, service) : null);
  const rawDataTable =
    current?.rawDataTable ?? pricingRawTableFqn(catalog, medallion.bronze, service);
  const appName = env.DATABRICKS_APP_NAME?.trim();
  if (!appName) {
    throw new DataSourceSetupError('DATABRICKS_APP_NAME must be configured.', 400);
  }
  const wc = deps.workspaceClient ?? buildAppWorkspaceClient(env);
  if (!wc) {
    throw new DataSourceSetupError(
      'Databricks service principal workspace client is not configured.',
      400,
    );
  }
  if (!targetTable) {
    throw new DataSourceSetupError('Pricing target table could not be resolved.', 500);
  }
  if (service.kind === 'aws' && !rawDataPath) {
    throw new DataSourceSetupError('Raw data path could not be resolved.', 500);
  }
  if (!rawDataTable) {
    throw new DataSourceSetupError('Raw data table could not be resolved.', 500);
  }

  const desiredWorkspacePath = pricingNotebookWorkspacePath(appName, service.notebookName);
  const notebookWorkspacePath = await (deps.uploadNotebook ?? uploadPricingNotebook)(
    wc,
    desiredWorkspacePath,
    service.notebookName,
  );
  const notebookStatus = await wc.workspace.getStatus({ path: notebookWorkspacePath });
  const notebookId =
    notebookStatus.object_id === undefined || notebookStatus.object_id === null
      ? null
      : String(notebookStatus.object_id);
  return db.repos.pricingData.upsert({
    id: service.id,
    provider: service.provider,
    service: service.service,
    table: targetTable,
    rawDataTable,
    rawDataPath,
    notebookPath: notebookWorkspacePath,
    notebookId,
    metadata: {
      ...(current?.metadata ?? {}),
      source: service.source,
    },
    ...runFieldsFromPricingData(current),
  });
}

export async function ensurePricingDataForId(
  env: Env,
  db: DatabaseClient,
  id: string,
  deps: PricingNotebookDeps = {},
): Promise<PricingData> {
  const existing = await db.repos.pricingData.getById(id);
  if (isRunnablePricingData(existing)) return existing;

  const pricingData = await upsertPricingNotebook(env, db, id, deps);
  if (isRunnablePricingData(pricingData)) return pricingData;
  throw new DataSourceSetupError('Pricing metadata could not be prepared.', 500);
}

function isRunnablePricingData(pricingData: PricingData | null): pricingData is PricingData {
  if (!pricingData?.notebookPath || !pricingData.table) {
    return false;
  }
  const service = PRICING_SERVICES.find((candidate) => candidate.id === pricingData.id);
  if (!service) return false;
  return Boolean(
    pricingData.notebookPath.endsWith(`/${service.notebookName}`) &&
    (service.kind !== 'aws' || (pricingData.rawDataPath && pricingData.rawDataTable)) &&
    typeof pricingData.metadata.source === 'string',
  );
}

function stateFromPricingData(
  pricingData: Awaited<ReturnType<DatabaseClient['repos']['pricingData']['get']>>,
  service: PricingService,
): PricingNotebookState {
  return {
    id: pricingData?.id ?? service.id,
    provider: pricingData?.provider ?? service.provider,
    service: pricingData?.service ?? service.service,
    table: pricingData?.table ?? null,
    rawDataTable: pricingData?.rawDataTable ?? null,
    rawDataPath: pricingData?.rawDataPath ?? null,
    notebookWorkspacePath: pricingData?.notebookPath ?? null,
    notebookId: pricingData?.notebookId ?? null,
    metadata: pricingData?.metadata ?? { source: service.source },
    ...runFieldsFromPricingData(pricingData ?? null),
  };
}

async function syncActivePricingRun(
  env: Env,
  db: DatabaseClient,
  row: PricingData | null,
  deps: PricingNotebookDeps,
): Promise<PricingData | null> {
  if (!row?.runId || !isActivePricingRunStatus(row.runStatus)) return row;
  const wc = deps.workspaceClient ?? buildAppWorkspaceClient(env);
  if (!wc) return row;
  const snapshot = await getDatabricksRunSnapshot(wc, env, row.runId);
  if (!snapshot) return row;
  return (
    (await db.repos.pricingData.updateRun(row.id, {
      runId: snapshot.runId,
      runStatus: snapshot.runStatus,
      runUrl: snapshot.runUrl,
      runStartedAt: snapshot.runStartedAt,
      runFinishedAt: snapshot.runFinishedAt,
      runCheckedAt: snapshot.runCheckedAt,
    })) ?? row
  );
}

function runFieldsFromPricingData(pricingData: PricingData | null) {
  return {
    runId: pricingData?.runId ?? null,
    runStatus: pricingData?.runStatus ?? 'not_started',
    runUrl: pricingData?.runUrl ?? null,
    runStartedAt: pricingData?.runStartedAt ?? null,
    runFinishedAt: pricingData?.runFinishedAt ?? null,
    runCheckedAt: pricingData?.runCheckedAt ?? null,
  };
}

function quoteThreePartTable(value: string): string {
  const parts = value.split('.');
  if (parts.length !== 3) {
    throw new DataSourceSetupError(
      `Invalid pricing table "${value}": expected a three-part table name.`,
      500,
    );
  }
  return parts.map((part) => quoteIdent(part)).join('.');
}
