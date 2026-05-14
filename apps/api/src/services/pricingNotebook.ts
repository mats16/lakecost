import { readFile } from 'node:fs/promises';
import { settingsToRecord, type DatabaseClient } from '@finlake/db';
import {
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
  type Env,
  type PricingData,
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

const PRICING_NOTEBOOK_NAME = 'pricing_ingest_aws.ipynb';
const AWS_PROVIDER = 'AWS';

interface AwsPricingService {
  service: string;
  id: AwsPricingId;
  tableName: string;
  rawTableName: string;
  priceListFile: string;
  source: string;
}

function defineAwsPricingService(
  service: string,
  id: AwsPricingId,
  tableName: string,
): AwsPricingService {
  return {
    service,
    id,
    tableName,
    rawTableName: `pricing_${id}`,
    priceListFile: `pricing_${id}.csv`,
    source: `https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/${service}/current/index.csv`,
  };
}

const AWS_PRICING_SERVICES: readonly AwsPricingService[] = [
  defineAwsPricingService('AmazonEC2', 'aws_ec2', AWS_EC2_PRICING_TABLE_DEFAULT),
  defineAwsPricingService('AmazonRDS', 'aws_rds', AWS_RDS_PRICING_TABLE_DEFAULT),
];

const PRICING_NOTEBOOK_SOURCE_URL = new URL(
  '../notebooks/pricing_ingest_aws.ipynb',
  import.meta.url,
);

let cachedNotebookContent: string | null = null;
async function readPricingNotebook(): Promise<string> {
  cachedNotebookContent ??= await readFile(PRICING_NOTEBOOK_SOURCE_URL, 'utf8');
  return cachedNotebookContent;
}

export function pricingNotebookWorkspacePath(appName: string): string {
  return `/Workspace/Shared/${appName}/pricing/${PRICING_NOTEBOOK_NAME}`;
}

export function awsPricingServiceById(id: string): AwsPricingService {
  const service = AWS_PRICING_SERVICES.find((candidate) => candidate.id === id);
  if (!service) {
    throw new DataSourceSetupError(`Unsupported pricing service id: ${id}`, 400);
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
  service: AwsPricingService,
): string {
  return unquotedFqn(catalog, bronzeSchema, service.rawTableName);
}

export async function uploadPricingNotebook(
  wc: WorkspaceClient,
  workspacePath: string,
): Promise<string> {
  const content = await readPricingNotebook();
  await uploadPipelineFile(wc, workspacePath, content, { format: 'JUPYTER', language: 'PYTHON' });
  return workspacePath;
}

export async function pricingNotebookState(
  db: DatabaseClient,
  env: Env,
  deps: PricingNotebookDeps = {},
): Promise<PricingNotebookListResponse> {
  const pricingRows = await Promise.all(
    AWS_PRICING_SERVICES.map((service) => db.repos.pricingData.get(AWS_PROVIDER, service.service)),
  );
  const resolvedRows = await Promise.all(
    pricingRows.map((row) => syncActivePricingRun(env, db, row ?? null, deps)),
  );
  return {
    items: AWS_PRICING_SERVICES.map((service, index) =>
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
  const service = awsPricingServiceById(id);
  const pricingRow = await db.repos.pricingData.get(AWS_PROVIDER, service.service);
  const resolvedRow = await syncActivePricingRun(env, db, pricingRow, deps);
  return stateFromPricingData(resolvedRow, service);
}

interface PricingNotebookDeps {
  workspaceClient?: WorkspaceClient;
  uploadNotebook?: (wc: WorkspaceClient, workspacePath: string) => Promise<string>;
}

export async function deletePricingNotebookData(
  env: Env,
  db: DatabaseClient,
  userToken: string | undefined,
  id: string,
  deps: { executor?: StatementExecutor } = {},
): Promise<PricingNotebookDeleteResult> {
  const service = awsPricingServiceById(id);
  const pricingData = await db.repos.pricingData.get(AWS_PROVIDER, service.service);
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
  const service = awsPricingServiceById(id);
  const [settings, current] = await Promise.all([
    db.repos.appSettings.list().then(settingsToRecord),
    db.repos.pricingData.get(AWS_PROVIDER, service.service),
  ]);
  const catalog = catalogFromSettings(settings);
  if (!catalog) {
    throw new DataSourceSetupError('Main catalog is not configured.', 400);
  }
  const medallion = medallionSchemaNamesFromSettings(settings);
  const targetTable =
    current?.table ?? unquotedFqn(catalog, PRICING_SCHEMA_DEFAULT, service.tableName);
  const rawDataPath =
    current?.rawDataPath ?? pricingDownloadFilePath(catalog, medallion.bronze, service);
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
  if (!rawDataPath) {
    throw new DataSourceSetupError('Raw data path could not be resolved.', 500);
  }
  if (!rawDataTable) {
    throw new DataSourceSetupError('Raw data table could not be resolved.', 500);
  }

  const desiredWorkspacePath = pricingNotebookWorkspacePath(appName);
  const notebookWorkspacePath = await (deps.uploadNotebook ?? uploadPricingNotebook)(
    wc,
    desiredWorkspacePath,
  );
  const notebookStatus = await wc.workspace.getStatus({ path: notebookWorkspacePath });
  const notebookId =
    notebookStatus.object_id === undefined || notebookStatus.object_id === null
      ? null
      : String(notebookStatus.object_id);
  return db.repos.pricingData.upsert({
    id: service.id,
    provider: AWS_PROVIDER,
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
  return Boolean(
    pricingData?.notebookPath &&
    pricingData.notebookPath.endsWith(`/${PRICING_NOTEBOOK_NAME}`) &&
    pricingData.rawDataPath &&
    pricingData.rawDataTable &&
    pricingData.table &&
    typeof pricingData.metadata.source === 'string',
  );
}

function stateFromPricingData(
  pricingData: Awaited<ReturnType<DatabaseClient['repos']['pricingData']['get']>>,
  service: AwsPricingService,
): PricingNotebookState {
  return {
    id: pricingData?.id ?? service.id,
    provider: pricingData?.provider ?? AWS_PROVIDER,
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
