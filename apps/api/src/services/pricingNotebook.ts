import { readFile } from 'node:fs/promises';
import { settingsToRecord, type DatabaseClient } from '@finlake/db';
import {
  AWS_EC2_PRICING_TABLE_DEFAULT,
  AWS_RDS_PRICING_TABLE_DEFAULT,
  CATALOG_SETTING_KEY,
  DOWNLOADS_VOLUME_DEFAULT,
  PRICING_SCHEMA_DEFAULT,
  isActivePricingRunStatus,
  medallionSchemaNamesFromSettings,
  quoteIdent,
  unquotedFqn,
  type AwsPricingSlug,
  type Env,
  type PricingData,
  type PricingNotebookDeleteResult,
  type PricingNotebookListResponse,
  type PricingNotebookSetupResult,
  type PricingNotebookState,
} from '@finlake/shared';
import { getDatabricksRunSnapshot } from './databricksRunStatus.js';
import { DataSourceSetupError } from './dataSourceErrors.js';
import { uploadPipelineFile } from './databricksJobs.js';
import {
  buildAppWorkspaceClient,
  buildAppExecutor,
  buildUserWorkspaceClient,
  type StatementExecutor,
  type WorkspaceClient,
} from './statementExecution.js';
import { z } from 'zod';

const PRICING_NOTEBOOK_NAME = 'pricing_ingest_aws.ipynb';
const AWS_PROVIDER = 'AWS';

interface AwsPricingService {
  service: string;
  slug: AwsPricingSlug;
  tableName: string;
  rawTableName: string;
  priceListFile: string;
  source: string;
}

function defineAwsPricingService(
  service: string,
  slug: AwsPricingSlug,
  tableName: string,
): AwsPricingService {
  return {
    service,
    slug,
    tableName,
    rawTableName: `pricing_${slug}`,
    priceListFile: `pricing_${slug}.csv`,
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
  return `/Workspace/Shared/${appName}/${PRICING_NOTEBOOK_NAME}`;
}

export function awsPricingServiceBySlug(slug: string): AwsPricingService {
  const service = AWS_PRICING_SERVICES.find((candidate) => candidate.slug === slug);
  if (!service) {
    throw new DataSourceSetupError(`Unsupported pricing service slug: ${slug}`, 400);
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
  const [settingsRows, pricingRows] = await Promise.all([
    db.repos.appSettings.list(),
    Promise.all(
      AWS_PRICING_SERVICES.map((service) =>
        db.repos.pricingData.get(AWS_PROVIDER, service.service),
      ),
    ),
  ]);
  const resolvedRows = await Promise.all(
    pricingRows.map((row) => syncActivePricingRun(env, db, row ?? null, deps)),
  );
  const settings = settingsToRecord(settingsRows);
  return {
    items: AWS_PRICING_SERVICES.map((service, index) =>
      stateFromSettings(settings, resolvedRows[index] ?? null, service),
    ),
  };
}

export async function pricingNotebookStateBySlug(
  db: DatabaseClient,
  env: Env,
  slug: string,
  deps: PricingNotebookDeps = {},
): Promise<PricingNotebookState> {
  const service = awsPricingServiceBySlug(slug);
  const [settingsRows, pricingRow] = await Promise.all([
    db.repos.appSettings.list(),
    db.repos.pricingData.get(AWS_PROVIDER, service.service),
  ]);
  const resolvedRow = await syncActivePricingRun(env, db, pricingRow, deps);
  return stateFromSettings(settingsToRecord(settingsRows), resolvedRow, service);
}

interface PricingNotebookDeps {
  workspaceClient?: WorkspaceClient;
  uploadNotebook?: (wc: WorkspaceClient, workspacePath: string) => Promise<string>;
}

export async function setupPricingNotebook(
  env: Env,
  db: DatabaseClient,
  userToken: string | undefined,
  slug: string,
  deps: PricingNotebookDeps = {},
): Promise<PricingNotebookSetupResult> {
  return setupPricingNotebookWithDeps(env, db, userToken, slug, deps);
}

export async function setupPricingNotebookWithDeps(
  env: Env,
  db: DatabaseClient,
  userToken: string | undefined,
  slug: string,
  deps: PricingNotebookDeps = {},
): Promise<PricingNotebookSetupResult> {
  const { settings, service, pricingData, notebookWorkspacePath } = await upsertPricingNotebook(
    env,
    db,
    userToken,
    slug,
    deps,
  );
  return {
    ...stateFromSettings(settings, pricingData, service),
    notebookWorkspacePath,
    warnings: [],
  };
}

export async function deletePricingNotebookData(
  env: Env,
  db: DatabaseClient,
  slug: string,
  deps: { executor?: StatementExecutor } = {},
): Promise<PricingNotebookDeleteResult> {
  const service = awsPricingServiceBySlug(slug);
  const pricingData = await db.repos.pricingData.get(AWS_PROVIDER, service.service);
  if (!pricingData) {
    throw new DataSourceSetupError(`Pricing data is not configured for ${slug}.`, 404);
  }

  if (pricingData.table) {
    const executor = deps.executor ?? buildAppExecutor(env);
    if (!executor) {
      throw new DataSourceSetupError(
        'Databricks service principal SQL executor is not configured.',
        400,
      );
    }
    await executor.run(
      `DROP TABLE IF EXISTS ${quoteThreePartTable(pricingData.table)}`,
      [],
      z.unknown(),
    );
  }

  const deletedPricingData = await db.repos.pricingData.deleteBySlug(slug);
  return {
    slug: service.slug,
    table: pricingData.table,
    droppedTable: Boolean(pricingData.table),
    deletedPricingData,
  };
}

interface UpsertPricingNotebookResult {
  settings: Record<string, string | undefined>;
  service: AwsPricingService;
  pricingData: PricingData;
  notebookWorkspacePath: string;
}

async function upsertPricingNotebook(
  env: Env,
  db: DatabaseClient,
  userToken: string | undefined,
  slug: string,
  deps: PricingNotebookDeps,
): Promise<UpsertPricingNotebookResult> {
  const service = awsPricingServiceBySlug(slug);
  const [settings, current] = await Promise.all([
    db.repos.appSettings.list().then(settingsToRecord),
    db.repos.pricingData.get(AWS_PROVIDER, service.service),
  ]);
  const state = stateFromSettings(settings, current, service);
  if (!state.catalog) {
    throw new DataSourceSetupError('Main catalog is not configured.', 400);
  }
  const medallion = medallionSchemaNamesFromSettings(settings);
  const targetTable =
    current?.table ?? unquotedFqn(state.catalog, PRICING_SCHEMA_DEFAULT, service.tableName);
  const rawDataPath =
    current?.rawDataPath ?? pricingDownloadFilePath(state.catalog, medallion.bronze, service);
  const rawDataTable =
    current?.rawDataTable ?? pricingRawTableFqn(state.catalog, medallion.bronze, service);
  if (!userToken) {
    throw new DataSourceSetupError('OBO access token required', 401);
  }
  const appName = env.DATABRICKS_APP_NAME?.trim();
  if (!appName) {
    throw new DataSourceSetupError('DATABRICKS_APP_NAME must be configured.', 400);
  }
  const wc = deps.workspaceClient ?? buildUserWorkspaceClient(env, userToken);
  if (!wc) {
    throw new DataSourceSetupError('DATABRICKS_HOST must be configured.', 400);
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
  const pricingData = await db.repos.pricingData.upsert({
    provider: AWS_PROVIDER,
    service: service.service,
    slug: service.slug,
    table: targetTable,
    rawDataTable,
    rawDataPath,
    notebookPath: notebookWorkspacePath,
    notebookId,
    metadata: {
      ...state.metadata,
      source: service.source,
    },
    ...runFieldsFromPricingData(current),
  });

  return { settings, service, pricingData, notebookWorkspacePath };
}

export async function ensurePricingDataForSlug(
  env: Env,
  db: DatabaseClient,
  userToken: string | undefined,
  slug: string,
  deps: PricingNotebookDeps = {},
): Promise<PricingData> {
  const existing = await db.repos.pricingData.getBySlug(slug);
  if (isRunnablePricingData(existing)) return existing;

  const { pricingData } = await upsertPricingNotebook(env, db, userToken, slug, deps);
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

function stateFromSettings(
  settings: Record<string, string | undefined>,
  pricingData: Awaited<ReturnType<DatabaseClient['repos']['pricingData']['get']>>,
  service: AwsPricingService,
): PricingNotebookState {
  const catalog = settings[CATALOG_SETTING_KEY]?.trim() || null;
  return {
    provider: pricingData?.provider ?? AWS_PROVIDER,
    service: pricingData?.service ?? service.service,
    slug: pricingData?.slug ?? service.slug,
    catalog,
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
    (await db.repos.pricingData.updateRun(row.slug, {
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
