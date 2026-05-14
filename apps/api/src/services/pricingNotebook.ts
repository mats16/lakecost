import { readFile } from 'node:fs/promises';
import { settingsToRecord, type DatabaseClient } from '@finlake/db';
import {
  AWS_EC2_PRICING_TABLE_DEFAULT,
  AWS_RDS_PRICING_TABLE_DEFAULT,
  CATALOG_SETTING_KEY,
  DOWNLOADS_VOLUME_DEFAULT,
  PRICING_SCHEMA_DEFAULT,
  medallionSchemaNamesFromSettings,
  unquotedFqn,
  type AwsPricingSlug,
  type Env,
  type PricingData,
  type PricingNotebookListResponse,
  type PricingNotebookSetupResult,
  type PricingNotebookState,
} from '@finlake/shared';
import { DataSourceSetupError } from './dataSourceErrors.js';
import { uploadPipelineFile } from './databricksJobs.js';
import { buildUserWorkspaceClient, type WorkspaceClient } from './statementExecution.js';

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
): Promise<PricingNotebookListResponse> {
  const [settingsRows, pricingRows] = await Promise.all([
    db.repos.appSettings.list(),
    Promise.all(
      AWS_PRICING_SERVICES.map((service) =>
        db.repos.pricingData.get(AWS_PROVIDER, service.service),
      ),
    ),
  ]);
  const settings = settingsToRecord(settingsRows);
  return {
    items: AWS_PRICING_SERVICES.map((service, index) =>
      stateFromSettings(settings, pricingRows[index] ?? null, service),
    ),
  };
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
  if (!state.table) {
    throw new DataSourceSetupError('Pricing target table could not be resolved.', 500);
  }
  if (!state.rawDataPath) {
    throw new DataSourceSetupError('Raw data path could not be resolved.', 500);
  }
  if (!state.rawDataTable) {
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
    table: state.table,
    rawDataTable: state.rawDataTable,
    rawDataPath: state.rawDataPath,
    notebookPath: notebookWorkspacePath,
    notebookId,
    metadata: {
      ...state.metadata,
      source: service.source,
    },
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
  const medallion = medallionSchemaNamesFromSettings(settings);
  const defaultTable = catalog
    ? unquotedFqn(catalog, PRICING_SCHEMA_DEFAULT, service.tableName)
    : null;
  return {
    provider: pricingData?.provider ?? AWS_PROVIDER,
    service: pricingData?.service ?? service.service,
    slug: pricingData?.slug ?? service.slug,
    catalog,
    table: pricingData?.table ?? defaultTable,
    rawDataTable:
      pricingData?.rawDataTable ??
      (catalog ? pricingRawTableFqn(catalog, medallion.bronze, service) : null),
    rawDataPath:
      pricingData?.rawDataPath ??
      (catalog ? pricingDownloadFilePath(catalog, medallion.bronze, service) : null),
    notebookWorkspacePath: pricingData?.notebookPath ?? null,
    notebookId: pricingData?.notebookId ?? null,
    metadata: pricingData?.metadata ?? { source: service.source },
  };
}
