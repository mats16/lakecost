import { readFile } from 'node:fs/promises';
import { settingsToRecord, type DatabaseClient } from '@finlake/db';
import {
  AWS_EC2_PRICING_TABLE_DEFAULT,
  CATALOG_SETTING_KEY,
  DOWNLOADS_VOLUME_DEFAULT,
  PRICING_SCHEMA_DEFAULT,
  medallionSchemaNamesFromSettings,
  unquotedFqn,
  type Env,
  type PricingNotebookSetupResult,
  type PricingNotebookState,
} from '@finlake/shared';
import { DataSourceSetupError } from './dataSourceErrors.js';
import { uploadPipelineFile } from './databricksJobs.js';
import { buildUserWorkspaceClient, type WorkspaceClient } from './statementExecution.js';

const PRICING_NOTEBOOK_NAME = 'pricing_ingest_aws_ec2.ipynb';
const AWS_PROVIDER = 'AWS';
const AWS_EC2_SERVICE = 'AmazonEC2';
const AWS_EC2_SLUG = 'aws_ec2';
const AWS_EC2_PRICE_LIST_FILE = 'pricing_aws_ec2.csv';
const AWS_EC2_PRICE_LIST_SOURCE =
  'https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonEC2/current/index.csv';

const PRICING_NOTEBOOK_SOURCE_URL = new URL(
  '../notebooks/pricing_ingest_aws_ec2.ipynb',
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

function pricingDownloadFilePath(catalog: string, bronzeSchema: string): string {
  return `/Volumes/${catalog}/${bronzeSchema}/${DOWNLOADS_VOLUME_DEFAULT}/${AWS_EC2_PRICE_LIST_FILE}`;
}

function pricingRawTableFqn(catalog: string, bronzeSchema: string): string {
  return unquotedFqn(catalog, bronzeSchema, 'pricing_aws_ec2');
}

export async function uploadPricingNotebook(
  wc: WorkspaceClient,
  workspacePath: string,
): Promise<string> {
  const content = await readPricingNotebook();
  await uploadPipelineFile(wc, workspacePath, content, { format: 'JUPYTER', language: 'PYTHON' });
  return workspacePath;
}

export async function pricingNotebookState(db: DatabaseClient): Promise<PricingNotebookState> {
  const [settings, pricingData] = await Promise.all([
    db.repos.appSettings.list().then(settingsToRecord),
    db.repos.pricingData.get(AWS_PROVIDER, AWS_EC2_SERVICE),
  ]);
  return stateFromSettings(settings, pricingData);
}

interface PricingNotebookDeps {
  workspaceClient?: WorkspaceClient;
  uploadNotebook?: (wc: WorkspaceClient, workspacePath: string) => Promise<string>;
}

export async function setupPricingNotebook(
  env: Env,
  db: DatabaseClient,
  userToken: string | undefined,
  deps: PricingNotebookDeps = {},
): Promise<PricingNotebookSetupResult> {
  return setupPricingNotebookWithDeps(env, db, userToken, deps);
}

export async function setupPricingNotebookWithDeps(
  env: Env,
  db: DatabaseClient,
  userToken: string | undefined,
  deps: PricingNotebookDeps = {},
): Promise<PricingNotebookSetupResult> {
  const [settings, current] = await Promise.all([
    db.repos.appSettings.list().then(settingsToRecord),
    db.repos.pricingData.get(AWS_PROVIDER, AWS_EC2_SERVICE),
  ]);
  const state = stateFromSettings(settings, current);
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
    service: AWS_EC2_SERVICE,
    slug: AWS_EC2_SLUG,
    table: state.table,
    rawDataTable: state.rawDataTable,
    rawDataPath: state.rawDataPath,
    notebookPath: notebookWorkspacePath,
    notebookId,
    metadata: {
      ...state.metadata,
      source: AWS_EC2_PRICE_LIST_SOURCE,
    },
  });

  return {
    ...stateFromSettings(settings, pricingData),
    notebookWorkspacePath,
    warnings: [],
  };
}

function stateFromSettings(
  settings: Record<string, string | undefined>,
  pricingData: Awaited<ReturnType<DatabaseClient['repos']['pricingData']['get']>>,
): PricingNotebookState {
  const catalog = settings[CATALOG_SETTING_KEY]?.trim() || null;
  const medallion = medallionSchemaNamesFromSettings(settings);
  const defaultTable = catalog
    ? unquotedFqn(catalog, PRICING_SCHEMA_DEFAULT, AWS_EC2_PRICING_TABLE_DEFAULT)
    : null;
  return {
    provider: pricingData?.provider ?? AWS_PROVIDER,
    service: pricingData?.service ?? AWS_EC2_SERVICE,
    slug: pricingData?.slug ?? AWS_EC2_SLUG,
    catalog,
    table: pricingData?.table ?? defaultTable,
    rawDataTable:
      pricingData?.rawDataTable ?? (catalog ? pricingRawTableFqn(catalog, medallion.bronze) : null),
    rawDataPath:
      pricingData?.rawDataPath ??
      (catalog ? pricingDownloadFilePath(catalog, medallion.bronze) : null),
    notebookWorkspacePath: pricingData?.notebookPath ?? null,
    notebookId: pricingData?.notebookId ?? null,
    metadata: pricingData?.metadata ?? { source: AWS_EC2_PRICE_LIST_SOURCE },
  };
}
