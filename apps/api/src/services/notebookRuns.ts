import type { DatabaseClient } from '@finlake/db';
import type {
  DatabricksRunLinkResult,
  Env,
  PricingData,
  PricingNotebookRunResult,
} from '@finlake/shared';
import { getDatabricksRunSnapshot } from './databricksRunStatus.js';
import { DataSourceSetupError } from './dataSourceErrors.js';
import { ensurePricingDataForSlug } from './pricingNotebook.js';
import {
  buildAppWorkspaceClient,
  buildUserWorkspaceClient,
  type WorkspaceClient,
} from './statementExecution.js';

interface NotebookRunDeps {
  workspaceClient?: WorkspaceClient;
  setupWorkspaceClient?: WorkspaceClient;
}

interface JobsSubmitResponse {
  run_id?: number;
  run_page_url?: string;
}

const PRICING_SERVERLESS_ENVIRONMENT_KEY = 'pricing_serverless';
const PRICING_SERVERLESS_ENVIRONMENT_VERSION = '4';

export async function submitManagedNotebookRunBySlug(
  env: Env,
  db: DatabaseClient,
  userToken: string | undefined,
  slug: string,
  deps: NotebookRunDeps = {},
): Promise<PricingNotebookRunResult> {
  const wc = deps.workspaceClient ?? buildAppWorkspaceClient(env);
  if (!wc) {
    throw new DataSourceSetupError(
      'Databricks service principal workspace client is not configured.',
      400,
    );
  }
  const pricingData = await ensureRunnablePricingData(env, db, userToken, slug, deps);
  return submitPricingNotebookRun(db, wc, pricingData);
}

async function submitPricingNotebookRun(
  db: DatabaseClient,
  wc: WorkspaceClient,
  pricingData: PricingData,
): Promise<PricingNotebookRunResult> {
  if (!pricingData.notebookPath) {
    throw new DataSourceSetupError('Notebook path is not configured.', 400);
  }
  if (!pricingData.rawDataPath) {
    throw new DataSourceSetupError('Notebook volume_path parameter is not configured.', 400);
  }
  if (!pricingData.rawDataTable) {
    throw new DataSourceSetupError('Notebook raw_table parameter is not configured.', 400);
  }

  const sourceUrl =
    typeof pricingData.metadata.source === 'string' ? pricingData.metadata.source : null;
  if (!sourceUrl) {
    throw new DataSourceSetupError('Notebook source_url parameter is not configured.', 400);
  }

  let response: JobsSubmitResponse;
  try {
    response = (await wc.apiClient.request({
      path: '/api/2.2/jobs/runs/submit',
      method: 'POST',
      headers: new Headers({
        Accept: 'application/json',
        'Content-Type': 'application/json',
      }),
      raw: false,
      payload: {
        run_name: `${pricingData.slug}-${Date.now()}`,
        performance_target: 'PERFORMANCE_OPTIMIZED',
        environments: [
          {
            environment_key: PRICING_SERVERLESS_ENVIRONMENT_KEY,
            spec: {
              environment_version: PRICING_SERVERLESS_ENVIRONMENT_VERSION,
            },
          },
        ],
        tasks: [
          {
            task_key: safeTaskKey(pricingData.slug),
            environment_key: PRICING_SERVERLESS_ENVIRONMENT_KEY,
            notebook_task: {
              notebook_path: pricingData.notebookPath,
              source: 'WORKSPACE',
              base_parameters: {
                source_url: sourceUrl,
                volume_path: pricingData.rawDataPath,
                raw_table: pricingData.rawDataTable,
                target_table: pricingData.table,
                aws_service_code: pricingData.service,
              },
            },
          },
        ],
      },
    })) as JobsSubmitResponse;
  } catch (err) {
    throw new DataSourceSetupError(`Failed to submit notebook run: ${(err as Error).message}`, 500);
  }

  if (typeof response.run_id !== 'number') {
    throw new DataSourceSetupError('Databricks Jobs API returned no run_id.', 500);
  }

  const runUrl = response.run_page_url ?? null;
  await db.repos.pricingData.updateRun(pricingData.slug, {
    runId: response.run_id,
    runStatus: 'pending',
    runUrl,
    runStartedAt: null,
    runFinishedAt: null,
    runCheckedAt: new Date().toISOString(),
  });

  return {
    provider: pricingData.provider,
    service: pricingData.service,
    slug: pricingData.slug,
    runId: response.run_id,
    runStatus: 'pending',
    runUrl,
  };
}

export async function getDatabricksRunLink(
  env: Env,
  runId: number,
  deps: NotebookRunDeps = {},
): Promise<DatabricksRunLinkResult> {
  const wc = deps.workspaceClient ?? buildAppWorkspaceClient(env);
  if (!wc) {
    throw new DataSourceSetupError(
      'Databricks service principal workspace client is not configured.',
      400,
    );
  }

  const run = await getDatabricksRunSnapshot(wc, env, runId);
  return {
    jobId: run?.jobId ?? null,
    runId,
    runUrl: run?.runUrl ?? null,
  };
}

function safeTaskKey(slug: string): string {
  return slug.replace(/[^A-Za-z0-9_-]/g, '_') || 'notebook';
}

async function ensureRunnablePricingData(
  env: Env,
  db: DatabaseClient,
  userToken: string | undefined,
  slug: string,
  deps: NotebookRunDeps,
): Promise<PricingData> {
  const existing = await db.repos.pricingData.getBySlug(slug);
  if (
    existing?.notebookPath &&
    existing.rawDataPath &&
    existing.rawDataTable &&
    existing.table &&
    typeof existing.metadata.source === 'string'
  ) {
    return existing;
  }

  if (!userToken) {
    throw new DataSourceSetupError('OBO access token required to prepare pricing notebook.', 401);
  }

  return ensurePricingDataForSlug(env, db, userToken, slug, {
    workspaceClient:
      deps.setupWorkspaceClient ?? deps.workspaceClient ?? buildUserWorkspaceClient(env, userToken),
  });
}
