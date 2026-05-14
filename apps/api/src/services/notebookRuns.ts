import type { DatabaseClient } from '@finlake/db';
import type { Env, PricingNotebookRunResult } from '@finlake/shared';
import { DataSourceSetupError } from './dataSourceErrors.js';
import { normalizeHost } from './normalizeHost.js';
import { buildUserWorkspaceClient, type WorkspaceClient } from './statementExecution.js';

interface NotebookRunDeps {
  workspaceClient?: WorkspaceClient;
}

interface JobsSubmitResponse {
  run_id?: number;
}

interface JobsRunGetResponse {
  job_id?: number;
  run_page_url?: string;
}

const PRICING_SERVERLESS_ENVIRONMENT_KEY = 'pricing_serverless';
const PRICING_SERVERLESS_ENVIRONMENT_VERSION = '4';

export async function runManagedNotebookById(
  env: Env,
  db: DatabaseClient,
  userToken: string | undefined,
  notebookId: string,
  deps: NotebookRunDeps = {},
): Promise<PricingNotebookRunResult> {
  const trimmedNotebookId = notebookId.trim();
  if (!trimmedNotebookId) {
    throw new DataSourceSetupError('Notebook ID is required.', 400);
  }
  if (!userToken) {
    throw new DataSourceSetupError('OBO access token required', 401);
  }

  const pricingData = await db.repos.pricingData.getByNotebookId(trimmedNotebookId);
  if (!pricingData) {
    throw new DataSourceSetupError('Managed notebook not found.', 404);
  }
  if (!pricingData.notebookPath) {
    throw new DataSourceSetupError('Notebook path is not configured.', 400);
  }

  if (!pricingData.rawDataPath) {
    throw new DataSourceSetupError('Notebook volume_path parameter is not configured.', 400);
  }
  if (!pricingData.rawDataTable) {
    throw new DataSourceSetupError('Notebook raw_table parameter is not configured.', 400);
  }

  const wc = deps.workspaceClient ?? buildUserWorkspaceClient(env, userToken);
  if (!wc) {
    throw new DataSourceSetupError('DATABRICKS_HOST must be configured.', 400);
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
                volume_path: pricingData.rawDataPath,
                raw_table: pricingData.rawDataTable,
                target_table: pricingData.table,
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

  const run = await getRunInfo(wc, response.run_id);

  return {
    provider: pricingData.provider,
    service: pricingData.service,
    slug: pricingData.slug,
    jobId: run?.job_id ?? null,
    runId: response.run_id,
    runUrl:
      run?.run_page_url ?? databricksRunUrl(env.DATABRICKS_HOST, run?.job_id, response.run_id),
  };
}

function safeTaskKey(slug: string): string {
  return slug.replace(/[^A-Za-z0-9_-]/g, '_') || 'notebook';
}

async function getRunInfo(wc: WorkspaceClient, runId: number): Promise<JobsRunGetResponse | null> {
  try {
    return (await wc.apiClient.request({
      path: '/api/2.2/jobs/runs/get',
      method: 'GET',
      headers: new Headers({
        Accept: 'application/json',
      }),
      query: { run_id: runId },
      raw: false,
    })) as JobsRunGetResponse;
  } catch {
    return null;
  }
}

function databricksRunUrl(
  host: string | undefined,
  jobId: number | undefined,
  runId: number,
): string | null {
  const consoleHost = normalizeHost(host);
  if (!consoleHost || typeof jobId !== 'number') return null;
  return `${consoleHost}/jobs/${jobId}/runs/${runId}`;
}
