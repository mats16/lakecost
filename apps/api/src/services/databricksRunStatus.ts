import type { Env, PricingRunStatus } from '@finlake/shared';
import { normalizeHost } from './normalizeHost.js';
import type { WorkspaceClient } from './statementExecution.js';

export interface JobsRunGetResponse {
  job_id?: number;
  run_id?: number;
  run_page_url?: string;
  state?: {
    life_cycle_state?: string;
    result_state?: string;
  };
  start_time?: number;
  end_time?: number;
}

export interface DatabricksRunSnapshot {
  jobId: number | null;
  runId: number;
  runStatus: PricingRunStatus;
  runUrl: string | null;
  runStartedAt: string | null;
  runFinishedAt: string | null;
  runCheckedAt: string;
}

export async function getDatabricksRunSnapshot(
  wc: WorkspaceClient,
  env: Env,
  runId: number,
): Promise<DatabricksRunSnapshot | null> {
  const run = await getRunInfo(wc, runId);
  if (!run) return null;
  return {
    jobId: run.job_id ?? null,
    runId,
    runStatus: databricksRunStatus(run),
    runUrl: run.run_page_url ?? databricksRunUrl(env.DATABRICKS_HOST, run.job_id, runId),
    runStartedAt: millisToIso(run.start_time),
    runFinishedAt: millisToIso(run.end_time),
    runCheckedAt: new Date().toISOString(),
  };
}

export function databricksRunUrl(
  host: string | undefined,
  jobId: number | undefined,
  runId: number,
): string | null {
  const consoleHost = normalizeHost(host);
  if (!consoleHost || typeof jobId !== 'number') return null;
  return `${consoleHost}/jobs/${jobId}/runs/${runId}`;
}

function databricksRunStatus(run: JobsRunGetResponse): PricingRunStatus {
  const lifeCycleState = normalizeState(run.state?.life_cycle_state);
  const resultState = normalizeState(run.state?.result_state);

  if (resultState === 'SUCCESS') return 'succeeded';
  if (resultState === 'CANCELED') return 'canceled';
  if (resultState) return 'failed';

  if (
    lifeCycleState === 'PENDING' ||
    lifeCycleState === 'QUEUED' ||
    lifeCycleState === 'BLOCKED' ||
    lifeCycleState === 'WAITING_FOR_RETRY'
  ) {
    return 'pending';
  }
  if (lifeCycleState === 'RUNNING' || lifeCycleState === 'TERMINATING') return 'running';
  if (lifeCycleState === 'CANCELED') return 'canceled';
  if (
    lifeCycleState === 'TERMINATED' ||
    lifeCycleState === 'INTERNAL_ERROR' ||
    lifeCycleState === 'SKIPPED'
  ) {
    return 'failed';
  }
  return 'unknown';
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

function normalizeState(value: string | undefined): string | null {
  return value?.trim().toUpperCase() || null;
}

function millisToIso(value: number | undefined): string | null {
  if (typeof value !== 'number' || !Number.isFinite(value) || value <= 0) return null;
  return new Date(value).toISOString();
}
