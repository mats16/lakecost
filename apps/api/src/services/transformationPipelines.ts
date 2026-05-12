import { settingsToRecord, type DatabaseClient } from '@finlake/db';
import {
  type Env,
  type TransformationResource,
  type TransformationPipelinesResponse,
} from '@finlake/shared';
import {
  buildAppWorkspaceClient,
  buildUserWorkspaceClient,
  type WorkspaceClient,
} from './statementExecution.js';
import { normalizeHost } from './normalizeHost.js';
import { WorkspaceServiceError } from './workspaceClientErrors.js';
import {
  LEGACY_SHARED_PIPELINE_SETTING_KEYS,
  SHARED_PIPELINE_SETTING_KEYS,
} from './dataSourceSetup.js';

interface SharedPipelineIds {
  jobId: number | null;
  pipelineId: string | null;
}

export class TransformationPipelineAuthError extends WorkspaceServiceError {}

const LOOKBACK_DAYS = 7;
const MAX_HISTORY_ITEMS = 100;

export async function listTransformationPipelines(
  db: DatabaseClient,
  env: Env,
  userToken: string | undefined,
): Promise<TransformationPipelinesResponse> {
  const rawSettings = await db.repos.appSettings.list();
  const appSettings = settingsToRecord(rawSettings);
  const shared = sharedPipelineIds(appSettings);
  const generatedAt = new Date().toISOString();
  const fallbackDays = lastLookbackLocalDays();
  const consoleHost = normalizeHost(env.DATABRICKS_HOST);
  const resources = await listTransformationResources(
    env,
    userToken,
    shared,
    consoleHost,
    fallbackDays,
  );

  return {
    resources,
    generatedAt,
  };
}

async function listTransformationResources(
  env: Env,
  userToken: string | undefined,
  shared: SharedPipelineIds,
  consoleHost: string | null,
  days: string[],
): Promise<TransformationResource[]> {
  const resources: Array<Promise<TransformationResource>> = [];
  if (shared.jobId !== null) {
    resources.push(jobResource(env, userToken, shared, consoleHost, days));
  }
  if (shared.pipelineId) {
    resources.push(pipelineResource(env, userToken, shared, consoleHost, days));
  }
  return Promise.all(resources);
}

async function workspaceClient(env: Env, userToken: string | undefined): Promise<WorkspaceClient> {
  const wc =
    buildAppWorkspaceClient(env) ??
    (userToken ? buildUserWorkspaceClient(env, userToken) : undefined);
  if (!wc) {
    throw new TransformationPipelineAuthError(
      'DATABRICKS_HOST and app credentials or an OBO access token are required to read Databricks Jobs/Pipelines APIs.',
      401,
    );
  }
  return wc;
}

async function jobResource(
  env: Env,
  userToken: string | undefined,
  shared: SharedPipelineIds,
  consoleHost: string | null,
  days: string[],
): Promise<TransformationResource> {
  const jobId = shared.jobId;
  if (jobId === null) {
    throw new Error('jobResource called without a job id');
  }
  const wc = await workspaceClient(env, userToken);
  const fallback = baseResource('job', String(jobId), jobUrl(jobId, consoleHost), days);

  try {
    const [job, runs] = await Promise.all([wc.jobs.get({ job_id: jobId }), listJobRuns(wc, jobId)]);
    const latestRun = runs[0];
    const periodStartTime = millisToIso(latestRun?.start_time);
    const periodEndTime = millisToIso(latestRun?.end_time);
    return {
      ...fallback,
      name: job.settings?.name ?? fallback.name,
      owner: job.run_as_user_name ?? job.creator_user_name ?? latestRun?.creator_user_name ?? null,
      cronExpression: job.settings?.schedule?.quartz_cron_expression ?? null,
      timezoneId: job.settings?.schedule?.timezone_id ?? null,
      createTime: millisToIso(job.created_time),
      updateId: latestRun?.run_id ? String(latestRun.run_id) : null,
      resultState: latestRun ? jobRunResultState(latestRun) : null,
      periodStartTime,
      periodEndTime,
      durationSeconds: jobRunDurationSeconds(latestRun),
      statusDays: statusDays(
        days,
        runs
          .map((run) => ({
            at: run.start_time ?? run.end_time ?? null,
            resultState: jobRunResultState(run),
          }))
          .filter((item): item is StatusItem => item.at !== null),
      ),
    };
  } catch {
    return fallback;
  }
}

async function pipelineResource(
  env: Env,
  userToken: string | undefined,
  shared: SharedPipelineIds,
  consoleHost: string | null,
  days: string[],
): Promise<TransformationResource> {
  const pipelineId = shared.pipelineId;
  if (!pipelineId) {
    throw new Error('pipelineResource called without a pipeline id');
  }
  const wc = await workspaceClient(env, userToken);
  const fallback = baseResource('pipeline', pipelineId, pipelineUrl(pipelineId, consoleHost), days);

  try {
    const [pipeline, updates] = await Promise.all([
      wc.pipelines.get({ pipeline_id: pipelineId }),
      listPipelineUpdates(wc, pipelineId),
    ]);
    const latestUpdate = updates[0];
    const latestTime = latestUpdate?.creation_time;
    return {
      ...fallback,
      name: pipeline.name ?? pipeline.spec?.name ?? fallback.name,
      owner:
        pipeline.run_as_user_name ??
        pipelineRunAsName(pipeline.run_as) ??
        pipeline.creator_user_name ??
        null,
      changeTime: millisToIso(pipeline.last_modified),
      updateId: latestUpdate?.update_id ?? null,
      resultState: latestUpdate?.state ?? pipeline.state ?? null,
      periodStartTime: millisToIso(latestTime),
      statusDays: statusDays(
        days,
        updates.flatMap((update): StatusItem[] =>
          typeof update.creation_time === 'number'
            ? [{ at: update.creation_time, resultState: update.state ?? null }]
            : [],
        ),
      ),
    };
  } catch {
    return fallback;
  }
}

function baseResource(
  resourceType: TransformationResource['resourceType'],
  resourceId: string,
  url: string | null,
  days: string[],
): TransformationResource {
  return {
    resourceType,
    resourceId,
    name: `${resourceType === 'job' ? 'Job' : 'Pipeline'} ${resourceId}`,
    url,
    owner: null,
    cronExpression: null,
    timezoneId: null,
    createTime: null,
    changeTime: null,
    updateId: null,
    resultState: null,
    periodStartTime: null,
    periodEndTime: null,
    durationSeconds: null,
    statusDays: emptyStatusDays(days),
  };
}

type JobRun = Awaited<ReturnType<WorkspaceClient['jobs']['getRun']>>;

async function listJobRuns(wc: WorkspaceClient, jobId: number): Promise<JobRun[]> {
  const runs: JobRun[] = [];
  const startTimeFrom = Date.now() - LOOKBACK_DAYS * 24 * 60 * 60 * 1000;
  for await (const run of wc.jobs.listRuns({
    job_id: jobId,
    start_time_from: startTimeFrom,
    limit: 24,
    expand_tasks: false,
  })) {
    runs.push(run);
    if (runs.length >= MAX_HISTORY_ITEMS) break;
  }
  return runs.sort((a, b) => (b.start_time ?? b.end_time ?? 0) - (a.start_time ?? a.end_time ?? 0));
}

type PipelineUpdate = NonNullable<
  Awaited<ReturnType<WorkspaceClient['pipelines']['listUpdates']>>['updates']
>[number];

async function listPipelineUpdates(
  wc: WorkspaceClient,
  pipelineId: string,
): Promise<PipelineUpdate[]> {
  const updates: PipelineUpdate[] = [];
  const startTimeFrom = Date.now() - LOOKBACK_DAYS * 24 * 60 * 60 * 1000;
  let pageToken: string | undefined;

  do {
    const response = await wc.pipelines.listUpdates({
      pipeline_id: pipelineId,
      max_results: 25,
      page_token: pageToken,
    });
    const page = response.updates ?? [];
    updates.push(...page.filter((update) => (update.creation_time ?? 0) >= startTimeFrom));
    pageToken = response.next_page_token;
    if (page.length > 0 && page.every((update) => (update.creation_time ?? 0) < startTimeFrom)) {
      break;
    }
  } while (pageToken && updates.length < MAX_HISTORY_ITEMS);

  return updates
    .slice(0, MAX_HISTORY_ITEMS)
    .sort((a, b) => (b.creation_time ?? 0) - (a.creation_time ?? 0));
}

interface StatusItem {
  at: number;
  resultState: string | null;
}

function emptyStatusDays(days: string[]): TransformationResource['statusDays'] {
  return days.map((date) => ({
    date,
    resultState: null,
    updateCount: 0,
  }));
}

function statusDays(days: string[], items: StatusItem[]): TransformationResource['statusDays'] {
  const byDate = new Map<
    string,
    { resultState: string | null; updateCount: number; latestAt: number }
  >();
  for (const item of items) {
    const date = new Date(item.at).toISOString().slice(0, 10);
    const current = byDate.get(date);
    if (!current) {
      byDate.set(date, {
        resultState: item.resultState,
        updateCount: 1,
        latestAt: item.at,
      });
    } else {
      current.updateCount += 1;
      if (item.at >= current.latestAt) {
        current.latestAt = item.at;
        current.resultState = item.resultState;
      }
    }
  }

  return days.map((date) => {
    const day = byDate.get(date);
    return {
      date,
      resultState: day?.resultState ?? null,
      updateCount: day?.updateCount ?? 0,
    };
  });
}

function jobRunResultState(run: JobRun): string | null {
  const result = run.state?.result_state;
  if (result === 'SUCCESS' || result === 'SUCCESS_WITH_FAILURES') return 'COMPLETED';
  if (result === 'FAILED') return 'FAILED';
  if (result === 'CANCELED' || result === 'TIMEDOUT') return 'CANCELED';
  if (run.state?.life_cycle_state === 'TERMINATED') return result ?? 'COMPLETED';
  if (run.state?.life_cycle_state === 'SKIPPED') return 'CANCELED';
  if (run.state?.life_cycle_state) return 'RUNNING';
  return null;
}

function jobRunDurationSeconds(run: JobRun | undefined): number | null {
  if (!run) return null;
  if (typeof run.run_duration === 'number' && run.run_duration > 0) {
    return Math.round(run.run_duration / 1000);
  }
  if (typeof run.start_time === 'number' && typeof run.end_time === 'number' && run.end_time > 0) {
    return Math.max(0, Math.round((run.end_time - run.start_time) / 1000));
  }
  return null;
}

function pipelineRunAsName(
  runAs: { user_name?: string; service_principal_name?: string } | undefined,
): string | null {
  return runAs?.user_name ?? runAs?.service_principal_name ?? null;
}

function millisToIso(value: number | null | undefined): string | null {
  if (typeof value !== 'number' || value <= 0) return null;
  return new Date(value).toISOString();
}

function sharedPipelineIds(settings: Record<string, string>): SharedPipelineIds {
  const rawJobId =
    settings[SHARED_PIPELINE_SETTING_KEYS.jobId] ??
    settings[LEGACY_SHARED_PIPELINE_SETTING_KEYS.jobId];
  const jobId = rawJobId ? Number(rawJobId) : null;
  return {
    jobId: Number.isSafeInteger(jobId) && jobId !== null && jobId > 0 ? jobId : null,
    pipelineId:
      settings[SHARED_PIPELINE_SETTING_KEYS.pipelineId] ??
      settings[LEGACY_SHARED_PIPELINE_SETTING_KEYS.pipelineId] ??
      null,
  };
}

function pipelineUrl(pipelineId: string | null, consoleHost: string | null): string | null {
  if (!pipelineId || !consoleHost) return null;
  return `${consoleHost}/pipelines/${encodeURIComponent(pipelineId)}`;
}

function jobUrl(jobId: number | null, consoleHost: string | null): string | null {
  if (!jobId || !consoleHost) return null;
  return `${consoleHost}/jobs/${jobId}`;
}

function lastLookbackLocalDays(): string[] {
  const days: string[] = [];
  const today = new Date();
  for (let offset = LOOKBACK_DAYS - 1; offset >= 0; offset -= 1) {
    const date = new Date(today);
    date.setDate(today.getDate() - offset);
    days.push(date.toISOString().slice(0, 10));
  }
  return days;
}
