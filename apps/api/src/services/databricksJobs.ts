import type { WorkspaceClient } from './statementExecution.js';

export interface PipelineScheduleParams {
  /** Display name for the Lakeflow pipeline (e.g. `finops-databricks-focus-pipeline`). */
  pipelineName: string;
  /** Display name for the Databricks Job (e.g. `finops-databricks-focus-job`). */
  jobName: string;
  /** DLT SQL body — must use `CREATE OR REFRESH` syntax (no catalog/schema). */
  pipelineSql: string;
  /** Absolute workspace path for the uploaded pipeline SQL source. */
  workspacePath: string;
  /** Unity Catalog target catalog. */
  catalog: string;
  /** Unity Catalog target schema (e.g. `silver`). */
  schema: string;
  /** Lakeflow pipeline parameters exposed to SQL as `${key}` references. */
  configuration?: Record<string, string>;
  /** Quartz cron expression: `seconds minutes hours day-of-month month day-of-week`. */
  cronExpression: string;
  /** Java timezone id, e.g. `UTC`. */
  timezoneId: string;
  /** Optional application ID for service-principal-owned pipeline runs. */
  servicePrincipalId?: string;
}

export interface UpsertPipelineScheduleResult {
  jobId: number;
  pipelineId: string;
  workspacePath: string;
  createdJob: boolean;
}

async function ensureWorkspaceDir(wc: WorkspaceClient, dir: string): Promise<void> {
  try {
    await wc.workspace.mkdirs({ path: dir });
  } catch {
    /* `mkdirs` is idempotent in practice; ignore failures for already-existing dirs */
  }
}

export async function uploadPipelineFile(
  wc: WorkspaceClient,
  path: string,
  content: string,
): Promise<void> {
  const lastSlash = path.lastIndexOf('/');
  if (lastSlash > 0) {
    await ensureWorkspaceDir(wc, path.slice(0, lastSlash));
  }
  await wc.workspace.import({
    path,
    content: Buffer.from(content, 'utf8').toString('base64'),
    format: 'SOURCE',
    language: 'SQL',
    overwrite: true,
  });
}

/**
 * Create or update the Lakeflow Declarative Pipeline that builds the FOCUS
 * materialized view. Photon is managed by Databricks for serverless pipelines,
 * so we don't pass any photon-related flag — leaving the platform default in
 * place is the supported configuration.
 */
async function upsertPipeline(
  wc: WorkspaceClient,
  params: PipelineScheduleParams,
  existingPipelineId: string | null,
): Promise<string> {
  const settings = {
    name: params.pipelineName,
    catalog: params.catalog,
    schema: params.schema,
    serverless: true,
    development: false,
    continuous: false,
    channel: 'CURRENT',
    libraries: [{ file: { path: params.workspacePath } }],
    ...(params.configuration ? { configuration: params.configuration } : {}),
    ...(params.servicePrincipalId
      ? { run_as: { service_principal_name: params.servicePrincipalId } }
      : {}),
    tags: { managed_by: 'lakecost' },
  };

  if (existingPipelineId) {
    try {
      await wc.pipelines.update({ pipeline_id: existingPipelineId, ...settings });
      return existingPipelineId;
    } catch (err) {
      if (!isManagePermissionDenied(err)) throw err;
      // The saved pipeline may be owned by a different principal. Create a
      // replacement owned by the current OBO user.
      await wc.pipelines.delete({ pipeline_id: existingPipelineId }).catch(() => {});
    }
  }
  const created = await wc.pipelines.create({ ...settings, allow_duplicate_names: true });
  if (!created.pipeline_id) {
    throw new Error('Databricks Pipelines API returned no pipeline_id');
  }
  return created.pipeline_id;
}

export async function dryRunPipelineCreate(
  wc: WorkspaceClient,
  params: PipelineScheduleParams,
): Promise<void> {
  await wc.pipelines.create({
    name: params.pipelineName,
    catalog: params.catalog,
    schema: params.schema,
    serverless: true,
    development: false,
    continuous: false,
    channel: 'CURRENT',
    libraries: [{ file: { path: params.workspacePath } }],
    ...(params.configuration ? { configuration: params.configuration } : {}),
    ...(params.servicePrincipalId
      ? { run_as: { service_principal_name: params.servicePrincipalId } }
      : {}),
    tags: { managed_by: 'lakecost' },
    dry_run: true,
  });
}

/**
 * Create or replace the Databricks Job that triggers the pipeline on cron.
 * `pipeline_task` runs an update of the named pipeline; cron lives on the job
 * so we can change the schedule without touching the pipeline definition.
 */
export async function upsertPipelineSchedule(
  wc: WorkspaceClient,
  params: PipelineScheduleParams,
  existing: { jobId: number | null; pipelineId: string | null },
): Promise<UpsertPipelineScheduleResult> {
  await uploadPipelineFile(wc, params.workspacePath, params.pipelineSql);
  const pipelineId = await upsertPipeline(wc, params, existing.pipelineId);

  const jobSettings = {
    name: params.jobName,
    max_concurrent_runs: 1,
    tags: { managed_by: 'lakecost' },
    schedule: {
      quartz_cron_expression: params.cronExpression,
      timezone_id: params.timezoneId,
      pause_status: 'UNPAUSED' as const,
    },
    tasks: [
      {
        task_key: 'refresh',
        pipeline_task: { pipeline_id: pipelineId, full_refresh: false },
      },
    ],
  };

  if (existing.jobId !== null) {
    try {
      await wc.jobs.reset({ job_id: existing.jobId, new_settings: jobSettings });
      return {
        jobId: existing.jobId,
        pipelineId,
        workspacePath: params.workspacePath,
        createdJob: false,
      };
    } catch (err) {
      if (!isManagePermissionDenied(err)) throw err;
      // The saved job may be owned by a different principal. Create a new
      // user-owned job and persist its id in the data source row.
      await wc.jobs.delete({ job_id: existing.jobId }).catch(() => {});
    }
  }
  const created = await wc.jobs.create(jobSettings);
  if (typeof created.job_id !== 'number') {
    throw new Error('Databricks Jobs API returned no job_id');
  }
  return {
    jobId: created.job_id,
    pipelineId,
    workspacePath: params.workspacePath,
    createdJob: true,
  };
}

function isManagePermissionDenied(err: unknown): boolean {
  const code = hasErrorCode(err) ? err.errorCode : '';
  const message = err instanceof Error ? err.message : String(err);
  const isPermDenied = code === 'PERMISSION_DENIED' || /PERMISSION_DENIED/i.test(message);
  return isPermDenied && /Manage permissions/i.test(message);
}

function hasErrorCode(err: unknown): err is { errorCode: string } {
  return (
    err != null &&
    typeof err === 'object' &&
    'errorCode' in err &&
    typeof (err as { errorCode: unknown }).errorCode === 'string'
  );
}

/**
 * Delete the job, the pipeline, and the workspace SQL file. Each delete is
 * best-effort so a partial setup can still be torn down.
 */
export async function deletePipelineSchedule(
  wc: WorkspaceClient,
  ids: { jobId: number | null; pipelineId: string | null; workspacePath: string | null },
): Promise<void> {
  const ops: Promise<unknown>[] = [];
  if (ids.jobId !== null) {
    ops.push(wc.jobs.delete({ job_id: ids.jobId }).catch(() => {}));
  }
  if (ids.pipelineId) {
    ops.push(wc.pipelines.delete({ pipeline_id: ids.pipelineId }).catch(() => {}));
  }
  if (ids.workspacePath) {
    ops.push(wc.workspace.delete({ path: ids.workspacePath, recursive: false }).catch(() => {}));
  }
  await Promise.allSettled(ops);
}
