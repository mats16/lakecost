import type { DatabaseClient } from '@lakecost/db';
import { z } from 'zod';
import {
  type Env,
  type TransformationPipelineRow,
  type TransformationPipelinesResponse,
} from '@lakecost/shared';
import { buildUserExecutor, type SqlParam } from './statementExecution.js';
import { WorkspaceServiceError } from './workspaceClientErrors.js';

interface SourceForPipeline {
  id: number;
  name: string;
  providerName: string;
  tableName: string;
  jobId: number | null;
  pipelineId: string | null;
  config: Record<string, unknown>;
}

export class TransformationPipelineAuthError extends WorkspaceServiceError {}

const LOOKBACK_DAYS = 7;
const DAY_INDICES = [0, 1, 2, 3, 4, 5, 6] as const;

const dayFields = Object.fromEntries(
  DAY_INDICES.flatMap((i) => [
    [`day${i}Date`, z.string()],
    [`day${i}ResultState`, z.string().nullable()],
    [`day${i}UpdateCount`, z.number().int().nonnegative().nullable()],
  ]),
);

const PipelineQueryRowSchema = z.object({
  dataSourceId: z.number().int().positive(),
  dataSourceName: z.string(),
  providerName: z.string(),
  tableName: z.string(),
  jobId: z.number().int().positive().nullable(),
  pipelineId: z.string().nullable(),
  cronExpression: z.string().nullable(),
  timezoneId: z.string().nullable(),
  accountId: z.string().nullable(),
  workspaceId: z.string().nullable(),
  pipelineName: z.string().nullable(),
  pipelineType: z.string().nullable(),
  createdBy: z.string().nullable(),
  runAs: z.string().nullable(),
  createTime: z.string().nullable(),
  changeTime: z.string().nullable(),
  deleteTime: z.string().nullable(),
  updateId: z.string().nullable(),
  updateType: z.string().nullable(),
  triggerType: z.string().nullable(),
  resultState: z.string().nullable(),
  runAsUserName: z.string().nullable(),
  periodStartTime: z.string().nullable(),
  periodEndTime: z.string().nullable(),
  durationSeconds: z.number().nullable(),
  ...dayFields,
});
type PipelineQueryRow = z.infer<typeof PipelineQueryRowSchema>;

export async function listTransformationPipelines(
  db: DatabaseClient,
  env: Env,
  userToken: string | undefined,
): Promise<TransformationPipelinesResponse> {
  const sources = (await db.repos.dataSources.list()).map(toSourceForPipeline);
  const configured = sources.filter((source) => source.pipelineId);
  const generatedAt = new Date().toISOString();
  const fallbackDays = lastLookbackLocalDays();

  if (configured.length === 0) {
    return { rows: sources.map((s) => localOnlyRow(s, fallbackDays)), generatedAt };
  }
  if (!userToken) {
    throw new TransformationPipelineAuthError('Missing OBO access token', 401);
  }

  const executor = buildUserExecutor(env, userToken);
  if (!executor) {
    throw new Error(
      'DATABRICKS_HOST, SQL_WAREHOUSE_ID, and an OBO access token are required to read system.lakeflow tables.',
    );
  }

  const rows = await executor.run(
    buildPipelineSql(configured),
    buildPipelineParams(configured, env.DATABRICKS_WORKSPACE_ID),
    PipelineQueryRowSchema,
  );
  const consoleHost = normalizeHost(env.DATABRICKS_HOST);
  const rowsByDataSourceId = new Map(
    rows.map((row) => [row.dataSourceId, toResponseRow(row, consoleHost)]),
  );
  return {
    rows: sources.map(
      (source) => rowsByDataSourceId.get(source.id) ?? localOnlyRow(source, fallbackDays),
    ),
    generatedAt,
  };
}

function toSourceForPipeline(
  source: Awaited<ReturnType<DatabaseClient['repos']['dataSources']['list']>>[number],
): SourceForPipeline {
  return {
    id: source.id,
    name: source.name,
    providerName: source.providerName,
    tableName: source.tableName,
    jobId: source.jobId,
    pipelineId: source.pipelineId,
    config: source.config,
  };
}

function localOnlyRow(source: SourceForPipeline, days: string[]): TransformationPipelineRow {
  return {
    dataSourceId: source.id,
    dataSourceName: source.name,
    providerName: source.providerName,
    tableName: source.tableName,
    jobId: source.jobId,
    pipelineId: source.pipelineId,
    cronExpression: stringConfig(source.config.cronExpression),
    timezoneId: stringConfig(source.config.timezoneId),
    accountId: null,
    workspaceId: null,
    pipelineUrl: null,
    pipelineName: null,
    pipelineType: null,
    createdBy: null,
    runAs: null,
    createTime: null,
    changeTime: null,
    deleteTime: null,
    updateId: null,
    updateType: null,
    triggerType: null,
    resultState: null,
    runAsUserName: null,
    periodStartTime: null,
    periodEndTime: null,
    durationSeconds: null,
    statusDays: days.map((date) => ({
      date,
      resultState: null,
      updateCount: 0,
    })),
  };
}

function toResponseRow(
  row: PipelineQueryRow,
  consoleHost: string | null,
): TransformationPipelineRow {
  return {
    dataSourceId: row.dataSourceId,
    dataSourceName: row.dataSourceName,
    providerName: row.providerName,
    tableName: row.tableName,
    jobId: row.jobId,
    pipelineId: row.pipelineId,
    cronExpression: row.cronExpression,
    timezoneId: row.timezoneId,
    accountId: row.accountId,
    workspaceId: row.workspaceId,
    pipelineUrl: pipelineUrl(row.pipelineId, consoleHost),
    pipelineName: row.pipelineName,
    pipelineType: row.pipelineType,
    createdBy: row.createdBy,
    runAs: row.runAs,
    createTime: row.createTime,
    changeTime: row.changeTime,
    deleteTime: row.deleteTime,
    updateId: row.updateId,
    updateType: row.updateType,
    triggerType: row.triggerType,
    resultState: row.resultState,
    runAsUserName: row.runAsUserName,
    periodStartTime: row.periodStartTime,
    periodEndTime: row.periodEndTime,
    durationSeconds: row.durationSeconds,
    statusDays: DAY_INDICES.map((i) => {
      const r = row as Record<string, unknown>;
      return {
        date: r[`day${i}Date`] as string,
        resultState: r[`day${i}ResultState`] as string | null,
        updateCount: (r[`day${i}UpdateCount`] as number | null) ?? 0,
      };
    }),
  };
}

function buildPipelineSql(sources: SourceForPipeline[]): string {
  const requested = sources
    .map(
      (_source, i) => `
      SELECT
        CAST(:data_source_id_${i} AS BIGINT) AS data_source_id,
        :data_source_name_${i} AS data_source_name,
        :provider_name_${i} AS provider_name,
        :table_name_${i} AS table_name,
        CAST(:job_id_${i} AS BIGINT) AS job_id,
        :pipeline_id_${i} AS pipeline_id,
        :cron_expression_${i} AS cron_expression,
        :timezone_id_${i} AS timezone_id`,
    )
    .join('\n      UNION ALL\n');
  const pipelineIds = sources.map((_source, i) => `:pipeline_id_${i}`).join(', ');

  return `
    WITH requested AS (
      ${requested}
    ),
    latest_pipelines AS (
      SELECT
        account_id,
        workspace_id,
        pipeline_id,
        name AS pipeline_name,
        pipeline_type,
        created_by,
        run_as,
        create_time,
        change_time,
        delete_time
      FROM (
        SELECT
          account_id,
          workspace_id,
          pipeline_id,
          name,
          pipeline_type,
          created_by,
          run_as,
          create_time,
          change_time,
          delete_time,
          ROW_NUMBER() OVER (
            PARTITION BY pipeline_id
            ORDER BY CASE WHEN delete_time IS NULL THEN 1 ELSE 0 END DESC, change_time DESC
          ) AS rn
        FROM system.lakeflow.pipelines
        WHERE pipeline_id IN (${pipelineIds})
          AND (:workspace_id IS NULL OR workspace_id = :workspace_id)
      )
      WHERE rn = 1
    ),
    latest_updates AS (
      SELECT
        workspace_id,
        pipeline_id,
        update_id,
        update_type,
        trigger_type,
        result_state,
        run_as_user_name,
        period_start_time,
        period_end_time,
        CAST(unix_timestamp(period_end_time) - unix_timestamp(period_start_time) AS BIGINT) AS duration_seconds
      FROM (
        SELECT
          workspace_id,
          pipeline_id,
          update_id,
          update_type,
          trigger_type,
          result_state,
          run_as_user_name,
          period_start_time,
          period_end_time,
          ROW_NUMBER() OVER (
            PARTITION BY pipeline_id
            ORDER BY period_end_time DESC, period_start_time DESC
          ) AS rn
        FROM system.lakeflow.pipeline_update_timeline
        WHERE pipeline_id IN (${pipelineIds})
          AND (:workspace_id IS NULL OR workspace_id = :workspace_id)
          AND period_start_time > CURRENT_TIMESTAMP() - INTERVAL ${LOOKBACK_DAYS} DAYS
      )
      WHERE rn = 1
    ),
    daily_updates AS (
      SELECT
        pipeline_id,
        to_date(period_start_time) AS status_date,
        result_state,
        ROW_NUMBER() OVER (
          PARTITION BY pipeline_id, to_date(period_start_time)
          ORDER BY period_end_time DESC NULLS LAST, period_start_time DESC
        ) AS rn
      FROM system.lakeflow.pipeline_update_timeline
      WHERE pipeline_id IN (${pipelineIds})
        AND (:workspace_id IS NULL OR workspace_id = :workspace_id)
        AND period_start_time > CURRENT_TIMESTAMP() - INTERVAL ${LOOKBACK_DAYS} DAYS
    ),
    daily_counts AS (
      SELECT
        pipeline_id,
        to_date(period_start_time) AS status_date,
        COUNT(DISTINCT update_id) AS update_count
      FROM system.lakeflow.pipeline_update_timeline
      WHERE pipeline_id IN (${pipelineIds})
        AND (:workspace_id IS NULL OR workspace_id = :workspace_id)
        AND period_start_time > CURRENT_TIMESTAMP() - INTERVAL ${LOOKBACK_DAYS} DAYS
      GROUP BY pipeline_id, to_date(period_start_time)
    ),
    daily_status AS (
      SELECT
        u.pipeline_id,
        u.status_date,
        u.result_state,
        c.update_count
      FROM daily_updates u
      LEFT JOIN daily_counts c
        ON u.pipeline_id = c.pipeline_id
        AND u.status_date = c.status_date
      WHERE u.rn = 1
    )
    SELECT
      r.data_source_id,
      r.data_source_name,
      r.provider_name,
      r.table_name,
      r.job_id,
      r.pipeline_id,
      r.cron_expression,
      r.timezone_id,
      p.account_id,
      COALESCE(p.workspace_id, u.workspace_id) AS workspace_id,
      p.pipeline_name,
      p.pipeline_type,
      p.created_by,
      p.run_as,
      p.create_time,
      p.change_time,
      p.delete_time,
      u.update_id,
      u.update_type,
      u.trigger_type,
      u.result_state,
      u.run_as_user_name,
      u.period_start_time,
      u.period_end_time,
      u.duration_seconds,
      CAST(date_sub(current_date(), 6) AS STRING) AS day0_date,
      d0.result_state AS day0_result_state,
      d0.update_count AS day0_update_count,
      CAST(date_sub(current_date(), 5) AS STRING) AS day1_date,
      d1.result_state AS day1_result_state,
      d1.update_count AS day1_update_count,
      CAST(date_sub(current_date(), 4) AS STRING) AS day2_date,
      d2.result_state AS day2_result_state,
      d2.update_count AS day2_update_count,
      CAST(date_sub(current_date(), 3) AS STRING) AS day3_date,
      d3.result_state AS day3_result_state,
      d3.update_count AS day3_update_count,
      CAST(date_sub(current_date(), 2) AS STRING) AS day4_date,
      d4.result_state AS day4_result_state,
      d4.update_count AS day4_update_count,
      CAST(date_sub(current_date(), 1) AS STRING) AS day5_date,
      d5.result_state AS day5_result_state,
      d5.update_count AS day5_update_count,
      CAST(current_date() AS STRING) AS day6_date,
      d6.result_state AS day6_result_state,
      d6.update_count AS day6_update_count
    FROM requested r
    LEFT JOIN latest_pipelines p
      ON r.pipeline_id = p.pipeline_id
    LEFT JOIN latest_updates u
      ON r.pipeline_id = u.pipeline_id
    LEFT JOIN daily_status d0
      ON r.pipeline_id = d0.pipeline_id
      AND d0.status_date = date_sub(current_date(), 6)
    LEFT JOIN daily_status d1
      ON r.pipeline_id = d1.pipeline_id
      AND d1.status_date = date_sub(current_date(), 5)
    LEFT JOIN daily_status d2
      ON r.pipeline_id = d2.pipeline_id
      AND d2.status_date = date_sub(current_date(), 4)
    LEFT JOIN daily_status d3
      ON r.pipeline_id = d3.pipeline_id
      AND d3.status_date = date_sub(current_date(), 3)
    LEFT JOIN daily_status d4
      ON r.pipeline_id = d4.pipeline_id
      AND d4.status_date = date_sub(current_date(), 2)
    LEFT JOIN daily_status d5
      ON r.pipeline_id = d5.pipeline_id
      AND d5.status_date = date_sub(current_date(), 1)
    LEFT JOIN daily_status d6
      ON r.pipeline_id = d6.pipeline_id
      AND d6.status_date = current_date()
    ORDER BY r.data_source_name
  `;
}

function buildPipelineParams(
  sources: SourceForPipeline[],
  workspaceId: string | undefined,
): SqlParam[] {
  return [
    { name: 'workspace_id', value: workspaceId ?? null, type: 'STRING' as const },
    ...sources.flatMap((source, i) => [
      { name: `data_source_id_${i}`, value: source.id, type: 'BIGINT' as const },
      { name: `data_source_name_${i}`, value: source.name, type: 'STRING' as const },
      { name: `provider_name_${i}`, value: source.providerName, type: 'STRING' as const },
      { name: `table_name_${i}`, value: source.tableName, type: 'STRING' as const },
      { name: `job_id_${i}`, value: source.jobId, type: 'BIGINT' as const },
      { name: `pipeline_id_${i}`, value: source.pipelineId, type: 'STRING' as const },
      {
        name: `cron_expression_${i}`,
        value: stringConfig(source.config.cronExpression),
        type: 'STRING' as const,
      },
      {
        name: `timezone_id_${i}`,
        value: stringConfig(source.config.timezoneId),
        type: 'STRING' as const,
      },
    ]),
  ];
}

function stringConfig(value: unknown): string | null {
  return typeof value === 'string' && value.trim() ? value.trim() : null;
}

function normalizeHost(host: string | undefined): string | null {
  if (!host) return null;
  if (host.startsWith('http://') || host.startsWith('https://')) return host.replace(/\/+$/, '');
  return `https://${host.replace(/\/+$/, '')}`;
}

function pipelineUrl(pipelineId: string | null, consoleHost: string | null): string | null {
  if (!pipelineId || !consoleHost) return null;
  return `${consoleHost}/pipelines/${encodeURIComponent(pipelineId)}`;
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
