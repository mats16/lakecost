import { Router, type RequestHandler } from 'express';
import { z } from 'zod';
import { settingsToRecord, type DatabaseClient } from '@finlake/db';
import {
  CATALOG_SETTING_KEY,
  DataSourceTableNameSchema,
  GOLD_USAGE_TABLES,
  MEDALLION_SCHEMA_DEFAULTS,
  UsageRangeSchema,
  medallionSchemaNamesFromSettings,
  quoteIdent,
  type DataSource,
  type Env,
  type UsageRange,
} from '@finlake/shared';
import {
  buildUserExecutor,
  type SqlParam,
  type StatementExecutor,
} from '../services/statementExecution.js';

const FocusDailyRowSchema = z.object({
  dataSourceId: z.number(),
  usageDate: z.string(),
  providerName: z.string(),
  serviceCategory: z.string(),
  serviceName: z.string(),
  costUsd: z.number(),
});

const FocusServiceRowSchema = z.object({
  dataSourceId: z.number(),
  providerName: z.string(),
  serviceName: z.string(),
  costUsd: z.number(),
});

const FocusSkuRowSchema = z.object({
  dataSourceId: z.number(),
  providerName: z.string(),
  skuName: z.string(),
  costUsd: z.number(),
});

const FocusCoverageRowSchema = z.object({
  dataSourceId: z.number(),
  providerName: z.string(),
  subAccountId: z.string().nullable(),
  subAccountName: z.string().nullable(),
  rowCount: z.number(),
  taggedRows: z.number(),
  tagCoveragePct: z.number(),
  lastChargeAt: z.string().nullable(),
});

type FocusDailyRow = z.infer<typeof FocusDailyRowSchema>;
type FocusServiceRow = z.infer<typeof FocusServiceRowSchema>;
type FocusSkuRow = z.infer<typeof FocusSkuRowSchema>;
type FocusCoverageRow = z.infer<typeof FocusCoverageRowSchema>;

export function overviewRouter(db: DatabaseClient, env: Env): Router {
  const router = Router();
  router.get('/focus', focusOverviewHandler(db, env));
  return router;
}

function focusOverviewHandler(db: DatabaseClient, env: Env): RequestHandler {
  return async (req, res, next) => {
    try {
      const token = req.user?.accessToken;
      if (!token) {
        res.status(401).json({ error: { message: 'Missing OBO access token' } });
        return;
      }
      const executor = buildUserExecutor(env, token);
      if (!executor) {
        res
          .status(500)
          .json({ error: { message: 'DATABRICKS_HOST or SQL_WAREHOUSE_ID not configured' } });
        return;
      }

      const range = parseRange(req.query);
      const sources = (await db.repos.dataSources.list()).filter((source) => source.enabled);
      const appSettings = settingsToRecord(await db.repos.appSettings.list());
      const catalog = (appSettings[CATALOG_SETTING_KEY] ?? '').trim();
      const goldSchema = medallionSchemaNamesFromSettings(appSettings).gold;
      const errors: Array<{
        dataSourceId: number;
        name: string;
        tableName: string;
        message: string;
      }> = [];
      const resolved = usageTableName('daily', catalog, goldSchema);
      const monthlyResolved = usageTableName('monthly', catalog, goldSchema);
      let daily: FocusDailyRow[] = [];
      let services: FocusServiceRow[] = [];
      let skus: FocusSkuRow[] = [];
      let coverage: FocusCoverageRow[] = [];
      if (sources.length > 0) {
        const cte = joinedBillingRowsSql(sources, resolved.sql);
        const monthlyCte = joinedBillingRowsSql(sources, monthlyResolved.sql);
        const params = baseParams(sources, range);
        const monthlyParams = sourceJoinParams(sources);
        try {
          [daily, services, skus, coverage] = await Promise.all([
            queryDaily(executor, cte, params),
            queryServices(executor, cte, params),
            querySkus(executor, cte, params),
            queryCoverage(executor, monthlyCte, monthlyParams),
          ]);
        } catch (err) {
          errors.push({
            dataSourceId: 0,
            name: resolved.display,
            tableName: resolved.display,
            message: (err as Error).message,
          });
        }
      }

      res.json({
        sources: sources.map((source) => sourceSummary(source, catalog, goldSchema)),
        daily,
        services,
        skus,
        coverage,
        errors,
        generatedAt: new Date().toISOString(),
      });
    } catch (err) {
      next(err);
    }
  };
}

function parseRange(query: unknown): UsageRange {
  const parsed = UsageRangeSchema.safeParse(query);
  if (parsed.success) return parsed.data;
  const now = new Date();
  const start = new Date(now.getFullYear() - 1, now.getMonth(), 1);
  return { start: start.toISOString(), end: now.toISOString() };
}

function quoteTableName(value: string): string {
  const parsed = DataSourceTableNameSchema.parse(value);
  return parsed
    .split('.')
    .map((part) => quoteIdent(part))
    .join('.');
}

function usageTableName(
  kind: keyof typeof GOLD_USAGE_TABLES,
  catalog?: string,
  goldSchema: string = MEDALLION_SCHEMA_DEFAULTS.gold,
): { display: string; sql: string } {
  const table = GOLD_USAGE_TABLES[kind];
  const parts = catalog ? [catalog, goldSchema, table] : [goldSchema, table];
  const display = parts.join('.');
  return { display, sql: quoteTableName(display) };
}

export function requestedSourcesSql(sources: DataSource[]): string {
  return sources
    .map(
      (_source, i) => `
  SELECT
    CAST(:data_source_id_${i} AS BIGINT) AS data_source_id,
    :provider_name_${i} AS provider_name,
    :billing_account_id_${i} AS billing_account_id`,
    )
    .join('\n  UNION ALL\n');
}

export function baseParams(sources: DataSource[], range: UsageRange): SqlParam[] {
  return [
    { name: 'start_ts', value: range.start, type: 'TIMESTAMP' },
    { name: 'end_ts', value: range.end, type: 'TIMESTAMP' },
    ...sourceJoinParams(sources),
  ];
}

export function sourceJoinParams(sources: DataSource[]): SqlParam[] {
  return sources.flatMap((source, i) => [
    { name: `data_source_id_${i}`, value: source.id, type: 'BIGINT' as const },
    { name: `provider_name_${i}`, value: source.providerName, type: 'STRING' as const },
    {
      name: `billing_account_id_${i}`,
      value: source.billingAccountId,
      type: 'STRING' as const,
    },
  ]);
}

export function joinedBillingRowsSql(sources: DataSource[], table: string): string {
  return /* sql */ `
WITH requested AS (
${requestedSourcesSql(sources)}
),
matched AS (
  SELECT
    r.data_source_id,
    r.provider_name AS source_provider_name,
    b.*
  FROM ${table} b
  JOIN requested r
    ON (
      r.billing_account_id IS NOT NULL
      AND b.BillingAccountId = r.billing_account_id
    )
    OR (
      r.billing_account_id IS NULL
      AND COALESCE(b.ProviderName, r.provider_name) = r.provider_name
    )
)
`;
}

export function buildDailySql(cte: string): string {
  return /* sql */ `
${cte}
SELECT
  data_source_id,
  date_format(x_ChargeDate, 'yyyy-MM-dd') AS usage_date,
  COALESCE(ProviderName, source_provider_name) AS provider_name,
  COALESCE(NULLIF(TRIM(ServiceCategory), ''), 'Unknown') AS service_category,
  COALESCE(NULLIF(TRIM(ServiceName), ''), 'Unknown') AS service_name,
  CAST(SUM(COALESCE(EffectiveCost, 0)) AS DOUBLE) AS cost_usd
FROM matched
WHERE CAST(x_ChargeDate AS TIMESTAMP) >= :start_ts
  AND CAST(x_ChargeDate AS TIMESTAMP) <  :end_ts
GROUP BY 1, 2, 3, 4, 5
ORDER BY 2
`;
}

async function queryDaily(
  executor: StatementExecutor,
  cte: string,
  params: SqlParam[],
): Promise<FocusDailyRow[]> {
  return executor.run(buildDailySql(cte), params, FocusDailyRowSchema);
}

async function queryServices(
  executor: StatementExecutor,
  cte: string,
  params: SqlParam[],
): Promise<FocusServiceRow[]> {
  return executor.run(
    /* sql */ `
${cte}
SELECT
  data_source_id,
  COALESCE(ProviderName, source_provider_name) AS provider_name,
  COALESCE(ServiceName, ServiceCategory, 'Unknown') AS service_name,
  CAST(SUM(COALESCE(EffectiveCost, 0)) AS DOUBLE) AS cost_usd
FROM matched
WHERE CAST(x_ChargeDate AS TIMESTAMP) >= :start_ts
  AND CAST(x_ChargeDate AS TIMESTAMP) <  :end_ts
GROUP BY 1, 2, 3
ORDER BY 4 DESC
LIMIT 20
`,
    params,
    FocusServiceRowSchema,
  );
}

async function querySkus(
  executor: StatementExecutor,
  cte: string,
  params: SqlParam[],
): Promise<FocusSkuRow[]> {
  return executor.run(
    /* sql */ `
${cte}
SELECT
  data_source_id,
  COALESCE(ProviderName, source_provider_name) AS provider_name,
  COALESCE(SkuId, SkuMeter, ServiceName, 'Unknown') AS sku_name,
  CAST(SUM(COALESCE(EffectiveCost, 0)) AS DOUBLE) AS cost_usd
FROM matched
WHERE CAST(x_ChargeDate AS TIMESTAMP) >= :start_ts
  AND CAST(x_ChargeDate AS TIMESTAMP) <  :end_ts
GROUP BY 1, 2, 3
ORDER BY 4 DESC
LIMIT 50
`,
    params,
    FocusSkuRowSchema,
  );
}

export function buildCoverageSql(cte: string): string {
  return /* sql */ `
${cte}
, resources AS (
  SELECT
    data_source_id,
    COALESCE(ProviderName, source_provider_name) AS provider_name,
    SubAccountId,
    MAX(SubAccountName) AS SubAccountName,
    x_BillingMonth,
    ResourceType,
    ResourceId,
    MAX(CASE WHEN Tags IS NOT NULL AND size(Tags) > 0 THEN 1 ELSE 0 END) AS has_tags
  FROM matched
  WHERE ResourceId IS NOT NULL
    AND TRIM(ResourceId) <> ''
  GROUP BY 1, 2, 3, 5, 6, 7
)
, latest_month_per_source AS (
  SELECT
    data_source_id,
    MAX(x_BillingMonth) AS max_month
  FROM resources
  GROUP BY data_source_id
)
SELECT
  r.data_source_id,
  r.provider_name,
  r.SubAccountId AS sub_account_id,
  r.SubAccountName AS sub_account_name,
  CAST(COUNT(*) AS DOUBLE) AS row_count,
  CAST(SUM(r.has_tags) AS DOUBLE) AS tagged_rows,
  CASE
    WHEN COUNT(*) > 0
      THEN CAST(SUM(r.has_tags) * 100.0 / COUNT(*) AS DOUBLE)
    ELSE CAST(0 AS DOUBLE)
  END AS tag_coverage_pct,
  CAST(MAX(r.x_BillingMonth) AS STRING) AS last_charge_at
FROM resources r
JOIN latest_month_per_source lm
  ON r.data_source_id = lm.data_source_id
  AND r.x_BillingMonth = lm.max_month
GROUP BY 1, 2, 3, 4
ORDER BY tag_coverage_pct DESC, row_count DESC
`;
}

async function queryCoverage(
  executor: StatementExecutor,
  cte: string,
  params: SqlParam[],
): Promise<FocusCoverageRow[]> {
  return executor.run(buildCoverageSql(cte), params, FocusCoverageRowSchema);
}

function sourceSummary(source: DataSource, catalog?: string, goldSchema?: string) {
  return {
    id: source.id,
    templateId: source.templateId,
    name: source.name,
    providerName: source.providerName,
    tableName: usageTableName('daily', catalog, goldSchema).display,
    focusVersion: source.focusVersion,
    updatedAt: source.updatedAt,
  };
}
