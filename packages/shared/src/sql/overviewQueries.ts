import type { DataSource } from '../schemas/dataSource.js';
import {
  CATALOG_SETTING_KEY,
  dataSourceKeyString,
  GOLD_USAGE_TABLES,
  isDatabricksDefaultAccount,
  MEDALLION_SCHEMA_DEFAULTS,
  medallionSchemaNamesFromSettings,
} from '../schemas/dataSource.js';
import type { SqlParam } from '../schemas/sql.js';
import type { UsageRange } from '../schemas/usage.js';
import { quoteIdent } from './focusView.sql.js';

export interface FocusOverviewDailyRow {
  dataSourceId: string;
  usageDate: string;
  providerName: string;
  serviceCategory: string;
  serviceName: string;
  costUsd: number;
}

export interface FocusOverviewServiceRow {
  dataSourceId: string;
  providerName: string;
  serviceName: string;
  costUsd: number;
}

export interface FocusOverviewSkuRow {
  dataSourceId: string;
  providerName: string;
  skuName: string;
  costUsd: number;
}

export interface FocusOverviewCoverageRow {
  dataSourceId: string;
  providerName: string;
  subAccountId: string | null;
  subAccountName: string | null;
  rowCount: number;
  taggedRows: number;
  tagCoveragePct: number;
  lastChargeAt: string | null;
}

export interface SqlStatementInput {
  query: string;
  params: SqlParam[];
}

export function enabledFocusSources(sources: DataSource[]): DataSource[] {
  return sources.filter((source) => source.enabled);
}

export function buildOverviewDailyStatement(
  sources: DataSource[],
  settings: Record<string, string | undefined>,
  range: UsageRange,
): SqlStatementInput | null {
  if (sources.length === 0) return null;
  const cte = joinedBillingRowsSql(sources, usageTableName('daily', settings).sql);
  return { query: buildDailySql(cte), params: baseParams(sources, range) };
}

export function buildOverviewServicesStatement(
  sources: DataSource[],
  settings: Record<string, string | undefined>,
  range: UsageRange,
): SqlStatementInput | null {
  if (sources.length === 0) return null;
  const cte = joinedBillingRowsSql(sources, usageTableName('daily', settings).sql);
  return {
    query: /* sql */ `
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
    params: baseParams(sources, range),
  };
}

export function buildOverviewSkusStatement(
  sources: DataSource[],
  settings: Record<string, string | undefined>,
  range: UsageRange,
): SqlStatementInput | null {
  if (sources.length === 0) return null;
  const cte = joinedBillingRowsSql(sources, usageTableName('daily', settings).sql);
  return {
    query: /* sql */ `
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
    params: baseParams(sources, range),
  };
}

export function buildOverviewCoverageStatement(
  sources: DataSource[],
  settings: Record<string, string | undefined>,
): SqlStatementInput | null {
  if (sources.length === 0) return null;
  const cte = joinedBillingRowsSql(sources, usageTableName('monthly', settings).sql);
  return { query: buildCoverageSql(cte), params: sourceJoinParams(sources) };
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
    { name: `data_source_id_${i}`, value: dataSourceKeyString(source), type: 'STRING' as const },
    { name: `provider_name_${i}`, value: source.providerName, type: 'STRING' as const },
    {
      name: `account_id_${i}`,
      value: billingAccountFilter(source),
      type: 'STRING' as const,
    },
  ]);
}

export function requestedSourcesSql(sources: DataSource[]): string {
  return sources
    .map(
      (_source, i) => `
  SELECT
    :data_source_id_${i} AS data_source_id,
    :provider_name_${i} AS provider_name,
    :account_id_${i} AS account_id`,
    )
    .join('\n  UNION ALL\n');
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
      r.account_id IS NOT NULL
      AND b.BillingAccountId = r.account_id
    )
    OR (
      r.account_id IS NULL
      AND LOWER(TRIM(COALESCE(b.ProviderName, r.provider_name))) = r.provider_name
    )
)
`;
}

function billingAccountFilter(source: DataSource): string | null {
  return isDatabricksDefaultAccount(source) ? null : source.accountId;
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

export function usageTableName(
  kind: keyof typeof GOLD_USAGE_TABLES,
  settings: Record<string, string | undefined>,
): { display: string; sql: string } {
  const catalog = (settings[CATALOG_SETTING_KEY] ?? '').trim();
  const goldSchema =
    medallionSchemaNamesFromSettings(settings).gold || MEDALLION_SCHEMA_DEFAULTS.gold;
  const table = GOLD_USAGE_TABLES[kind];
  const parts = catalog ? [catalog, goldSchema, table] : [goldSchema, table];
  return {
    display: parts.join('.'),
    sql: parts.map((part) => quoteIdent(part)).join('.'),
  };
}
