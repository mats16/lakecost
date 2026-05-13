import type { DataSource } from '../schemas/dataSource.js';
import {
  CATALOG_SETTING_KEY,
  MEDALLION_SCHEMA_DEFAULTS,
  medallionSchemaNamesFromSettings,
} from '../schemas/dataSource.js';
import type { DatabricksOptimizationRange } from '../schemas/optimization.js';
import type { SqlParam } from '../schemas/sql.js';
import { quoteIdent } from './focusView.sql.js';
import type { SqlStatementInput } from './overviewQueries.js';

const DEFAULT_DATABRICKS_TABLE = 'databricks_usage';
const KNOWN_COST_DENOMINATOR = '(serverless_cost_usd + non_serverless_cost_usd)';

export type DatabricksTrendGrain = 'day' | 'month';

export interface DatabricksOptimizeSource {
  tableDisplay: string;
  tableSql: string;
  billingAccountId: string | null;
}

export function resolveDatabricksOptimizeSources(
  dataSources: DataSource[],
  settings: Record<string, string | undefined>,
): DatabricksOptimizeSource[] {
  const catalog = (settings[CATALOG_SETTING_KEY] ?? '').trim();
  const silverSchema =
    medallionSchemaNamesFromSettings(settings).silver || MEDALLION_SCHEMA_DEFAULTS.silver;
  const configured = dataSources
    .filter(
      (source) =>
        source.enabled &&
        (source.providerName === 'Databricks' || source.templateId === 'databricks_focus13'),
    )
    .map((source) => databricksOptimizeSource(catalog, silverSchema, source));
  return configured.length > 0
    ? configured
    : [
        databricksOptimizeSource(catalog, silverSchema, {
          tableName: DEFAULT_DATABRICKS_TABLE,
          billingAccountId: null,
        }),
      ];
}

export function databricksOptimizeParams(
  sources: DatabricksOptimizeSource[],
  range: DatabricksOptimizationRange,
): SqlParam[] {
  return [
    { name: 'start_ts', value: range.start, type: 'TIMESTAMP' },
    { name: 'end_ts', value: range.end, type: 'TIMESTAMP' },
    { name: 'workspace_id', value: range.workspaceId ?? null, type: 'STRING' },
    ...sources.map((source, index) => ({
      name: `billing_account_id_${index}`,
      value: source.billingAccountId,
      type: 'STRING' as const,
    })),
  ];
}

export function buildDatabricksSummaryStatement(
  sources: DatabricksOptimizeSource[],
  range: DatabricksOptimizationRange,
): SqlStatementInput {
  const cte = buildDatabricksOptimizeCte(sources);
  return {
    query: buildDatabricksSummarySql(cte),
    params: databricksOptimizeParams(sources, range),
  };
}

export function buildDatabricksWorkspacesStatement(
  sources: DatabricksOptimizeSource[],
  range: DatabricksOptimizationRange,
): SqlStatementInput {
  const cte = buildDatabricksOptimizeCte(sources);
  return {
    query: buildDatabricksWorkspacesSql(cte),
    params: databricksOptimizeParams(sources, range),
  };
}

export function buildDatabricksTrendStatement(
  sources: DatabricksOptimizeSource[],
  range: DatabricksOptimizationRange,
  grain: DatabricksTrendGrain,
): SqlStatementInput {
  const cte = buildDatabricksOptimizeCte(sources);
  return {
    query: buildDatabricksTrendSql(cte, grain),
    params: databricksOptimizeParams(sources, range),
  };
}

export function buildDatabricksServicesStatement(
  sources: DatabricksOptimizeSource[],
  range: DatabricksOptimizationRange,
): SqlStatementInput {
  const cte = buildDatabricksOptimizeCte(sources);
  return {
    query: buildDatabricksServicesSql(cte),
    params: databricksOptimizeParams(sources, range),
  };
}

export function buildDatabricksRecommendationsStatement(
  sources: DatabricksOptimizeSource[],
  range: DatabricksOptimizationRange,
): SqlStatementInput {
  const cte = buildDatabricksOptimizeCte(sources);
  return {
    query: buildDatabricksRecommendationsSql(cte),
    params: databricksOptimizeParams(sources, range),
  };
}

export function buildDatabricksOptimizeCte(sources: DatabricksOptimizeSource[]): string {
  const selects = sources
    .map(
      (source, index) => /* sql */ `
  SELECT
    CAST(ChargePeriodStart AS TIMESTAMP) AS charge_period_start,
    BillingAccountId AS billing_account_id,
    BillingAccountName AS billing_account_name,
    SubAccountId AS workspace_id,
    SubAccountName AS workspace_name,
    COALESCE(NULLIF(TRIM(ServiceCategory), ''), 'Unknown') AS service_category,
    COALESCE(NULLIF(TRIM(ServiceName), ''), 'Unknown') AS service_name,
    COALESCE(NULLIF(TRIM(ResourceType), ''), 'Unknown') AS resource_type,
    ResourceId AS resource_id,
    ResourceName AS resource_name,
    NULLIF(TRIM(SkuPriceDetails['InstanceType']), '') AS instance_type,
    CAST(COALESCE(EffectiveCost, 0) AS DOUBLE) AS cost_usd,
    CAST(${quoteIdent('x_Serverless')} AS BOOLEAN) AS x_serverless
  FROM ${source.tableSql}
  WHERE ProviderName = 'Databricks'
    AND CAST(ChargePeriodStart AS TIMESTAMP) >= :start_ts
    AND CAST(ChargePeriodStart AS TIMESTAMP) < :end_ts
    AND (:billing_account_id_${index} IS NULL OR BillingAccountId = :billing_account_id_${index})`,
    )
    .join('\n  UNION ALL\n');

  return /* sql */ `
WITH usage_rows AS (
${selects}
),
filtered AS (
  SELECT *
  FROM usage_rows
  WHERE charge_period_start >= :start_ts
    AND charge_period_start < :end_ts
    AND (:workspace_id IS NULL OR workspace_id = :workspace_id)
)
`;
}

export function buildDatabricksSummarySql(cte: string): string {
  return /* sql */ `
${cte}
, totals AS (
  SELECT
    CAST(COALESCE(SUM(cost_usd), 0) AS DOUBLE) AS total_cost_usd,
    CAST(COALESCE(SUM(CASE WHEN x_serverless = true THEN cost_usd ELSE 0 END), 0) AS DOUBLE) AS serverless_cost_usd,
    CAST(COALESCE(SUM(CASE WHEN x_serverless = false THEN cost_usd ELSE 0 END), 0) AS DOUBLE) AS non_serverless_cost_usd,
    CAST(COALESCE(SUM(CASE WHEN x_serverless IS NULL THEN cost_usd ELSE 0 END), 0) AS DOUBLE) AS unknown_cost_usd
  FROM filtered
),
candidate_resources AS (
  SELECT resource_id
  FROM filtered
  WHERE resource_id IS NOT NULL
    AND TRIM(resource_id) <> ''
  GROUP BY workspace_id, service_name, resource_type, resource_id
  HAVING SUM(CASE WHEN x_serverless = false THEN cost_usd ELSE 0 END) > 0
)
SELECT
  total_cost_usd,
  serverless_cost_usd,
  non_serverless_cost_usd,
  unknown_cost_usd,
  CASE
    WHEN ${KNOWN_COST_DENOMINATOR} > 0
      THEN CAST(serverless_cost_usd * 100.0 / ${KNOWN_COST_DENOMINATOR} AS DOUBLE)
    ELSE CAST(NULL AS DOUBLE)
  END AS serverless_ratio,
  CAST((SELECT COUNT(*) FROM candidate_resources) AS DOUBLE) AS candidate_resource_count
FROM totals
`;
}

export function buildDatabricksWorkspacesSql(cte: string): string {
  return /* sql */ `
${cte}
, metrics AS (
  SELECT
    workspace_id,
    MAX(workspace_name) AS workspace_name,
    CAST(SUM(cost_usd) AS DOUBLE) AS total_cost_usd,
    CAST(SUM(CASE WHEN x_serverless = true THEN cost_usd ELSE 0 END) AS DOUBLE) AS serverless_cost_usd,
    CAST(SUM(CASE WHEN x_serverless = false THEN cost_usd ELSE 0 END) AS DOUBLE) AS non_serverless_cost_usd
  FROM filtered
  GROUP BY workspace_id
)
SELECT
  workspace_id,
  workspace_name,
  total_cost_usd,
  serverless_cost_usd,
  non_serverless_cost_usd,
  CASE
    WHEN ${KNOWN_COST_DENOMINATOR} > 0
      THEN CAST(serverless_cost_usd * 100.0 / ${KNOWN_COST_DENOMINATOR} AS DOUBLE)
    ELSE CAST(NULL AS DOUBLE)
  END AS serverless_ratio
FROM metrics
ORDER BY total_cost_usd DESC
`;
}

export function buildDatabricksTrendSql(cte: string, grain: DatabricksTrendGrain): string {
  const unit = grain === 'day' ? 'DAY' : 'MONTH';
  const format = grain === 'day' ? 'yyyy-MM-dd' : 'yyyy-MM';
  const periodExpression = `date_format(date_trunc('${unit}', charge_period_start), '${format}')`;
  return /* sql */ `
${cte}
, metrics AS (
  SELECT
    ${periodExpression} AS period,
    CAST(SUM(cost_usd) AS DOUBLE) AS total_cost_usd,
    CAST(SUM(CASE WHEN x_serverless = true THEN cost_usd ELSE 0 END) AS DOUBLE) AS serverless_cost_usd,
    CAST(SUM(CASE WHEN x_serverless = false THEN cost_usd ELSE 0 END) AS DOUBLE) AS non_serverless_cost_usd,
    CAST(SUM(CASE WHEN x_serverless IS NULL THEN cost_usd ELSE 0 END) AS DOUBLE) AS unknown_cost_usd
  FROM filtered
  GROUP BY ${periodExpression}
)
SELECT
  period,
  total_cost_usd,
  serverless_cost_usd,
  non_serverless_cost_usd,
  unknown_cost_usd,
  CASE
    WHEN ${KNOWN_COST_DENOMINATOR} > 0
      THEN CAST(serverless_cost_usd * 100.0 / ${KNOWN_COST_DENOMINATOR} AS DOUBLE)
    ELSE CAST(NULL AS DOUBLE)
  END AS serverless_ratio
FROM metrics
ORDER BY period
`;
}

export function buildDatabricksServicesSql(cte: string): string {
  return /* sql */ `
${cte}
, service_rows AS (
  SELECT
    CASE
      WHEN service_name IN ('ALL_PURPOSE', 'INTERACTIVE') THEN 'Compute'
      ELSE service_category
    END AS service_category,
    CASE
      WHEN service_name IN ('ALL_PURPOSE', 'INTERACTIVE') THEN 'INTERACTIVE / ALL_PURPOSE'
      ELSE service_name
    END AS service_name,
    cost_usd,
    x_serverless
  FROM filtered
  WHERE service_name IN ('SQL', 'ALL_PURPOSE', 'INTERACTIVE', 'DLT', 'JOBS', 'LAKEFLOW_CONNECT')
),
metrics AS (
  SELECT
    service_category,
    service_name,
    CAST(SUM(cost_usd) AS DOUBLE) AS total_cost_usd,
    CAST(SUM(CASE WHEN x_serverless = true THEN cost_usd ELSE 0 END) AS DOUBLE) AS serverless_cost_usd,
    CAST(SUM(CASE WHEN x_serverless = false THEN cost_usd ELSE 0 END) AS DOUBLE) AS non_serverless_cost_usd
  FROM service_rows
  GROUP BY service_category, service_name
)
SELECT
  service_category,
  service_name,
  total_cost_usd,
  serverless_cost_usd,
  non_serverless_cost_usd,
  CASE
    WHEN ${KNOWN_COST_DENOMINATOR} > 0
      THEN CAST(serverless_cost_usd * 100.0 / ${KNOWN_COST_DENOMINATOR} AS DOUBLE)
    ELSE CAST(NULL AS DOUBLE)
  END AS serverless_ratio
FROM metrics
ORDER BY
  CASE service_name
    WHEN 'SQL' THEN 1
    WHEN 'INTERACTIVE / ALL_PURPOSE' THEN 2
    WHEN 'DLT' THEN 3
    WHEN 'JOBS' THEN 4
    WHEN 'LAKEFLOW_CONNECT' THEN 5
    ELSE 99
  END
`;
}

export function buildDatabricksRecommendationsSql(cte: string): string {
  return /* sql */ `
${cte}
, resource_metrics AS (
  SELECT
    workspace_id,
    MAX(workspace_name) AS workspace_name,
    service_category,
    service_name,
    resource_type,
    resource_id,
    MAX_BY(resource_name, charge_period_start) AS resource_name,
    MAX_BY(instance_type, charge_period_start) AS instance_type,
    CAST(SUM(cost_usd) AS DOUBLE) AS total_cost_usd,
    CAST(SUM(CASE WHEN x_serverless = true THEN cost_usd ELSE 0 END) AS DOUBLE) AS serverless_cost_usd,
    CAST(SUM(CASE WHEN x_serverless = false THEN cost_usd ELSE 0 END) AS DOUBLE) AS non_serverless_cost_usd
  FROM filtered
  WHERE resource_id IS NOT NULL
    AND TRIM(resource_id) <> ''
  GROUP BY workspace_id, service_category, service_name, resource_type, resource_id
),
scored AS (
  SELECT
    *,
    CASE
      WHEN service_name IN ('SQL', 'JOBS', 'DLT') THEN 1.35
      WHEN service_name IN ('INTERACTIVE', 'NOTEBOOKS', 'ALL_PURPOSE') THEN 1.2
      WHEN service_category IN ('Analytics', 'Compute') THEN 1.1
      ELSE 1.0
    END AS eligibility_weight
  FROM resource_metrics
  WHERE non_serverless_cost_usd > 0
),
ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      ORDER BY non_serverless_cost_usd * eligibility_weight DESC, non_serverless_cost_usd DESC
    ) AS recommendation_rank
  FROM scored
)
SELECT
  CAST(recommendation_rank AS DOUBLE) AS rank,
  CASE
    WHEN non_serverless_cost_usd * eligibility_weight >= 1000 THEN 'high'
    WHEN non_serverless_cost_usd * eligibility_weight >= 250 THEN 'medium'
    ELSE 'low'
  END AS priority,
  workspace_id,
  workspace_name,
  service_category,
  service_name,
  resource_type,
  resource_id,
  resource_name,
  instance_type,
  total_cost_usd,
  non_serverless_cost_usd,
  CASE
    WHEN ${KNOWN_COST_DENOMINATOR} > 0
      THEN CAST(serverless_cost_usd * 100.0 / ${KNOWN_COST_DENOMINATOR} AS DOUBLE)
    ELSE CAST(NULL AS DOUBLE)
  END AS serverless_ratio
FROM ranked
WHERE recommendation_rank <= 25
ORDER BY recommendation_rank
`;
}

function databricksOptimizeSource(
  catalog: string,
  silverSchema: string,
  source: Pick<DataSource, 'tableName' | 'billingAccountId'>,
): DatabricksOptimizeSource {
  const parts = catalog
    ? [catalog, silverSchema, source.tableName]
    : [silverSchema, source.tableName];
  const tableDisplay = parts.join('.');
  return {
    tableDisplay,
    tableSql: parts.map((part) => quoteIdent(part)).join('.'),
    billingAccountId: source.billingAccountId,
  };
}
