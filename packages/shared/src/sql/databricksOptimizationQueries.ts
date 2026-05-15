import type { DataSource } from '../schemas/dataSource.js';
import {
  CATALOG_SETTING_KEY,
  DEFAULT_DATABRICKS_ACCOUNT_ID,
  isDatabricksProvider,
  PROVIDER_DATABRICKS,
  isDatabricksDefaultAccount,
  MEDALLION_SCHEMA_DEFAULTS,
  medallionSchemaNamesFromSettings,
} from '../schemas/dataSource.js';
import type { DatabricksOptimizationRange } from '../schemas/optimization.js';
import type { SqlParam } from '../schemas/sql.js';
import { quoteIdent } from './focusView.sql.js';
import type { SqlStatementInput } from './overviewQueries.js';

const DEFAULT_DATABRICKS_TABLE = 'databricks_usage';
const KNOWN_COST_DENOMINATOR = '(serverless_cost_usd + non_serverless_cost_usd)';
const AWS_EC2_PRICING_TABLE_SQL = '`finops`.`pricing`.`aws_ec2`';
const EC2_REFERENCE_INSTANCE_TYPE = 'r6i.xlarge';

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
    .filter((source) => source.enabled && isDatabricksProvider(source.providerName))
    .map((source) => databricksOptimizeSource(catalog, silverSchema, source));
  return configured.length > 0
    ? configured
    : [
        databricksOptimizeSource(catalog, silverSchema, {
          tableName: DEFAULT_DATABRICKS_TABLE,
          providerName: PROVIDER_DATABRICKS,
          accountId: DEFAULT_DATABRICKS_ACCOUNT_ID,
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
    RegionId AS region_id,
    COALESCE(NULLIF(TRIM(ServiceCategory), ''), 'Unknown') AS service_category,
    COALESCE(NULLIF(TRIM(ServiceName), ''), 'Unknown') AS service_name,
    COALESCE(NULLIF(TRIM(ResourceType), ''), 'Unknown') AS resource_type,
    ResourceId AS resource_id,
    ResourceName AS resource_name,
    NULLIF(TRIM(SkuId), '') AS sku_id,
    NULLIF(TRIM(SkuPriceDetails['InstanceType']), '') AS instance_type,
    CAST(ConsumedQuantity AS DOUBLE) AS consumed_quantity,
    CAST(ListCost AS DOUBLE) AS list_cost_usd,
    CAST(ListUnitPrice AS DOUBLE) AS list_unit_price_usd,
    NULLIF(TRIM(PricingUnit), '') AS pricing_unit,
    CAST(COALESCE(EffectiveCost, 0) AS DOUBLE) AS cost_usd,
    CAST(${quoteIdent('x_Serverless')} AS BOOLEAN) AS x_serverless,
    CAST(${quoteIdent('x_Photon')} AS BOOLEAN) AS x_photon
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
, ec2_reference_prices AS (
  SELECT
    RegionId AS region_id,
    CAST(MIN(CAST(ListUnitPrice AS DOUBLE)) AS DOUBLE) AS ec2_hourly_price_usd
  FROM ${AWS_EC2_PRICING_TABLE_SQL}
  WHERE SkuPriceDetails['InstanceType'] = '${EC2_REFERENCE_INSTANCE_TYPE}'
    AND PricingCategory = 'Standard'
    AND SkuPriceDetails['OperatingSystem'] = 'Linux'
    AND CAST(ListUnitPrice AS DOUBLE) > 0
  GROUP BY RegionId
),
ec2_global_reference_price AS (
  SELECT CAST(MIN(ec2_hourly_price_usd) AS DOUBLE) AS ec2_hourly_price_usd
  FROM ec2_reference_prices
),
filtered_with_dbu AS (
  SELECT *,
    CASE
      WHEN x_serverless = false THEN
        CASE
          WHEN consumed_quantity IS NOT NULL THEN consumed_quantity
          WHEN list_cost_usd IS NOT NULL AND list_unit_price_usd > 0
            THEN list_cost_usd / list_unit_price_usd
          ELSE NULL
        END
      ELSE NULL
    END AS base_dbu
  FROM filtered
  WHERE resource_id IS NOT NULL
    AND TRIM(resource_id) <> ''
),
resource_metrics AS (
  SELECT
    workspace_id,
    MAX(workspace_name) AS workspace_name,
    MAX_BY(region_id, charge_period_start) AS region_id,
    service_category,
    service_name,
    resource_type,
    resource_id,
    MAX_BY(resource_name, charge_period_start) AS resource_name,
    MAX_BY(sku_id, charge_period_start) AS sku_id,
    MAX_BY(instance_type, charge_period_start) AS instance_type,
    CAST(SUM(cost_usd) AS DOUBLE) AS total_cost_usd,
    CAST(SUM(CASE WHEN x_serverless = true THEN cost_usd ELSE 0 END) AS DOUBLE) AS serverless_cost_usd,
    CAST(SUM(CASE WHEN x_serverless = false THEN cost_usd ELSE 0 END) AS DOUBLE) AS non_serverless_cost_usd,
    CAST(SUM(base_dbu) AS DOUBLE) AS dbu_quantity_estimate,
    CAST(SUM(base_dbu / CASE WHEN x_photon = true THEN 2.0 ELSE 1.0 END) AS DOUBLE) AS ec2_dbu_quantity_estimate
  FROM filtered_with_dbu
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
),
priced AS (
  SELECT
    ranked.*,
    COALESCE(regional_ref.ec2_hourly_price_usd, global_ref.ec2_hourly_price_usd) AS ec2_hourly_price_usd
  FROM ranked
    CROSS JOIN ec2_global_reference_price global_ref
    LEFT JOIN ec2_reference_prices regional_ref
      ON ranked.region_id = regional_ref.region_id
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
  sku_id,
  instance_type,
  total_cost_usd,
  non_serverless_cost_usd,
  dbu_quantity_estimate,
  CASE
    WHEN ec2_hourly_price_usd IS NOT NULL THEN '${EC2_REFERENCE_INSTANCE_TYPE}'
    ELSE CAST(NULL AS STRING)
  END AS ec2_reference_instance_type,
  CAST(ec2_hourly_price_usd AS DOUBLE) AS ec2_hourly_price_usd,
  CASE
    WHEN ec2_dbu_quantity_estimate IS NOT NULL AND ec2_hourly_price_usd IS NOT NULL
      THEN CAST(ec2_dbu_quantity_estimate * ec2_hourly_price_usd AS DOUBLE)
    ELSE CAST(NULL AS DOUBLE)
  END AS estimated_ec2_cost_usd,
  CASE
    WHEN ec2_dbu_quantity_estimate IS NOT NULL AND ec2_hourly_price_usd IS NOT NULL
      THEN CAST(non_serverless_cost_usd + ec2_dbu_quantity_estimate * ec2_hourly_price_usd AS DOUBLE)
    ELSE CAST(NULL AS DOUBLE)
  END AS estimated_current_total_cost_usd,
  CASE
    WHEN ${KNOWN_COST_DENOMINATOR} > 0
      THEN CAST(serverless_cost_usd * 100.0 / ${KNOWN_COST_DENOMINATOR} AS DOUBLE)
    ELSE CAST(NULL AS DOUBLE)
  END AS serverless_ratio
FROM priced
WHERE recommendation_rank <= 25
ORDER BY recommendation_rank
`;
}

function databricksOptimizeSource(
  catalog: string,
  silverSchema: string,
  source: Pick<DataSource, 'providerName' | 'tableName' | 'accountId'>,
): DatabricksOptimizeSource {
  const parts = catalog
    ? [catalog, silverSchema, source.tableName]
    : [silverSchema, source.tableName];
  const tableDisplay = parts.join('.');
  return {
    tableDisplay,
    tableSql: parts.map((part) => quoteIdent(part)).join('.'),
    billingAccountId: isDatabricksDefaultAccount(source) ? null : source.accountId,
  };
}
