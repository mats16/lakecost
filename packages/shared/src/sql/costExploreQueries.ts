import type { DataSource } from '../schemas/dataSource.js';
import type { SqlParam } from '../schemas/sql.js';
import type { UsageRange } from '../schemas/usage.js';
import {
  baseParams,
  joinedBillingRowsSql,
  usageTableName,
  type SqlStatementInput,
} from './overviewQueries.js';

export const COST_EXPLORE_GROUP_KEYS = [
  'provider',
  'billingAccount',
  'subAccount',
  'serviceCategory',
  'serviceSubcategory',
  'serviceName',
  'skuId',
  'skuMeter',
] as const;

export type CostExploreGroupKey = (typeof COST_EXPLORE_GROUP_KEYS)[number];

export const COST_EXPLORE_FILTER_KEYS = ['provider', 'billingAccount', 'subAccount'] as const;
export type CostExploreFilterKey = (typeof COST_EXPLORE_FILTER_KEYS)[number];

export const COST_EXPLORE_COST_METRICS = [
  'EffectiveCost',
  'ListCost',
  'BilledCost',
  'ContractedCost',
] as const;

export type CostExploreCostMetric = (typeof COST_EXPLORE_COST_METRICS)[number];

export const COST_EXPLORE_DATE_GRAINS = ['daily', 'weekly', 'monthly', 'quarterly'] as const;
export type CostExploreDateGrain = (typeof COST_EXPLORE_DATE_GRAINS)[number];

export interface CostExploreFilterSelection {
  include?: string[];
  exclude?: string[];
}

export type CostExploreFilters = Partial<Record<CostExploreFilterKey, CostExploreFilterSelection>>;

export interface CostExploreStatementOptions {
  sources: DataSource[];
  settings: Record<string, string | undefined>;
  range: UsageRange;
  groupBy: CostExploreGroupKey[];
  filters?: CostExploreFilters;
  costMetric?: CostExploreCostMetric;
  dateGrain?: CostExploreDateGrain;
}

interface GroupField {
  valueSql: string;
  labelSql: string;
}

const GROUP_FIELDS: Record<CostExploreGroupKey, GroupField> = {
  provider: {
    valueSql: "COALESCE(NULLIF(TRIM(ProviderName), ''), source_provider_name, 'Unknown')",
    labelSql: "COALESCE(NULLIF(TRIM(ProviderName), ''), source_provider_name, 'Unknown')",
  },
  billingAccount: {
    valueSql: "COALESCE(NULLIF(TRIM(BillingAccountId), ''), 'Unknown')",
    labelSql:
      "COALESCE(NULLIF(TRIM(BillingAccountName), ''), NULLIF(TRIM(BillingAccountId), ''), 'Unknown')",
  },
  subAccount: {
    valueSql: "COALESCE(NULLIF(TRIM(SubAccountId), ''), 'Unknown')",
    labelSql:
      "COALESCE(NULLIF(TRIM(SubAccountName), ''), NULLIF(TRIM(SubAccountId), ''), 'Unknown')",
  },
  serviceCategory: {
    valueSql: "COALESCE(NULLIF(TRIM(ServiceCategory), ''), 'Unknown')",
    labelSql: "COALESCE(NULLIF(TRIM(ServiceCategory), ''), 'Unknown')",
  },
  serviceSubcategory: {
    valueSql: "COALESCE(NULLIF(TRIM(ServiceSubcategory), ''), 'Unknown')",
    labelSql: "COALESCE(NULLIF(TRIM(ServiceSubcategory), ''), 'Unknown')",
  },
  serviceName: {
    valueSql: "COALESCE(NULLIF(TRIM(ServiceName), ''), 'Unknown')",
    labelSql: "COALESCE(NULLIF(TRIM(ServiceName), ''), 'Unknown')",
  },
  skuId: {
    valueSql: "COALESCE(NULLIF(TRIM(SkuId), ''), 'Unknown')",
    labelSql: "COALESCE(NULLIF(TRIM(SkuId), ''), 'Unknown')",
  },
  skuMeter: {
    valueSql: "COALESCE(NULLIF(TRIM(SkuMeter), ''), 'Unknown')",
    labelSql: "COALESCE(NULLIF(TRIM(SkuMeter), ''), 'Unknown')",
  },
};

const FILTER_FIELDS: Record<CostExploreFilterKey, string> = {
  provider: GROUP_FIELDS.provider.valueSql,
  billingAccount: GROUP_FIELDS.billingAccount.valueSql,
  subAccount: GROUP_FIELDS.subAccount.valueSql,
};

export function buildCostExploreStatement({
  sources,
  settings,
  range,
  groupBy,
  filters = {},
  costMetric = 'EffectiveCost',
  dateGrain = 'daily',
}: CostExploreStatementOptions): SqlStatementInput | null {
  if (sources.length === 0) return null;
  assertCostMetric(costMetric);
  const cte = joinedBillingRowsSql(sources, usageTableName('daily', settings).sql);
  const groupKeys = normalizeGroupKeys(groupBy);
  const { whereSql, params } = buildFilterSql(filters);
  const periodSql = dateBucketSql(dateGrain);
  const selectGroupSql = groupKeys.flatMap((key, index) => {
    const field = GROUP_FIELDS[key];
    return [`${field.valueSql} AS group_${index}`, `${field.labelSql} AS group_${index}_label`];
  });
  const groupPathSql =
    groupKeys.length === 0
      ? "'Ungrouped'"
      : `concat_ws(' / ', ${groupKeys.map((key) => GROUP_FIELDS[key].labelSql).join(', ')})`;
  const selectItems = [
    `${periodSql} AS period_start`,
    `${groupPathSql} AS group_path`,
    ...selectGroupSql,
    `CAST(SUM(COALESCE(${costMetric}, 0)) AS DOUBLE) AS cost_usd`,
  ];
  const groupByIndexes = Array.from({ length: selectItems.length - 1 }, (_, i) => String(i + 1));

  return {
    query: /* sql */ `
${cte}
SELECT
  ${selectItems.join(',\n  ')}
FROM matched
WHERE CAST(x_ChargeDate AS TIMESTAMP) >= :start_ts
  AND CAST(x_ChargeDate AS TIMESTAMP) <  :end_ts
  ${whereSql}
GROUP BY ${groupByIndexes.join(', ')}
ORDER BY 1, ${selectItems.length} DESC
`,
    params: [...baseParams(sources, range), ...params],
  };
}

export function buildCostExploreFilterValuesStatement(
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
  ${GROUP_FIELDS.provider.valueSql} AS provider,
  ${GROUP_FIELDS.billingAccount.valueSql} AS billing_account,
  ${GROUP_FIELDS.billingAccount.labelSql} AS billing_account_label,
  ${GROUP_FIELDS.subAccount.valueSql} AS sub_account,
  ${GROUP_FIELDS.subAccount.labelSql} AS sub_account_label,
  CAST(SUM(COALESCE(EffectiveCost, 0)) AS DOUBLE) AS cost_usd
FROM matched
WHERE CAST(x_ChargeDate AS TIMESTAMP) >= :start_ts
  AND CAST(x_ChargeDate AS TIMESTAMP) <  :end_ts
GROUP BY 1, 2, 3, 4, 5
ORDER BY 6 DESC
`,
    params: baseParams(sources, range),
  };
}

function normalizeGroupKeys(groupBy: CostExploreGroupKey[]): CostExploreGroupKey[] {
  const seen = new Set<CostExploreGroupKey>();
  return groupBy.filter((key) => {
    if (!COST_EXPLORE_GROUP_KEYS.includes(key) || seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

function assertCostMetric(metric: CostExploreCostMetric): void {
  if (!COST_EXPLORE_COST_METRICS.includes(metric)) {
    throw new Error(`Unsupported cost metric: ${metric}`);
  }
}

function dateBucketSql(grain: CostExploreDateGrain): string {
  switch (grain) {
    case 'daily':
      return "date_format(x_ChargeDate, 'yyyy-MM-dd')";
    case 'weekly':
      return "date_format(date_trunc('week', x_ChargeDate), 'yyyy-MM-dd')";
    case 'monthly':
      return "date_format(date_trunc('month', x_ChargeDate), 'yyyy-MM-dd')";
    case 'quarterly':
      return "date_format(date_trunc('quarter', x_ChargeDate), 'yyyy-MM-dd')";
  }
}

function buildFilterSql(filters: CostExploreFilters): { whereSql: string; params: SqlParam[] } {
  const clauses: string[] = [];
  const params: SqlParam[] = [];

  for (const key of COST_EXPLORE_FILTER_KEYS) {
    const fieldSql = FILTER_FIELDS[key];
    const include = normalizedFilterValues(filters[key]?.include);
    const exclude = normalizedFilterValues(filters[key]?.exclude);

    if (include.length > 0) {
      const names = include.map((value, index) => {
        const name = `${key}_include_${index}`;
        params.push({ name, value, type: 'STRING' });
        return `:${name}`;
      });
      clauses.push(`${fieldSql} IN (${names.join(', ')})`);
    }

    if (exclude.length > 0) {
      const names = exclude.map((value, index) => {
        const name = `${key}_exclude_${index}`;
        params.push({ name, value, type: 'STRING' });
        return `:${name}`;
      });
      clauses.push(`${fieldSql} NOT IN (${names.join(', ')})`);
    }
  }

  return {
    whereSql: clauses.length === 0 ? '' : `AND ${clauses.join('\n  AND ')}`,
    params,
  };
}

function normalizedFilterValues(values: string[] | undefined): string[] {
  if (!values) return [];
  const seen = new Set<string>();
  const normalized: string[] = [];
  for (const value of values) {
    const trimmed = value.trim();
    if (!trimmed || seen.has(trimmed)) continue;
    seen.add(trimmed);
    normalized.push(trimmed);
  }
  return normalized;
}
