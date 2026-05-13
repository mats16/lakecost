import assert from 'node:assert/strict';
import test from 'node:test';
import { DatabricksOptimizationResponseSchema } from '@finlake/shared';
import {
  buildDatabricksOptimizeCte,
  buildDatabricksRecommendationsSql,
  buildDatabricksServicesSql,
  buildDatabricksSummarySql,
  buildDatabricksTrendSql,
  buildDatabricksWorkspacesSql,
  databricksOptimizeParams,
  resolveDatabricksOptimizeSources,
  type DatabricksOptimizeSource,
} from '@finlake/shared';

const sources: DatabricksOptimizeSource[] = [
  {
    tableDisplay: 'finops.focus.databricks_usage',
    tableSql: '`finops`.`focus`.`databricks_usage`',
    billingAccountId: 'abc-123',
  },
];

test('buildDatabricksOptimizeCte reads x_Serverless and EffectiveCost from quoted source table', () => {
  const sql = buildDatabricksOptimizeCte(sources);

  assert.match(sql, /FROM `finops`\.`focus`\.`databricks_usage`/);
  assert.match(sql, /CAST\(COALESCE\(EffectiveCost, 0\) AS DOUBLE\) AS cost_usd/);
  assert.match(sql, /CAST\(`x_Serverless` AS BOOLEAN\) AS x_serverless/);
  assert.match(sql, /NULLIF\(TRIM\(SkuPriceDetails\['InstanceType'\]\), ''\) AS instance_type/);
  assert.match(sql, /ProviderName = 'Databricks'/);
  assert.match(sql, /CAST\(ChargePeriodStart AS TIMESTAMP\) >= :start_ts/);
  assert.match(sql, /CAST\(ChargePeriodStart AS TIMESTAMP\) < :end_ts/);
  assert.match(sql, /charge_period_start >= :start_ts/);
  assert.match(sql, /charge_period_start < :end_ts/);
  assert.doesNotMatch(sql, /x_BillingMonth/);
  assert.doesNotMatch(sql, /finops\.focus\.databricks_usage/);
});

test('resolveDatabricksOptimizeSources follows overview catalog defaults', () => {
  const [withoutCatalog] = resolveDatabricksOptimizeSources([], {});
  assert.equal(withoutCatalog?.tableDisplay, 'focus.databricks_usage');
  assert.equal(withoutCatalog?.tableSql, '`focus`.`databricks_usage`');

  const [withCatalog] = resolveDatabricksOptimizeSources([], { catalog_name: 'finops' });
  assert.equal(withCatalog?.tableDisplay, 'finops.focus.databricks_usage');
  assert.equal(withCatalog?.tableSql, '`finops`.`focus`.`databricks_usage`');
});

test('databricks optimization params include date, workspace, and billing account filters', () => {
  const params = databricksOptimizeParams(sources, {
    start: '2026-01-01T00:00:00.000Z',
    end: '2026-02-01T00:00:00.000Z',
    workspaceId: '123456789',
  });

  assert.equal(params.find((p) => p.name === 'start_ts')?.value, '2026-01-01T00:00:00.000Z');
  assert.equal(params.find((p) => p.name === 'end_ts')?.value, '2026-02-01T00:00:00.000Z');
  assert.equal(params.find((p) => p.name === 'workspace_id')?.value, '123456789');
  assert.equal(params.find((p) => p.name === 'billing_account_id_0')?.value, 'abc-123');
});

test('buildDatabricksSummarySql separates serverless, non-serverless, and unknown cost', () => {
  const sql = buildDatabricksSummarySql('-- cte --');

  assert.match(sql, /x_serverless = true/);
  assert.match(sql, /x_serverless = false/);
  assert.match(sql, /x_serverless IS NULL/);
  assert.match(sql, /candidate_resource_count/);
  assert.match(
    sql,
    /serverless_cost_usd \* 100\.0 \/ \(serverless_cost_usd \+ non_serverless_cost_usd\)/,
  );
});

test('Databricks ratio SQL excludes unknown x_Serverless cost from denominator', () => {
  const sql = [
    buildDatabricksSummarySql('-- cte --'),
    buildDatabricksWorkspacesSql('-- cte --'),
    buildDatabricksTrendSql('-- cte --', 'month'),
    buildDatabricksServicesSql('-- cte --'),
    buildDatabricksRecommendationsSql('-- cte --'),
  ].join('\n');

  const ratioExpressions = sql.match(
    /serverless_cost_usd \* 100\.0 \/ \(serverless_cost_usd \+ non_serverless_cost_usd\)/g,
  );
  assert.equal(ratioExpressions?.length, 5);
  assert.doesNotMatch(sql, /serverless_cost_usd \+ non_serverless_cost_usd \+ unknown_cost_usd/);
});

test('buildDatabricksMonthlySql groups trends by month', () => {
  const sql = buildDatabricksTrendSql('-- cte --', 'month');

  assert.match(
    sql,
    /date_format\(date_trunc\('MONTH', charge_period_start\), 'yyyy-MM'\) AS period/,
  );
  assert.match(
    sql,
    /GROUP BY date_format\(date_trunc\('MONTH', charge_period_start\), 'yyyy-MM'\)/,
  );
  assert.match(sql, /ORDER BY period/);
});

test('buildDatabricksTrendSql groups 30 day trends by day', () => {
  const sql = buildDatabricksTrendSql('-- cte --', 'day');

  assert.match(
    sql,
    /date_format\(date_trunc\('DAY', charge_period_start\), 'yyyy-MM-dd'\) AS period/,
  );
  assert.match(
    sql,
    /GROUP BY date_format\(date_trunc\('DAY', charge_period_start\), 'yyyy-MM-dd'\)/,
  );
  assert.match(sql, /ORDER BY period/);
});

test('buildDatabricksServicesSql only returns target serverless migration services', () => {
  const sql = buildDatabricksServicesSql('-- cte --');

  assert.match(
    sql,
    /WHERE service_name IN \('SQL', 'ALL_PURPOSE', 'INTERACTIVE', 'DLT', 'JOBS', 'LAKEFLOW_CONNECT'\)/,
  );
  assert.match(
    sql,
    /WHEN service_name IN \('ALL_PURPOSE', 'INTERACTIVE'\) THEN 'INTERACTIVE \/ ALL_PURPOSE'/,
  );
  assert.match(sql, /WHEN 'INTERACTIVE \/ ALL_PURPOSE' THEN 2/);
  assert.doesNotMatch(sql, /LIMIT 50/);
});

test('buildDatabricksRecommendationsSql excludes blank resources and uses eligibility weighting', () => {
  const sql = buildDatabricksRecommendationsSql('-- cte --');

  assert.match(sql, /resource_id IS NOT NULL/);
  assert.match(sql, /TRIM\(resource_id\) <> ''/);
  assert.match(sql, /service_name IN \('SQL', 'JOBS', 'DLT'\) THEN 1\.35/);
  assert.match(sql, /MAX_BY\(instance_type, charge_period_start\) AS instance_type/);
  assert.match(sql, /ORDER BY non_serverless_cost_usd \* eligibility_weight DESC/);
  assert.match(sql, /WHERE recommendation_rank <= 25/);
});

test('DatabricksOptimizationResponseSchema parses API response shape', () => {
  const response = DatabricksOptimizationResponseSchema.parse({
    summary: {
      totalCostUsd: 1200,
      serverlessCostUsd: 700,
      nonServerlessCostUsd: 400,
      unknownCostUsd: 100,
      serverlessRatio: 63.6,
      candidateResourceCount: 2,
    },
    workspaces: [
      {
        workspaceId: '123',
        workspaceName: 'workspace-a',
        totalCostUsd: 1200,
        serverlessCostUsd: 700,
        nonServerlessCostUsd: 400,
        serverlessRatio: 63.6,
      },
    ],
    monthly: [
      {
        month: '2026-01',
        totalCostUsd: 1200,
        serverlessCostUsd: 700,
        nonServerlessCostUsd: 400,
        unknownCostUsd: 100,
        serverlessRatio: 63.6,
      },
    ],
    services: [
      {
        serviceCategory: 'Analytics',
        serviceName: 'SQL',
        totalCostUsd: 1200,
        serverlessCostUsd: 700,
        nonServerlessCostUsd: 400,
        serverlessRatio: 63.6,
      },
    ],
    recommendations: [
      {
        rank: 1,
        priority: 'high',
        workspaceId: '123',
        workspaceName: 'workspace-a',
        serviceCategory: 'Analytics',
        serviceName: 'SQL',
        resourceType: 'SQL Warehouse',
        resourceId: 'warehouse-1',
        resourceName: 'BI Warehouse',
        instanceType: '2X-Small',
        totalCostUsd: 800,
        nonServerlessCostUsd: 500,
        serverlessRatio: 37.5,
      },
    ],
    errors: [],
    generatedAt: '2026-01-31T00:00:00.000Z',
  });

  assert.equal(response.recommendations[0]?.resourceId, 'warehouse-1');
});
