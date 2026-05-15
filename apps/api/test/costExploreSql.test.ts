import assert from 'node:assert/strict';
import test from 'node:test';
import {
  buildCostExploreFilterValuesStatement,
  buildCostExploreStatement,
  type CostExploreCostMetric,
  type DataSource,
} from '@finlake/shared';

function fakeSource(overrides: Partial<DataSource> = {}): DataSource {
  const accountId = overrides.accountId ?? 'default';
  return {
    name: `source-${accountId}`,
    providerName: 'databricks',
    accountId,
    tableName: 'usage',
    focusVersion: null,
    enabled: true,
    config: {},
    updatedAt: new Date().toISOString(),
    ...overrides,
  };
}

const range = { start: '2026-05-01T00:00:00Z', end: '2026-06-01T00:00:00Z' };
const settings = { catalog_name: 'finops', gold_schema_name: 'gold' };

test('buildCostExploreStatement emits stable aliases for multiple group keys', () => {
  const statement = buildCostExploreStatement({
    sources: [fakeSource()],
    settings,
    range,
    groupBy: ['provider', 'serviceName', 'skuMeter'],
  });

  assert.ok(statement);
  assert.match(statement.query, /AS group_0\b/);
  assert.match(statement.query, /AS group_0_label\b/);
  assert.match(statement.query, /AS group_1\b/);
  assert.match(statement.query, /AS group_1_label\b/);
  assert.match(statement.query, /AS group_2\b/);
  assert.match(statement.query, /AS group_2_label\b/);
  assert.match(statement.query, /concat_ws\(' \/ ',/);
  assert.match(statement.query, /GROUP BY 1, 2, 3, 4, 5, 6, 7, 8/);
});

test('buildCostExploreStatement handles ungrouped queries', () => {
  const statement = buildCostExploreStatement({
    sources: [fakeSource()],
    settings,
    range,
    groupBy: [],
  });

  assert.ok(statement);
  assert.match(statement.query, /'Ungrouped' AS group_path/);
  assert.doesNotMatch(statement.query, /AS group_0\b/);
  assert.match(statement.query, /GROUP BY 1, 2/);
});

test('buildCostExploreStatement parameterizes include and exclude filters', () => {
  const statement = buildCostExploreStatement({
    sources: [fakeSource()],
    settings,
    range,
    groupBy: ['provider'],
    filters: {
      provider: { include: ['databricks'], exclude: ['aws'] },
      billingAccount: { include: ['123456789012'] },
      subAccount: { exclude: ['workspace-1'] },
    },
  });

  assert.ok(statement);
  assert.match(statement.query, /provider_include_0/);
  assert.match(statement.query, /provider_exclude_0/);
  assert.match(statement.query, /billingAccount_include_0/);
  assert.match(statement.query, /subAccount_exclude_0/);
  assert.equal(statement.params.find((p) => p.name === 'provider_include_0')?.value, 'databricks');
  assert.equal(statement.params.find((p) => p.name === 'provider_exclude_0')?.value, 'aws');
  assert.equal(
    statement.params.find((p) => p.name === 'billingAccount_include_0')?.value,
    '123456789012',
  );
  assert.equal(
    statement.params.find((p) => p.name === 'subAccount_exclude_0')?.value,
    'workspace-1',
  );
});

test('buildCostExploreStatement rejects unsupported cost metrics', () => {
  assert.throws(
    () =>
      buildCostExploreStatement({
        sources: [fakeSource()],
        settings,
        range,
        groupBy: ['provider'],
        costMetric: 'BadCost' as CostExploreCostMetric,
      }),
    /Unsupported cost metric/,
  );
});

test('buildCostExploreStatement emits expected date bucket expressions', () => {
  const weekly = buildCostExploreStatement({
    sources: [fakeSource()],
    settings,
    range,
    groupBy: ['provider'],
    dateGrain: 'weekly',
  });
  const monthly = buildCostExploreStatement({
    sources: [fakeSource()],
    settings,
    range,
    groupBy: ['provider'],
    dateGrain: 'monthly',
  });
  const quarterly = buildCostExploreStatement({
    sources: [fakeSource()],
    settings,
    range,
    groupBy: ['provider'],
    dateGrain: 'quarterly',
  });

  assert.match(weekly?.query ?? '', /date_trunc\('week', x_ChargeDate\)/);
  assert.match(monthly?.query ?? '', /date_trunc\('month', x_ChargeDate\)/);
  assert.match(quarterly?.query ?? '', /date_trunc\('quarter', x_ChargeDate\)/);
});

test('buildCostExploreFilterValuesStatement groups filter dimensions', () => {
  const statement = buildCostExploreFilterValuesStatement([fakeSource()], settings, range);

  assert.ok(statement);
  assert.match(statement.query, /AS provider/);
  assert.match(statement.query, /AS billing_account/);
  assert.match(statement.query, /AS sub_account/);
  assert.match(statement.query, /GROUP BY 1, 2, 3, 4, 5/);
});
