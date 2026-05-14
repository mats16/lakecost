import assert from 'node:assert/strict';
import test from 'node:test';
import {
  requestedSourcesSql,
  baseParams,
  buildCoverageSql,
  buildDailySql,
  joinedBillingRowsSql,
} from '@finlake/shared';
import type { DataSource } from '@finlake/shared';

function fakeSource(overrides: Partial<DataSource> = {}): DataSource {
  const accountId = overrides.accountId ?? 'default';
  return {
    name: `source-${accountId}`,
    providerName: 'Databricks',
    accountId,
    tableName: 'usage',
    focusVersion: null,
    enabled: true,
    config: {},
    updatedAt: new Date().toISOString(),
    ...overrides,
  };
}

test('requestedSourcesSql generates UNION ALL for multiple sources', () => {
  const sources = [fakeSource(), fakeSource({ accountId: '123456789012' })];
  const sql = requestedSourcesSql(sources);
  assert.ok(sql.includes(':data_source_id_0'));
  assert.ok(sql.includes(':data_source_id_1'));
  assert.ok(sql.includes('UNION ALL'));
});

test('requestedSourcesSql generates single SELECT for one source', () => {
  const sources = [fakeSource()];
  const sql = requestedSourcesSql(sources);
  assert.ok(sql.includes(':data_source_id_0'));
  assert.ok(!sql.includes('UNION ALL'));
});

test('baseParams includes time range and per-source params', () => {
  const sources = [
    fakeSource({ providerName: 'Databricks', accountId: 'default' }),
    fakeSource({ providerName: 'AWS', accountId: '123456789012' }),
  ];
  const range = { start: '2025-01-01T00:00:00Z', end: '2025-02-01T00:00:00Z' };
  const params = baseParams(sources, range);

  const names = params.map((p) => p.name);
  assert.ok(names.includes('start_ts'));
  assert.ok(names.includes('end_ts'));
  assert.ok(names.includes('data_source_id_0'));
  assert.ok(names.includes('provider_name_0'));
  assert.ok(names.includes('account_id_0'));
  assert.ok(names.includes('data_source_id_1'));
  assert.ok(names.includes('account_id_1'));

  const billingParam = params.find((p) => p.name === 'account_id_1');
  assert.equal(billingParam?.value, '123456789012');
  const nullBillingParam = params.find((p) => p.name === 'account_id_0');
  assert.equal(nullBillingParam?.value, null);
});

test('joinedBillingRowsSql generates CTE with requested + matched', () => {
  const sources = [fakeSource({ accountId: '123456789012' })];
  const sql = joinedBillingRowsSql(sources, '`catalog`.`gold`.`usage_daily`');

  assert.ok(sql.includes('WITH requested AS'));
  assert.ok(sql.includes('matched AS'));
  assert.ok(sql.includes('`catalog`.`gold`.`usage_daily`'));
  assert.ok(sql.includes('r.account_id IS NOT NULL'));
  assert.ok(sql.includes('r.account_id IS NULL'));
});

test('buildDailySql preserves all months and groups by 5 dimensions', () => {
  const sql = buildDailySql('-- cte --');

  assert.ok(sql.includes('-- cte --'));
  assert.ok(sql.includes('service_category'));
  assert.ok(sql.includes('service_name'));
  assert.ok(sql.includes('GROUP BY 1, 2, 3, 4, 5'));
  assert.ok(sql.includes('ORDER BY 2'));
  assert.ok(
    !/LIMIT\s+\d+/i.test(sql),
    'queryDaily must not LIMIT — would silently drop older months',
  );
});

test('buildCoverageSql filters by per-source latest billing month', () => {
  const sql = buildCoverageSql('-- cte --');

  assert.ok(sql.includes('-- cte --'));
  assert.ok(sql.includes('resources AS'));
  assert.ok(sql.includes('latest_month_per_source AS'));
  assert.ok(sql.includes('GROUP BY data_source_id'));
  assert.ok(sql.includes('JOIN latest_month_per_source'));
  assert.ok(sql.includes('r.data_source_id = lm.data_source_id'));
  assert.ok(sql.includes('r.x_BillingMonth = lm.max_month'));
  assert.ok(
    !sql.includes('WHERE r.x_BillingMonth = (SELECT MAX(x_BillingMonth) FROM resources)'),
    'cross-source MAX would drop data sources whose latest month lags behind',
  );
});
