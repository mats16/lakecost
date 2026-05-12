import assert from 'node:assert/strict';
import test from 'node:test';
import { requestedSourcesSql, baseParams, joinedBillingRowsSql } from '../src/routes/overview.js';
import type { DataSource } from '@finlake/shared';

function fakeSource(overrides: Partial<DataSource> & { id: number }): DataSource {
  return {
    templateId: 'test',
    name: `source-${overrides.id}`,
    providerName: 'Databricks',
    billingAccountId: null,
    tableName: 'usage',
    focusVersion: null,
    enabled: true,
    config: {},
    updatedAt: new Date().toISOString(),
    ...overrides,
  };
}

test('requestedSourcesSql generates UNION ALL for multiple sources', () => {
  const sources = [fakeSource({ id: 1 }), fakeSource({ id: 2 })];
  const sql = requestedSourcesSql(sources);
  assert.ok(sql.includes(':data_source_id_0'));
  assert.ok(sql.includes(':data_source_id_1'));
  assert.ok(sql.includes('UNION ALL'));
});

test('requestedSourcesSql generates single SELECT for one source', () => {
  const sources = [fakeSource({ id: 1 })];
  const sql = requestedSourcesSql(sources);
  assert.ok(sql.includes(':data_source_id_0'));
  assert.ok(!sql.includes('UNION ALL'));
});

test('baseParams includes time range and per-source params', () => {
  const sources = [
    fakeSource({ id: 1, providerName: 'Databricks', billingAccountId: null }),
    fakeSource({ id: 2, providerName: 'AWS', billingAccountId: '123456789012' }),
  ];
  const range = { start: '2025-01-01T00:00:00Z', end: '2025-02-01T00:00:00Z' };
  const params = baseParams(sources, range);

  const names = params.map((p) => p.name);
  assert.ok(names.includes('start_ts'));
  assert.ok(names.includes('end_ts'));
  assert.ok(names.includes('data_source_id_0'));
  assert.ok(names.includes('provider_name_0'));
  assert.ok(names.includes('billing_account_id_0'));
  assert.ok(names.includes('data_source_id_1'));
  assert.ok(names.includes('billing_account_id_1'));

  const billingParam = params.find((p) => p.name === 'billing_account_id_1');
  assert.equal(billingParam?.value, '123456789012');
  const nullBillingParam = params.find((p) => p.name === 'billing_account_id_0');
  assert.equal(nullBillingParam?.value, null);
});

test('joinedBillingRowsSql generates CTE with requested + matched', () => {
  const sources = [fakeSource({ id: 1, billingAccountId: '123456789012' })];
  const sql = joinedBillingRowsSql(sources, '`catalog`.`gold`.`usage_daily`');

  assert.ok(sql.includes('WITH requested AS'));
  assert.ok(sql.includes('matched AS'));
  assert.ok(sql.includes('`catalog`.`gold`.`usage_daily`'));
  assert.ok(sql.includes('r.billing_account_id IS NOT NULL'));
  assert.ok(sql.includes('r.billing_account_id IS NULL'));
});
