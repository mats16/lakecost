import assert from 'node:assert/strict';
import test from 'node:test';
import {
  DataSourceKeySchema,
  DEFAULT_DATABRICKS_ACCOUNT_ID,
  PROVIDER_AWS,
  PROVIDER_DATABRICKS,
  dataSourceKeyString,
  isAwsProvider,
  isDatabricksDefaultAccount,
  isDatabricksProvider,
  normalizeProviderName,
  toDataSourceKey,
} from '@finlake/shared';

test('normalizeProviderName collapses Databricks variants to lowercase slug', () => {
  assert.equal(normalizeProviderName('Databricks'), PROVIDER_DATABRICKS);
  assert.equal(normalizeProviderName('databricks'), PROVIDER_DATABRICKS);
  assert.equal(normalizeProviderName('  DATABRICKS '), PROVIDER_DATABRICKS);
});

test('normalizeProviderName collapses AWS display names', () => {
  assert.equal(normalizeProviderName('AWS'), PROVIDER_AWS);
  assert.equal(normalizeProviderName('aws'), PROVIDER_AWS);
  assert.equal(normalizeProviderName('Amazon Web Services'), PROVIDER_AWS);
  assert.equal(normalizeProviderName('amazon web services'), PROVIDER_AWS);
});

test('normalizeProviderName trims unknown providers without lowercasing', () => {
  assert.equal(normalizeProviderName(' Azure '), 'Azure');
  assert.equal(normalizeProviderName('GCP'), 'GCP');
});

test('isDatabricksProvider and isAwsProvider accept legacy display names', () => {
  assert.ok(isDatabricksProvider('Databricks'));
  assert.ok(isDatabricksProvider('databricks'));
  assert.ok(!isDatabricksProvider('AWS'));

  assert.ok(isAwsProvider('AWS'));
  assert.ok(isAwsProvider('Amazon Web Services'));
  assert.ok(!isAwsProvider('Databricks'));
});

test('isDatabricksDefaultAccount only true for the synthetic default key', () => {
  assert.ok(
    isDatabricksDefaultAccount({
      providerName: PROVIDER_DATABRICKS,
      accountId: DEFAULT_DATABRICKS_ACCOUNT_ID,
    }),
  );
  assert.ok(!isDatabricksDefaultAccount({ providerName: PROVIDER_DATABRICKS, accountId: 'other' }));
  assert.ok(
    !isDatabricksDefaultAccount({
      providerName: PROVIDER_AWS,
      accountId: DEFAULT_DATABRICKS_ACCOUNT_ID,
    }),
  );
});

test('toDataSourceKey and dataSourceKeyString round-trip the composite PK', () => {
  const key = toDataSourceKey({ providerName: PROVIDER_AWS, accountId: '123456789012' });
  assert.deepEqual(key, { providerName: PROVIDER_AWS, accountId: '123456789012' });
  assert.equal(dataSourceKeyString(key), 'aws:123456789012');
});

test('DataSourceKeySchema normalizes mixed-case providerName', () => {
  const parsed = DataSourceKeySchema.parse({ providerName: 'Databricks', accountId: 'default' });
  assert.equal(parsed.providerName, PROVIDER_DATABRICKS);
  assert.equal(parsed.accountId, 'default');
});

test('DataSourceKeySchema rejects empty accountId', () => {
  const result = DataSourceKeySchema.safeParse({ providerName: 'aws', accountId: '' });
  assert.equal(result.success, false);
});
