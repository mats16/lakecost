import assert from 'node:assert/strict';
import test from 'node:test';
import {
  awsBillingTableName,
  buildAwsFocusSilverPipelineSql,
} from '../src/services/awsFocusTransformPipelineSql.js';
import { buildBillingDailyGoldSql } from '../src/services/dataSourceSetup.js';
import { buildFocusSilverPipelineSql } from '../src/services/databricksFocusTransformPipelineSql.js';

test('awsBillingTableName derives canonical AWS silver table name', () => {
  assert.equal(awsBillingTableName('123456789012'), 'aws_billing_123456789012');
});

test('awsBillingTableName rejects non-account identifiers', () => {
  assert.throws(() => awsBillingTableName('aws_billing'));
  assert.throws(() => awsBillingTableName('12345'));
});

test('buildAwsFocusSilverPipelineSql embeds source-specific values without gold rollup', () => {
  const sql = buildAwsFocusSilverPipelineSql({
    tableName: 'aws_billing_123456789012',
    s3Bucket: 'finlake-billing-123456789012',
    s3Prefix: 'exports/focus',
    exportName: 'finlake-focus-1-2',
  });

  assert.match(sql, /CREATE OR REFRESH MATERIALIZED VIEW `aws_billing_123456789012`/);
  assert.match(
    sql,
    /FROM read_files\(\s*'s3:\/\/finlake-billing-123456789012\/exports\/focus\/finlake-focus-1-2\/data\/\*\*\/\*\.parquet'/,
  );
  assert.doesNotMatch(sql, /billing_daily/);
  assert.doesNotMatch(sql, /gold_schema_name/);
});

test('buildFocusSilverPipelineSql keeps Databricks SkuPriceDetails as a map', () => {
  const sql = buildFocusSilverPipelineSql({
    table: 'databricks_billing',
    accountPricesTable: 'system.billing.list_prices',
  });

  assert.match(sql, /CREATE OR REFRESH MATERIALIZED VIEW `databricks_billing` \(/);
  assert.match(
    sql,
    /`BillingAccountId` STRING COMMENT 'Provider-assigned identifier of the billing account where the charge is invoiced\.'/,
  );
  assert.match(
    sql,
    /`EffectiveCost` DECIMAL\(30, 15\) COMMENT 'Amortized cost after rates, discounts, and applicable prepaid purchases in BillingCurrency\.'/,
  );
  assert.match(
    sql,
    /`SkuPriceDetails` MAP<STRING, STRING> COMMENT 'Properties of the SkuPriceId that are meaningful and common to that price identifier\.'/,
  );
  assert.match(sql, /map_from_entries\(/);
  assert.match(sql, /\) AS SkuPriceDetails/);
  assert.doesNotMatch(sql, /to_json\(\s*map_from_entries/);
  assert.doesNotMatch(sql, /HostProviderName/);
  assert.doesNotMatch(sql, /ServiceProviderName/);
});

test('buildBillingDailyGoldSql unions source silver tables without FinLake metadata columns', () => {
  const sql = buildBillingDailyGoldSql({
    catalog: 'finops',
    silverSchema: 'silver',
    goldSchema: 'gold',
    sources: [
      { tableName: 'databricks_billing', providerName: 'Databricks' },
      { tableName: 'aws_billing_123456789012', providerName: 'Amazon Web Services' },
    ],
  });

  assert.match(sql, /CREATE VIEW `billing`/);
  assert.match(sql, /`AvailabilityZone`,/);
  assert.match(sql, /`SkuPriceDetails`,/);
  assert.doesNotMatch(sql, /from_json\(CAST\(`SkuPriceDetails` AS STRING\)/);
  assert.doesNotMatch(sql, /CAST\(`AvailabilityZone` AS STRING\)/);
  assert.doesNotMatch(sql, /CAST\(`EffectiveCost` AS DOUBLE\)/);
  assert.doesNotMatch(sql, /`x_Discounts`/);
  assert.doesNotMatch(sql, /`x_Operation`/);
  assert.doesNotMatch(sql, /`x_ServiceCode`/);
  assert.match(sql, /CREATE OR REFRESH MATERIALIZED VIEW `gold`\.`billing_daily`/);
  assert.match(sql, /FROM `finops`\.`silver`\.`databricks_billing`/);
  assert.match(sql, /UNION ALL/);
  assert.match(sql, /FROM `finops`\.`silver`\.`aws_billing_123456789012`/);
  assert.match(sql, /FROM `silver`\.`billing`/);
  assert.doesNotMatch(sql, /SELECT \*/);
  assert.doesNotMatch(sql, /x_FinLakeDataSourceId/);
  assert.doesNotMatch(sql, /x_FinLakeDataSourceName/);
  assert.doesNotMatch(sql, /x_FinLakeSourceTable/);
});
