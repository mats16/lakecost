import assert from 'node:assert/strict';
import test from 'node:test';
import {
  awsUsageTableName,
  buildAwsFocusSilverPipelineSql,
} from '../src/services/awsFocusTransformPipelineSql.js';
import { buildUsageGoldSql } from '../src/services/dataSourceSetup.js';
import { buildFocusSilverPipelineSql } from '../src/services/databricksFocusTransformPipelineSql.js';
import {
  AWS_FOCUS_12_WITH_AWS_COLUMNS_QUERY_STATEMENT,
  MEDALLION_SCHEMA_DEFAULTS,
  medallionSchemaNamesFromSettings,
} from '@finlake/shared';

test('medallion schema defaults use FinLake schema names', () => {
  assert.deepEqual(MEDALLION_SCHEMA_DEFAULTS, {
    bronze: 'ingest',
    silver: 'focus',
    gold: 'analytics',
  });
  assert.deepEqual(medallionSchemaNamesFromSettings({}), MEDALLION_SCHEMA_DEFAULTS);
});

test('awsUsageTableName derives canonical AWS silver table name', () => {
  assert.equal(awsUsageTableName('123456789012'), 'aws_123456789012_usage');
});

test('awsUsageTableName rejects non-account identifiers', () => {
  assert.throws(() => awsUsageTableName('aws_usage'));
  assert.throws(() => awsUsageTableName('12345'));
});

test('buildAwsFocusSilverPipelineSql embeds source-specific values without gold rollup', () => {
  const sql = buildAwsFocusSilverPipelineSql({
    tableName: 'aws_123456789012_usage',
    s3Bucket: 'finlake-billing-123456789012',
    s3Prefix: 'exports/focus',
    exportName: 'finlake-focus-1-2',
  });

  assert.match(sql, /CREATE OR REFRESH MATERIALIZED VIEW `aws_123456789012_usage`/);
  assert.match(
    sql,
    /FROM read_files\(\s*'s3:\/\/finlake-billing-123456789012\/exports\/focus\/finlake-focus-1-2\/data\/\*\*\/\*\.parquet'/,
  );
  assert.match(
    sql,
    /`x_Discounts` MAP<STRING, DOUBLE> COMMENT 'AWS extension containing discount key-value pairs that apply to the line item\.'/,
  );
  assert.match(sql, /`x_Operation`,/);
  assert.match(sql, /`x_ServiceCode`/);
  assert.doesNotMatch(sql, /usage_daily/);
  assert.doesNotMatch(sql, /usage_monthly/);
  assert.doesNotMatch(sql, /gold_schema_name/);
});

test('AWS FOCUS data export query includes AWS extension columns', () => {
  assert.match(AWS_FOCUS_12_WITH_AWS_COLUMNS_QUERY_STATEMENT, /^SELECT /);
  assert.match(AWS_FOCUS_12_WITH_AWS_COLUMNS_QUERY_STATEMENT, /x_Discounts/);
  assert.match(AWS_FOCUS_12_WITH_AWS_COLUMNS_QUERY_STATEMENT, /x_Operation/);
  assert.match(AWS_FOCUS_12_WITH_AWS_COLUMNS_QUERY_STATEMENT, /x_ServiceCode/);
  assert.match(AWS_FOCUS_12_WITH_AWS_COLUMNS_QUERY_STATEMENT, / FROM FOCUS_1_2_AWS$/);
});

test('buildFocusSilverPipelineSql keeps Databricks SkuPriceDetails as a map', () => {
  const sql = buildFocusSilverPipelineSql({
    table: 'databricks_usage',
    accountPricesTable: 'system.billing.list_prices',
  });

  assert.match(sql, /CREATE OR REFRESH MATERIALIZED VIEW `databricks_usage` \(/);
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

test('buildUsageGoldSql unions source silver tables without FinLake metadata columns', () => {
  const sql = buildUsageGoldSql({
    catalog: 'finops',
    silverSchema: 'silver',
    goldSchema: 'gold',
    sources: [
      { tableName: 'databricks_usage', providerName: 'Databricks' },
      { tableName: 'aws_123456789012_usage', providerName: 'Amazon Web Services' },
    ],
  });

  assert.match(sql, /CREATE VIEW `usage`/);
  assert.match(sql, /`AvailabilityZone`,/);
  assert.match(sql, /`SkuPriceDetails`,/);
  assert.doesNotMatch(sql, /from_json\(CAST\(`SkuPriceDetails` AS STRING\)/);
  assert.doesNotMatch(sql, /CAST\(`AvailabilityZone` AS STRING\)/);
  assert.doesNotMatch(sql, /CAST\(`EffectiveCost` AS DOUBLE\)/);
  assert.doesNotMatch(sql, /`x_Discounts`/);
  assert.doesNotMatch(sql, /`x_Operation`/);
  assert.doesNotMatch(sql, /`x_ServiceCode`/);
  assert.match(sql, /CREATE OR REFRESH MATERIALIZED VIEW `gold`\.`usage_daily`/);
  assert.match(sql, /FROM `finops`\.`silver`\.`databricks_usage`/);
  assert.match(sql, /UNION ALL/);
  assert.match(sql, /FROM `finops`\.`silver`\.`aws_123456789012_usage`/);
  assert.match(sql, /FROM `silver`\.`usage`/);
  assert.doesNotMatch(sql, /SELECT \*/);
  assert.doesNotMatch(sql, /x_FinLakeDataSourceId/);
  assert.doesNotMatch(sql, /x_FinLakeDataSourceName/);
  assert.doesNotMatch(sql, /x_FinLakeSourceTable/);
});

test('buildUsageGoldSql emits resource-grain usage_monthly with latest Tags', () => {
  const sql = buildUsageGoldSql({
    catalog: 'finops',
    silverSchema: 'silver',
    goldSchema: 'gold',
    sources: [
      { tableName: 'databricks_usage', providerName: 'Databricks' },
      { tableName: 'aws_123456789012_usage', providerName: 'Amazon Web Services' },
    ],
  });

  assert.match(sql, /CREATE OR REFRESH MATERIALIZED VIEW `gold`\.`usage_monthly`/);
  assert.match(
    sql,
    /COMMENT 'FOCUS monthly usage rollup aggregated at the resource level[^']*latest Tags[^']*'/,
  );
  assert.match(sql, /`ResourceType`,/);
  assert.match(sql, /`ResourceId`,/);
  assert.match(sql, /`ResourceName`,/);
  assert.match(sql, /`Tags`,/);
  assert.match(sql, /MAX_BY\(Tags, ChargePeriodStart\) AS Tags/);
  assert.match(
    sql,
    /usage_monthly[\s\S]+?CAST\(DATE_TRUNC\('MONTH', BillingPeriodStart\) AS DATE\) AS x_BillingMonth/,
  );
  assert.doesNotMatch(sql, /usage_monthly[\s\S]+?x_ChargeDate/);
});
