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

const databricksSilverSql = buildFocusSilverPipelineSql({
  table: 'databricks_usage',
  accountPricesTable: 'system.billing.list_prices',
});

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
  const sql = databricksSilverSql;

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
  assert.match(
    sql,
    /`x_Serverless` BOOLEAN COMMENT 'Databricks extension indicating whether the usage was serverless\.'/,
  );
  assert.match(
    sql,
    /`x_Photon` BOOLEAN COMMENT 'Databricks extension indicating whether the usage used Photon\.'/,
  );
  assert.match(
    sql,
    /`x_NodeType` STRING COMMENT 'Databricks extension containing the node type from usage metadata when available\.'/,
  );
  assert.match(sql, /map_from_entries\(/);
  assert.match(sql, /map_concat\(/);
  assert.match(sql, /\) AS SkuPriceDetails/);
  assert.match(
    sql,
    /named_struct\('key', 'InstanceType', 'value', CAST\(u\.usage_metadata\.node_type AS STRING\)\)/,
  );
  assert.match(sql, /named_struct\(\s*'key',\s*'InstanceSeries',\s*'value',/);
  assert.match(sql, /THEN split\(CAST\(u\.usage_metadata\.node_type AS STRING\), '\\\\\.'\)\[0\]/);
  assert.match(
    sql,
    /-- TODO: Add Azure node type normalization once the desired series format is defined\./,
  );
  assert.match(sql, /kv -> kv\.value IS NOT NULL/);
  assert.match(sql, /CAST\(u\.product_features\.is_serverless AS BOOLEAN\) AS x_Serverless/);
  assert.match(sql, /CAST\(u\.product_features\.is_photon AS BOOLEAN\) AS x_Photon/);
  assert.match(sql, /CAST\(u\.usage_metadata\.node_type AS STRING\) AS x_NodeType/);
  assert.doesNotMatch(sql, /to_json\(\s*map_from_entries/);
  assert.doesNotMatch(sql, /HostProviderName/);
  assert.doesNotMatch(sql, /ServiceProviderName/);
});

test('buildFocusSilverPipelineSql falls back to dlt_pipeline_id for SQL/VECTOR_SEARCH without warehouse/endpoint id', () => {
  const sql = databricksSilverSql;

  assert.match(
    sql,
    /u\.billing_origin_product = 'SQL'\s+THEN COALESCE\(u\.usage_metadata\.warehouse_id, u\.usage_metadata\.dlt_pipeline_id\)/,
  );
  assert.match(
    sql,
    /u\.billing_origin_product = 'VECTOR_SEARCH'\s+THEN COALESCE\(u\.usage_metadata\.endpoint_id, u\.usage_metadata\.dlt_pipeline_id\)/,
  );

  assert.match(
    sql,
    /u\.billing_origin_product = 'SQL'\s+THEN COALESCE\(\s+u\.warehouse_name,\s+u\.usage_metadata\.warehouse_id,\s+u\.pipeline_name,\s+u\.usage_metadata\.dlt_pipeline_id\s+\)/,
  );
  assert.match(
    sql,
    /u\.billing_origin_product = 'VECTOR_SEARCH'\s+THEN COALESCE\(\s+u\.usage_metadata\.endpoint_name,\s+u\.usage_metadata\.endpoint_id,\s+u\.pipeline_name,\s+u\.usage_metadata\.dlt_pipeline_id\s+\)/,
  );

  assert.match(
    sql,
    /u\.billing_origin_product = 'SQL' THEN\s+CASE WHEN u\.usage_metadata\.warehouse_id IS NOT NULL\s+THEN 'SQL Warehouse'\s+ELSE 'Spark Declarative Pipeline'/,
  );
  assert.match(
    sql,
    /u\.billing_origin_product = 'VECTOR_SEARCH' THEN\s+CASE WHEN u\.usage_metadata\.endpoint_id IS NOT NULL\s+THEN 'Vector Search Endpoint'\s+ELSE 'Spark Declarative Pipeline'/,
  );

  // VECTOR_SEARCH is no longer in the generic endpoint_id catch-all
  assert.doesNotMatch(
    sql,
    /IN \('MODEL_SERVING', 'AI_FUNCTIONS', 'VECTOR_SEARCH', 'AI_GATEWAY', 'LAKEBASE'\)/,
  );
  assert.doesNotMatch(
    sql,
    /IN \('MODEL_SERVING', 'AI_GATEWAY', 'AI_FUNCTIONS', 'VECTOR_SEARCH', 'LAKEBASE'\)/,
  );

  // The old sku_name LIKE branches are gone
  assert.doesNotMatch(sql, /u\.sku_name LIKE '%_JOBS_SERVERLESS_COMPUTE%'/);
  assert.doesNotMatch(sql, /u\.sku_name LIKE 'ENTERPRISE_SERVERLESS_SQL_COMPUTE%'/);
});

test('buildFocusSilverPipelineSql falls back to cluster_id for MODEL_SERVING without endpoint_id', () => {
  const sql = databricksSilverSql;

  assert.match(
    sql,
    /u\.billing_origin_product = 'MODEL_SERVING'\s+THEN COALESCE\(u\.usage_metadata\.endpoint_id, u\.usage_metadata\.cluster_id\)/,
  );
  assert.match(
    sql,
    /u\.billing_origin_product = 'MODEL_SERVING'\s+THEN COALESCE\(\s+u\.usage_metadata\.endpoint_name,\s+u\.usage_metadata\.endpoint_id,\s+u\.cluster_name,\s+u\.usage_metadata\.cluster_id\s+\)/,
  );
  assert.match(
    sql,
    /u\.billing_origin_product = 'MODEL_SERVING' THEN\s+CASE WHEN u\.usage_metadata\.endpoint_id IS NOT NULL\s+THEN 'Model Serving Endpoint'\s+ELSE 'Cluster'/,
  );

  // The old sku_name = 'ENTERPRISE_ALL_PURPOSE_COMPUTE' branch is gone
  assert.doesNotMatch(sql, /u\.sku_name = 'ENTERPRISE_ALL_PURPOSE_COMPUTE'/);

  // MODEL_SERVING is no longer in the generic endpoint_id IN(...) list
  assert.doesNotMatch(sql, /IN \('MODEL_SERVING', 'AI_FUNCTIONS', 'AI_GATEWAY', 'LAKEBASE'\)/);
});

test('buildFocusSilverPipelineSql maps LAKEBASE to project_id with Lakebase Project type', () => {
  const sql = databricksSilverSql;

  assert.match(
    sql,
    /u\.billing_origin_product IN \('AI_FUNCTIONS', 'AI_GATEWAY'\)\s+THEN u\.usage_metadata\.endpoint_id/,
  );
  assert.match(
    sql,
    /u\.billing_origin_product IN \('AI_GATEWAY', 'AI_FUNCTIONS'\)\s+THEN COALESCE\(u\.usage_metadata\.endpoint_name, u\.usage_metadata\.endpoint_id\)/,
  );

  // Two LAKEBASE = project_id branches: one in ResourceId, one in ResourceName
  const lakebaseProjectIdMatches = sql.match(
    /u\.billing_origin_product = 'LAKEBASE'\s+THEN u\.usage_metadata\.project_id/g,
  );
  assert.equal(lakebaseProjectIdMatches?.length, 2);

  assert.match(sql, /u\.billing_origin_product = 'LAKEBASE' THEN 'Lakebase Project'/);
  assert.doesNotMatch(sql, /u\.billing_origin_product = 'LAKEBASE' THEN 'Database Endpoint'/);
});

test('buildFocusSilverPipelineSql sets FOUNDATION_MODEL_TRAINING ResourceType to MLflow Experiment Run', () => {
  const sql = databricksSilverSql;

  assert.match(
    sql,
    /u\.billing_origin_product = 'FOUNDATION_MODEL_TRAINING' THEN 'MLflow Experiment Run'/,
  );
  assert.doesNotMatch(
    sql,
    /u\.billing_origin_product = 'FOUNDATION_MODEL_TRAINING' THEN 'Foundation Model Training Run'/,
  );
});

test('buildFocusSilverPipelineSql maps DEFAULT_STORAGE to metastore_id', () => {
  const sql = databricksSilverSql;

  assert.match(
    sql,
    /u\.billing_origin_product = 'DEFAULT_STORAGE'\s+THEN u\.usage_metadata\.metastore_id/,
  );
  assert.match(sql, /u\.billing_origin_product = 'DEFAULT_STORAGE' THEN 'Metastore'/);
});

test('buildUsageGoldSql unions source silver tables with provider extension columns', () => {
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
  assert.match(sql, /`x_Discounts`,/);
  assert.match(sql, /`x_Operation`,/);
  assert.match(sql, /`x_ServiceCode`,/);
  assert.match(sql, /`x_Serverless`,/);
  assert.match(sql, /`x_Photon`,/);
  assert.match(sql, /`x_NodeType`/);
  assert.match(
    sql,
    /CAST\(NULL AS MAP<STRING, DOUBLE>\) AS `x_Discounts`,\s+CAST\(NULL AS STRING\) AS `x_Operation`,\s+CAST\(NULL AS STRING\) AS `x_ServiceCode`,\s+`x_Serverless`,\s+`x_Photon`,\s+`x_NodeType`\s+FROM `finops`\.`silver`\.`databricks_usage`/,
  );
  assert.match(
    sql,
    /`x_Discounts`,\s+`x_Operation`,\s+`x_ServiceCode`,\s+CAST\(NULL AS BOOLEAN\) AS `x_Serverless`,\s+CAST\(NULL AS BOOLEAN\) AS `x_Photon`,\s+CAST\(NULL AS STRING\) AS `x_NodeType`\s+FROM `finops`\.`silver`\.`aws_123456789012_usage`/,
  );
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
