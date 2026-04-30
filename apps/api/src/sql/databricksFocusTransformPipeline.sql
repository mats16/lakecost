CREATE TEMPORARY VIEW pipeline_names AS
SELECT account_id, workspace_id, pipeline_id, name AS pipeline_name
FROM system.lakeflow.pipelines
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY account_id, workspace_id, pipeline_id ORDER BY create_time DESC
) = 1;

CREATE TEMPORARY VIEW cluster_names AS
SELECT account_id, workspace_id, cluster_id, cluster_name
FROM system.compute.clusters
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY account_id, workspace_id, cluster_id ORDER BY change_time DESC
) = 1;

CREATE TEMPORARY VIEW warehouse_names AS
SELECT account_id, workspace_id, warehouse_id, warehouse_name
FROM system.compute.warehouses
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY account_id, workspace_id, warehouse_id ORDER BY change_time DESC
) = 1;

CREATE TEMPORARY VIEW workspace_names AS
SELECT account_id, workspace_id, workspace_name
FROM system.access.workspaces_latest;

CREATE TEMPORARY VIEW list_prices AS
SELECT COALESCE(price_end_time, date_add(current_date, 1)) AS coalesced_price_end_time, *
FROM system.billing.list_prices
WHERE currency_code = 'USD';

CREATE TEMPORARY VIEW account_prices AS
SELECT COALESCE(price_end_time, date_add(current_date, 1)) AS coalesced_price_end_time, *
FROM ${account_prices}
WHERE currency_code = 'USD';

CREATE TEMPORARY VIEW usage_with_pricing AS
SELECT
  u.record_id,
  u.account_id,
  u.workspace_id,
  w.workspace_name,
  u.sku_name,
  u.cloud,
  u.usage_start_time,
  u.usage_end_time,
  u.usage_date,
  u.usage_quantity,
  u.usage_unit,
  u.usage_type,
  u.custom_tags,
  u.usage_metadata,
  u.product_features,
  u.billing_origin_product,
  pip.pipeline_name,
  cl.cluster_name,
  wh.warehouse_name,
  lp.currency_code,
  lp.price_start_time,
  CAST(lp.pricing.default AS DECIMAL(30, 15)) AS list_unit_price,
  CAST(ap.pricing.default AS DECIMAL(30, 15)) AS account_unit_price
FROM system.billing.usage u
  LEFT JOIN list_prices lp
    ON u.sku_name = lp.sku_name
    AND u.usage_unit = lp.usage_unit
    AND u.account_id = lp.account_id
    AND u.usage_end_time BETWEEN lp.price_start_time AND lp.coalesced_price_end_time
  LEFT JOIN account_prices ap
    ON u.sku_name = ap.sku_name
    AND u.usage_unit = ap.usage_unit
    AND u.account_id = ap.account_id
    AND u.usage_end_time BETWEEN ap.price_start_time AND ap.coalesced_price_end_time
  LEFT JOIN workspace_names w
    ON u.account_id = w.account_id
    AND u.workspace_id = w.workspace_id
  LEFT JOIN pipeline_names pip
    ON u.account_id = pip.account_id
    AND u.workspace_id = pip.workspace_id
    AND u.usage_metadata.dlt_pipeline_id = pip.pipeline_id
  LEFT JOIN cluster_names cl
    ON u.account_id = cl.account_id
    AND u.workspace_id = cl.workspace_id
    AND u.usage_metadata.cluster_id = cl.cluster_id
  LEFT JOIN warehouse_names wh
    ON u.account_id = wh.account_id
    AND u.workspace_id = wh.workspace_id
    AND u.usage_metadata.warehouse_id = wh.warehouse_id;

CREATE OR REFRESH MATERIALIZED VIEW `${table_name}` AS
SELECT
  CAST(NULL AS STRING) AS AvailabilityZone,
  CAST(COALESCE(usage_quantity * account_unit_price, 0) AS DECIMAL(30, 15)) AS BilledCost,
  u.account_id AS BillingAccountId,
  u.account_id AS BillingAccountName,
  CAST(NULL AS STRING) AS BillingAccountType,
  u.currency_code AS BillingCurrency,
  DATE_TRUNC('MONTH', u.usage_date) + INTERVAL 1 MONTH AS BillingPeriodEnd,
  DATE_TRUNC('MONTH', u.usage_date) AS BillingPeriodStart,
  CAST(NULL AS STRING) AS CapacityReservationId,
  CAST(NULL AS STRING) AS CapacityReservationStatus,
  'Usage' AS ChargeCategory,
  CAST(NULL AS STRING) AS ChargeClass,
  u.sku_name AS ChargeDescription,
  'Usage-Based' AS ChargeFrequency,
  u.usage_end_time AS ChargePeriodEnd,
  u.usage_start_time AS ChargePeriodStart,
  CAST(NULL AS STRING) AS CommitmentDiscountCategory,
  CAST(NULL AS STRING) AS CommitmentDiscountId,
  CAST(NULL AS STRING) AS CommitmentDiscountName,
  CAST(NULL AS DECIMAL(30, 15)) AS CommitmentDiscountQuantity,
  CAST(NULL AS STRING) AS CommitmentDiscountStatus,
  CAST(NULL AS STRING) AS CommitmentDiscountType,
  CAST(NULL AS STRING) AS CommitmentDiscountUnit,
  CAST(u.usage_quantity AS DECIMAL(30, 15)) AS ConsumedQuantity,
  u.usage_unit AS ConsumedUnit,
  CAST(COALESCE(usage_quantity * account_unit_price, 0) AS DECIMAL(30, 15)) AS ContractedCost,
  CAST(u.account_unit_price AS DECIMAL(30, 15)) AS ContractedUnitPrice,
  CAST(COALESCE(usage_quantity * account_unit_price, 0) AS DECIMAL(30, 15)) AS EffectiveCost,
  CASE u.cloud
    WHEN 'AWS' THEN 'Amazon Web Services'
    WHEN 'AZURE' THEN 'Microsoft Azure'
    WHEN 'GCP' THEN 'Google Cloud Platform'
    ELSE u.cloud
  END AS HostProviderName,
  CAST(NULL AS STRING) AS InvoiceId,
  'Databricks' AS InvoiceIssuerName,
  CAST(COALESCE(u.usage_quantity * u.list_unit_price, 0) AS DECIMAL(30, 15)) AS ListCost,
  CAST(u.list_unit_price AS DECIMAL(30, 15)) AS ListUnitPrice,
  'Standard' AS PricingCategory,
  u.currency_code AS PricingCurrency,
  CAST(u.account_unit_price AS DECIMAL(30, 15)) AS PricingCurrencyContractedUnitPrice,
  CAST(COALESCE(usage_quantity * account_unit_price, 0) AS DECIMAL(30, 15)) AS PricingCurrencyEffectiveCost,
  CAST(u.list_unit_price AS DECIMAL(30, 15)) AS PricingCurrencyListUnitPrice,
  CAST(u.usage_quantity AS DECIMAL(30, 15)) AS PricingQuantity,
  u.usage_unit AS PricingUnit,
  'Databricks' AS ProviderName,
  'Databricks' AS PublisherName,
  split(current_metastore(), ':')[1] AS RegionId,
  split(current_metastore(), ':')[1] AS RegionName,
  CASE
    WHEN u.billing_origin_product IN ('JOBS')
      THEN COALESCE(u.usage_metadata.job_id, u.billing_origin_product)
    WHEN u.billing_origin_product IN ('LAKEHOUSE_MONITORING')
      THEN COALESCE(u.custom_tags['LakehouseMonitoringTableId'], u.billing_origin_product)
    WHEN u.billing_origin_product IN ('PREDICTIVE_OPTIMIZATION')
      THEN u.billing_origin_product
    WHEN u.billing_origin_product IN ('DLT', 'ONLINE_TABLES', 'LAKEFLOW_CONNECT')
      THEN COALESCE(u.usage_metadata.dlt_pipeline_id, u.billing_origin_product)
    WHEN u.billing_origin_product IN ('MODEL_SERVING')
      AND u.sku_name = 'ENTERPRISE_ALL_PURPOSE_COMPUTE'
      THEN COALESCE(u.usage_metadata.cluster_id, u.billing_origin_product)
    WHEN u.billing_origin_product IN ('VECTOR_SEARCH')
      AND (u.sku_name LIKE 'ENTERPRISE_JOBS_SERVERLESS_COMPUTE%'
        OR u.sku_name LIKE 'ENTERPRISE_SERVERLESS_SQL_COMPUTE%')
      THEN COALESCE(u.usage_metadata.dlt_pipeline_id, u.billing_origin_product)
    WHEN u.billing_origin_product IN ('MODEL_SERVING', 'AI_FUNCTIONS', 'VECTOR_SEARCH', 'AI_GATEWAY')
      THEN COALESCE(u.usage_metadata.endpoint_id, u.billing_origin_product)
    WHEN u.billing_origin_product IN ('DATABASE')
      AND (u.sku_name LIKE 'ENTERPRISE_JOBS_SERVERLESS_COMPUTE%')
      THEN COALESCE(u.usage_metadata.dlt_pipeline_id, u.billing_origin_product)
    WHEN u.billing_origin_product = 'DATABASE'
      THEN COALESCE(u.usage_metadata.database_instance_id, u.billing_origin_product)
    WHEN u.billing_origin_product = 'ALL_PURPOSE'
      THEN COALESCE(u.usage_metadata.cluster_id, u.billing_origin_product)
    WHEN u.billing_origin_product = 'DATA_CLASSIFICATION'
      THEN COALESCE(u.usage_metadata.catalog_id, u.billing_origin_product)
    WHEN u.billing_origin_product = 'FINE_GRAINED_ACCESS_CONTROL'
      THEN COALESCE(u.custom_tags['Name'], u.billing_origin_product)
    WHEN u.billing_origin_product IN ('NETWORKING', 'AGENT_EVALUATION', 'SHARED_SERVERLESS_COMPUTE')
      THEN u.billing_origin_product
    WHEN u.billing_origin_product = 'FOUNDATION_MODEL_TRAINING'
      THEN COALESCE(u.usage_metadata.run_name, u.billing_origin_product)
    WHEN u.billing_origin_product = 'AI_RUNTIME'
      THEN COALESCE(u.usage_metadata.ai_runtime_workload_id, u.billing_origin_product)
    WHEN u.billing_origin_product = 'CLEAN_ROOM'
      THEN COALESCE(u.usage_metadata.central_clean_room_id, u.billing_origin_product)
    WHEN u.billing_origin_product = 'APPS'
      THEN COALESCE(u.usage_metadata.app_id, u.billing_origin_product)
    WHEN u.billing_origin_product = 'SQL'
      AND (u.sku_name LIKE '%_JOBS_SERVERLESS_COMPUTE%')
      THEN COALESCE(u.usage_metadata.dlt_pipeline_id, u.billing_origin_product)
    WHEN u.billing_origin_product = 'SQL'
      THEN COALESCE(u.usage_metadata.warehouse_id, u.billing_origin_product)
    WHEN u.billing_origin_product = 'AGENT_BRICKS'
      THEN COALESCE(u.usage_metadata.agent_bricks_id, u.billing_origin_product)
    WHEN u.billing_origin_product = 'BASE_ENVIRONMENTS'
      THEN COALESCE(u.usage_metadata.base_environment_id, u.billing_origin_product)
    WHEN u.billing_origin_product = 'DATA_QUALITY_MONITORING'
      THEN COALESCE(u.usage_metadata.schema_id, u.billing_origin_product)
    WHEN u.billing_origin_product = 'DATA_SHARING'
      THEN COALESCE(u.usage_metadata.sharing_materialization_id, u.billing_origin_product)
    WHEN u.billing_origin_product IN ('INTERACTIVE', 'NOTEBOOKS')
      THEN COALESCE(u.usage_metadata.notebook_id, u.billing_origin_product)
    ELSE u.billing_origin_product
  END AS ResourceId,
  CASE
    WHEN u.billing_origin_product IN ('JOBS')
      THEN COALESCE(u.usage_metadata.job_name, u.usage_metadata.job_id)
    WHEN u.billing_origin_product IN ('DLT', 'LAKEFLOW_CONNECT', 'ONLINE_TABLES')
      THEN COALESCE(u.pipeline_name, u.usage_metadata.dlt_pipeline_id)
    WHEN u.billing_origin_product = 'ALL_PURPOSE'
      THEN COALESCE(u.cluster_name, u.usage_metadata.cluster_id)
    WHEN u.billing_origin_product = 'SQL'
      THEN COALESCE(u.warehouse_name, u.usage_metadata.warehouse_id)
    WHEN u.billing_origin_product IN ('MODEL_SERVING', 'AI_GATEWAY', 'AI_FUNCTIONS', 'VECTOR_SEARCH')
      THEN COALESCE(u.usage_metadata.endpoint_name, u.usage_metadata.endpoint_id)
    WHEN u.billing_origin_product = 'APPS'
      THEN COALESCE(u.usage_metadata.app_name, u.usage_metadata.app_id)
    WHEN u.billing_origin_product IN ('INTERACTIVE', 'NOTEBOOKS')
      THEN COALESCE(u.usage_metadata.notebook_path, u.usage_metadata.notebook_id)
    WHEN u.billing_origin_product = 'FOUNDATION_MODEL_TRAINING'
      THEN u.usage_metadata.run_name
    WHEN u.billing_origin_product = 'AI_RUNTIME'
      THEN u.usage_metadata.ai_runtime_workload_id
    WHEN u.billing_origin_product = 'DATABASE'
      THEN u.usage_metadata.database_instance_id
    WHEN u.billing_origin_product = 'AGENT_BRICKS'
      THEN u.usage_metadata.agent_bricks_id
    WHEN u.billing_origin_product = 'CLEAN_ROOM'
      THEN u.usage_metadata.central_clean_room_id
    WHEN u.billing_origin_product = 'BASE_ENVIRONMENTS'
      THEN u.usage_metadata.base_environment_id
    WHEN u.billing_origin_product = 'DATA_SHARING'
      THEN u.usage_metadata.sharing_materialization_id
    ELSE u.billing_origin_product
  END AS ResourceName,
  CASE
    WHEN u.billing_origin_product = 'JOBS' THEN 'Job'
    WHEN u.billing_origin_product = 'DLT' THEN 'Spark Declarative Pipeline'
    WHEN u.billing_origin_product = 'LAKEFLOW_CONNECT' THEN 'LakeFlow Connect'
    WHEN u.billing_origin_product = 'ALL_PURPOSE' THEN 'Cluster'
    WHEN u.billing_origin_product = 'INTERACTIVE' THEN 'Compute'
    WHEN u.billing_origin_product = 'NOTEBOOKS' THEN 'Notebook'
    WHEN u.billing_origin_product = 'SQL' THEN 'SQL Warehouse'
    WHEN u.billing_origin_product = 'MODEL_SERVING' THEN 'Model Serving Endpoint'
    WHEN u.billing_origin_product = 'VECTOR_SEARCH' THEN 'Vector Search Endpoint'
    WHEN u.billing_origin_product = 'AI_GATEWAY' THEN 'AI Gateway'
    WHEN u.billing_origin_product = 'AI_FUNCTIONS' THEN 'AI Function'
    WHEN u.billing_origin_product = 'FOUNDATION_MODEL_TRAINING' THEN 'Foundation Model Training Run'
    WHEN u.billing_origin_product = 'AGENT_EVALUATION' THEN 'Agent Evaluation'
    WHEN u.billing_origin_product = 'AGENT_BRICKS' THEN 'Agent'
    WHEN u.billing_origin_product = 'AI_RUNTIME' THEN 'AI Runtime Workload'
    WHEN u.billing_origin_product = 'DATABASE' THEN 'Database Instance'
    WHEN u.billing_origin_product = 'ONLINE_TABLES' THEN 'Online Table'
    WHEN u.billing_origin_product = 'DEFAULT_STORAGE' THEN 'Storage'
    WHEN u.billing_origin_product = 'LAKEHOUSE_MONITORING' THEN 'Lakehouse Monitoring'
    WHEN u.billing_origin_product = 'DATA_QUALITY_MONITORING' THEN 'Data Quality Monitor'
    WHEN u.billing_origin_product = 'PREDICTIVE_OPTIMIZATION' THEN 'Predictive Optimization'
    WHEN u.billing_origin_product = 'CLEAN_ROOM' THEN 'Clean Room'
    WHEN u.billing_origin_product = 'DATA_CLASSIFICATION' THEN 'Data Classification'
    WHEN u.billing_origin_product = 'FINE_GRAINED_ACCESS_CONTROL' THEN 'Access Control Policy'
    WHEN u.billing_origin_product = 'NETWORKING' THEN 'Networking'
    WHEN u.billing_origin_product = 'SHARED_SERVERLESS_COMPUTE' THEN 'Serverless Compute'
    WHEN u.billing_origin_product = 'BASE_ENVIRONMENTS' THEN 'Base Environment'
    WHEN u.billing_origin_product = 'APPS' THEN 'Application'
    WHEN u.billing_origin_product = 'DATA_SHARING' THEN 'Data Share'
    ELSE COALESCE(u.billing_origin_product, 'Other')
  END AS ResourceType,
  CASE
    WHEN u.billing_origin_product IN ('ALL_PURPOSE', 'INTERACTIVE', 'NOTEBOOKS', 'SHARED_SERVERLESS_COMPUTE')
      THEN 'Compute'
    WHEN u.billing_origin_product IN ('JOBS', 'DLT')
      THEN 'Analytics'
    WHEN u.billing_origin_product IN (
      'MODEL_SERVING', 'VECTOR_SEARCH', 'FOUNDATION_MODEL_TRAINING', 'AGENT_EVALUATION',
      'AI_GATEWAY', 'AI_FUNCTIONS', 'AGENT_BRICKS', 'AI_RUNTIME'
    ) THEN 'AI and Machine Learning'
    WHEN u.billing_origin_product IN ('DEFAULT_STORAGE') THEN 'Storage'
    WHEN u.billing_origin_product IN ('DATABASE', 'ONLINE_TABLES', 'SQL') THEN 'Databases'
    WHEN u.billing_origin_product IN (
      'LAKEHOUSE_MONITORING', 'DATA_QUALITY_MONITORING', 'PREDICTIVE_OPTIMIZATION',
      'CLEAN_ROOM', 'DATA_SHARING'
    ) THEN 'Management and Governance'
    WHEN u.billing_origin_product IN ('FINE_GRAINED_ACCESS_CONTROL', 'DATA_CLASSIFICATION')
      THEN 'Security'
    WHEN u.billing_origin_product IN ('NETWORKING') THEN 'Networking'
    WHEN u.billing_origin_product IN ('LAKEFLOW_CONNECT') THEN 'Integration'
    WHEN u.billing_origin_product IN ('BASE_ENVIRONMENTS') THEN 'Developer Tools'
    WHEN u.billing_origin_product IN ('APPS') THEN 'Web'
    ELSE 'Other'
  END AS ServiceCategory,
  u.billing_origin_product AS ServiceName,
  'Databricks' AS ServiceProviderName,
  CASE
    WHEN u.billing_origin_product = 'ALL_PURPOSE' THEN 'Virtual Machines'
    WHEN u.billing_origin_product = 'INTERACTIVE'
      THEN CASE WHEN upper(u.sku_name) LIKE '%SERVERLESS%' THEN 'Serverless Compute' ELSE 'Virtual Machines' END
    WHEN u.billing_origin_product IN ('NOTEBOOKS', 'SHARED_SERVERLESS_COMPUTE')
      THEN 'Serverless Compute'
    WHEN u.billing_origin_product IN ('JOBS', 'DLT') THEN 'Data Processing'
    WHEN u.billing_origin_product = 'MODEL_SERVING' THEN 'AI Platforms'
    WHEN u.billing_origin_product IN ('FOUNDATION_MODEL_TRAINING', 'AGENT_BRICKS', 'AI_RUNTIME')
      THEN 'Generative AI'
    WHEN u.billing_origin_product IN ('AI_GATEWAY', 'AI_FUNCTIONS') THEN 'AI Platforms'
    WHEN u.billing_origin_product IN ('VECTOR_SEARCH', 'AGENT_EVALUATION')
      THEN 'Other (AI and Machine Learning)'
    WHEN u.billing_origin_product = 'DEFAULT_STORAGE' THEN 'Object Storage'
    WHEN u.billing_origin_product = 'SQL' THEN 'Data Warehouses'
    WHEN u.billing_origin_product IN ('DATABASE', 'ONLINE_TABLES') THEN 'Relational Databases'
    WHEN u.billing_origin_product IN ('LAKEHOUSE_MONITORING', 'DATA_QUALITY_MONITORING')
      THEN 'Observability'
    WHEN u.billing_origin_product = 'PREDICTIVE_OPTIMIZATION' THEN 'Cost Management'
    WHEN u.billing_origin_product IN ('CLEAN_ROOM', 'DATA_SHARING')
      THEN 'Other (Management and Governance)'
    WHEN u.billing_origin_product = 'DATA_CLASSIFICATION' THEN 'Security Posture Management'
    WHEN u.billing_origin_product = 'FINE_GRAINED_ACCESS_CONTROL' THEN 'Other (Security)'
    WHEN u.billing_origin_product = 'NETWORKING' THEN 'Network Connectivity'
    WHEN u.billing_origin_product = 'LAKEFLOW_CONNECT' THEN 'Other (Integration)'
    WHEN u.billing_origin_product = 'BASE_ENVIRONMENTS' THEN 'Development Environments'
    WHEN u.billing_origin_product = 'APPS' THEN 'Application Platforms'
    ELSE 'Other (Other)'
  END AS ServiceSubcategory,
  u.sku_name AS SkuId,
  u.usage_type AS SkuMeter,
  to_json(
    map_from_entries(
      filter(
        transform(
          map_entries(from_json(to_json(u.product_features), 'map<string, string>')),
          e -> named_struct('key', concat('x_', e.key), 'value', e.value)
        ),
        kv -> kv.value IS NOT NULL
      )
    )
  ) AS SkuPriceDetails,
  u.sku_name AS SkuPriceId,
  u.workspace_id AS SubAccountId,
  u.workspace_name AS SubAccountName,
  'Workspace' AS SubAccountType,
  u.custom_tags AS Tags
FROM usage_with_pricing u;

CREATE OR REFRESH MATERIALIZED VIEW gold.`${table_name}_daily`
COMMENT 'Databricks FOCUS daily billing rollup managed by FinLake'
AS
SELECT
  CAST(ChargePeriodEnd AS DATE) AS ChargeDate,
  DATE_FORMAT(BillingPeriodStart, 'yyyy-MM') AS BillingMonth,
  BillingAccountId AS billing_account_id,
  BillingAccountName AS billing_account_name,
  BillingCurrency AS billing_currency,
  SubAccountId AS workspace_id,
  SubAccountName AS workspace_name,
  SubAccountType AS workspace_type,
  ProviderName AS provider_name,
  PublisherName AS publisher_name,
  ServiceProviderName AS service_provider_name,
  ServiceCategory AS service_category,
  ServiceSubcategory AS service_subcategory,
  ServiceName AS service_name,
  ResourceId AS resource_id,
  ResourceName AS resource_name,
  ResourceType AS resource_type,
  SkuId AS sku_id,
  SkuMeter AS sku_meter,
  ChargeDescription AS charge_description,
  PricingUnit AS pricing_unit,
  ConsumedUnit AS consumed_unit,
  CAST(SUM(COALESCE(ConsumedQuantity, 0)) AS DECIMAL(30, 15)) AS consumed_quantity,
  CAST(SUM(COALESCE(ListCost, 0)) AS DECIMAL(30, 15)) AS list_cost,
  CAST(SUM(COALESCE(BilledCost, 0)) AS DECIMAL(30, 15)) AS billed_cost,
  CAST(SUM(COALESCE(ContractedCost, 0)) AS DECIMAL(30, 15)) AS contracted_cost,
  CAST(SUM(COALESCE(EffectiveCost, 0)) AS DECIMAL(30, 15)) AS effective_cost
FROM `${table_name}`
GROUP BY
  CAST(ChargePeriodEnd AS DATE),
  DATE_FORMAT(BillingPeriodStart, 'yyyy-MM'),
  BillingAccountId,
  BillingAccountName,
  BillingCurrency,
  SubAccountId,
  SubAccountName,
  SubAccountType,
  ProviderName,
  PublisherName,
  ServiceProviderName,
  ServiceCategory,
  ServiceSubcategory,
  ServiceName,
  ResourceId,
  ResourceName,
  ResourceType,
  SkuId,
  SkuMeter,
  ChargeDescription,
  PricingUnit,
  ConsumedUnit;

CREATE OR REFRESH MATERIALIZED VIEW gold.`${table_name}_hourly`
COMMENT 'Databricks FOCUS hourly billing rollup managed by FinLake'
AS
SELECT
  CAST(DATE_TRUNC('HOUR', ChargePeriodStart) AS TIMESTAMP) AS ChargeHour,
  DATE_FORMAT(BillingPeriodStart, 'yyyy-MM') AS BillingMonth,
  BillingAccountId AS billing_account_id,
  BillingAccountName AS billing_account_name,
  BillingCurrency AS billing_currency,
  SubAccountId AS workspace_id,
  SubAccountName AS workspace_name,
  SubAccountType AS workspace_type,
  ProviderName AS provider_name,
  PublisherName AS publisher_name,
  ServiceProviderName AS service_provider_name,
  ServiceCategory AS service_category,
  ServiceSubcategory AS service_subcategory,
  ServiceName AS service_name,
  ResourceId AS resource_id,
  ResourceName AS resource_name,
  ResourceType AS resource_type,
  SkuId AS sku_id,
  SkuMeter AS sku_meter,
  ChargeDescription AS charge_description,
  PricingUnit AS pricing_unit,
  ConsumedUnit AS consumed_unit,
  CAST(SUM(COALESCE(ConsumedQuantity, 0)) AS DECIMAL(30, 15)) AS consumed_quantity,
  CAST(SUM(COALESCE(ListCost, 0)) AS DECIMAL(30, 15)) AS list_cost,
  CAST(SUM(COALESCE(BilledCost, 0)) AS DECIMAL(30, 15)) AS billed_cost,
  CAST(SUM(COALESCE(ContractedCost, 0)) AS DECIMAL(30, 15)) AS contracted_cost,
  CAST(SUM(COALESCE(EffectiveCost, 0)) AS DECIMAL(30, 15)) AS effective_cost
FROM `${table_name}`
GROUP BY
  CAST(DATE_TRUNC('HOUR', ChargePeriodStart) AS TIMESTAMP),
  DATE_FORMAT(BillingPeriodStart, 'yyyy-MM'),
  BillingAccountId,
  BillingAccountName,
  BillingCurrency,
  SubAccountId,
  SubAccountName,
  SubAccountType,
  ProviderName,
  PublisherName,
  ServiceProviderName,
  ServiceCategory,
  ServiceSubcategory,
  ServiceName,
  ResourceId,
  ResourceName,
  ResourceType,
  SkuId,
  SkuMeter,
  ChargeDescription,
  PricingUnit,
  ConsumedUnit;
