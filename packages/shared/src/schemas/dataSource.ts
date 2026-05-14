import { z } from 'zod';
import { IDENT_RE, type MedallionSchema } from '../sql/focusView.sql.js';

/** `app_settings` key holding the default Unity Catalog name. */
export const CATALOG_SETTING_KEY = 'catalog_name';

/** `app_settings` key holding the group that can read configured catalog data. */
export const CATALOG_USER_GROUP_SETTING_KEY = 'catalog_user_group';
export const CATALOG_USER_GROUP_DEFAULT = 'account users';

/** `app_settings` keys holding Unity Catalog schema names by medallion layer. */
export const MEDALLION_SCHEMA_SETTING_KEYS = {
  gold: 'gold_schema_name',
  silver: 'silver_schema_name',
  bronze: 'bronze_schema_name',
} as const;

export const MEDALLION_SCHEMA_DEFAULTS = {
  bronze: 'ingest',
  silver: 'focus',
  gold: 'analytics',
} as const satisfies Record<MedallionSchema, string>;

/** Fixed Unity Catalog objects used by instance price ingestion. */
export const PRICING_SCHEMA_DEFAULT = 'pricing';
export const DOWNLOADS_VOLUME_DEFAULT = 'downloads';
export const AWS_EC2_PRICING_TABLE_DEFAULT = 'aws_ec2';
export const AWS_RDS_PRICING_TABLE_DEFAULT = 'aws_rds';
export const PRICING_NOTEBOOK_WORKSPACE_PATH_SETTING_KEY = 'pricing_notebook_workspace_path';

/** Materialized View names FinLake creates in the gold schema. */
export const GOLD_USAGE_TABLES = {
  daily: 'usage_daily',
  monthly: 'usage_monthly',
} as const;

/** `app_settings` keys holding the shared Lakeflow pipeline/job identifiers. */
export const LAKEFLOW_PIPELINE_SETTING_KEYS = {
  pipelineId: 'lakeflow_pipeline_id',
  jobId: 'lakeflow_pipeline_job_id',
} as const;

export function medallionSchemaNamesFromSettings(
  settings: Record<string, string | undefined>,
): Record<MedallionSchema, string> {
  return {
    bronze:
      settings[MEDALLION_SCHEMA_SETTING_KEYS.bronze]?.trim() || MEDALLION_SCHEMA_DEFAULTS.bronze,
    silver:
      settings[MEDALLION_SCHEMA_SETTING_KEYS.silver]?.trim() || MEDALLION_SCHEMA_DEFAULTS.silver,
    gold: settings[MEDALLION_SCHEMA_SETTING_KEYS.gold]?.trim() || MEDALLION_SCHEMA_DEFAULTS.gold,
  };
}

export function catalogUserGroupFromSettings(settings: Record<string, string | undefined>): string {
  return settings[CATALOG_USER_GROUP_SETTING_KEY]?.trim() || CATALOG_USER_GROUP_DEFAULT;
}

export function catalogFromSettings(settings: Record<string, string | undefined>): string | null {
  return settings[CATALOG_SETTING_KEY]?.trim() || null;
}

export const DataSourceIdentifierSchema = z
  .string()
  .min(1)
  .max(128)
  .regex(IDENT_RE, 'must match /^[A-Za-z_][A-Za-z0-9_]*$/');

export const DataSourceAccountIdSchema = z.string().min(1).max(128);

export const DEFAULT_DATABRICKS_ACCOUNT_ID = 'default';
export const PROVIDER_DATABRICKS = 'databricks';
export const PROVIDER_AWS = 'aws';

export function normalizeProviderName(providerName: string): string {
  const lower = providerName.trim().toLowerCase();
  if (lower === 'databricks') return PROVIDER_DATABRICKS;
  if (lower === 'aws' || lower === 'amazon web services') return PROVIDER_AWS;
  return providerName.trim();
}

export function isDatabricksProvider(providerName: string): boolean {
  return normalizeProviderName(providerName) === PROVIDER_DATABRICKS;
}

export function isAwsProvider(providerName: string): boolean {
  return normalizeProviderName(providerName) === PROVIDER_AWS;
}

export const DataSourceProviderNameSchema = z
  .string()
  .min(1)
  .max(64)
  .transform(normalizeProviderName);

export const DataSourceKeySchema = z.object({
  providerName: DataSourceProviderNameSchema,
  accountId: DataSourceAccountIdSchema,
});
export type DataSourceKey = z.infer<typeof DataSourceKeySchema>;

export function toDataSourceKey(source: {
  providerName: string;
  accountId: string;
}): DataSourceKey {
  return { providerName: source.providerName, accountId: source.accountId };
}

export function dataSourceKeyString(key: { providerName: string; accountId: string }): string {
  return `${key.providerName}:${key.accountId}`;
}

export function isDatabricksDefaultAccount(source: {
  providerName: string;
  accountId: string;
}): boolean {
  return (
    isDatabricksProvider(source.providerName) && source.accountId === DEFAULT_DATABRICKS_ACCOUNT_ID
  );
}

export const DataSourceTableNameSchema = z
  .string()
  .min(1)
  .max(384)
  .regex(
    /^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*){0,2}$/,
    'must be one to three dot-separated identifiers',
  );

export const DataSourceSchema = z.object({
  name: z.string().min(1).max(256),
  providerName: DataSourceProviderNameSchema,
  accountId: DataSourceAccountIdSchema,
  tableName: DataSourceIdentifierSchema,
  focusVersion: z.string().min(1).max(32).nullable(),
  enabled: z.boolean(),
  config: z.record(z.string(), z.unknown()),
  updatedAt: z.string().datetime(),
});
export type DataSource = z.infer<typeof DataSourceSchema>;

export const DataSourceCreateBodySchema = z.object({
  templateId: z.string().min(1).max(128),
  name: z.string().min(1).max(256),
  providerName: DataSourceProviderNameSchema,
  accountId: DataSourceAccountIdSchema.optional(),
  tableName: DataSourceIdentifierSchema,
  enabled: z.boolean().optional(),
  config: z.record(z.string(), z.unknown()).optional(),
});
export type DataSourceCreateBody = z.infer<typeof DataSourceCreateBodySchema>;

export const DataSourceUpdateBodySchema = z.object({
  name: z.string().min(1).max(256).optional(),
  tableName: DataSourceIdentifierSchema.optional(),
  enabled: z.boolean().optional(),
  config: z.record(z.string(), z.unknown()).optional(),
});
export type DataSourceUpdateBody = z.infer<typeof DataSourceUpdateBodySchema>;

export const DATABRICKS_FOCUS_VERSION = '1.3';
export const AWS_FOCUS_VERSION = '1.2';

export const DataSourceTemplateSchema = z.object({
  id: z.string().min(1).max(128),
  name: z.string().min(1).max(256),
  description: z.string().max(2048),
  subtitle: z.string().max(256),
  focus_version: z.string().min(1).max(32).nullable(),
  available: z.boolean(),
  appearance: z.object({
    brandColor: z.string().min(1).max(32),
    brandTextColor: z.string().min(1).max(32).optional(),
  }),
});
export type DataSourceTemplate = z.infer<typeof DataSourceTemplateSchema>;

export const DATA_SOURCE_TEMPLATES = [
  {
    id: 'databricks_focus13',
    name: 'Databricks',
    description: 'System tables transformed to FOCUS format',
    subtitle: '',
    focus_version: DATABRICKS_FOCUS_VERSION,
    available: true,
    appearance: {
      brandColor: '#FF3621',
    },
  },
  {
    id: 'aws',
    name: 'Amazon Web Services',
    description: 'Billing and Cost Management',
    subtitle: '',
    focus_version: AWS_FOCUS_VERSION,
    available: true,
    appearance: {
      brandColor: '#FF9900',
      brandTextColor: '#232F3E',
    },
  },
  {
    id: 'gcp',
    name: 'Google Cloud',
    description: 'Google Cloud billing export support is coming soon.',
    subtitle: 'by Google Cloud',
    focus_version: '1.0',
    available: false,
    appearance: {
      brandColor: '#4285F4',
    },
  },
  {
    id: 'snowflake',
    name: 'Snowflake',
    description: 'Snowflake credits support is coming soon.',
    subtitle: 'by Snowflake',
    focus_version: '1.0',
    available: false,
    appearance: {
      brandColor: '#29B5E8',
    },
  },
] satisfies DataSourceTemplate[];

/**
 * Default Quartz cron for Databricks system.billing usage refresh — daily at
 * 21:00 UTC, which is 06:00 the next day in Japan Standard Time.
 */
export const FOCUS_REFRESH_CRON_DEFAULT = '0 0 21 * * ?';
export const FOCUS_REFRESH_TIMEZONE_DEFAULT = 'UTC';

export const DataSourceSetupBodySchema = z.object({
  tableName: DataSourceIdentifierSchema.optional(),
  accountPricesTable: z.string().min(1).max(256).optional(),
});
export type DataSourceSetupBody = z.infer<typeof DataSourceSetupBodySchema>;

export const DataSourceSetupResultSchema = z.object({
  dataSourceKey: DataSourceKeySchema,
  jobId: z.number().int().positive(),
  pipelineId: z.string().min(1),
  fqn: z.string(),
  goldFqn: z.string(),
  cronExpression: z.string(),
  timezoneId: z.string(),
  createdView: z.boolean(),
});
export type DataSourceSetupResult = z.infer<typeof DataSourceSetupResultSchema>;

export const DataSourcePermissionStepSchema = z.object({
  label: z.string(),
  status: z.enum(['ok', 'warning', 'error']),
  message: z.string(),
});
export type DataSourcePermissionStep = z.infer<typeof DataSourcePermissionStepSchema>;

export const DataSourceSystemTableGrantsBodySchema = z.object({
  accountPricesTable: z.string().min(1).max(256).optional(),
});
export type DataSourceSystemTableGrantsBody = z.infer<typeof DataSourceSystemTableGrantsBodySchema>;

export const DataSourceSystemTableGrantsResultSchema = z.object({
  dataSourceKey: DataSourceKeySchema,
  servicePrincipalId: z.string().min(1).nullable(),
  tables: z.array(z.string()),
  steps: z.array(DataSourcePermissionStepSchema),
  remediationSql: z.string().nullable(),
  warnings: z.array(z.string()),
});
export type DataSourceSystemTableGrantsResult = z.infer<
  typeof DataSourceSystemTableGrantsResultSchema
>;

export const DataSourcePreflightBodySchema = DataSourceSetupBodySchema;
export type DataSourcePreflightBody = z.infer<typeof DataSourcePreflightBodySchema>;

export const DataSourcePreflightResultSchema = z.object({
  dataSourceKey: DataSourceKeySchema,
  servicePrincipalId: z.string().min(1).nullable(),
  ok: z.boolean(),
  steps: z.array(DataSourcePermissionStepSchema),
  remediationSql: z.string().nullable(),
  warnings: z.array(z.string()),
});
export type DataSourcePreflightResult = z.infer<typeof DataSourcePreflightResultSchema>;

export const DataSourceRunResultSchema = z.object({
  dataSourceKey: DataSourceKeySchema,
  jobId: z.number().int().positive(),
  runId: z.number().int().positive(),
});
export type DataSourceRunResult = z.infer<typeof DataSourceRunResultSchema>;

/** Extracts the last dot-separated segment of a qualified table name. */
export function tableLeafName(tableName: string): string {
  const parts = tableName.split('.');
  return parts[parts.length - 1] ?? tableName;
}

/** Joins catalog, schema, table into an unquoted fully-qualified name. */
export function unquotedFqn(catalog: string, schema: string, table: string): string {
  return `${catalog}.${schema}.${table}`;
}
