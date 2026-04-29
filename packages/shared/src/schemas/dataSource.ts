import { z } from 'zod';
import { IDENT_RE } from '../sql/focusView.sql.js';

/** `app_settings` key holding the default Unity Catalog name. */
export const CATALOG_SETTING_KEY = 'catalog_name';

export const DataSourceIdentifierSchema = z
  .string()
  .min(1)
  .max(128)
  .regex(IDENT_RE, 'must match /^[A-Za-z_][A-Za-z0-9_]*$/');

export const DataSourceTableNameSchema = z
  .string()
  .min(1)
  .max(384)
  .regex(
    /^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*){0,2}$/,
    'must be one to three dot-separated identifiers',
  );

export const DataSourceSchema = z.object({
  id: z.number().int().positive(),
  templateId: z.string().min(1).max(128),
  name: z.string().min(1).max(256),
  description: z.string().max(2048).nullable(),
  providerName: z.string().min(1).max(64),
  billingAccountId: z.string().max(128).nullable(),
  tableName: DataSourceTableNameSchema,
  jobId: z.number().int().positive().nullable(),
  pipelineId: z.string().min(1).max(128).nullable(),
  focusVersion: z.string().min(1).max(32).nullable(),
  enabled: z.boolean(),
  config: z.record(z.string(), z.unknown()),
  updatedAt: z.string().datetime(),
});
export type DataSource = z.infer<typeof DataSourceSchema>;

export const DataSourceCreateBodySchema = z.object({
  templateId: z.string().min(1).max(128),
  name: z.string().min(1).max(256),
  description: z.string().max(2048).nullable().optional(),
  providerName: z.string().min(1).max(64),
  billingAccountId: z.string().max(128).nullable().optional(),
  tableName: DataSourceTableNameSchema,
  enabled: z.boolean().optional(),
  config: z.record(z.string(), z.unknown()).optional(),
});
export type DataSourceCreateBody = z.infer<typeof DataSourceCreateBodySchema>;

export const DataSourceUpdateBodySchema = z.object({
  name: z.string().min(1).max(256).optional(),
  description: z.string().max(2048).nullable().optional(),
  providerName: z.string().min(1).max(64).optional(),
  billingAccountId: z.string().max(128).nullable().optional(),
  tableName: DataSourceTableNameSchema.optional(),
  enabled: z.boolean().optional(),
  config: z.record(z.string(), z.unknown()).optional(),
});
export type DataSourceUpdateBody = z.infer<typeof DataSourceUpdateBodySchema>;

export const DATABRICKS_FOCUS_VERSION = '1.3';

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
    description: 'Databricks usage and list prices normalized to FOCUS 1.3',
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
    description: 'AWS Cost & Usage Report support is coming soon.',
    subtitle: 'by Amazon Web Services',
    focus_version: '1.2',
    available: false,
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
 * 02:00 UTC, picked because system.billing.usage publishes once per day.
 */
export const FOCUS_REFRESH_CRON_DEFAULT = '0 0 2 * * ?';
export const FOCUS_REFRESH_TIMEZONE_DEFAULT = 'UTC';

export const DataSourceSetupBodySchema = z.object({
  tableName: DataSourceIdentifierSchema.optional(),
  accountPricesTable: z.string().min(1).max(256).optional(),
  cronExpression: z.string().min(1).max(120).optional(),
  timezoneId: z.string().min(1).max(64).optional(),
});
export type DataSourceSetupBody = z.infer<typeof DataSourceSetupBodySchema>;

export const DataSourceSetupResultSchema = z.object({
  dataSourceId: z.number().int().positive(),
  jobId: z.number().int().positive(),
  pipelineId: z.string().min(1),
  fqn: z.string(),
  cronExpression: z.string(),
  timezoneId: z.string(),
  createdView: z.boolean(),
});
export type DataSourceSetupResult = z.infer<typeof DataSourceSetupResultSchema>;

export const DataSourceRunResultSchema = z.object({
  dataSourceId: z.number().int().positive(),
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
