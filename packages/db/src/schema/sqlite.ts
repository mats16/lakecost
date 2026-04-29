import { sqliteTable, text, integer, real, unique } from 'drizzle-orm/sqlite-core';

export const budgets = sqliteTable('budgets', {
  id: text('id').primaryKey(),
  workspaceId: text('workspace_id'),
  name: text('name').notNull(),
  scopeType: text('scope_type').notNull(),
  scopeValue: text('scope_value').notNull(),
  amountUsd: real('amount_usd').notNull(),
  period: text('period').notNull(),
  thresholdsPctJson: text('thresholds_pct_json').notNull().default('[80,100]'),
  notifyEmailsJson: text('notify_emails_json').notNull().default('[]'),
  createdBy: text('created_by').notNull(),
  createdAt: text('created_at').notNull(),
});

export const budgetAlerts = sqliteTable('budget_alerts', {
  id: text('id').primaryKey(),
  budgetId: text('budget_id').notNull(),
  thresholdPct: integer('threshold_pct').notNull(),
  triggeredAt: text('triggered_at').notNull(),
  actualUsd: real('actual_usd').notNull(),
  notifiedChannelsJson: text('notified_channels_json').notNull().default('[]'),
});

export const userPreferences = sqliteTable('user_preferences', {
  userId: text('user_id').primaryKey(),
  currency: text('currency').notNull().default('USD'),
  defaultWorkspaceId: text('default_workspace_id'),
  theme: text('theme').notNull().default('system'),
  prefsJson: text('prefs_json').notNull().default('{}'),
  updatedAt: text('updated_at').notNull(),
});

export const cachedAggregations = sqliteTable('cached_aggregations', {
  cacheKey: text('cache_key').primaryKey(),
  queryHash: text('query_hash').notNull(),
  payloadJson: text('payload_json').notNull(),
  computedAt: text('computed_at').notNull(),
  expiresAt: text('expires_at').notNull(),
});

export const tagChargebackRules = sqliteTable('tag_chargeback_rules', {
  id: text('id').primaryKey(),
  tagKey: text('tag_key').notNull(),
  tagValuePattern: text('tag_value_pattern').notNull(),
  costCenter: text('cost_center').notNull(),
  ownerEmail: text('owner_email'),
  priority: integer('priority').notNull().default(100),
});

export const appSettings = sqliteTable('app_settings', {
  key: text('key').primaryKey(),
  value: text('value').notNull(),
  updatedAt: text('updated_at').notNull(),
});

export const dataSources = sqliteTable(
  'data_sources',
  {
    id: integer('id').primaryKey({ autoIncrement: true }),
    templateId: text('template_id').notNull(),
    name: text('name').notNull(),
    description: text('description'),
    providerName: text('provider_name').notNull(),
    billingAccountId: text('billing_account_id'),
    tableName: text('table_name').notNull(),
    jobId: integer('job_id'),
    pipelineId: text('pipeline_id'),
    focusVersion: text('focus_version'),
    enabled: integer('enabled', { mode: 'boolean' }).notNull().default(true),
    configJson: text('config_json').notNull().default('{}'),
    updatedAt: text('updated_at').notNull(),
  },
  (table) => ({
    uniqProvider: unique().on(table.providerName, table.billingAccountId),
  }),
);

export const setupState = sqliteTable('setup_state', {
  workspaceId: text('workspace_id').primaryKey(),
  systemTablesOk: integer('system_tables_ok', { mode: 'boolean' }).notNull().default(false),
  permissionsOk: integer('permissions_ok', { mode: 'boolean' }).notNull().default(false),
  curConfigured: integer('cur_configured', { mode: 'boolean' }).notNull().default(false),
  azureExportConfigured: integer('azure_export_configured', { mode: 'boolean' })
    .notNull()
    .default(false),
  lastCheckedAt: text('last_checked_at').notNull(),
  detailsJson: text('details_json').notNull().default('{}'),
});
