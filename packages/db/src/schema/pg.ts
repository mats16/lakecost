import {
  pgTable,
  text,
  integer,
  doublePrecision,
  boolean,
  jsonb,
  serial,
  timestamp,
  unique,
  primaryKey,
} from 'drizzle-orm/pg-core';

export const budgets = pgTable('budgets', {
  id: text('id').primaryKey(),
  workspaceId: text('workspace_id'),
  name: text('name').notNull(),
  scopeType: text('scope_type').notNull(),
  scopeValue: text('scope_value').notNull(),
  amountUsd: doublePrecision('amount_usd').notNull(),
  period: text('period').notNull(),
  thresholdsPct: jsonb('thresholds_pct').notNull().default([80, 100]),
  notifyEmails: jsonb('notify_emails').notNull().default([]),
  createdBy: text('created_by').notNull(),
  createdAt: timestamp('created_at', { withTimezone: true }).notNull().defaultNow(),
});

export const budgetAlerts = pgTable('budget_alerts', {
  id: text('id').primaryKey(),
  budgetId: text('budget_id').notNull(),
  thresholdPct: integer('threshold_pct').notNull(),
  triggeredAt: timestamp('triggered_at', { withTimezone: true }).notNull().defaultNow(),
  actualUsd: doublePrecision('actual_usd').notNull(),
  notifiedChannels: jsonb('notified_channels').notNull().default([]),
});

export const userPreferences = pgTable('user_preferences', {
  userId: text('user_id').primaryKey(),
  currency: text('currency').notNull().default('USD'),
  defaultWorkspaceId: text('default_workspace_id'),
  theme: text('theme').notNull().default('system'),
  prefs: jsonb('prefs').notNull().default({}),
  updatedAt: timestamp('updated_at', { withTimezone: true }).notNull().defaultNow(),
});

export const cachedAggregations = pgTable('cached_aggregations', {
  cacheKey: text('cache_key').primaryKey(),
  queryHash: text('query_hash').notNull(),
  payload: jsonb('payload').notNull(),
  computedAt: timestamp('computed_at', { withTimezone: true }).notNull().defaultNow(),
  expiresAt: timestamp('expires_at', { withTimezone: true }).notNull(),
});

export const tagChargebackRules = pgTable('tag_chargeback_rules', {
  id: text('id').primaryKey(),
  tagKey: text('tag_key').notNull(),
  tagValuePattern: text('tag_value_pattern').notNull(),
  costCenter: text('cost_center').notNull(),
  ownerEmail: text('owner_email'),
  priority: integer('priority').notNull().default(100),
});

export const appSettings = pgTable('app_settings', {
  key: text('key').primaryKey(),
  value: text('value').notNull(),
  updatedAt: timestamp('updated_at', { withTimezone: true }).notNull().defaultNow(),
});

export const dataSources = pgTable(
  'data_sources',
  {
    id: serial('id').primaryKey(),
    templateId: text('template_id').notNull(),
    name: text('name').notNull(),
    providerName: text('provider_name').notNull(),
    billingAccountId: text('billing_account_id'),
    tableName: text('table_name').notNull(),
    focusVersion: text('focus_version'),
    enabled: boolean('enabled').notNull().default(true),
    config: jsonb('config').notNull().default({}),
    updatedAt: timestamp('updated_at', { withTimezone: true }).notNull().defaultNow(),
  },
  (table) => ({
    uniqProvider: unique().on(table.providerName, table.billingAccountId),
  }),
);

export const pricingData = pgTable(
  'pricing_data',
  {
    provider: text('provider').notNull(),
    service: text('service').notNull(),
    slug: text('slug').notNull(),
    tableName: text('table').notNull(),
    rawDataTable: text('raw_data_table'),
    rawDataPath: text('raw_data_path'),
    notebookPath: text('notebook_path'),
    notebookId: text('notebook_id'),
    metadata: jsonb('metadata').notNull().default({}),
    runId: integer('run_id'),
    runStatus: text('run_status').notNull().default('not_started'),
    runUrl: text('run_url'),
    runStartedAt: timestamp('run_started_at', { withTimezone: true }),
    runFinishedAt: timestamp('run_finished_at', { withTimezone: true }),
    runCheckedAt: timestamp('run_checked_at', { withTimezone: true }),
    updatedAt: timestamp('updated_at', { withTimezone: true }).notNull().defaultNow(),
  },
  (table) => ({
    pk: primaryKey({ columns: [table.provider, table.service] }),
    uniqSlug: unique().on(table.slug),
  }),
);

export const setupState = pgTable('setup_state', {
  workspaceId: text('workspace_id').primaryKey(),
  systemTablesOk: boolean('system_tables_ok').notNull().default(false),
  permissionsOk: boolean('permissions_ok').notNull().default(false),
  curConfigured: boolean('cur_configured').notNull().default(false),
  azureExportConfigured: boolean('azure_export_configured').notNull().default(false),
  lastCheckedAt: timestamp('last_checked_at', { withTimezone: true }).notNull().defaultNow(),
  details: jsonb('details').notNull().default({}),
});
