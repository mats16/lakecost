import Database from 'better-sqlite3';
import { drizzle } from 'drizzle-orm/better-sqlite3';
import type { BetterSQLite3Database } from 'drizzle-orm/better-sqlite3';
import { eq, sql } from 'drizzle-orm';
import { randomUUID } from 'node:crypto';
import * as s from './schema/sqlite.js';
import type { DatabaseClient } from './DatabaseClient.js';
import type {
  AppSettingValue,
  AppSettingsRepo,
  BudgetsRepo,
  CachedAggregationValue,
  CachedAggregationsRepo,
  DataSourceCreateInput,
  DataSourceUpdatePatch,
  DataSourceValue,
  DataSourcesRepo,
  Repositories,
  SetupStateRepo,
  SetupStateValue,
  UserPreferencesRepo,
  UserPreferencesValue,
} from './repositories/index.js';
import { ensureParentDir } from './paths.js';
import { logger } from './logger.js';
import type { Budget, CreateBudgetInput, SetupCheckResult } from '@lakecost/shared';

type Db = BetterSQLite3Database<typeof s>;

export class SqliteClient implements DatabaseClient {
  readonly backend = 'sqlite' as const;
  readonly repos: Repositories;

  private constructor(
    private readonly raw: Database.Database,
    private readonly db: Db,
  ) {
    this.repos = {
      budgets: new SqliteBudgetsRepo(db),
      userPreferences: new SqliteUserPreferencesRepo(db),
      cachedAggregations: new SqliteCachedAggregationsRepo(db),
      setupState: new SqliteSetupStateRepo(db),
      appSettings: new SqliteAppSettingsRepo(db),
      dataSources: new SqliteDataSourcesRepo(db),
    };
  }

  static create(opts: { sqlitePath: string }): SqliteClient {
    ensureParentDir(opts.sqlitePath);
    const raw = new Database(opts.sqlitePath);
    raw.pragma('journal_mode = WAL');
    raw.pragma('foreign_keys = ON');
    const db = drizzle(raw, { schema: s });
    const client = new SqliteClient(raw, db);
    client.bootstrapSchema();
    return client;
  }

  private bootstrapSchema(): void {
    this.raw.exec(`
      CREATE TABLE IF NOT EXISTS budgets (
        id TEXT PRIMARY KEY,
        workspace_id TEXT,
        name TEXT NOT NULL,
        scope_type TEXT NOT NULL,
        scope_value TEXT NOT NULL,
        amount_usd REAL NOT NULL,
        period TEXT NOT NULL,
        thresholds_pct_json TEXT NOT NULL DEFAULT '[80,100]',
        notify_emails_json TEXT NOT NULL DEFAULT '[]',
        created_by TEXT NOT NULL,
        created_at TEXT NOT NULL
      );
      CREATE TABLE IF NOT EXISTS budget_alerts (
        id TEXT PRIMARY KEY,
        budget_id TEXT NOT NULL,
        threshold_pct INTEGER NOT NULL,
        triggered_at TEXT NOT NULL,
        actual_usd REAL NOT NULL,
        notified_channels_json TEXT NOT NULL DEFAULT '[]'
      );
      CREATE TABLE IF NOT EXISTS user_preferences (
        user_id TEXT PRIMARY KEY,
        currency TEXT NOT NULL DEFAULT 'USD',
        default_workspace_id TEXT,
        theme TEXT NOT NULL DEFAULT 'system',
        prefs_json TEXT NOT NULL DEFAULT '{}',
        updated_at TEXT NOT NULL
      );
      CREATE TABLE IF NOT EXISTS cached_aggregations (
        cache_key TEXT PRIMARY KEY,
        query_hash TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        computed_at TEXT NOT NULL,
        expires_at TEXT NOT NULL
      );
      CREATE TABLE IF NOT EXISTS tag_chargeback_rules (
        id TEXT PRIMARY KEY,
        tag_key TEXT NOT NULL,
        tag_value_pattern TEXT NOT NULL,
        cost_center TEXT NOT NULL,
        owner_email TEXT,
        priority INTEGER NOT NULL DEFAULT 100
      );
      CREATE TABLE IF NOT EXISTS app_settings (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL,
        updated_at TEXT NOT NULL
      );
      CREATE TABLE IF NOT EXISTS data_sources (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        template_id TEXT NOT NULL,
        name TEXT NOT NULL,
        description TEXT,
        provider_name TEXT NOT NULL,
        billing_account_id TEXT,
        table_name TEXT NOT NULL,
        job_id INTEGER,
        pipeline_id TEXT,
        focus_version TEXT,
        enabled INTEGER NOT NULL DEFAULT 1,
        config_json TEXT NOT NULL DEFAULT '{}',
        updated_at TEXT NOT NULL,
        UNIQUE(provider_name, billing_account_id)
      );
      CREATE TABLE IF NOT EXISTS setup_state (
        workspace_id TEXT PRIMARY KEY,
        system_tables_ok INTEGER NOT NULL DEFAULT 0,
        permissions_ok INTEGER NOT NULL DEFAULT 0,
        cur_configured INTEGER NOT NULL DEFAULT 0,
        azure_export_configured INTEGER NOT NULL DEFAULT 0,
        last_checked_at TEXT NOT NULL,
        details_json TEXT NOT NULL DEFAULT '{}'
      );
    `);
    logger.debug('SQLite schema bootstrap complete');
  }

  async healthCheck(): Promise<{ ok: true; backend: 'sqlite' }> {
    this.db.run(sql`select 1`);
    return { ok: true, backend: 'sqlite' };
  }

  async migrate(): Promise<void> {
    // bootstrapSchema runs idempotent CREATE IF NOT EXISTS at construction.
    // For richer migrations, generate via drizzle-kit and apply here.
  }

  async close(): Promise<void> {
    this.raw.close();
  }
}

class SqliteBudgetsRepo implements BudgetsRepo {
  constructor(private db: Db) {}

  async list(workspaceId: string | null): Promise<Budget[]> {
    const rows = workspaceId
      ? await this.db.select().from(s.budgets).where(eq(s.budgets.workspaceId, workspaceId))
      : await this.db.select().from(s.budgets);
    return rows.map(toBudget);
  }

  async create(input: CreateBudgetInput, createdBy: string): Promise<Budget> {
    const row = {
      id: randomUUID(),
      workspaceId: input.workspaceId,
      name: input.name,
      scopeType: input.scopeType,
      scopeValue: input.scopeValue,
      amountUsd: input.amountUsd,
      period: input.period,
      thresholdsPctJson: JSON.stringify(input.thresholdsPct),
      notifyEmailsJson: JSON.stringify(input.notifyEmails),
      createdBy,
      createdAt: new Date().toISOString(),
    };
    await this.db.insert(s.budgets).values(row);
    return toBudget(row);
  }

  async delete(id: string): Promise<void> {
    await this.db.delete(s.budgets).where(eq(s.budgets.id, id));
  }
}

function toBudget(row: typeof s.budgets.$inferSelect): Budget {
  return {
    id: row.id,
    workspaceId: row.workspaceId,
    name: row.name,
    scopeType: row.scopeType as Budget['scopeType'],
    scopeValue: row.scopeValue,
    amountUsd: row.amountUsd,
    period: row.period as Budget['period'],
    thresholdsPct: JSON.parse(row.thresholdsPctJson) as number[],
    notifyEmails: JSON.parse(row.notifyEmailsJson) as string[],
    createdBy: row.createdBy,
    createdAt: row.createdAt,
  };
}

class SqliteUserPreferencesRepo implements UserPreferencesRepo {
  constructor(private db: Db) {}

  async get(userId: string): Promise<UserPreferencesValue | null> {
    const rows = await this.db
      .select()
      .from(s.userPreferences)
      .where(eq(s.userPreferences.userId, userId))
      .limit(1);
    const row = rows[0];
    if (!row) return null;
    return {
      userId: row.userId,
      currency: row.currency,
      defaultWorkspaceId: row.defaultWorkspaceId,
      theme: row.theme,
      prefs: JSON.parse(row.prefsJson) as Record<string, unknown>,
      updatedAt: row.updatedAt,
    };
  }

  async upsert(value: UserPreferencesValue): Promise<UserPreferencesValue> {
    const row = {
      userId: value.userId,
      currency: value.currency,
      defaultWorkspaceId: value.defaultWorkspaceId,
      theme: value.theme,
      prefsJson: JSON.stringify(value.prefs),
      updatedAt: value.updatedAt,
    };
    await this.db
      .insert(s.userPreferences)
      .values(row)
      .onConflictDoUpdate({
        target: s.userPreferences.userId,
        set: {
          currency: row.currency,
          defaultWorkspaceId: row.defaultWorkspaceId,
          theme: row.theme,
          prefsJson: row.prefsJson,
          updatedAt: row.updatedAt,
        },
      });
    return value;
  }
}

class SqliteCachedAggregationsRepo implements CachedAggregationsRepo {
  constructor(private db: Db) {}

  async get(cacheKey: string): Promise<CachedAggregationValue | null> {
    const rows = await this.db
      .select()
      .from(s.cachedAggregations)
      .where(eq(s.cachedAggregations.cacheKey, cacheKey))
      .limit(1);
    const row = rows[0];
    if (!row) return null;
    if (new Date(row.expiresAt).getTime() < Date.now()) return null;
    return {
      cacheKey: row.cacheKey,
      queryHash: row.queryHash,
      payload: JSON.parse(row.payloadJson),
      computedAt: row.computedAt,
      expiresAt: row.expiresAt,
    };
  }

  async set(value: CachedAggregationValue): Promise<void> {
    const row = {
      cacheKey: value.cacheKey,
      queryHash: value.queryHash,
      payloadJson: JSON.stringify(value.payload),
      computedAt: value.computedAt,
      expiresAt: value.expiresAt,
    };
    await this.db
      .insert(s.cachedAggregations)
      .values(row)
      .onConflictDoUpdate({
        target: s.cachedAggregations.cacheKey,
        set: {
          queryHash: row.queryHash,
          payloadJson: row.payloadJson,
          computedAt: row.computedAt,
          expiresAt: row.expiresAt,
        },
      });
  }

  async prune(now: string): Promise<number> {
    const result = this.db.run(sql`delete from cached_aggregations where expires_at < ${now}`);
    return Number(result.changes ?? 0);
  }
}

class SqliteSetupStateRepo implements SetupStateRepo {
  constructor(private db: Db) {}

  async get(workspaceId: string): Promise<SetupStateValue | null> {
    const rows = await this.db
      .select()
      .from(s.setupState)
      .where(eq(s.setupState.workspaceId, workspaceId))
      .limit(1);
    const row = rows[0];
    if (!row) return null;
    return {
      workspaceId: row.workspaceId,
      systemTablesOk: row.systemTablesOk,
      permissionsOk: row.permissionsOk,
      curConfigured: row.curConfigured,
      azureExportConfigured: row.azureExportConfigured,
      lastCheckedAt: row.lastCheckedAt,
      details: JSON.parse(row.detailsJson) as Record<string, unknown>,
    };
  }

  async upsert(value: SetupStateValue): Promise<SetupStateValue> {
    const row = {
      workspaceId: value.workspaceId,
      systemTablesOk: value.systemTablesOk,
      permissionsOk: value.permissionsOk,
      curConfigured: value.curConfigured,
      azureExportConfigured: value.azureExportConfigured,
      lastCheckedAt: value.lastCheckedAt,
      detailsJson: JSON.stringify(value.details),
    };
    await this.db
      .insert(s.setupState)
      .values(row)
      .onConflictDoUpdate({
        target: s.setupState.workspaceId,
        set: {
          systemTablesOk: row.systemTablesOk,
          permissionsOk: row.permissionsOk,
          curConfigured: row.curConfigured,
          azureExportConfigured: row.azureExportConfigured,
          lastCheckedAt: row.lastCheckedAt,
          detailsJson: row.detailsJson,
        },
      });
    return value;
  }

  async recordCheck(workspaceId: string, result: SetupCheckResult): Promise<void> {
    const existing = (await this.get(workspaceId)) ?? {
      workspaceId,
      systemTablesOk: false,
      permissionsOk: false,
      curConfigured: false,
      azureExportConfigured: false,
      lastCheckedAt: result.checkedAt,
      details: {},
    };

    const next: SetupStateValue = {
      ...existing,
      lastCheckedAt: result.checkedAt,
      details: { ...existing.details, [result.step]: result },
    };
    if (result.step === 'systemTables') next.systemTablesOk = result.status === 'ok';
    if (result.step === 'permissions') next.permissionsOk = result.status === 'ok';
    if (result.step === 'awsCur') next.curConfigured = result.status === 'ok';
    if (result.step === 'azureExport') next.azureExportConfigured = result.status === 'ok';
    await this.upsert(next);
  }
}

class SqliteDataSourcesRepo implements DataSourcesRepo {
  constructor(private db: Db) {}

  async list(): Promise<DataSourceValue[]> {
    const rows = await this.db.select().from(s.dataSources);
    return rows.map(toDataSource);
  }

  async get(id: number): Promise<DataSourceValue | null> {
    const rows = await this.db
      .select()
      .from(s.dataSources)
      .where(eq(s.dataSources.id, id))
      .limit(1);
    const row = rows[0];
    return row ? toDataSource(row) : null;
  }

  async create(input: DataSourceCreateInput): Promise<DataSourceValue> {
    const inserted = await this.db
      .insert(s.dataSources)
      .values({
        templateId: input.templateId,
        name: input.name,
        description: input.description ?? null,
        providerName: input.providerName,
        billingAccountId: input.billingAccountId ?? null,
        tableName: input.tableName,
        jobId: input.jobId ?? null,
        pipelineId: input.pipelineId ?? null,
        focusVersion: input.focusVersion ?? null,
        enabled: input.enabled,
        configJson: JSON.stringify(input.config ?? {}),
        updatedAt: new Date().toISOString(),
      })
      .returning();
    const row = inserted[0];
    if (!row) throw new Error('Failed to insert data source');
    return toDataSource(row);
  }

  async update(id: number, patch: DataSourceUpdatePatch): Promise<DataSourceValue> {
    const set: Partial<typeof s.dataSources.$inferInsert> = {
      updatedAt: new Date().toISOString(),
    };
    if (patch.name !== undefined) set.name = patch.name;
    if (patch.description !== undefined) set.description = patch.description;
    if (patch.providerName !== undefined) set.providerName = patch.providerName;
    if (patch.billingAccountId !== undefined) set.billingAccountId = patch.billingAccountId;
    if (patch.tableName !== undefined) set.tableName = patch.tableName;
    if (patch.jobId !== undefined) set.jobId = patch.jobId;
    if (patch.pipelineId !== undefined) set.pipelineId = patch.pipelineId;
    if (patch.focusVersion !== undefined) set.focusVersion = patch.focusVersion;
    if (patch.enabled !== undefined) set.enabled = patch.enabled;
    if (patch.config !== undefined) set.configJson = JSON.stringify(patch.config);

    const updated = await this.db
      .update(s.dataSources)
      .set(set)
      .where(eq(s.dataSources.id, id))
      .returning();
    const row = updated[0];
    if (!row) throw new Error(`Data source ${id} not found`);
    return toDataSource(row);
  }

  async delete(id: number): Promise<void> {
    await this.db.delete(s.dataSources).where(eq(s.dataSources.id, id));
  }
}

function toDataSource(row: typeof s.dataSources.$inferSelect): DataSourceValue {
  return {
    id: row.id,
    templateId: row.templateId,
    name: row.name,
    description: row.description,
    providerName: row.providerName,
    billingAccountId: row.billingAccountId,
    tableName: row.tableName,
    jobId: row.jobId,
    pipelineId: row.pipelineId,
    focusVersion: row.focusVersion,
    enabled: row.enabled,
    config: JSON.parse(row.configJson) as Record<string, unknown>,
    updatedAt: row.updatedAt,
  };
}

class SqliteAppSettingsRepo implements AppSettingsRepo {
  constructor(private db: Db) {}

  async get(key: string): Promise<AppSettingValue | null> {
    const rows = await this.db
      .select()
      .from(s.appSettings)
      .where(eq(s.appSettings.key, key))
      .limit(1);
    const row = rows[0];
    if (!row) return null;
    return { key: row.key, value: row.value, updatedAt: row.updatedAt };
  }

  async list(): Promise<AppSettingValue[]> {
    const rows = await this.db.select().from(s.appSettings);
    return rows.map((row) => ({ key: row.key, value: row.value, updatedAt: row.updatedAt }));
  }

  async upsert(key: string, value: string): Promise<AppSettingValue> {
    const updatedAt = new Date().toISOString();
    await this.db.insert(s.appSettings).values({ key, value, updatedAt }).onConflictDoUpdate({
      target: s.appSettings.key,
      set: { value, updatedAt },
    });
    return { key, value, updatedAt };
  }

  async delete(key: string): Promise<void> {
    await this.db.delete(s.appSettings).where(eq(s.appSettings.key, key));
  }
}
