import { createClient, type Client } from '@libsql/client';
import { drizzle } from 'drizzle-orm/libsql';
import type { LibSQLDatabase } from 'drizzle-orm/libsql';
import { and, eq, inArray, sql } from 'drizzle-orm';
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
  PricingDataRepo,
  PricingDataRunPatch,
  PricingDataUpsertInput,
  PricingDataValue,
  Repositories,
  SetupStateRepo,
  SetupStateValue,
  UserPreferencesRepo,
  UserPreferencesValue,
} from './repositories/index.js';
import { ensureParentDir } from './paths.js';
import { logger } from './logger.js';
import type {
  Budget,
  CreateBudgetInput,
  SetupCheckResult,
  UpdateBudgetInput,
} from '@finlake/shared';

type Db = LibSQLDatabase<typeof s>;

export class SqliteClient implements DatabaseClient {
  readonly backend = 'sqlite' as const;
  readonly repos: Repositories;

  private constructor(
    private readonly raw: Client,
    private readonly db: Db,
  ) {
    this.repos = {
      budgets: new SqliteBudgetsRepo(db),
      userPreferences: new SqliteUserPreferencesRepo(db),
      cachedAggregations: new SqliteCachedAggregationsRepo(db),
      setupState: new SqliteSetupStateRepo(db),
      appSettings: new SqliteAppSettingsRepo(db),
      dataSources: new SqliteDataSourcesRepo(db),
      pricingData: new SqlitePricingDataRepo(db),
    };
  }

  static async create(opts: { sqlitePath: string }): Promise<SqliteClient> {
    if (opts.sqlitePath !== ':memory:') ensureParentDir(opts.sqlitePath);
    const raw = createClient({ url: toLibsqlUrl(opts.sqlitePath) });
    await raw.execute('PRAGMA journal_mode = WAL');
    await raw.execute('PRAGMA foreign_keys = ON');
    const db = drizzle(raw, { schema: s });
    const client = new SqliteClient(raw, db);
    await client.bootstrapSchema();
    return client;
  }

  private async bootstrapSchema(): Promise<void> {
    await this.migratePricingDataSchema();
    const statements = [
      `CREATE TABLE IF NOT EXISTS budgets (
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
      )`,
      `CREATE TABLE IF NOT EXISTS budget_alerts (
        id TEXT PRIMARY KEY,
        budget_id TEXT NOT NULL,
        threshold_pct INTEGER NOT NULL,
        triggered_at TEXT NOT NULL,
        actual_usd REAL NOT NULL,
        notified_channels_json TEXT NOT NULL DEFAULT '[]'
      )`,
      `CREATE TABLE IF NOT EXISTS user_preferences (
        user_id TEXT PRIMARY KEY,
        currency TEXT NOT NULL DEFAULT 'USD',
        default_workspace_id TEXT,
        theme TEXT NOT NULL DEFAULT 'system',
        prefs_json TEXT NOT NULL DEFAULT '{}',
        updated_at TEXT NOT NULL
      )`,
      `CREATE TABLE IF NOT EXISTS cached_aggregations (
        cache_key TEXT PRIMARY KEY,
        query_hash TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        computed_at TEXT NOT NULL,
        expires_at TEXT NOT NULL
      )`,
      `CREATE TABLE IF NOT EXISTS tag_chargeback_rules (
        id TEXT PRIMARY KEY,
        tag_key TEXT NOT NULL,
        tag_value_pattern TEXT NOT NULL,
        cost_center TEXT NOT NULL,
        owner_email TEXT,
        priority INTEGER NOT NULL DEFAULT 100
      )`,
      `CREATE TABLE IF NOT EXISTS app_settings (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL,
        updated_at TEXT NOT NULL
      )`,
      `CREATE TABLE IF NOT EXISTS data_sources (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        template_id TEXT NOT NULL,
        name TEXT NOT NULL,
        provider_name TEXT NOT NULL,
        billing_account_id TEXT,
        table_name TEXT NOT NULL,
        focus_version TEXT,
        enabled INTEGER NOT NULL DEFAULT 1,
        config_json TEXT NOT NULL DEFAULT '{}',
        updated_at TEXT NOT NULL,
        UNIQUE(provider_name, billing_account_id)
      )`,
      `CREATE TABLE IF NOT EXISTS pricing_data (
        provider TEXT NOT NULL,
        service TEXT NOT NULL,
        slug TEXT NOT NULL,
        "table" TEXT NOT NULL,
        raw_data_table TEXT,
        raw_data_path TEXT,
        notebook_path TEXT,
        notebook_id TEXT,
        metadata TEXT NOT NULL DEFAULT '{}',
        run_id INTEGER,
        run_status TEXT NOT NULL DEFAULT 'not_started',
        run_url TEXT,
        run_started_at TEXT,
        run_finished_at TEXT,
        run_checked_at TEXT,
        updated_at TEXT NOT NULL,
        PRIMARY KEY (provider, service)
      )`,
      `CREATE UNIQUE INDEX IF NOT EXISTS pricing_data_slug_unique ON pricing_data(slug)`,
      `CREATE TABLE IF NOT EXISTS setup_state (
        workspace_id TEXT PRIMARY KEY,
        system_tables_ok INTEGER NOT NULL DEFAULT 0,
        permissions_ok INTEGER NOT NULL DEFAULT 0,
        cur_configured INTEGER NOT NULL DEFAULT 0,
        azure_export_configured INTEGER NOT NULL DEFAULT 0,
        last_checked_at TEXT NOT NULL,
        details_json TEXT NOT NULL DEFAULT '{}'
      )`,
    ];
    await this.raw.batch(
      statements.map((sql) => ({ sql, args: [] })),
      'write',
    );
    await this.dropColumnIfExists('data_sources', 'job_id');
    await this.dropColumnIfExists('data_sources', 'pipeline_id');
    await this.migrateAppSettingKey('focus_pipeline_job_id', 'lakeflow_pipeline_job_id');
    await this.migrateAppSettingKey('focus_pipeline_id', 'lakeflow_pipeline_id');
    logger.debug('SQLite schema bootstrap complete');
  }

  private async migratePricingDataSchema(): Promise<void> {
    const result = await this.raw.execute('PRAGMA table_info(pricing_data)');
    const columns = result.rows.map((row) => String(row.name));
    if (columns.length === 0) return;

    if (columns.includes('id') && !columns.includes('provider')) {
      await this.raw.execute('ALTER TABLE pricing_data RENAME TO pricing_data_legacy');
      await this.raw.execute(`CREATE TABLE pricing_data (
        provider TEXT NOT NULL,
        service TEXT NOT NULL,
        slug TEXT NOT NULL,
        "table" TEXT NOT NULL,
        raw_data_table TEXT,
        raw_data_path TEXT,
        notebook_path TEXT,
        notebook_id TEXT,
        metadata TEXT NOT NULL DEFAULT '{}',
        run_id INTEGER,
        run_status TEXT NOT NULL DEFAULT 'not_started',
        run_url TEXT,
        run_started_at TEXT,
        run_finished_at TEXT,
        run_checked_at TEXT,
        updated_at TEXT NOT NULL,
        PRIMARY KEY (provider, service)
      )`);
      await this.raw.execute(`INSERT INTO pricing_data (
        provider,
        service,
        slug,
        "table",
        raw_data_table,
        raw_data_path,
        notebook_path,
        notebook_id,
        metadata,
        run_id,
        run_status,
        run_url,
        run_started_at,
        run_finished_at,
        run_checked_at,
        updated_at
      )
      SELECT
        'AWS',
        'AmazonEC2',
        'aws_ec2',
        "table",
        NULL,
        NULL,
        notebook_path,
        notebook_id,
        metadata,
        NULL,
        'not_started',
        NULL,
        NULL,
        NULL,
        NULL,
        updated_at
      FROM pricing_data_legacy
      WHERE id = 'aws_ec2'
      LIMIT 1`);
      return;
    }

    if (!columns.includes('provider')) return;
    const addColumns: Array<{ name: string; type: string }> = [
      { name: 'slug', type: 'TEXT' },
      { name: 'raw_data_table', type: 'TEXT' },
      { name: 'raw_data_path', type: 'TEXT' },
      { name: 'run_id', type: 'INTEGER' },
      { name: 'run_status', type: "TEXT NOT NULL DEFAULT 'not_started'" },
      { name: 'run_url', type: 'TEXT' },
      { name: 'run_started_at', type: 'TEXT' },
      { name: 'run_finished_at', type: 'TEXT' },
      { name: 'run_checked_at', type: 'TEXT' },
    ];
    for (const col of addColumns) {
      if (!columns.includes(col.name)) {
        await this.raw.execute(`ALTER TABLE pricing_data ADD COLUMN ${col.name} ${col.type}`);
      }
    }
    if (!columns.includes('slug')) {
      await this.raw.execute({
        sql: `UPDATE pricing_data SET slug = ? WHERE provider = ? AND service = ?`,
        args: ['aws_ec2', 'AWS', 'AmazonEC2'],
      });
    }
  }

  private async migrateAppSettingKey(oldKey: string, newKey: string): Promise<void> {
    await this.raw.execute({
      sql: `UPDATE app_settings
        SET key = ?
        WHERE key = ?
          AND NOT EXISTS (SELECT 1 FROM app_settings WHERE key = ?)`,
      args: [newKey, oldKey, newKey],
    });
    await this.raw.execute({
      sql: 'DELETE FROM app_settings WHERE key = ?',
      args: [oldKey],
    });
  }

  private async dropColumnIfExists(table: string, column: string): Promise<void> {
    try {
      await this.raw.execute(`ALTER TABLE ${table} DROP COLUMN ${column}`);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      if (/no such column|unknown column/i.test(message)) return;
      // Older SQLite/libSQL builds may not support DROP COLUMN. Existing
      // columns are harmless because the repo no longer reads or writes them.
      if (/syntax error|near "DROP"/i.test(message)) return;
      throw err;
    }
  }

  async healthCheck(): Promise<{ ok: true; backend: 'sqlite' }> {
    await this.db.run(sql`select 1`);
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

function toLibsqlUrl(sqlitePath: string): string {
  return sqlitePath === ':memory:' ? ':memory:' : `file:${sqlitePath}`;
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

  async update(id: string, input: UpdateBudgetInput): Promise<Budget | null> {
    const updated = await this.db
      .update(s.budgets)
      .set({
        name: input.name,
        scopeType: input.scopeType,
        scopeValue: input.scopeValue,
        amountUsd: input.amountUsd,
        period: input.period,
        thresholdsPctJson: JSON.stringify(input.thresholdsPct),
        notifyEmailsJson: JSON.stringify(input.notifyEmails),
      })
      .where(eq(s.budgets.id, id))
      .returning();
    return updated[0] ? toBudget(updated[0]) : null;
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
    const result = await this.db.run(
      sql`delete from cached_aggregations where expires_at < ${now}`,
    );
    return result.rowsAffected;
  }

  async clear(): Promise<number> {
    const result = await this.db.run(sql`delete from cached_aggregations`);
    return result.rowsAffected;
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

  async clear(): Promise<number> {
    const result = await this.db.run(sql`delete from setup_state`);
    return result.rowsAffected;
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
        providerName: input.providerName,
        billingAccountId: input.billingAccountId ?? null,
        tableName: input.tableName,
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
    if (patch.providerName !== undefined) set.providerName = patch.providerName;
    if (patch.billingAccountId !== undefined) set.billingAccountId = patch.billingAccountId;
    if (patch.tableName !== undefined) set.tableName = patch.tableName;
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

  async clear(): Promise<number> {
    const result = await this.db.run(sql`delete from data_sources`);
    return result.rowsAffected;
  }
}

function toDataSource(row: typeof s.dataSources.$inferSelect): DataSourceValue {
  return {
    id: row.id,
    templateId: row.templateId,
    name: row.name,
    providerName: row.providerName,
    billingAccountId: row.billingAccountId,
    tableName: row.tableName,
    focusVersion: row.focusVersion,
    enabled: row.enabled,
    config: JSON.parse(row.configJson) as Record<string, unknown>,
    updatedAt: row.updatedAt,
  };
}

class SqlitePricingDataRepo implements PricingDataRepo {
  constructor(private db: Db) {}

  async get(provider: string, service: string): Promise<PricingDataValue | null> {
    const rows = await this.db
      .select()
      .from(s.pricingData)
      .where(and(eq(s.pricingData.provider, provider), eq(s.pricingData.service, service)))
      .limit(1);
    const row = rows[0];
    return row ? toPricingData(row) : null;
  }

  async getBySlug(slug: string): Promise<PricingDataValue | null> {
    const rows = await this.db
      .select()
      .from(s.pricingData)
      .where(eq(s.pricingData.slug, slug))
      .limit(1);
    const row = rows[0];
    return row ? toPricingData(row) : null;
  }

  async getByNotebookId(notebookId: string): Promise<PricingDataValue | null> {
    const rows = await this.db
      .select()
      .from(s.pricingData)
      .where(eq(s.pricingData.notebookId, notebookId))
      .limit(1);
    const row = rows[0];
    return row ? toPricingData(row) : null;
  }

  async upsert(input: PricingDataUpsertInput): Promise<PricingDataValue> {
    const row = {
      provider: input.provider,
      service: input.service,
      slug: input.slug,
      tableName: input.table,
      rawDataTable: input.rawDataTable,
      rawDataPath: input.rawDataPath,
      notebookPath: input.notebookPath,
      notebookId: input.notebookId,
      metadataJson: JSON.stringify(input.metadata),
      runId: input.runId,
      runStatus: input.runStatus,
      runUrl: input.runUrl,
      runStartedAt: input.runStartedAt,
      runFinishedAt: input.runFinishedAt,
      runCheckedAt: input.runCheckedAt,
      updatedAt: new Date().toISOString(),
    };
    await this.db
      .insert(s.pricingData)
      .values(row)
      .onConflictDoUpdate({
        target: [s.pricingData.provider, s.pricingData.service],
        set: {
          tableName: row.tableName,
          slug: row.slug,
          rawDataTable: row.rawDataTable,
          rawDataPath: row.rawDataPath,
          notebookPath: row.notebookPath,
          notebookId: row.notebookId,
          metadataJson: row.metadataJson,
          runId: row.runId,
          runStatus: row.runStatus,
          runUrl: row.runUrl,
          runStartedAt: row.runStartedAt,
          runFinishedAt: row.runFinishedAt,
          runCheckedAt: row.runCheckedAt,
          updatedAt: row.updatedAt,
        },
      });
    return toPricingData(row);
  }

  async updateRun(slug: string, patch: PricingDataRunPatch): Promise<PricingDataValue | null> {
    const updatedAt = new Date().toISOString();
    const updated = await this.db
      .update(s.pricingData)
      .set({
        runId: patch.runId,
        runStatus: patch.runStatus,
        runUrl: patch.runUrl,
        runStartedAt: patch.runStartedAt,
        runFinishedAt: patch.runFinishedAt,
        runCheckedAt: patch.runCheckedAt,
        updatedAt,
      })
      .where(eq(s.pricingData.slug, slug))
      .returning();
    const row = updated[0];
    return row ? toPricingData(row) : null;
  }

  async deleteBySlug(slug: string): Promise<boolean> {
    const result = await this.db.delete(s.pricingData).where(eq(s.pricingData.slug, slug));
    return result.rowsAffected > 0;
  }

  async clear(): Promise<number> {
    const result = await this.db.run(sql`delete from pricing_data`);
    return result.rowsAffected;
  }
}

function toPricingData(row: typeof s.pricingData.$inferSelect): PricingDataValue {
  return {
    provider: row.provider,
    service: row.service,
    slug: row.slug,
    table: row.tableName,
    rawDataTable: row.rawDataTable,
    rawDataPath: row.rawDataPath,
    notebookPath: row.notebookPath,
    notebookId: row.notebookId,
    metadata: JSON.parse(row.metadataJson) as Record<string, unknown>,
    runId: row.runId,
    runStatus: row.runStatus as PricingDataValue['runStatus'],
    runUrl: row.runUrl,
    runStartedAt: row.runStartedAt,
    runFinishedAt: row.runFinishedAt,
    runCheckedAt: row.runCheckedAt,
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

  async deleteMany(keys: readonly string[]): Promise<number> {
    if (keys.length === 0) return 0;
    const result = await this.db.delete(s.appSettings).where(inArray(s.appSettings.key, [...keys]));
    return result.rowsAffected;
  }
}
