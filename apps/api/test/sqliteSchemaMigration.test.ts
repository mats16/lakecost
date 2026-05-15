import assert from 'node:assert/strict';
import test from 'node:test';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createClient } from '@libsql/client';
import { SqliteClient } from '@finlake/db';

async function withTempDb<T>(fn: (path: string) => Promise<T>): Promise<T> {
  const dir = await mkdtemp(join(tmpdir(), 'finlake-sqlite-test-'));
  const path = join(dir, 'finlake.db');
  try {
    return await fn(path);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
}

async function createLegacyDataSources(path: string): Promise<void> {
  const raw = createClient({ url: `file:${path}` });
  try {
    await raw.execute(
      `CREATE TABLE data_sources (
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
    );
    await raw.execute({
      sql: `INSERT INTO data_sources
        (template_id, name, provider_name, billing_account_id, table_name, focus_version, enabled, config_json, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      args: [
        'databricks_focus13',
        'Legacy row',
        'Databricks',
        null,
        'databricks_usage',
        '1.3',
        1,
        '{}',
        new Date().toISOString(),
      ],
    });
  } finally {
    raw.close();
  }
}

async function readColumnNames(path: string): Promise<string[]> {
  const raw = createClient({ url: `file:${path}` });
  try {
    const info = await raw.execute('PRAGMA table_info(data_sources)');
    return info.rows.map((row) => String(row.name));
  } finally {
    raw.close();
  }
}

test('SqliteClient migrates legacy data_sources table on bootstrap', async () => {
  await withTempDb(async (path) => {
    await createLegacyDataSources(path);

    const beforeColumns = await readColumnNames(path);
    assert.ok(beforeColumns.includes('id'), 'precondition: legacy id column exists');
    assert.ok(beforeColumns.includes('template_id'), 'precondition: template_id exists');

    const db = await SqliteClient.create({ sqlitePath: path });
    try {
      const rows = await db.repos.dataSources.list();
      assert.equal(rows.length, 0, 'legacy rows are discarded by the one-time migration');
    } finally {
      await db.close();
    }

    const afterColumns = await readColumnNames(path);
    assert.ok(afterColumns.includes('account_id'), 'account_id column is created');
    assert.ok(!afterColumns.includes('id'), 'legacy id column is removed');
    assert.ok(!afterColumns.includes('template_id'), 'legacy template_id column is removed');
    assert.ok(
      !afterColumns.includes('billing_account_id'),
      'legacy billing_account_id column is removed',
    );
  });
});

test('SqliteClient bootstrap is a no-op when schema is already current', async () => {
  await withTempDb(async (path) => {
    const first = await SqliteClient.create({ sqlitePath: path });
    try {
      await first.repos.dataSources.create({
        name: 'AWS',
        providerName: 'aws',
        accountId: '123456789012',
        tableName: 'aws_usage',
        focusVersion: '1.2',
        enabled: false,
        config: {},
      });
    } finally {
      await first.close();
    }

    const second = await SqliteClient.create({ sqlitePath: path });
    try {
      const rows = await second.repos.dataSources.list();
      assert.equal(rows.length, 1, 'no migration runs against the current schema');
      assert.equal(rows[0]?.providerName, 'aws');
      assert.equal(rows[0]?.accountId, '123456789012');
    } finally {
      await second.close();
    }
  });
});
