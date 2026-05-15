import assert from 'node:assert/strict';
import test from 'node:test';
import type { AddressInfo } from 'node:net';
import type { Server } from 'node:http';
import express from 'express';
import { SqliteClient } from '@finlake/db';
import {
  DEFAULT_DATABRICKS_ACCOUNT_ID,
  EnvSchema,
  PROVIDER_AWS,
  PROVIDER_DATABRICKS,
  type DataSource,
  type Env,
} from '@finlake/shared';
import { dataSourcesRouter } from '../src/routes/dataSources.js';

interface Harness {
  db: SqliteClient;
  base: string;
  close: () => Promise<void>;
}

async function startServer(): Promise<Harness> {
  const db = await SqliteClient.create({ sqlitePath: ':memory:' });
  const env: Env = EnvSchema.parse({});
  const app = express();
  app.use(express.json());
  app.use('/api/integrations', dataSourcesRouter(db, env));
  const server: Server = app.listen(0);
  await new Promise<void>((resolve) => server.once('listening', () => resolve()));
  const { port } = server.address() as AddressInfo;
  return {
    db,
    base: `http://127.0.0.1:${port}`,
    close: async () => {
      await new Promise<void>((resolve, reject) => {
        server.close((err) => (err ? reject(err) : resolve()));
      });
      await db.close();
    },
  };
}

async function postJson<T = unknown>(
  base: string,
  path: string,
  body: unknown,
): Promise<{ status: number; body: T }> {
  const res = await fetch(`${base}${path}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  });
  const parsed = (await res.json().catch(() => null)) as T;
  return { status: res.status, body: parsed };
}

test('POST /configurations rejects AWS without accountId', async () => {
  const env = await startServer();
  try {
    const { status, body } = await postJson<{ error: { message: string } }>(
      env.base,
      '/api/integrations/configurations',
      {
        templateId: 'aws',
        name: 'AWS',
        providerName: 'AWS',
        tableName: 'aws_usage',
      },
    );
    assert.equal(status, 400);
    assert.equal(body.error.message, 'accountId is required');
  } finally {
    await env.close();
  }
});

test('POST /configurations defaults Databricks accountId to "default"', async () => {
  const env = await startServer();
  try {
    const { status, body } = await postJson<DataSource>(
      env.base,
      '/api/integrations/configurations',
      {
        templateId: 'databricks_focus13',
        name: 'Databricks',
        providerName: 'Databricks',
        tableName: 'databricks_usage',
      },
    );
    assert.equal(status, 201);
    assert.equal(body.providerName, PROVIDER_DATABRICKS);
    assert.equal(body.accountId, DEFAULT_DATABRICKS_ACCOUNT_ID);
  } finally {
    await env.close();
  }
});

test('POST /configurations creates AWS row with composite PK reflected', async () => {
  const env = await startServer();
  try {
    const { status, body } = await postJson<DataSource>(
      env.base,
      '/api/integrations/configurations',
      {
        templateId: 'aws',
        name: 'AWS prod',
        providerName: 'AWS',
        accountId: '123456789012',
        tableName: 'aws_usage',
      },
    );
    assert.equal(status, 201);
    assert.equal(body.providerName, PROVIDER_AWS);
    assert.equal(body.accountId, '123456789012');

    const stored = await env.db.repos.dataSources.get({
      providerName: PROVIDER_AWS,
      accountId: '123456789012',
    });
    assert.ok(stored, 'row should be retrievable via composite PK');
    assert.equal(stored.name, 'AWS prod');
  } finally {
    await env.close();
  }
});

test('GET /configurations/:providerName/:accountId normalizes provider casing', async () => {
  const env = await startServer();
  try {
    await postJson<DataSource>(env.base, '/api/integrations/configurations', {
      templateId: 'databricks_focus13',
      name: 'Databricks',
      providerName: 'Databricks',
      tableName: 'databricks_usage',
    });
    const res = await fetch(`${env.base}/api/integrations/configurations/Databricks/default`);
    assert.equal(res.status, 200);
    const row = (await res.json()) as DataSource;
    assert.equal(row.providerName, PROVIDER_DATABRICKS);
    assert.equal(row.accountId, DEFAULT_DATABRICKS_ACCOUNT_ID);
  } finally {
    await env.close();
  }
});

test('POST /configurations rejects unknown templateId', async () => {
  const env = await startServer();
  try {
    const { status, body } = await postJson<{ error: { message: string } }>(
      env.base,
      '/api/integrations/configurations',
      {
        templateId: 'bogus',
        name: 'X',
        providerName: 'aws',
        accountId: '123456789012',
        tableName: 'aws_usage',
      },
    );
    assert.equal(status, 400);
    assert.equal(body.error.message, 'Invalid templateId');
  } finally {
    await env.close();
  }
});
