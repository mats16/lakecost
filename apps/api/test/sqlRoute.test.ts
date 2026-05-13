import assert from 'node:assert/strict';
import test from 'node:test';
import type { AddressInfo } from 'node:net';
import type { Server } from 'node:http';
import express from 'express';
import { SqliteClient } from '@finlake/db';
import type { Env } from '@finlake/shared';
import { sqlRouter, validateReadOnlySql } from '../src/routes/sql.js';
import {
  StatementExecutor,
  type StatementExecutorOpts,
} from '../src/services/statementExecution.js';
import type { WorkspaceClient } from '../src/services/statementExecution.js';

async function startServer(
  executor: Partial<StatementExecutor>,
  options: { env?: Partial<Env> } = {},
) {
  const db = await SqliteClient.create({ sqlitePath: ':memory:' });
  const app = express();
  app.use(express.json());
  app.use((req, _res, next) => {
    const userId = req.header('x-test-user') ?? 'user-1';
    const accessToken = req.header('x-test-token') ?? `obo-token-${userId}`;
    req.user = { accessToken, userId, email: `${userId}@example.com` };
    next();
  });
  const routeEnv = {
    DATABRICKS_HOST: 'https://example.cloud.databricks.com',
    SQL_WAREHOUSE_ID: 'warehouse-1',
    SQL_API_CACHE_TTL_SEC: 300,
    SQL_API_STATEMENT_TTL_SEC: 900,
    SQL_API_SUBMIT_RATE_LIMIT_PER_MINUTE: 60,
    ...options.env,
  } as Env;
  app.use(
    '/api/sql',
    sqlRouter(db, routeEnv, () => executor as StatementExecutor),
  );
  const server: Server = app.listen(0);
  await new Promise<void>((resolve) => server.once('listening', () => resolve()));
  const { port } = server.address() as AddressInfo;
  return {
    base: `http://127.0.0.1:${port}`,
    close: async () => {
      await new Promise<void>((resolve, reject) => {
        server.close((err) => (err ? reject(err) : resolve()));
      });
      await db.close();
    },
  };
}

test('POST /api/sql submits one read-only statement and returns statement_id', async () => {
  const calls: Array<{ query: string; params: unknown[]; warehouseId?: string }> = [];
  const env = await startServer({
    submitRaw: async (query, params, warehouseId) => {
      calls.push({ query, params, warehouseId });
      return { statement_id: 'stmt-123', status: 'PENDING' };
    },
  });
  try {
    const res = await fetch(`${env.base}/api/sql`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        query: 'SELECT :value AS sample_value',
        warehouse_id: 'warehouse-override',
        params: [{ name: 'value', value: 'abc', type: 'STRING' }],
      }),
    });
    assert.equal(res.status, 200);
    const body = (await res.json()) as { statement_id: string; status: string };
    assert.equal(body.statement_id, 'stmt-123');
    assert.equal(body.status, 'PENDING');
    assert.equal('result' in body, false);
    assert.equal(calls[0]?.query, 'SELECT :value AS sample_value');
    assert.equal(calls[0]?.warehouseId, 'warehouse-override');
    assert.deepEqual(calls[0]?.params, [{ name: 'value', value: 'abc', type: 'STRING' }]);
  } finally {
    await env.close();
  }
});

test('POST /api/sql falls back to env SQL_WAREHOUSE_ID when no override is supplied', async () => {
  const calls: Array<{ query: string; params: unknown[]; warehouseId?: string }> = [];
  const env = await startServer({
    submitRaw: async (query, params, warehouseId) => {
      calls.push({ query, params, warehouseId });
      return { statement_id: 'stmt-env', status: 'PENDING' };
    },
  });
  try {
    const res = await fetch(`${env.base}/api/sql`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ query: 'SELECT 1', params: [] }),
    });
    assert.equal(res.status, 200);
    assert.equal(calls[0]?.query, 'SELECT 1');
    assert.equal(calls[0]?.warehouseId, undefined);
  } finally {
    await env.close();
  }
});

test('GET /api/sql/:statement_id returns succeeded rows', async () => {
  const env = await startServer({
    submitRaw: async () => ({ statement_id: 'stmt-123', status: 'PENDING' }),
    getRaw: async (statementId) => ({
      statement_id: statementId,
      status: 'SUCCEEDED',
      columns: [{ name: 'sample_value', typeName: 'STRING' }],
      rows: [{ sampleValue: 'ok' }],
    }),
  });
  try {
    await fetch(`${env.base}/api/sql`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ query: 'SELECT 1', params: [] }),
    });
    const res = await fetch(`${env.base}/api/sql/stmt-123`);
    assert.equal(res.status, 200);
    const body = (await res.json()) as {
      statement_id: string;
      status: string;
      result: { rows: Array<{ sampleValue: string }> };
    };
    assert.equal(body.statement_id, 'stmt-123');
    assert.equal(body.status, 'SUCCEEDED');
    assert.equal(body.result.rows[0]?.sampleValue, 'ok');
  } finally {
    await env.close();
  }
});

test('POST /api/sql reuses cached succeeded results across warehouse_id overrides', async () => {
  let submitCount = 0;
  const env = await startServer({
    submitRaw: async () => {
      submitCount += 1;
      return { statement_id: 'stmt-cache', status: 'PENDING' };
    },
    getRaw: async (statementId) => ({
      statement_id: statementId,
      status: 'SUCCEEDED',
      columns: [{ name: 'sample_value', typeName: 'STRING' }],
      rows: [{ sampleValue: 'ok' }],
    }),
  });
  try {
    const first = await fetch(`${env.base}/api/sql`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        query: 'SELECT :value AS sample_value',
        warehouse_id: 'warehouse-a',
        params: [{ name: 'value', value: 'abc', type: 'STRING' }],
      }),
    });
    assert.equal(first.status, 200);
    await fetch(`${env.base}/api/sql/stmt-cache`);

    const second = await fetch(`${env.base}/api/sql`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        query: 'SELECT :value AS sample_value',
        warehouse_id: 'warehouse-b',
        params: [{ name: 'value', value: 'abc', type: 'STRING' }],
      }),
    });
    assert.equal(second.status, 200);
    const submitted = (await second.json()) as {
      statement_id?: string;
      status: string;
      result: { rows: Array<{ sampleValue: string }> };
    };
    assert.equal(submitted.status, 'SUCCEEDED');
    assert.equal(submitted.statement_id, undefined);
    assert.equal(submitted.result.rows[0]?.sampleValue, 'ok');
    assert.equal(submitCount, 1);
  } finally {
    await env.close();
  }
});

test('POST /api/sql does not cache non-succeeded statement results', async () => {
  let submitCount = 0;
  const env = await startServer({
    submitRaw: async () => {
      submitCount += 1;
      return { statement_id: `stmt-failed-${submitCount}`, status: 'PENDING' };
    },
    getRaw: async (statementId) => ({
      statement_id: statementId,
      status: 'FAILED',
      error: 'boom',
    }),
  });
  try {
    const first = await fetch(`${env.base}/api/sql`, {
      method: 'POST',
      headers: { 'content-type': 'application/json', 'x-test-user': 'noncache-user' },
      body: JSON.stringify({ query: 'SELECT 1', params: [] }),
    });
    const firstBody = (await first.json()) as { statement_id: string };
    const failed = await fetch(`${env.base}/api/sql/${firstBody.statement_id}`, {
      headers: { 'x-test-user': 'noncache-user' },
    });
    assert.equal(failed.status, 200);
    const failedBody = (await failed.json()) as {
      statement_id: string;
      status: string;
      error: string;
      result?: unknown;
    };
    assert.equal(failedBody.status, 'FAILED');
    assert.equal(failedBody.error, 'boom');
    assert.equal(failedBody.result, undefined);

    const second = await fetch(`${env.base}/api/sql`, {
      method: 'POST',
      headers: { 'content-type': 'application/json', 'x-test-user': 'noncache-user' },
      body: JSON.stringify({ query: 'SELECT 1', params: [] }),
    });
    assert.equal(second.status, 200);
    assert.equal(submitCount, 2);
  } finally {
    await env.close();
  }
});

test('POST /api/sql rate limits uncached submissions per user', async () => {
  const env = await startServer(
    {
      submitRaw: async () => ({ statement_id: 'stmt-rate', status: 'PENDING' }),
    },
    { env: { SQL_API_SUBMIT_RATE_LIMIT_PER_MINUTE: 1 } },
  );
  try {
    const first = await fetch(`${env.base}/api/sql`, {
      method: 'POST',
      headers: { 'content-type': 'application/json', 'x-test-user': 'rate-user' },
      body: JSON.stringify({ query: 'SELECT 1', params: [] }),
    });
    assert.equal(first.status, 200);

    const second = await fetch(`${env.base}/api/sql`, {
      method: 'POST',
      headers: { 'content-type': 'application/json', 'x-test-user': 'rate-user' },
      body: JSON.stringify({ query: 'SELECT 2', params: [] }),
    });
    assert.equal(second.status, 429);
  } finally {
    await env.close();
  }
});

test('GET /api/sql/:statement_id rejects unknown statements and owner mismatches', async () => {
  const env = await startServer({
    submitRaw: async () => ({ statement_id: 'stmt-owned', status: 'PENDING' }),
    getRaw: async (statementId) => ({ statement_id: statementId, status: 'RUNNING' }),
  });
  try {
    const unknown = await fetch(`${env.base}/api/sql/missing-stmt`);
    assert.equal(unknown.status, 404);

    await fetch(`${env.base}/api/sql`, {
      method: 'POST',
      headers: { 'content-type': 'application/json', 'x-test-user': 'alice' },
      body: JSON.stringify({ query: 'SELECT 1', params: [] }),
    });
    const mismatch = await fetch(`${env.base}/api/sql/stmt-owned`, {
      headers: { 'x-test-user': 'bob' },
    });
    assert.equal(mismatch.status, 403);
  } finally {
    await env.close();
  }
});

test('validateReadOnlySql allows SELECT and WITH statements only', () => {
  assert.equal(validateReadOnlySql('SELECT 1'), undefined);
  assert.equal(validateReadOnlySql('WITH x AS (SELECT 1) SELECT * FROM x'), undefined);
  assert.match(validateReadOnlySql('DROP TABLE t') ?? '', /Only SELECT or WITH/);
  assert.match(validateReadOnlySql('SELECT 1; SELECT 2') ?? '', /single SQL statement/);
  assert.match(validateReadOnlySql('WITH x AS (DELETE FROM t) SELECT 1') ?? '', /DELETE/);
  assert.equal(validateReadOnlySql("SELECT 'DROP TABLE t' AS text"), undefined);
  assert.equal(validateReadOnlySql('SELECT 1 -- DROP TABLE t'), undefined);
  assert.equal(validateReadOnlySql("SELECT 'REFRESH TABLE t' AS text"), undefined);
  assert.equal(validateReadOnlySql('SELECT 1 -- CACHE TABLE t'), undefined);
  assert.equal(validateReadOnlySql('SELECT `USE` FROM t'), undefined);
  for (const keyword of [
    'SET',
    'USE',
    'RESET',
    'ANALYZE',
    'MSCK',
    'CACHE',
    'UNCACHE',
    'REFRESH',
    'CLEAR',
    'LOAD',
  ]) {
    assert.match(
      validateReadOnlySql(`WITH x AS (${keyword} TABLE t) SELECT 1`) ?? '',
      new RegExp(keyword),
    );
  }
});

test('StatementExecutor.getRaw converts snake_case result columns to camelCase rows', async () => {
  const workspaceClient = {
    statementExecution: {
      getStatement: async () => ({
        statement_id: 'stmt-123',
        status: { state: 'SUCCEEDED' },
        manifest: {
          schema: {
            columns: [
              { name: 'sample_value', type_name: 'STRING' },
              { name: 'cost_usd', type_name: 'DOUBLE' },
            ],
          },
        },
        result: { data_array: [['ok', '12.5']] },
      }),
    },
  } as unknown as WorkspaceClient;
  const executor = new StatementExecutor({
    workspaceClient,
    warehouseId: 'warehouse-1',
  } as StatementExecutorOpts);

  const result = await executor.getRaw('stmt-123');
  assert.deepEqual(result.rows, [{ sampleValue: 'ok', costUsd: 12.5 }]);
});

test('StatementExecutor.submitRaw can override the default warehouse_id', async () => {
  const calls: Array<{ warehouse_id?: string; statement?: string }> = [];
  const workspaceClient = {
    statementExecution: {
      executeStatement: async (input: { warehouse_id?: string; statement?: string }) => {
        calls.push(input);
        return {
          statement_id: 'stmt-override',
          status: { state: 'PENDING' },
        };
      },
    },
  } as unknown as WorkspaceClient;
  const executor = new StatementExecutor({
    workspaceClient,
    warehouseId: 'default-warehouse',
  } as StatementExecutorOpts);

  const result = await executor.submitRaw('SELECT 1', [], 'override-warehouse');
  assert.equal(result.statement_id, 'stmt-override');
  assert.equal(calls[0]?.warehouse_id, 'override-warehouse');
});
