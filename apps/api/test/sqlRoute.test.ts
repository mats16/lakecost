import assert from 'node:assert/strict';
import test from 'node:test';
import type { AddressInfo } from 'node:net';
import type { Server } from 'node:http';
import express from 'express';
import type { Env } from '@finlake/shared';
import { sqlRouter, validateReadOnlySql } from '../src/routes/sql.js';
import {
  StatementExecutor,
  type StatementExecutorOpts,
} from '../src/services/statementExecution.js';
import type { WorkspaceClient } from '../src/services/statementExecution.js';

async function startServer(executor: Partial<StatementExecutor>) {
  const app = express();
  app.use(express.json());
  app.use((req, _res, next) => {
    req.user = { accessToken: 'obo-token' };
    next();
  });
  app.use(
    '/api/sql',
    sqlRouter(
      {
        DATABRICKS_HOST: 'https://example.cloud.databricks.com',
        SQL_WAREHOUSE_ID: 'warehouse-1',
      } as Env,
      () => executor as StatementExecutor,
    ),
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
    assert.equal(calls[0]?.query, 'SELECT :value AS sample_value');
    assert.equal(calls[0]?.warehouseId, 'warehouse-override');
    assert.deepEqual(calls[0]?.params, [{ name: 'value', value: 'abc', type: 'STRING' }]);
  } finally {
    await env.close();
  }
});

test('GET /api/sql/:statement_id returns succeeded rows', async () => {
  const env = await startServer({
    getRaw: async (statementId) => ({
      statement_id: statementId,
      status: 'SUCCEEDED',
      columns: [{ name: 'sample_value', typeName: 'STRING' }],
      rows: [{ sampleValue: 'ok' }],
    }),
  });
  try {
    const res = await fetch(`${env.base}/api/sql/stmt-123`);
    assert.equal(res.status, 200);
    const body = (await res.json()) as {
      statement_id: string;
      status: string;
      rows: Array<{ sampleValue: string }>;
    };
    assert.equal(body.statement_id, 'stmt-123');
    assert.equal(body.status, 'SUCCEEDED');
    assert.equal(body.rows[0]?.sampleValue, 'ok');
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
