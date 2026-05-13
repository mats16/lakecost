import assert from 'node:assert/strict';
import test from 'node:test';
import type { AddressInfo } from 'node:net';
import type { Server } from 'node:http';
import express from 'express';
import { SqliteClient } from '@finlake/db';
import type { Budget, CreateBudgetInput } from '@finlake/shared';
import { budgetsRouter } from '../src/routes/budgets.js';

type Env = {
  db: SqliteClient;
  base: string;
  close: () => Promise<void>;
};

async function startServer(): Promise<Env> {
  const db = await SqliteClient.create({ sqlitePath: ':memory:' });
  const app = express();
  app.use(express.json());
  app.use('/api/budgets', budgetsRouter(db));
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

const validInput: Omit<CreateBudgetInput, 'workspaceId'> = {
  name: 'Q1',
  scopeType: 'provider',
  scopeValue: 'AWS',
  amountUsd: 1000,
  period: 'monthly',
  thresholdsPct: [80, 100],
  notifyEmails: [],
};

test('PUT /api/budgets/:id returns 400 when input is invalid', async () => {
  const env = await startServer();
  try {
    const res = await fetch(`${env.base}/api/budgets/any-id`, {
      method: 'PUT',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ name: '' }),
    });
    assert.equal(res.status, 400);
    const body = (await res.json()) as { error: { message: string; issues: unknown[] } };
    assert.equal(body.error.message, 'Invalid input');
    assert.ok(Array.isArray(body.error.issues) && body.error.issues.length > 0);
  } finally {
    await env.close();
  }
});

test('PUT /api/budgets/:id returns 404 when budget does not exist', async () => {
  const env = await startServer();
  try {
    const res = await fetch(`${env.base}/api/budgets/nonexistent`, {
      method: 'PUT',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(validInput),
    });
    assert.equal(res.status, 404);
  } finally {
    await env.close();
  }
});

test('PUT /api/budgets/:id updates the budget and returns it', async () => {
  const env = await startServer();
  try {
    const created = await env.db.repos.budgets.create(
      { workspaceId: null, ...validInput },
      'tester@example.com',
    );

    const res = await fetch(`${env.base}/api/budgets/${created.id}`, {
      method: 'PUT',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        name: 'Annual prod cap',
        scopeType: 'tag',
        scopeValue: 'env:prod',
        amountUsd: 2500,
        period: 'yearly',
        thresholdsPct: [50, 90],
        notifyEmails: ['ops@example.com'],
      }),
    });
    assert.equal(res.status, 200);
    const body = (await res.json()) as Budget;
    assert.equal(body.id, created.id);
    assert.equal(body.name, 'Annual prod cap');
    assert.equal(body.scopeType, 'tag');
    assert.equal(body.scopeValue, 'env:prod');
    assert.equal(body.amountUsd, 2500);
    assert.equal(body.period, 'yearly');
    assert.deepEqual(body.thresholdsPct, [50, 90]);
    assert.deepEqual(body.notifyEmails, ['ops@example.com']);

    const [persisted] = await env.db.repos.budgets.list(null);
    assert.equal(persisted.name, 'Annual prod cap');
    assert.deepEqual(persisted.thresholdsPct, [50, 90]);
  } finally {
    await env.close();
  }
});

test('DELETE /api/budgets/:id removes the budget', async () => {
  const env = await startServer();
  try {
    const created = await env.db.repos.budgets.create(
      { workspaceId: null, ...validInput },
      'tester@example.com',
    );
    const res = await fetch(`${env.base}/api/budgets/${created.id}`, { method: 'DELETE' });
    assert.equal(res.status, 204);
    const remaining = await env.db.repos.budgets.list(null);
    assert.equal(remaining.length, 0);
  } finally {
    await env.close();
  }
});
