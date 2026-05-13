import assert from 'node:assert/strict';
import test from 'node:test';
import { SqliteClient } from '@finlake/db';
import type { CreateBudgetInput } from '@finlake/shared';

const seed: Omit<CreateBudgetInput, 'workspaceId'> = {
  name: 'Initial',
  scopeType: 'provider',
  scopeValue: 'AWS',
  amountUsd: 500,
  period: 'monthly',
  thresholdsPct: [80, 100],
  notifyEmails: [],
};

async function setup() {
  const db = await SqliteClient.create({ sqlitePath: ':memory:' });
  return {
    db,
    close: () => db.close(),
  };
}

test('SqliteBudgetsRepo.update returns the updated row and persists it', async () => {
  const { db, close } = await setup();
  try {
    const created = await db.repos.budgets.create(
      { workspaceId: null, ...seed },
      'tester@example.com',
    );

    const updated = await db.repos.budgets.update(created.id, {
      name: 'Updated',
      scopeType: 'tag',
      scopeValue: 'env:prod',
      amountUsd: 1234,
      period: 'yearly',
      thresholdsPct: [50, 90, 110],
      notifyEmails: ['ops@example.com', 'fin@example.com'],
    });
    assert.ok(updated, 'update should return a Budget when the id exists');
    assert.equal(updated.id, created.id);
    assert.equal(updated.name, 'Updated');
    assert.equal(updated.scopeType, 'tag');
    assert.equal(updated.amountUsd, 1234);
    assert.equal(updated.period, 'yearly');
    assert.deepEqual(updated.thresholdsPct, [50, 90, 110]);
    assert.deepEqual(updated.notifyEmails, ['ops@example.com', 'fin@example.com']);
    // Fields not in UpdateBudgetInput are preserved.
    assert.equal(updated.createdBy, 'tester@example.com');
    assert.equal(updated.createdAt, created.createdAt);

    const [persisted] = await db.repos.budgets.list(null);
    assert.equal(persisted.id, created.id);
    assert.equal(persisted.name, 'Updated');
    assert.deepEqual(persisted.thresholdsPct, [50, 90, 110]);
    assert.deepEqual(persisted.notifyEmails, ['ops@example.com', 'fin@example.com']);
  } finally {
    await close();
  }
});

test('SqliteBudgetsRepo.update returns null when the id is unknown', async () => {
  const { db, close } = await setup();
  try {
    const result = await db.repos.budgets.update('does-not-exist', {
      name: 'X',
      scopeType: 'provider',
      scopeValue: '*',
      amountUsd: 1,
      period: 'monthly',
      thresholdsPct: [80, 100],
      notifyEmails: [],
    });
    assert.equal(result, null);
  } finally {
    await close();
  }
});
