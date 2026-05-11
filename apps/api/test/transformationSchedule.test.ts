import assert from 'node:assert/strict';
import test from 'node:test';
import { z } from 'zod';

const ScheduleBodySchema = z.object({
  cronExpression: z.string().min(1).max(120),
  timezoneId: z.string().min(1).max(64),
});

test('ScheduleBodySchema accepts valid Quartz cron + timezone', () => {
  const result = ScheduleBodySchema.safeParse({
    cronExpression: '0 0 6 * * ?',
    timezoneId: 'UTC',
  });
  assert.ok(result.success);
  assert.equal(result.data.cronExpression, '0 0 6 * * ?');
  assert.equal(result.data.timezoneId, 'UTC');
});

test('ScheduleBodySchema rejects empty cronExpression', () => {
  const result = ScheduleBodySchema.safeParse({
    cronExpression: '',
    timezoneId: 'UTC',
  });
  assert.ok(!result.success);
});

test('ScheduleBodySchema rejects empty timezoneId', () => {
  const result = ScheduleBodySchema.safeParse({
    cronExpression: '0 0 6 * * ?',
    timezoneId: '',
  });
  assert.ok(!result.success);
});

test('ScheduleBodySchema rejects missing fields', () => {
  assert.ok(!ScheduleBodySchema.safeParse({}).success);
  assert.ok(!ScheduleBodySchema.safeParse({ cronExpression: '0 0 6 * * ?' }).success);
  assert.ok(!ScheduleBodySchema.safeParse({ timezoneId: 'UTC' }).success);
});

test('ScheduleBodySchema rejects overly long values', () => {
  const result = ScheduleBodySchema.safeParse({
    cronExpression: 'x'.repeat(121),
    timezoneId: 'UTC',
  });
  assert.ok(!result.success);
});
