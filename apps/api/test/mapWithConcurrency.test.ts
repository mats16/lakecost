import assert from 'node:assert/strict';
import test from 'node:test';
import { mapWithConcurrency } from '../src/utils/mapWithConcurrency.js';

test('mapWithConcurrency preserves order of results', async () => {
  const items = [10, 5, 20, 1, 15];
  const results = await mapWithConcurrency(items, 2, async (value) => {
    await new Promise((r) => setTimeout(r, value));
    return value * 2;
  });
  assert.deepEqual(results, [20, 10, 40, 2, 30]);
});

test('mapWithConcurrency caps in-flight work at limit', async () => {
  let inFlight = 0;
  let maxInFlight = 0;
  const items = Array.from({ length: 12 }, (_, i) => i);
  await mapWithConcurrency(items, 3, async (value) => {
    inFlight++;
    maxInFlight = Math.max(maxInFlight, inFlight);
    await new Promise((r) => setTimeout(r, 5));
    inFlight--;
    return value;
  });
  assert.equal(maxInFlight, 3);
});

test('mapWithConcurrency uses fewer workers than limit when items < limit', async () => {
  let inFlight = 0;
  let maxInFlight = 0;
  const items = [1, 2];
  await mapWithConcurrency(items, 8, async (value) => {
    inFlight++;
    maxInFlight = Math.max(maxInFlight, inFlight);
    await new Promise((r) => setTimeout(r, 5));
    inFlight--;
    return value;
  });
  assert.equal(maxInFlight, 2);
});

test('mapWithConcurrency rejects on first thrown error', async () => {
  await assert.rejects(
    mapWithConcurrency([1, 2, 3], 2, async (value) => {
      if (value === 2) throw new Error('boom');
      return value;
    }),
    /boom/,
  );
});

test('mapWithConcurrency returns empty result for empty input', async () => {
  const results = await mapWithConcurrency([], 4, async () => {
    throw new Error('should not run');
  });
  assert.deepEqual(results, []);
});

test('mapWithConcurrency rejects invalid limit', async () => {
  await assert.rejects(
    () => mapWithConcurrency([1], 0, async (v) => v),
    /limit must be a positive integer/,
  );
  await assert.rejects(
    () => mapWithConcurrency([1], -1, async (v) => v),
    /limit must be a positive integer/,
  );
  await assert.rejects(
    () => mapWithConcurrency([1], 1.5, async (v) => v),
    /limit must be a positive integer/,
  );
});
