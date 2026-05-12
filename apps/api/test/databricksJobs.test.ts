import assert from 'node:assert/strict';
import test from 'node:test';

import { finlakeResourceTags } from '../src/services/databricksJobs.js';

test('finlakeResourceTags includes FinLake cost allocation tags', () => {
  assert.deepEqual(finlakeResourceTags('production'), {
    ManagedBy: 'finlake',
    Project: 'finops',
    CostCenter: 'finlake',
    Environment: 'production',
  });
});

test('finlakeResourceTags falls back to local when environment is unavailable', () => {
  assert.equal(finlakeResourceTags().Environment, 'local');
  assert.equal(finlakeResourceTags('   ').Environment, 'local');
  assert.equal(finlakeResourceTags('${bundle.target}').Environment, 'local');
});
