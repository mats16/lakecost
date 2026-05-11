import assert from 'node:assert/strict';
import test from 'node:test';
import { summarizeStorageCredentialValidationResults } from '../src/services/awsStorageCredentialValidation.js';

test('summarizeStorageCredentialValidationResults returns null when validation passes or skips', () => {
  assert.equal(
    summarizeStorageCredentialValidationResults([
      { operation: 'LIST', result: 'PASS', message: undefined },
      { operation: 'WRITE', result: 'SKIP', message: 'read only' },
    ]),
    null,
  );
});

test('summarizeStorageCredentialValidationResults summarizes failed operations', () => {
  assert.equal(
    summarizeStorageCredentialValidationResults([
      { operation: 'LIST', result: 'PASS', message: undefined },
      { operation: 'READ', result: 'FAIL', message: 'Failed to get credentials' },
      { operation: 'PATH_EXISTS', result: 'FAIL', message: undefined },
    ]),
    'READ: Failed to get credentials; PATH_EXISTS: Databricks returned FAIL without a message.',
  );
});
