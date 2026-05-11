import assert from 'node:assert/strict';
import test from 'node:test';
import { mergeAwsDataExportBucketPolicy } from '../src/services/awsFocusExport.js';

const BUCKET = 'finlake-123456789012';
const ACCOUNT = '123456789012';
const EXPECTED_SID = 'EnableAWSDataExportsToWriteToS3AndCheckPolicy';

test('mergeAwsDataExportBucketPolicy creates a new policy when none exists', () => {
  const result = JSON.parse(mergeAwsDataExportBucketPolicy(undefined, BUCKET, ACCOUNT));
  assert.equal(result.Version, '2012-10-17');
  assert.equal(result.Statement.length, 1);
  assert.equal(result.Statement[0].Sid, EXPECTED_SID);
  assert.ok(result.Statement[0].Resource.includes(`arn:aws:s3:::${BUCKET}`));
});

test('mergeAwsDataExportBucketPolicy preserves existing statements', () => {
  const existing = JSON.stringify({
    Version: '2012-10-17',
    Statement: [{ Sid: 'OtherPolicy', Effect: 'Allow', Action: 's3:GetObject' }],
  });
  const result = JSON.parse(mergeAwsDataExportBucketPolicy(existing, BUCKET, ACCOUNT));
  assert.equal(result.Statement.length, 2);
  assert.equal(result.Statement[0].Sid, 'OtherPolicy');
  assert.equal(result.Statement[1].Sid, EXPECTED_SID);
});

test('mergeAwsDataExportBucketPolicy replaces existing export statement (idempotent)', () => {
  const existing = JSON.stringify({
    Version: '2012-10-17',
    Statement: [
      { Sid: EXPECTED_SID, Effect: 'Allow', Action: 's3:PutObject' },
      { Sid: 'KeepMe', Effect: 'Deny' },
    ],
  });
  const result = JSON.parse(mergeAwsDataExportBucketPolicy(existing, BUCKET, ACCOUNT));
  assert.equal(result.Statement.length, 2);
  assert.equal(result.Statement[0].Sid, 'KeepMe');
  assert.equal(result.Statement[1].Sid, EXPECTED_SID);
});

test('mergeAwsDataExportBucketPolicy preserves existing Version string', () => {
  const existing = JSON.stringify({ Version: '2008-10-17', Statement: [] });
  const result = JSON.parse(mergeAwsDataExportBucketPolicy(existing, BUCKET, ACCOUNT));
  assert.equal(result.Version, '2008-10-17');
});

test('mergeAwsDataExportBucketPolicy creates policy from empty JSON object', () => {
  const result = JSON.parse(mergeAwsDataExportBucketPolicy('{}', BUCKET, ACCOUNT));
  assert.equal(result.Version, '2012-10-17');
  assert.equal(result.Statement.length, 1);
});
