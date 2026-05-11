import assert from 'node:assert/strict';
import test from 'node:test';
import {
  roleNameFromArn,
  ucNameSuffixFromBucket,
  storageCredentialNameForBucket,
  externalLocationNameForBucket,
  isValidS3BucketName,
} from '@lakecost/shared';

// --- roleNameFromArn ---

test('roleNameFromArn extracts role name from valid ARN', () => {
  assert.equal(roleNameFromArn('arn:aws:iam::123456789012:role/MyRole'), 'MyRole');
});

test('roleNameFromArn handles path-based role names', () => {
  assert.equal(roleNameFromArn('arn:aws:iam::123456789012:role/path/MyRole'), 'path/MyRole');
});

test('roleNameFromArn returns null for null input', () => {
  assert.equal(roleNameFromArn(null), null);
});

test('roleNameFromArn returns null for invalid string', () => {
  assert.equal(roleNameFromArn('not-an-arn'), null);
});

test('roleNameFromArn handles gov-cloud partition', () => {
  assert.equal(roleNameFromArn('arn:aws-us-gov:iam::123456789012:role/GovRole'), 'GovRole');
});

// --- ucNameSuffixFromBucket ---

test('ucNameSuffixFromBucket passes through simple names', () => {
  assert.equal(ucNameSuffixFromBucket('my-bucket-123'), 'my-bucket-123');
});

test('ucNameSuffixFromBucket lowercases and replaces special chars', () => {
  assert.equal(ucNameSuffixFromBucket('MY.BUCKET_Name'), 'my-bucket-name');
});

test('ucNameSuffixFromBucket collapses consecutive hyphens', () => {
  assert.equal(ucNameSuffixFromBucket('a--b---c'), 'a-b-c');
});

test('ucNameSuffixFromBucket trims leading/trailing hyphens', () => {
  assert.equal(ucNameSuffixFromBucket('-bucket-'), 'bucket');
});

// --- storageCredentialNameForBucket / externalLocationNameForBucket ---

test('storageCredentialNameForBucket returns expected name', () => {
  assert.equal(storageCredentialNameForBucket('my-bucket'), 'db_s3_credential_my-bucket');
});

test('externalLocationNameForBucket returns expected name', () => {
  assert.equal(externalLocationNameForBucket('my-bucket'), 'db_s3_external_my-bucket');
});

test('storageCredentialNameForBucket uses fallback for empty suffix', () => {
  assert.equal(storageCredentialNameForBucket('...'), 'db_s3_credential_bucket');
});

// --- isValidS3BucketName ---

test('isValidS3BucketName accepts valid bucket names', () => {
  assert.ok(isValidS3BucketName('my-bucket-123'));
  assert.ok(isValidS3BucketName('a.b.c'));
  assert.ok(isValidS3BucketName('abc'));
});

test('isValidS3BucketName rejects too short', () => {
  assert.ok(!isValidS3BucketName('ab'));
});

test('isValidS3BucketName rejects uppercase', () => {
  assert.ok(!isValidS3BucketName('My-Bucket'));
});

test('isValidS3BucketName rejects IP-like names', () => {
  assert.ok(!isValidS3BucketName('192.168.1.1'));
});

test('isValidS3BucketName rejects consecutive dots', () => {
  assert.ok(!isValidS3BucketName('my..bucket'));
});

test('isValidS3BucketName rejects dot-hyphen adjacency', () => {
  assert.ok(!isValidS3BucketName('my.-bucket'));
  assert.ok(!isValidS3BucketName('my-.bucket'));
});
