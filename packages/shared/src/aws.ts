export function roleNameFromArn(roleArn: string | null): string | null {
  if (!roleArn) return null;
  const match = /^arn:aws(?:-[a-z]+)*:iam::\d{12}:role\/(.+)$/.exec(roleArn);
  return match?.[1] ?? null;
}

export function ucNameSuffixFromBucket(bucket: string): string {
  return bucket
    .toLowerCase()
    .replace(/[^a-z0-9-]+/g, '-')
    .replace(/-+/g, '-')
    .replace(/^-+|-+$/g, '');
}

export function storageCredentialNameForBucket(bucket: string): string {
  return `db_s3_credential_${ucNameSuffixFromBucket(bucket) || 'bucket'}`.slice(0, 128);
}

export function externalLocationNameForBucket(bucket: string): string {
  return `db_s3_external_${ucNameSuffixFromBucket(bucket) || 'bucket'}`.slice(0, 128);
}

export function isValidS3BucketName(bucket: string): boolean {
  return (
    /^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$/.test(bucket) &&
    !bucket.includes('..') &&
    !bucket.includes('.-') &&
    !bucket.includes('-.') &&
    !/^\d{1,3}(?:\.\d{1,3}){3}$/.test(bucket)
  );
}
