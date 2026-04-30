export function s3BucketFromUrl(url: string): string | null {
  const match = /^s3:\/\/([^/]+)/i.exec(url.trim());
  return match?.[1] ?? null;
}

export function normalizeS3Prefix(prefix: string): string {
  return prefix
    .trim()
    .replace(/^\/+/, '')
    .replace(/[/.]+$/, '');
}

export function s3ExportPath(bucket: string, prefix: string, exportName: string): string {
  return prefix ? `s3://${bucket}/${prefix}/${exportName}` : `s3://${bucket}/${exportName}`;
}
