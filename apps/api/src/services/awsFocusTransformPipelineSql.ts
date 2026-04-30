import { readFileSync } from 'node:fs';
import { IDENT_RE } from '@lakecost/shared';

export const AWS_FOCUS_TABLE_NAME_PARAMETER = 'table_name';
export const AWS_FOCUS_S3_BUCKET_PARAMETER = 's3_bucket';
export const AWS_FOCUS_S3_PREFIX_PARAMETER = 's3_prefix';
export const AWS_FOCUS_EXPORT_NAME_PARAMETER = 'export_name';

export function buildAwsFocusPipelineConfiguration(
  tableName: string,
  s3Bucket: string,
  s3Prefix: string,
  exportName: string,
): Record<string, string> {
  if (!IDENT_RE.test(tableName)) {
    throw new Error(`Invalid table identifier "${tableName}"`);
  }
  if (!s3Bucket || s3Bucket.includes('/')) {
    throw new Error(`Invalid S3 bucket "${s3Bucket}"`);
  }
  if (!s3Prefix || s3Prefix.endsWith('/') || s3Prefix.endsWith('.')) {
    throw new Error(`Invalid S3 prefix "${s3Prefix}"`);
  }
  if (!exportName) {
    throw new Error(`AWS export name is required`);
  }
  return {
    [AWS_FOCUS_TABLE_NAME_PARAMETER]: tableName,
    [AWS_FOCUS_S3_BUCKET_PARAMETER]: s3Bucket,
    [AWS_FOCUS_S3_PREFIX_PARAMETER]: s3Prefix,
    [AWS_FOCUS_EXPORT_NAME_PARAMETER]: exportName,
  };
}

export function buildAwsFocusPipelineSql(): string {
  return pipelineTemplate;
}

let pipelineTemplate: string;
const candidates = [
  new URL('../sql/awsFocusTransformPipeline.sql', import.meta.url),
  new URL('../../src/sql/awsFocusTransformPipeline.sql', import.meta.url),
];
for (const candidate of candidates) {
  try {
    pipelineTemplate = readFileSync(candidate, 'utf8');
    break;
  } catch {
    // Try the next location. Dev may run from src, production from dist.
  }
}
if (!pipelineTemplate!) {
  throw new Error('awsFocusTransformPipeline.sql template not found');
}
