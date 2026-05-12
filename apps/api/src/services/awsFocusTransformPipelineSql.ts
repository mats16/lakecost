import { readFileSync } from 'node:fs';
import { IDENT_RE, MEDALLION_SCHEMA_DEFAULTS, quoteIdent } from '@finlake/shared';

export const AWS_FOCUS_TABLE_NAME_PARAMETER = 'table_name';
export const AWS_FOCUS_S3_BUCKET_PARAMETER = 's3_bucket';
export const AWS_FOCUS_S3_PREFIX_PARAMETER = 's3_prefix';
export const AWS_FOCUS_EXPORT_NAME_PARAMETER = 'export_name';
export const AWS_FOCUS_GOLD_SCHEMA_PARAMETER = 'gold_schema_name';

export function buildAwsFocusPipelineConfiguration(
  tableName: string,
  s3Bucket: string,
  s3Prefix: string,
  exportName: string,
  goldSchema: string,
): Record<string, string> {
  if (!IDENT_RE.test(tableName)) {
    throw new Error(`Invalid table identifier "${tableName}"`);
  }
  if (!IDENT_RE.test(goldSchema)) {
    throw new Error(`Invalid gold schema identifier "${goldSchema}"`);
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
    [AWS_FOCUS_GOLD_SCHEMA_PARAMETER]: goldSchema,
  };
}

export function buildAwsFocusPipelineSql(): string {
  return pipelineTemplate;
}

export function awsUsageTableName(accountId: string): string {
  if (!/^\d{12}$/.test(accountId)) {
    throw new Error(`Invalid AWS account id "${accountId}": expected 12 digits`);
  }
  return `aws_${accountId}_usage`;
}

export function buildAwsFocusSilverPipelineSql(opts: {
  tableName: string;
  s3Bucket: string;
  s3Prefix: string;
  exportName: string;
}): string {
  buildAwsFocusPipelineConfiguration(
    opts.tableName,
    opts.s3Bucket,
    opts.s3Prefix,
    opts.exportName,
    MEDALLION_SCHEMA_DEFAULTS.gold,
  );
  return silverTemplate
    .replaceAll('${table_name}', quoteIdent(opts.tableName))
    .replaceAll('${s3_bucket}', sqlString(opts.s3Bucket))
    .replaceAll('${s3_prefix}', sqlString(opts.s3Prefix))
    .replaceAll('${export_name}', sqlString(opts.exportName));
}

function hasUnsafeSqlChars(value: string): boolean {
  for (let i = 0; i < value.length; i++) {
    const code = value.charCodeAt(i);
    if (code < 0x20 || value[i] === '\\') return true;
  }
  return false;
}

function sqlString(value: string): string {
  if (hasUnsafeSqlChars(value)) {
    throw new Error(
      `Unsafe characters in SQL string literal "${value}": control characters and backslashes are not allowed`,
    );
  }
  return value.replace(/'/g, "''");
}

// Definite assignment: the guard below throws if no candidate is found.
let pipelineTemplate!: string;
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
if (!pipelineTemplate) {
  throw new Error('awsFocusTransformPipeline.sql template not found');
}

const goldStart = pipelineTemplate.indexOf(
  'CREATE OR REFRESH MATERIALIZED VIEW `${gold_schema_name}`',
);
if (goldStart < 0) {
  throw new Error('awsFocusTransformPipeline.sql gold section marker not found');
}
const silverTemplate = pipelineTemplate.slice(0, goldStart).trimEnd();
