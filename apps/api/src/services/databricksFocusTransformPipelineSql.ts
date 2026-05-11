import { readFileSync } from 'node:fs';
import {
  ACCOUNT_PRICES_DEFAULT,
  IDENT_RE,
  quoteIdent,
  validateAccountPricesTable,
} from '@finlake/shared';

export const FOCUS_TABLE_NAME_PARAMETER = 'table_name';
export const FOCUS_ACCOUNT_PRICES_PARAMETER = 'account_prices';
export const FOCUS_GOLD_SCHEMA_PARAMETER = 'gold_schema_name';

export function buildFocusPipelineConfiguration(
  tableName: string,
  accountPricesTable: string,
  goldSchema: string,
): Record<string, string> {
  return {
    [FOCUS_TABLE_NAME_PARAMETER]: tableName,
    [FOCUS_ACCOUNT_PRICES_PARAMETER]: accountPricesTable,
    [FOCUS_GOLD_SCHEMA_PARAMETER]: goldSchema,
  };
}

export function buildFocusPipelineSql(opts: {
  catalog: string;
  table: string;
  goldSchema: string;
  accountPricesTable?: string;
}): string {
  if (!IDENT_RE.test(opts.catalog)) {
    throw new Error(`Invalid catalog identifier "${opts.catalog}"`);
  }
  if (!IDENT_RE.test(opts.table)) {
    throw new Error(`Invalid table identifier "${opts.table}"`);
  }
  if (!IDENT_RE.test(opts.goldSchema)) {
    throw new Error(`Invalid gold schema identifier "${opts.goldSchema}"`);
  }
  const rawAccountPrices =
    opts.accountPricesTable && opts.accountPricesTable.trim().length > 0
      ? opts.accountPricesTable.trim()
      : ACCOUNT_PRICES_DEFAULT;
  validateAccountPricesTable(rawAccountPrices);

  return pipelineTemplate;
}

export function buildFocusSilverPipelineSql(opts: {
  table: string;
  accountPricesTable?: string;
}): string {
  if (!IDENT_RE.test(opts.table)) {
    throw new Error(`Invalid table identifier "${opts.table}"`);
  }
  const rawAccountPrices =
    opts.accountPricesTable && opts.accountPricesTable.trim().length > 0
      ? opts.accountPricesTable.trim()
      : ACCOUNT_PRICES_DEFAULT;
  const accountPricesTable = validateAccountPricesTable(rawAccountPrices);
  return silverTemplate
    .replaceAll('`${table_name}`', quoteIdent(opts.table))
    .replaceAll('${account_prices}', quoteQualifiedTable(accountPricesTable));
}

function quoteQualifiedTable(value: string): string {
  return value.split('.').map(quoteIdent).join('.');
}

// Read the template once at module load — it's a static file that never changes at runtime.
let pipelineTemplate: string;
const candidates = [
  new URL('../sql/databricksFocusTransformPipeline.sql', import.meta.url),
  new URL('../../src/sql/databricksFocusTransformPipeline.sql', import.meta.url),
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
  throw new Error('databricksFocusTransformPipeline.sql template not found');
}

const goldStart = pipelineTemplate.indexOf(
  'CREATE OR REFRESH MATERIALIZED VIEW `${gold_schema_name}`',
);
if (goldStart < 0) {
  throw new Error('databricksFocusTransformPipeline.sql gold section marker not found');
}
const silverTemplate = pipelineTemplate.slice(0, goldStart).trimEnd();
