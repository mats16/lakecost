import { readFileSync } from 'node:fs';
import { ACCOUNT_PRICES_DEFAULT, IDENT_RE, validateAccountPricesTable } from '@lakecost/shared';

export const FOCUS_TABLE_NAME_PARAMETER = 'table_name';
export const FOCUS_ACCOUNT_PRICES_PARAMETER = 'account_prices';

export function buildFocusPipelineConfiguration(
  tableName: string,
  accountPricesTable: string,
): Record<string, string> {
  return {
    [FOCUS_TABLE_NAME_PARAMETER]: tableName,
    [FOCUS_ACCOUNT_PRICES_PARAMETER]: accountPricesTable,
  };
}

export function buildFocusPipelineSql(opts: {
  catalog: string;
  table: string;
  accountPricesTable?: string;
}): string {
  if (!IDENT_RE.test(opts.catalog)) {
    throw new Error(`Invalid catalog identifier "${opts.catalog}"`);
  }
  if (!IDENT_RE.test(opts.table)) {
    throw new Error(`Invalid table identifier "${opts.table}"`);
  }
  const rawAccountPrices =
    opts.accountPricesTable && opts.accountPricesTable.trim().length > 0
      ? opts.accountPricesTable.trim()
      : ACCOUNT_PRICES_DEFAULT;
  validateAccountPricesTable(rawAccountPrices);

  return pipelineTemplate;
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
