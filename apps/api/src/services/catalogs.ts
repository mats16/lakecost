import {
  MEDALLION_SCHEMAS,
  quoteIdent,
  quotePrincipal,
  schemaGrantPrivileges,
  type CatalogSummary,
  type Env,
  type ProvisionResult,
  type SchemaEnsureStatus,
} from '@lakecost/shared';
import { logger } from '../config/logger.js';
import {
  buildUserExecutor,
  buildUserWorkspaceClient,
  type StatementExecutor,
} from './statementExecution.js';
import { z } from 'zod';

/** Catalogs hidden from the picker — not user-selectable for FOCUS provisioning. */
const HIDDEN_CATALOG_NAMES = new Set(['system', 'samples', '__databricks_internal']);
/** Catalog types that can't host customer-managed schemas / FOCUS materialized views. */
const HIDDEN_CATALOG_TYPES = new Set(['DELTASHARING_CATALOG']);

interface CatalogInfoLike {
  name?: string;
  catalog_type?: string;
  comment?: string;
}

/** Pure filter — exposed for unit tests. */
export function filterSelectableCatalogs(items: CatalogInfoLike[]): CatalogSummary[] {
  const out: CatalogSummary[] = [];
  for (const c of items) {
    if (!c.name) continue;
    if (HIDDEN_CATALOG_NAMES.has(c.name)) continue;
    if (c.catalog_type && HIDDEN_CATALOG_TYPES.has(c.catalog_type)) continue;
    out.push({
      name: c.name,
      catalogType: c.catalog_type ?? null,
      comment: c.comment ?? null,
    });
  }
  out.sort((a, b) => a.name.localeCompare(b.name));
  return out;
}

/**
 * List Unity Catalog catalogs visible to the calling OBO user, minus the
 * non-selectable ones (system, samples, internal, Delta Sharing).
 */
export async function listAccessibleCatalogs(
  env: Env,
  userToken: string | undefined,
): Promise<CatalogSummary[]> {
  if (!userToken) throw new CatalogServiceError('OBO access token required', 401);
  const wc = buildUserWorkspaceClient(env, userToken);
  if (!wc) throw new CatalogServiceError('DATABRICKS_HOST not configured', 500);
  const collected: CatalogInfoLike[] = [];
  try {
    for await (const item of wc.catalogs.list({})) {
      collected.push(item as CatalogInfoLike);
    }
  } catch (err) {
    logger.error({ err }, 'wc.catalogs.list failed');
    throw new CatalogServiceError(
      `Failed to list catalogs: ${(err as Error).message}`,
      isPermissionDenied(err) ? 403 : 502,
    );
  }
  return filterSelectableCatalogs(collected);
}

export class CatalogServiceError extends Error {
  override readonly name = 'CatalogServiceError';
  constructor(
    message: string,
    readonly statusCode: number,
  ) {
    super(message);
  }
}

function isPermissionDenied(err: unknown): boolean {
  if (err != null && typeof err === 'object' && 'errorCode' in err) {
    return (err as { errorCode: unknown }).errorCode === 'PERMISSION_DENIED';
  }
  const message = err instanceof Error ? err.message : String(err);
  return /PERMISSION_DENIED|not authorized/i.test(message);
}

interface ProvisionOptions {
  createIfMissing?: boolean;
}

/**
 * Provisions the medallion layout (`bronze` / `silver` / `gold`) under
 * `catalog`, optionally creating the catalog itself, and grants the App
 * Service Principal the access it needs to run the FOCUS pipeline:
 * USE/SELECT on medallion schemas and CREATE TABLE on silver/gold outputs.
 *
 * All DDL/GRANT statements are run **as the calling user** (OBO) so the SP
 * does not need any prior privileges. Schema creates and GRANTs are
 * collected as best-effort: a single GRANT failure does not abort the
 * remaining ones — the per-step status is captured in the result instead.
 */
export async function provisionCatalog(
  env: Env,
  userToken: string | undefined,
  catalog: string,
  opts: ProvisionOptions = {},
): Promise<ProvisionResult> {
  // Fail fast on bad identifiers so we never interpolate them into SQL.
  const catalogIdent = quoteIdent(catalog);
  const schemaIdents = MEDALLION_SCHEMAS.map((s) => ({ name: s, ident: quoteIdent(s) }));

  const executor = buildUserExecutor(env, userToken);
  if (!executor) {
    throw new CatalogServiceError(
      'OBO access token + DATABRICKS_HOST + SQL_WAREHOUSE_ID required to provision a catalog.',
      400,
    );
  }

  const sp = (env.DATABRICKS_CLIENT_ID ?? '').trim();
  const warnings: string[] = [];

  let catalogCreated = false;
  if (opts.createIfMissing) {
    try {
      const before = await catalogExists(executor, catalog);
      await executor.run(`CREATE CATALOG IF NOT EXISTS ${catalogIdent}`, [], z.unknown());
      catalogCreated = !before;
    } catch (err) {
      throw new CatalogServiceError(
        `CREATE CATALOG failed for ${catalogIdent}: ${(err as Error).message}`,
        isPermissionDenied(err) ? 403 : 500,
      );
    }
  }

  // Schemas: independent CREATEs run in parallel.
  // Promise.all preserves input order in its output array, so warnings are
  // collected deterministically regardless of which SQL statement resolves first.
  const schemaResults = await Promise.all(
    schemaIdents.map(async ({ name, ident }) => {
      const { status, warning } = await ensureSchema(
        executor,
        `CREATE SCHEMA IF NOT EXISTS ${catalogIdent}.${ident}`,
      );
      return { name, status, warning };
    }),
  );
  const schemasEnsured = Object.fromEntries(
    schemaResults.map(({ name, status }) => [name, status]),
  ) as Record<(typeof MEDALLION_SCHEMAS)[number], SchemaEnsureStatus>;
  for (const r of schemaResults) {
    if (r.warning) warnings.push(r.warning);
  }

  // Grants: catalog-level + per-schema all independent — issue concurrently.
  const grants: ProvisionResult['grants'] = {
    catalog: 'skipped:sp_id_not_configured',
    bronze: 'skipped:sp_id_not_configured',
    silver: 'skipped:sp_id_not_configured',
    gold: 'skipped:sp_id_not_configured',
  };
  if (sp.length > 0) {
    const spIdent = quotePrincipal(sp);
    const grantStmts: Array<{ key: keyof ProvisionResult['grants']; sql: string }> = [
      { key: 'catalog', sql: `GRANT USE CATALOG ON CATALOG ${catalogIdent} TO ${spIdent}` },
      ...schemaIdents.map(({ name, ident }) => ({
        key: name,
        sql: `GRANT ${schemaGrantPrivileges(name)} ON SCHEMA ${catalogIdent}.${ident} TO ${spIdent}`,
      })),
    ];
    const grantResults = await Promise.all(grantStmts.map((g) => grant(executor, g.sql)));
    grantStmts.forEach((g, i) => {
      grants[g.key] = grantResults[i]!
        .status as ProvisionResult['grants'][keyof ProvisionResult['grants']];
    });
    for (const r of grantResults) {
      if (r.warning) warnings.push(r.warning);
    }
  } else {
    warnings.push('DATABRICKS_CLIENT_ID is not set — App Service Principal grants were skipped.');
  }

  return {
    catalog,
    catalogCreated,
    schemasEnsured,
    grants,
    servicePrincipalId: sp.length > 0 ? sp : null,
    warnings,
  };
}

async function catalogExists(executor: StatementExecutor, catalog: string): Promise<boolean> {
  try {
    const escaped = catalog.replace(/'/g, "''").replace(/[%_]/g, '\\$&');
    const rows = await executor.run(
      `SHOW CATALOGS LIKE '${escaped}' ESCAPE '\\\\'`,
      [],
      z.object({ catalog: z.string().optional() }),
    );
    return rows.some((r) => r.catalog === catalog);
  } catch {
    return false;
  }
}

async function ensureSchema(
  executor: StatementExecutor,
  sql: string,
): Promise<{ status: SchemaEnsureStatus; warning: string | null }> {
  try {
    await executor.run(sql, [], z.unknown());
    return { status: 'ensured', warning: null };
  } catch (err) {
    return { status: 'error', warning: `${sql} failed: ${(err as Error).message}` };
  }
}

async function grant(
  executor: StatementExecutor,
  sql: string,
): Promise<{ status: string; warning: string | null }> {
  try {
    await executor.run(sql, [], z.unknown());
    return { status: 'granted', warning: null };
  } catch (err) {
    const msg = (err as Error).message;
    return { status: `error:${msg}`, warning: `${sql} failed: ${msg}` };
  }
}
