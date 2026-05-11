import { WorkspaceClient as SdkWorkspaceClient } from '@databricks/sdk-experimental';
import { createHash } from 'node:crypto';
import type { ZodType } from 'zod';
import type { Env } from '@finlake/shared';
import { logger } from '../config/logger.js';
import { sleep } from '../utils/sleep.js';

export type WorkspaceClient = SdkWorkspaceClient;

export type SqlParam = {
  name: string;
  value: string | number | null;
  type?: 'STRING' | 'INT' | 'BIGINT' | 'TIMESTAMP' | 'DATE';
};

export interface StatementExecutorOpts {
  workspaceClient: WorkspaceClient;
  warehouseId: string;
  /** Server-side wait before returning PENDING/RUNNING. Max accepted by the API is 50s. */
  waitTimeoutSec?: number;
  /** Polling timeout (ms). Defaults to 5 minutes. */
  pollTimeoutMs?: number;
}

const DEFAULT_WAIT_TIMEOUT_SEC = 30;
const DEFAULT_POLL_TIMEOUT_MS = 5 * 60 * 1000;
const POLL_INITIAL_DELAY_MS = 500;
const POLL_MAX_DELAY_MS = 5_000;

type StatementResponse = Awaited<
  ReturnType<WorkspaceClient['statementExecution']['executeStatement']>
>;

export class StatementExecutor {
  constructor(private opts: StatementExecutorOpts) {}

  async run<T>(sqlText: string, params: SqlParam[], rowSchema: ZodType<T>): Promise<T[]> {
    const wc = this.opts.workspaceClient;
    const waitSec = this.opts.waitTimeoutSec ?? DEFAULT_WAIT_TIMEOUT_SEC;
    const pollDeadline = Date.now() + (this.opts.pollTimeoutMs ?? DEFAULT_POLL_TIMEOUT_MS);

    let response: StatementResponse;
    try {
      response = await wc.statementExecution.executeStatement({
        warehouse_id: this.opts.warehouseId,
        statement: sqlText,
        wait_timeout: `${waitSec}s`,
        on_wait_timeout: 'CONTINUE',
        disposition: 'INLINE',
        format: 'JSON_ARRAY',
        parameters: params.map((p) => ({
          name: p.name,
          // null -> omitted (interpreted as NULL by the API).
          value: p.value === null ? undefined : String(p.value),
          type: p.type,
        })),
      });
    } catch (err) {
      logger.error({ err }, 'executeStatement threw');
      throw new Error(`Statement Execution failed: ${(err as Error).message}`);
    }

    let delay = POLL_INITIAL_DELAY_MS;
    while (response.status?.state === 'PENDING' || response.status?.state === 'RUNNING') {
      if (Date.now() > pollDeadline) {
        throw new Error(
          `Statement Execution timed out after ${(this.opts.pollTimeoutMs ?? DEFAULT_POLL_TIMEOUT_MS) / 1000}s`,
        );
      }
      await sleep(delay);
      delay = Math.min(delay * 2, POLL_MAX_DELAY_MS);
      const id = response.statement_id;
      if (!id) throw new Error('Statement Execution returned no statement_id');
      try {
        response = await wc.statementExecution.getStatement({ statement_id: id });
      } catch (err) {
        logger.error({ err, statementId: id }, 'getStatement threw');
        throw new Error(`Statement Execution failed: ${(err as Error).message}`);
      }
    }

    if (response.status?.state !== 'SUCCEEDED') {
      logger.error({ response }, 'Statement Execution failed');
      const detail =
        response.status?.error?.message ??
        response.status?.error?.error_code ??
        response.status?.state ??
        'unknown error';
      throw new Error(`Statement Execution failed: ${detail}`);
    }

    const columns = response.manifest?.schema?.columns ?? [];
    const rows = response.result?.data_array ?? [];
    return rows.map((rawRow) => {
      const obj: Record<string, unknown> = {};
      columns.forEach((col, i) => {
        const name = col.name ?? `column_${i}`;
        obj[snakeToCamel(name)] = coerce(rawRow[i] ?? null, col.type_name);
      });
      return rowSchema.parse(obj);
    });
  }
}

interface CachedClient {
  client: WorkspaceClient;
  expiresAt: number;
}
const clientCache = new Map<string, CachedClient>();
const CLIENT_CACHE_TTL_MS = 5 * 60 * 1000;
const CLIENT_CACHE_MAX = 256;

function tokenCacheKey(host: string, token: string): string {
  return `${host}|${createHash('sha256').update(token).digest('hex')}`;
}

function pruneClientCache(now: number): void {
  for (const [k, v] of clientCache) {
    if (v.expiresAt <= now) clientCache.delete(k);
  }
  if (clientCache.size > CLIENT_CACHE_MAX) {
    // Drop oldest insertion order until under the cap.
    const drop = clientCache.size - CLIENT_CACHE_MAX;
    let i = 0;
    for (const k of clientCache.keys()) {
      if (i++ >= drop) break;
      clientCache.delete(k);
    }
  }
}

/**
 * User-scoped (OBO) workspace client constructed from the token forwarded by
 * Databricks Apps via `x-forwarded-access-token`. Cached per (host, tokenHash)
 * for the request burst window so dashboard polls reuse one SDK client.
 */
export function buildUserWorkspaceClient(env: Env, token: string): WorkspaceClient | undefined {
  if (!env.DATABRICKS_HOST) return undefined;
  const now = Date.now();
  const key = tokenCacheKey(env.DATABRICKS_HOST, token);
  const cached = clientCache.get(key);
  if (cached && cached.expiresAt > now) return cached.client;
  pruneClientCache(now);
  const client = new SdkWorkspaceClient({
    host: env.DATABRICKS_HOST,
    token,
    authType: 'pat',
  });
  clientCache.set(key, { client, expiresAt: now + CLIENT_CACHE_TTL_MS });
  return client;
}

/** Build a `StatementExecutor` that runs as the calling user. */
export function buildUserExecutor(
  env: Env,
  token: string | undefined,
): StatementExecutor | undefined {
  if (!token || !env.SQL_WAREHOUSE_ID) return undefined;
  const wc = buildUserWorkspaceClient(env, token);
  if (!wc) return undefined;
  return new StatementExecutor({ workspaceClient: wc, warehouseId: env.SQL_WAREHOUSE_ID });
}

export function buildAppWorkspaceClient(env: Env): WorkspaceClient | undefined {
  if (!env.DATABRICKS_HOST || !env.DATABRICKS_CLIENT_ID || !env.DATABRICKS_CLIENT_SECRET) {
    return undefined;
  }
  return new SdkWorkspaceClient({
    host: env.DATABRICKS_HOST,
    clientId: env.DATABRICKS_CLIENT_ID,
    clientSecret: env.DATABRICKS_CLIENT_SECRET,
  });
}

export function buildAppExecutor(env: Env): StatementExecutor | undefined {
  if (!env.SQL_WAREHOUSE_ID) return undefined;
  const wc = buildAppWorkspaceClient(env);
  if (!wc) return undefined;
  return new StatementExecutor({ workspaceClient: wc, warehouseId: env.SQL_WAREHOUSE_ID });
}

function snakeToCamel(s: string): string {
  return s.replace(/_([a-z])/g, (_, c: string) => c.toUpperCase());
}

function coerce(v: string | number | null | undefined, typeName: string | undefined): unknown {
  if (v === null || v === undefined) return null;
  if (
    typeName === 'INT' ||
    typeName === 'BIGINT' ||
    typeName === 'LONG' ||
    typeName === 'DOUBLE' ||
    typeName === 'FLOAT'
  ) {
    return typeof v === 'number' ? v : Number(v);
  }
  return v;
}
