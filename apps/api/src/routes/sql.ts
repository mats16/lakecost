import { createHash } from 'node:crypto';
import { Router, type Request, type RequestHandler, type Response } from 'express';
import { z } from 'zod';
import type { DatabaseClient } from '@finlake/db';
import {
  SqlStatementResultResponseSchema,
  SqlStatementSubmitRequestSchema,
  SqlStatementSubmitResponseSchema,
  type Env,
  type SqlStatementData,
} from '@finlake/shared';
import {
  buildUserExecutor,
  type RawStatementResult,
  type StatementExecutor,
} from '../services/statementExecution.js';

const StatementIdSchema = z.string().min(1).max(256);
const RESULT_CACHE_PREFIX = 'sql-result';
const STATEMENT_CACHE_PREFIX = 'sql-statement';

const WRITE_KEYWORDS = [
  'ALTER',
  'ANALYZE',
  'CACHE',
  'CLEAR',
  'CREATE',
  'COPY',
  'DELETE',
  'DROP',
  'GRANT',
  'INSERT',
  'LOAD',
  'MERGE',
  'MSCK',
  'OPTIMIZE',
  'REFRESH',
  'REPLACE',
  'RESET',
  'REVOKE',
  'SET',
  'TRUNCATE',
  'UNCACHE',
  'UPDATE',
  'USE',
  'VACUUM',
];

type ExecutorFactory = (
  env: Env,
  token: string | undefined,
  warehouseId?: string,
) => StatementExecutor | undefined;

interface SqlResultCachePayload {
  status: 'SUCCEEDED';
  result: SqlStatementData;
  generatedAt: string;
}

interface StatementCachePayload {
  ownerHash: string;
  resultCacheKey: string;
  warehouseId?: string;
}

const submitWindows = new Map<string, { count: number; limit: number; resetAt: number }>();

export function sqlRouter(
  db: DatabaseClient,
  env: Env,
  buildExecutor: ExecutorFactory = buildUserExecutor,
): Router {
  const router = Router();
  router.post('/', submitSqlHandler(db, env, buildExecutor));
  router.get('/:statement_id', getSqlHandler(db, env, buildExecutor));
  return router;
}

function submitSqlHandler(
  db: DatabaseClient,
  env: Env,
  buildExecutor: ExecutorFactory,
): RequestHandler {
  return async (req, res, next) => {
    try {
      const parsed = SqlStatementSubmitRequestSchema.safeParse(req.body ?? {});
      if (!parsed.success) {
        res.status(400).json({ error: { message: 'Invalid input', issues: parsed.error.issues } });
        return;
      }
      const validationError = validateReadOnlySql(parsed.data.query);
      if (validationError) {
        res.status(400).json({ error: { message: validationError } });
        return;
      }

      const user = userContextFromRequest(req, res);
      if (!user) return;
      const resultCacheKey = resultCacheKeyFor(
        user.ownerHash,
        parsed.data.query,
        parsed.data.params,
      );
      const cachedResult = await getResultCache(db, resultCacheKey);
      if (cachedResult) {
        res.json(
          SqlStatementSubmitResponseSchema.parse({
            status: cachedResult.status,
            result: cachedResult.result,
            generatedAt: cachedResult.generatedAt,
          }),
        );
        return;
      }

      if (!consumeSubmitQuota(env, user.rateLimitKey)) {
        res.status(429).json({ error: { message: 'Too many SQL statements submitted' } });
        return;
      }

      const executor = userExecutorFromRequest(
        env,
        req,
        res,
        buildExecutor,
        parsed.data.warehouse_id,
      );
      if (!executor) return;
      const submitted = await executor.submitRaw(
        parsed.data.query,
        parsed.data.params,
        parsed.data.warehouse_id,
      );
      await setStatementCache(db, env, submitted.statement_id, {
        ownerHash: user.ownerHash,
        resultCacheKey,
        warehouseId: parsed.data.warehouse_id,
      });
      res.json(
        SqlStatementSubmitResponseSchema.parse({
          ...submitted,
          generatedAt: new Date().toISOString(),
        }),
      );
    } catch (err) {
      next(err);
    }
  };
}

function getSqlHandler(
  db: DatabaseClient,
  env: Env,
  buildExecutor: ExecutorFactory,
): RequestHandler {
  return async (req, res, next) => {
    try {
      const parsed = StatementIdSchema.safeParse(req.params.statement_id);
      if (!parsed.success) {
        res.status(400).json({ error: { message: 'Invalid statement_id' } });
        return;
      }

      const user = userContextFromRequest(req, res);
      if (!user) return;
      const statement = await getStatementCache(db, parsed.data);
      if (!statement) {
        res.status(404).json({ error: { message: 'Unknown SQL statement' } });
        return;
      }
      if (statement.ownerHash !== user.ownerHash) {
        res.status(403).json({ error: { message: 'SQL statement belongs to another user' } });
        return;
      }

      const cachedResult = await getResultCache(db, statement.resultCacheKey);
      if (cachedResult) {
        res.json(
          SqlStatementResultResponseSchema.parse({
            statement_id: parsed.data,
            status: cachedResult.status,
            result: cachedResult.result,
            generatedAt: cachedResult.generatedAt,
          }),
        );
        return;
      }

      const executor = userExecutorFromRequest(env, req, res, buildExecutor, statement.warehouseId);
      if (!executor) return;
      const result = await executor.getRaw(parsed.data);
      if (result.status === 'SUCCEEDED') {
        await setResultCache(db, env, statement.resultCacheKey, result);
      }
      res.json(
        SqlStatementResultResponseSchema.parse({
          statement_id: result.statement_id,
          status: result.status,
          result:
            result.status === 'SUCCEEDED'
              ? { columns: result.columns, rows: result.rows }
              : undefined,
          error: result.error,
          generatedAt: new Date().toISOString(),
        }),
      );
    } catch (err) {
      next(err);
    }
  };
}

function userExecutorFromRequest(
  env: Env,
  req: Request,
  res: Response,
  buildExecutor: ExecutorFactory,
  warehouseId?: string,
) {
  const token = req.user?.accessToken;
  if (!token) {
    res.status(401).json({ error: { message: 'Missing OBO access token' } });
    return undefined;
  }
  const executor = buildExecutor(env, token, warehouseId);
  if (!executor) {
    res
      .status(500)
      .json({ error: { message: 'DATABRICKS_HOST or SQL_WAREHOUSE_ID not configured' } });
    return undefined;
  }
  return executor;
}

function userContextFromRequest(req: Request, res: Response) {
  const token = req.user?.accessToken;
  if (!token) {
    res.status(401).json({ error: { message: 'Missing OBO access token' } });
    return undefined;
  }
  const identity = req.user?.userId ?? req.user?.email ?? req.user?.userName ?? token;
  return {
    ownerHash: shortHash(identity, 32),
    rateLimitKey: req.user?.userId ?? req.user?.email ?? req.ip ?? shortHash(token, 16),
  };
}

function resultCacheKeyFor(ownerHash: string, query: string, params: unknown[]): string {
  return `${RESULT_CACHE_PREFIX}:${ownerHash}:${shortHash(JSON.stringify({ query, params }), 32)}`;
}

function statementCacheKey(statementId: string): string {
  return `${STATEMENT_CACHE_PREFIX}:${shortHash(statementId, 32)}`;
}

async function getResultCache(
  db: DatabaseClient,
  cacheKey: string,
): Promise<SqlResultCachePayload | null> {
  const hit = await db.repos.cachedAggregations.get(cacheKey);
  if (!hit || !isResultPayload(hit.payload)) return null;
  return hit.payload;
}

async function setResultCache(
  db: DatabaseClient,
  env: Env,
  cacheKey: string,
  result: RawStatementResult,
): Promise<void> {
  const now = new Date();
  const payload: SqlResultCachePayload = {
    status: 'SUCCEEDED',
    result: { columns: result.columns, rows: result.rows },
    generatedAt: now.toISOString(),
  };
  await db.repos.cachedAggregations.set({
    cacheKey,
    queryHash: cacheKey,
    payload,
    computedAt: now.toISOString(),
    expiresAt: new Date(now.getTime() + env.SQL_API_CACHE_TTL_SEC * 1000).toISOString(),
  });
}

async function getStatementCache(
  db: DatabaseClient,
  statementId: string,
): Promise<StatementCachePayload | null> {
  const hit = await db.repos.cachedAggregations.get(statementCacheKey(statementId));
  if (!hit || !isStatementPayload(hit.payload)) return null;
  return hit.payload;
}

async function setStatementCache(
  db: DatabaseClient,
  env: Env,
  statementId: string,
  payload: StatementCachePayload,
): Promise<void> {
  const now = new Date();
  await db.repos.cachedAggregations.set({
    cacheKey: statementCacheKey(statementId),
    queryHash: payload.resultCacheKey,
    payload,
    computedAt: now.toISOString(),
    expiresAt: new Date(now.getTime() + env.SQL_API_STATEMENT_TTL_SEC * 1000).toISOString(),
  });
}

function consumeSubmitQuota(env: Env, key: string): boolean {
  const limit = env.SQL_API_SUBMIT_RATE_LIMIT_PER_MINUTE;
  if (limit <= 0) return true;
  const now = Date.now();
  const window = submitWindows.get(key);
  if (!window || window.resetAt <= now || window.limit !== limit) {
    submitWindows.set(key, { count: 1, limit, resetAt: now + 60_000 });
    pruneSubmitWindows(now);
    return true;
  }
  if (window.count >= limit) return false;
  window.count += 1;
  return true;
}

function pruneSubmitWindows(now: number): void {
  for (const [key, window] of submitWindows) {
    if (window.resetAt <= now) submitWindows.delete(key);
  }
}

function isResultPayload(value: unknown): value is SqlResultCachePayload {
  if (!value || typeof value !== 'object') return false;
  const candidate = value as Partial<SqlResultCachePayload>;
  return (
    candidate.status === 'SUCCEEDED' &&
    typeof candidate.generatedAt === 'string' &&
    Boolean(candidate.result) &&
    typeof candidate.result === 'object'
  );
}

function isStatementPayload(value: unknown): value is StatementCachePayload {
  if (!value || typeof value !== 'object') return false;
  const candidate = value as Partial<StatementCachePayload>;
  return typeof candidate.ownerHash === 'string' && typeof candidate.resultCacheKey === 'string';
}

function shortHash(input: string, len = 16): string {
  return createHash('sha256').update(input).digest('hex').slice(0, len);
}

export function validateReadOnlySql(sql: string): string | undefined {
  const analysis = analyzeSql(sql);
  const stripped = analysis.stripped.trim();
  if (!stripped) return 'SQL statement is empty';

  if (analysis.statementTerminators.length > 1) {
    return 'Only a single SQL statement is allowed';
  }
  const terminator = analysis.statementTerminators[0];
  if (terminator !== undefined && analysis.stripped.slice(terminator + 1).trim().length > 0) {
    return 'Only a single SQL statement is allowed';
  }

  const firstToken = stripped.match(/^[A-Za-z_][A-Za-z0-9_]*/)?.[0]?.toUpperCase();
  if (firstToken !== 'SELECT' && firstToken !== 'WITH') {
    return 'Only SELECT or WITH statements are allowed';
  }

  const writeKeyword = WRITE_KEYWORDS.find((keyword) =>
    new RegExp(`\\b${keyword}\\b`, 'i').test(analysis.stripped),
  );
  if (writeKeyword) {
    return `Read-only SQL cannot contain ${writeKeyword}`;
  }
  return undefined;
}

function analyzeSql(sql: string): { stripped: string; statementTerminators: number[] } {
  let stripped = '';
  const statementTerminators: number[] = [];
  let i = 0;
  let state:
    | 'normal'
    | 'singleQuote'
    | 'doubleQuote'
    | 'backtick'
    | 'lineComment'
    | 'blockComment' = 'normal';

  while (i < sql.length) {
    const char = sql[i] ?? '';
    const next = sql[i + 1] ?? '';

    if (state === 'lineComment') {
      if (char === '\n') {
        stripped += '\n';
        state = 'normal';
      } else {
        stripped += ' ';
      }
      i += 1;
      continue;
    }

    if (state === 'blockComment') {
      if (char === '*' && next === '/') {
        stripped += '  ';
        state = 'normal';
        i += 2;
      } else {
        stripped += char === '\n' ? '\n' : ' ';
        i += 1;
      }
      continue;
    }

    if (state === 'singleQuote') {
      if (char === '\\' && next) {
        stripped += '  ';
        i += 2;
      } else if (char === "'" && next === "'") {
        stripped += '  ';
        i += 2;
      } else if (char === "'") {
        stripped += ' ';
        state = 'normal';
        i += 1;
      } else {
        stripped += char === '\n' ? '\n' : ' ';
        i += 1;
      }
      continue;
    }

    if (state === 'doubleQuote') {
      if (char === '"' && next === '"') {
        stripped += '  ';
        i += 2;
      } else if (char === '"') {
        stripped += ' ';
        state = 'normal';
        i += 1;
      } else {
        stripped += char === '\n' ? '\n' : ' ';
        i += 1;
      }
      continue;
    }

    if (state === 'backtick') {
      if (char === '`' && next === '`') {
        stripped += '  ';
        i += 2;
      } else if (char === '`') {
        stripped += ' ';
        state = 'normal';
        i += 1;
      } else {
        stripped += char === '\n' ? '\n' : ' ';
        i += 1;
      }
      continue;
    }

    if (char === '-' && next === '-') {
      stripped += '  ';
      state = 'lineComment';
      i += 2;
      continue;
    }
    if (char === '/' && next === '*') {
      stripped += '  ';
      state = 'blockComment';
      i += 2;
      continue;
    }
    if (char === "'") {
      stripped += ' ';
      state = 'singleQuote';
      i += 1;
      continue;
    }
    if (char === '"') {
      stripped += ' ';
      state = 'doubleQuote';
      i += 1;
      continue;
    }
    if (char === '`') {
      stripped += ' ';
      state = 'backtick';
      i += 1;
      continue;
    }
    if (char === ';') {
      statementTerminators.push(stripped.length);
    }
    stripped += char;
    i += 1;
  }

  return { stripped, statementTerminators };
}
