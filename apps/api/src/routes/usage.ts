import { Router, type Request, type RequestHandler } from 'express';
import { createHash } from 'node:crypto';
import type { DatabaseClient } from '@finlake/db';
import type { Env, UsageRange } from '@finlake/shared';
import { UsageRangeSchema } from '@finlake/shared';
import { buildUserExecutor } from '../services/statementExecution.js';
import { UsageQueries } from '../services/usageQueries.js';

const CACHE_TTL_SEC = 5 * 60;

interface UserContext {
  userId: string;
  queries: UsageQueries;
}

interface RouteSpec<T> {
  path: string;
  cachePrefix: string;
  fetch: (q: UsageQueries, range: UsageRange) => Promise<T>;
  format: (data: T) => unknown;
}

export function usageRouter(db: DatabaseClient, env: Env): Router {
  const router = Router();

  const routes: ReadonlyArray<RouteSpec<unknown>> = [
    {
      path: '/daily',
      cachePrefix: 'usage:daily',
      fetch: (q, range) => q.daily(range),
      format: (rows) => ({
        rows,
        totalUsd: (rows as { costUsd: number }[]).reduce((sum, r) => sum + r.costUsd, 0),
        cachedAt: null,
      }),
    },
    {
      path: '/by-sku',
      cachePrefix: 'usage:bySku',
      fetch: (q, range) => q.bySku(range),
      format: (rows) => ({ rows }),
    },
    {
      path: '/top-workloads',
      cachePrefix: 'usage:topWorkloads',
      fetch: (q, range) => q.topWorkloads(range),
      format: (rows) => ({ rows }),
    },
  ];

  for (const route of routes) {
    router.get(route.path, makeHandler(db, env, route));
  }

  return router;
}

function makeHandler<T>(db: DatabaseClient, env: Env, route: RouteSpec<T>): RequestHandler {
  return async (req, res, next) => {
    try {
      const ctx = buildUserContext(req, env);
      if (!ctx) {
        res.status(401).json({ error: { message: 'Missing OBO access token' } });
        return;
      }
      const range = parseRange(req.query);
      const data = await cached(db, route.cachePrefix, ctx.userId, range, () =>
        route.fetch(ctx.queries, range),
      );
      res.json(route.format(data));
    } catch (err) {
      next(err);
    }
  };
}

function buildUserContext(req: Request, env: Env): UserContext | undefined {
  const token = req.user?.accessToken;
  const userId = req.user?.userId ?? req.user?.email;
  if (!token || !userId) return undefined;
  const executor = buildUserExecutor(env, token);
  if (!executor) return undefined;
  return { userId, queries: new UsageQueries(executor) };
}

function parseRange(query: unknown): UsageRange {
  const parsed = UsageRangeSchema.safeParse(query);
  if (!parsed.success) {
    const now = new Date();
    const start = new Date(now.getTime() - 30 * 24 * 60 * 60 * 1000);
    return { start: start.toISOString(), end: now.toISOString() };
  }
  return parsed.data;
}

const inflight = new Map<string, Promise<unknown>>();

async function cached<T>(
  db: DatabaseClient,
  prefix: string,
  userId: string,
  range: UsageRange,
  compute: () => Promise<T>,
): Promise<T> {
  // Cache key includes the user id because results are scoped to the caller's
  // UC permissions — sharing across users would leak data they cannot read.
  const key = `${prefix}:${shortHash(userId)}:${shortHash(JSON.stringify(range))}`;
  const hit = await db.repos.cachedAggregations.get(key);
  if (hit) return hit.payload as T;

  const existing = inflight.get(key);
  if (existing) return existing as Promise<T>;

  const promise = (async () => {
    try {
      const data = await compute();
      const now = new Date();
      await db.repos.cachedAggregations.set({
        cacheKey: key,
        queryHash: shortHash(JSON.stringify(range)),
        payload: data,
        computedAt: now.toISOString(),
        expiresAt: new Date(now.getTime() + CACHE_TTL_SEC * 1000).toISOString(),
      });
      return data;
    } finally {
      inflight.delete(key);
    }
  })();
  inflight.set(key, promise);
  return promise;
}

function shortHash(input: string, len = 16): string {
  return createHash('sha256').update(input).digest('hex').slice(0, len);
}
