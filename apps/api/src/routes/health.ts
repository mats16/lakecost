import { Router } from 'express';
import type { DatabaseClient } from '@finlake/db';
import type { Env } from '@finlake/shared';

export function healthRouter(db: DatabaseClient, env: Env): Router {
  const startedAt = Date.now();
  const router = Router();

  router.get('/', async (_req, res, next) => {
    try {
      await db.healthCheck();
      res.json({
        ok: true,
        backend: db.backend,
        appName: env.DATABRICKS_APP_NAME,
        workspaceId: env.DATABRICKS_WORKSPACE_ID,
        uptimeSec: Math.floor((Date.now() - startedAt) / 1000),
      });
    } catch (err) {
      next(err);
    }
  });

  return router;
}
