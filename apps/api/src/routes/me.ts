import { Router } from 'express';
import type { Env } from '@finlake/shared';
import { normalizeHost } from '../services/normalizeHost.js';

export function meRouter(env: Env): Router {
  const router = Router();

  router.get('/', (req, res) => {
    res.json({
      email: req.user?.email ?? null,
      userId: req.user?.userId ?? null,
      userName: req.user?.userName ?? null,
      workspaceUrl: normalizeHost(env.DATABRICKS_HOST),
      workspaceId: env.DATABRICKS_WORKSPACE_ID ?? null,
      appName: env.DATABRICKS_APP_NAME ?? null,
    });
  });

  return router;
}
