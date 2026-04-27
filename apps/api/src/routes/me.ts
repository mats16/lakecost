import { Router } from 'express';
import type { Env } from '@lakecost/shared';

export function meRouter(env: Env): Router {
  const router = Router();

  router.get('/', (req, res) => {
    res.json({
      email: req.user?.email ?? null,
      userId: req.user?.userId ?? null,
      userName: req.user?.userName ?? null,
      workspaceUrl: env.DATABRICKS_HOST ? normalizeHost(env.DATABRICKS_HOST) : null,
      workspaceId: env.DATABRICKS_WORKSPACE_ID ?? null,
      appName: env.DATABRICKS_APP_NAME ?? null,
    });
  });

  return router;
}

function normalizeHost(host: string): string {
  if (host.startsWith('http://') || host.startsWith('https://')) return host.replace(/\/$/, '');
  return `https://${host.replace(/\/$/, '')}`;
}
