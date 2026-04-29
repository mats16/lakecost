import { Router } from 'express';
import type { Env } from '@lakecost/shared';
import { CatalogServiceError, listAccessibleCatalogs } from '../services/catalogs.js';

export function catalogsRouter(env: Env): Router {
  const router = Router();

  router.get('/', async (req, res, next) => {
    try {
      const catalogs = await listAccessibleCatalogs(env, req.user?.accessToken);
      res.setHeader('Cache-Control', 'no-store');
      res.json({ catalogs });
    } catch (err) {
      if (err instanceof CatalogServiceError) {
        res.status(err.statusCode).json({ error: { message: err.message } });
        return;
      }
      next(err);
    }
  });

  return router;
}
