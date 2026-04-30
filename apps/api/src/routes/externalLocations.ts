import { Router } from 'express';
import type { Env } from '@lakecost/shared';
import {
  ExternalLocationServiceError,
  listAccessibleExternalLocations,
} from '../services/externalLocations.js';

export function externalLocationsRouter(env: Env): Router {
  const router = Router();

  router.get('/', async (req, res, next) => {
    try {
      const externalLocations = await listAccessibleExternalLocations(env, req.user?.accessToken);
      res.setHeader('Cache-Control', 'no-store');
      res.json({ externalLocations });
    } catch (err) {
      if (err instanceof ExternalLocationServiceError) {
        res.status(err.statusCode).json({ error: { message: err.message } });
        return;
      }
      next(err);
    }
  });

  return router;
}
