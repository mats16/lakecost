import { Router } from 'express';
import { ExternalLocationCreateBodySchema, type Env } from '@lakecost/shared';
import {
  createExternalLocation,
  deleteExternalLocation,
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

  router.post('/', async (req, res, next) => {
    try {
      const parsed = ExternalLocationCreateBodySchema.safeParse(req.body);
      if (!parsed.success) {
        res.status(400).json({ error: { message: 'Invalid input', issues: parsed.error.issues } });
        return;
      }
      const registeredBy = req.user?.email ?? req.user?.userName ?? req.user?.userId ?? 'unknown';
      const externalLocation = await createExternalLocation(env, {
        ...parsed.data,
        comment: `Registered by ${registeredBy} via FinLake`,
      });
      res.status(201).json({ externalLocation });
    } catch (err) {
      if (err instanceof ExternalLocationServiceError) {
        res.status(err.statusCode).json({ error: { message: err.message } });
        return;
      }
      next(err);
    }
  });

  router.delete('/:name', async (req, res, next) => {
    try {
      await deleteExternalLocation(env, req.params.name);
      res.status(204).end();
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
