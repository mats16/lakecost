import { Router } from 'express';
import type { Env } from '@lakecost/shared';
import {
  listAccessibleStorageCredentials,
  StorageCredentialServiceError,
} from '../services/storageCredentials.js';

export function storageCredentialsRouter(env: Env): Router {
  const router = Router();

  router.get('/', async (req, res, next) => {
    try {
      const storageCredentials = await listAccessibleStorageCredentials(env, req.user?.accessToken);
      res.setHeader('Cache-Control', 'no-store');
      res.json({ storageCredentials });
    } catch (err) {
      if (err instanceof StorageCredentialServiceError) {
        res.status(err.statusCode).json({ error: { message: err.message } });
        return;
      }
      next(err);
    }
  });

  return router;
}
