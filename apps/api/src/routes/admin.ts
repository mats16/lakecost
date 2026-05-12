import { Router } from 'express';
import type { DatabaseClient } from '@finlake/db';
import { AdminCleanupRequestSchema, type Env } from '@finlake/shared';
import { cleanupFinLakeResources } from '../services/adminCleanup.js';

export function adminRouter(db: DatabaseClient, env: Env): Router {
  const router = Router();

  router.post('/cleanup', async (req, res, next) => {
    try {
      const parsed = AdminCleanupRequestSchema.safeParse(req.body ?? {});
      if (!parsed.success) {
        res.status(400).json({ error: { message: 'Invalid input', issues: parsed.error.issues } });
        return;
      }
      res.json(
        await cleanupFinLakeResources(db, env, {
          deleteCatalog: parsed.data.deleteCatalog,
          userToken: req.user?.accessToken,
        }),
      );
    } catch (err) {
      next(err);
    }
  });

  return router;
}
