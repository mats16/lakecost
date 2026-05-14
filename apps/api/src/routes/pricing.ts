import { Router } from 'express';
import type { DatabaseClient } from '@finlake/db';
import { PricingNotebookSetupInputSchema, type Env } from '@finlake/shared';
import { DataSourceSetupError } from '../services/dataSourceErrors.js';
import { pricingNotebookState, setupPricingNotebook } from '../services/pricingNotebook.js';

export function pricingRouter(db: DatabaseClient, env: Env): Router {
  const router = Router();

  router.get('/notebook', async (_req, res, next) => {
    try {
      res.json(await pricingNotebookState(db));
    } catch (err) {
      next(err);
    }
  });

  router.post('/notebook/setup', async (req, res, next) => {
    try {
      const parsed = PricingNotebookSetupInputSchema.safeParse(req.body);
      if (!parsed.success) {
        res.status(400).json({ error: { message: 'Invalid input', issues: parsed.error.issues } });
        return;
      }
      res.json(await setupPricingNotebook(env, db, req.user?.accessToken, parsed.data.slug));
    } catch (err) {
      if (err instanceof DataSourceSetupError) {
        res.status(err.statusCode).json({ error: { message: err.message } });
        return;
      }
      next(err);
    }
  });

  return router;
}
