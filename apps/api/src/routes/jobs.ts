import { Router } from 'express';
import type { DatabaseClient } from '@finlake/db';
import { JobRunLinkQuerySchema, JobRunSubmitInputSchema, type Env } from '@finlake/shared';
import { DataSourceSetupError } from '../services/dataSourceErrors.js';
import { getDatabricksRunLink, submitManagedNotebookRunById } from '../services/notebookRuns.js';

export function jobsRouter(db: DatabaseClient, env: Env): Router {
  const router = Router();

  router.post('/runs/submit', async (req, res, next) => {
    try {
      const parsed = JobRunSubmitInputSchema.safeParse(req.body);
      if (!parsed.success) {
        res.status(400).json({ error: { message: 'Invalid input', issues: parsed.error.issues } });
        return;
      }
      res.json(await submitManagedNotebookRunById(env, db, parsed.data.id));
    } catch (err) {
      if (err instanceof DataSourceSetupError) {
        res.status(err.statusCode).json({ error: { message: err.message } });
        return;
      }
      next(err);
    }
  });

  router.get('/runs/get', async (req, res, next) => {
    try {
      const parsed = JobRunLinkQuerySchema.safeParse(req.query);
      if (!parsed.success) {
        res.status(400).json({ error: { message: 'Invalid input', issues: parsed.error.issues } });
        return;
      }
      res.json(await getDatabricksRunLink(env, parsed.data.run_id));
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
