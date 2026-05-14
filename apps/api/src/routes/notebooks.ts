import { Router } from 'express';
import type { DatabaseClient } from '@finlake/db';
import type { Env } from '@finlake/shared';
import { DataSourceSetupError } from '../services/dataSourceErrors.js';
import { runManagedNotebookById } from '../services/notebookRuns.js';

export function notebooksRouter(db: DatabaseClient, env: Env): Router {
  const router = Router();

  router.post('/:notebookId/run', async (req, res, next) => {
    try {
      res.json(await runManagedNotebookById(env, db, req.user?.accessToken, req.params.notebookId));
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
