import { Router, type NextFunction, type Request, type Response } from 'express';
import type { DatabaseClient } from '@finlake/db';
import { PricingNotebookSetupInputSchema, type Env, type PricingId } from '@finlake/shared';
import { DataSourceSetupError } from '../services/dataSourceErrors.js';
import { submitManagedNotebookRunById } from '../services/notebookRuns.js';
import {
  deletePricingNotebookData,
  pricingNotebookStateById,
  pricingNotebookState,
} from '../services/pricingNotebook.js';

function parsePricingId(req: Request, res: Response): PricingId | null {
  const parsed = PricingNotebookSetupInputSchema.safeParse(req.params);
  if (!parsed.success) {
    res.status(400).json({ error: { message: 'Invalid input', issues: parsed.error.issues } });
    return null;
  }
  return parsed.data.id;
}

function handleSetupError(err: unknown, res: Response, next: NextFunction): void {
  if (err instanceof DataSourceSetupError) {
    res.status(err.statusCode).json({ error: { message: err.message } });
    return;
  }
  next(err);
}

export function pricingRouter(db: DatabaseClient, env: Env): Router {
  const router = Router();

  router.get('/', async (_req, res, next) => {
    try {
      res.json(await pricingNotebookState(db, env));
    } catch (err) {
      handleSetupError(err, res, next);
    }
  });

  router.get('/:id', async (req, res, next) => {
    try {
      const id = parsePricingId(req, res);
      if (!id) return;
      res.json(await pricingNotebookStateById(db, env, id));
    } catch (err) {
      handleSetupError(err, res, next);
    }
  });

  router.put('/:id', async (req, res, next) => {
    try {
      const id = parsePricingId(req, res);
      if (!id) return;
      res.json(await submitManagedNotebookRunById(env, db, id));
    } catch (err) {
      handleSetupError(err, res, next);
    }
  });

  router.delete('/:id', async (req, res, next) => {
    try {
      const id = parsePricingId(req, res);
      if (!id) return;
      res.json(await deletePricingNotebookData(env, db, req.user?.accessToken, id));
    } catch (err) {
      handleSetupError(err, res, next);
    }
  });

  return router;
}
