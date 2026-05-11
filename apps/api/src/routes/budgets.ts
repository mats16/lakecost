import { Router } from 'express';
import type { DatabaseClient } from '@finlake/db';
import { CreateBudgetInputSchema } from '@finlake/shared';

export function budgetsRouter(db: DatabaseClient): Router {
  const router = Router();

  router.get('/', async (req, res, next) => {
    try {
      const workspaceId = typeof req.query.workspaceId === 'string' ? req.query.workspaceId : null;
      const items = await db.repos.budgets.list(workspaceId);
      res.json({ items });
    } catch (err) {
      next(err);
    }
  });

  router.post('/', async (req, res, next) => {
    try {
      const parsed = CreateBudgetInputSchema.safeParse(req.body);
      if (!parsed.success) {
        res.status(400).json({ error: { message: 'Invalid input', issues: parsed.error.issues } });
        return;
      }
      const createdBy = req.user?.email ?? 'unknown';
      const budget = await db.repos.budgets.create(parsed.data, createdBy);
      res.status(201).json(budget);
    } catch (err) {
      next(err);
    }
  });

  router.delete('/:id', async (req, res, next) => {
    try {
      await db.repos.budgets.delete(req.params.id);
      res.status(204).end();
    } catch (err) {
      next(err);
    }
  });

  return router;
}
