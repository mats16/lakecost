import { Router, type Request, type Response } from 'express';
import type { DatabaseClient } from '@finlake/db';
import { AWS_PRICING_SLUGS, type Env } from '@finlake/shared';
import { submitManagedNotebookRunBySlug } from '../services/notebookRuns.js';
import {
  deletePricingNotebookData,
  pricingNotebookStateBySlug,
  pricingNotebookState,
} from '../services/pricingNotebook.js';
import { z } from 'zod';

const PricingNotebookSlugParamSchema = z.object({
  slug: z.enum(AWS_PRICING_SLUGS),
});

function parsePricingSlug(req: Request, res: Response): (typeof AWS_PRICING_SLUGS)[number] | null {
  const parsed = PricingNotebookSlugParamSchema.safeParse(req.params);
  if (!parsed.success) {
    res.status(400).json({ error: { message: 'Invalid input', issues: parsed.error.issues } });
    return null;
  }
  return parsed.data.slug;
}

export function pricingRouter(db: DatabaseClient, env: Env): Router {
  const router = Router();

  router.get('/', async (_req, res, next) => {
    try {
      res.json(await pricingNotebookState(db, env));
    } catch (err) {
      next(err);
    }
  });

  router.get('/:slug', async (req, res, next) => {
    try {
      const slug = parsePricingSlug(req, res);
      if (!slug) return;
      res.json(await pricingNotebookStateBySlug(db, env, slug));
    } catch (err) {
      next(err);
    }
  });

  router.put('/:slug', async (req, res, next) => {
    try {
      const slug = parsePricingSlug(req, res);
      if (!slug) return;
      res.json(await submitManagedNotebookRunBySlug(env, db, req.user?.accessToken, slug));
    } catch (err) {
      next(err);
    }
  });

  router.delete('/:slug', async (req, res, next) => {
    try {
      const slug = parsePricingSlug(req, res);
      if (!slug) return;
      res.json(await deletePricingNotebookData(env, db, slug));
    } catch (err) {
      next(err);
    }
  });

  return router;
}
