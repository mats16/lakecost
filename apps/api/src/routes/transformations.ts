import { Router } from 'express';
import { z } from 'zod';
import type { DatabaseClient } from '@finlake/db';
import type { Env } from '@finlake/shared';
import {
  listTransformationPipelines,
  TransformationPipelineAuthError,
  updateSharedTransformationSchedule,
} from '../services/transformationPipelines.js';
import { DataSourceSetupError } from '../services/dataSourceErrors.js';
import { runSharedFocusJob } from '../services/dataSourceSetup.js';

const ScheduleBodySchema = z.object({
  cronExpression: z.string().min(1).max(120),
  timezoneId: z.string().min(1).max(64),
});

export function transformationsRouter(db: DatabaseClient, env: Env): Router {
  const router = Router();

  router.get('/pipelines', async (req, res, next) => {
    try {
      res.json(await listTransformationPipelines(db, env, req.user?.accessToken));
    } catch (err) {
      if (err instanceof TransformationPipelineAuthError) {
        res.status(err.statusCode).json({ error: { message: err.message } });
        return;
      }
      next(err);
    }
  });

  router.patch('/shared-schedule', async (req, res, next) => {
    try {
      const parsed = ScheduleBodySchema.safeParse(req.body ?? {});
      if (!parsed.success) {
        res.status(400).json({ error: { message: 'Invalid input', issues: parsed.error.issues } });
        return;
      }
      res.json(await updateSharedTransformationSchedule(db, env, parsed.data));
    } catch (err) {
      if (err instanceof DataSourceSetupError) {
        res.status(err.statusCode).json({ error: { message: err.message } });
        return;
      }
      next(err);
    }
  });

  router.post('/shared-run', async (_req, res, next) => {
    try {
      res.json(await runSharedFocusJob(env, db));
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
