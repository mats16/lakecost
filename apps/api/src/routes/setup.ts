import { Router } from 'express';
import type { DatabaseClient } from '@finlake/db';
import type { Env, SetupCheckResult, SetupStateResponse, SetupStepId } from '@finlake/shared';
import { runSetupCheck } from '../services/setupChecks.js';

export function setupRouter(db: DatabaseClient, env: Env): Router {
  const router = Router();

  router.get('/state', async (req, res, next) => {
    try {
      const workspaceId =
        typeof req.query.workspaceId === 'string'
          ? req.query.workspaceId
          : (env.DATABRICKS_WORKSPACE_ID ?? 'unknown');
      const state = await db.repos.setupState.get(workspaceId);
      const steps: SetupCheckResult[] = state
        ? Object.values(state.details).filter(isCheckResult)
        : [];
      const response: SetupStateResponse = { workspaceId, steps };
      res.json(response);
    } catch (err) {
      next(err);
    }
  });

  router.post('/check/:step', async (req, res, next) => {
    try {
      const step = req.params.step as SetupStepId;
      const workspaceId =
        typeof req.body?.workspaceId === 'string'
          ? req.body.workspaceId
          : (env.DATABRICKS_WORKSPACE_ID ?? 'unknown');
      const result = await runSetupCheck(step, env, req.body ?? {}, req.user?.accessToken);
      await db.repos.setupState.recordCheck(workspaceId, result);
      res.json(result);
    } catch (err) {
      next(err);
    }
  });

  return router;
}

function isCheckResult(v: unknown): v is SetupCheckResult {
  return typeof v === 'object' && v !== null && 'step' in v && 'status' in v && 'checkedAt' in v;
}
