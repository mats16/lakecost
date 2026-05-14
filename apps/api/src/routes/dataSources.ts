import { Router } from 'express';
import type { DatabaseClient } from '@finlake/db';
import {
  DATA_SOURCE_TEMPLATES,
  DataSourceCreateBodySchema,
  DataSourceKeySchema,
  DataSourceSetupBodySchema,
  DataSourceUpdateBodySchema,
  DEFAULT_DATABRICKS_ACCOUNT_ID,
  isAwsProvider,
  isDatabricksProvider,
  type DataSourceKey,
  type Env,
} from '@finlake/shared';
import {
  runDataSourceJob,
  setupFocusDataSource,
  syncSharedFocusPipeline,
} from '../services/dataSourceSetup.js';
import { DataSourceSetupError } from '../services/dataSourceErrors.js';

const AWS_SOURCE_LOCKED_CONFIG_KEYS = [
  'awsAccountId',
  'externalLocationName',
  'externalLocationUrl',
  'storageCredentialName',
  's3Bucket',
  'exportName',
  's3Prefix',
  's3Region',
];

export function dataSourcesRouter(db: DatabaseClient, env: Env): Router {
  const router = Router();

  router.get('/templates', (_req, res) => {
    res.json({ items: DATA_SOURCE_TEMPLATES });
  });

  router.get('/configurations', async (_req, res, next) => {
    try {
      const items = await db.repos.dataSources.list();
      res.json({ items });
    } catch (err) {
      next(err);
    }
  });

  router.post('/configurations', async (req, res, next) => {
    try {
      const parsed = DataSourceCreateBodySchema.safeParse(req.body);
      if (!parsed.success) {
        res.status(400).json({ error: { message: 'Invalid input', issues: parsed.error.issues } });
        return;
      }
      const template = DATA_SOURCE_TEMPLATES.find((tpl) => tpl.id === parsed.data.templateId);
      if (!template || !template.available) {
        res.status(400).json({ error: { message: 'Invalid templateId' } });
        return;
      }
      const accountId = accountIdForCreate(parsed.data.providerName, parsed.data.accountId);
      if (!accountId) {
        res.status(400).json({ error: { message: 'accountId is required' } });
        return;
      }
      const created = await db.repos.dataSources.create({
        name: parsed.data.name,
        providerName: parsed.data.providerName,
        accountId,
        tableName: parsed.data.tableName,
        focusVersion: template.focus_version,
        enabled: parsed.data.enabled ?? false,
        config: parsed.data.config ?? {},
      });
      res.status(201).json(created);
    } catch (err) {
      next(err);
    }
  });

  router.get('/configurations/:providerName/:accountId', async (req, res, next) => {
    try {
      const key = parseDataSourceKey(req.params);
      if (!key) {
        res.status(400).json({ error: { message: 'Invalid data source key' } });
        return;
      }
      const row = await db.repos.dataSources.get(key);
      if (!row) {
        res.status(404).json({ error: { message: 'Not found' } });
        return;
      }
      res.json(row);
    } catch (err) {
      next(err);
    }
  });

  router.patch('/configurations/:providerName/:accountId', async (req, res, next) => {
    try {
      const key = parseDataSourceKey(req.params);
      if (!key) {
        res.status(400).json({ error: { message: 'Invalid data source key' } });
        return;
      }
      const parsed = DataSourceUpdateBodySchema.safeParse(req.body);
      if (!parsed.success) {
        res.status(400).json({ error: { message: 'Invalid input', issues: parsed.error.issues } });
        return;
      }
      const existing = await db.repos.dataSources.get(key);
      if (!existing) {
        res.status(404).json({ error: { message: 'Not found' } });
        return;
      }
      if (parsed.data.config && isRegisteredAwsSource(existing)) {
        const changedKeys = AWS_SOURCE_LOCKED_CONFIG_KEYS.filter(
          (key) => !sameJsonValue(existing.config[key], parsed.data.config?.[key]),
        );
        if (changedKeys.length > 0) {
          res.status(409).json({
            error: {
              message: `Registered AWS source settings cannot be changed: ${changedKeys.join(', ')}`,
            },
          });
          return;
        }
      }
      const updated = await db.repos.dataSources.update(key, parsed.data);
      res.json(updated);
    } catch (err) {
      next(err);
    }
  });

  router.delete('/configurations/:providerName/:accountId', async (req, res, next) => {
    try {
      const key = parseDataSourceKey(req.params);
      if (!key) {
        res.status(400).json({ error: { message: 'Invalid data source key' } });
        return;
      }
      const existing = await db.repos.dataSources.get(key);
      if (!existing) {
        res.status(404).json({ error: { message: 'Not found' } });
        return;
      }
      await db.repos.dataSources.delete(key);
      if (existing.enabled) {
        try {
          await syncSharedFocusPipeline(env, db);
        } catch (err) {
          console.warn(
            `[dataSources] Deleted DB row ${key.providerName}/${key.accountId} but failed to refresh the shared pipeline: ${(err as Error).message}`,
          );
        }
      }
      res.status(204).end();
    } catch (err) {
      next(err);
    }
  });

  router.post('/configurations/:providerName/:accountId/setup', async (req, res, next) => {
    try {
      const key = parseDataSourceKey(req.params);
      if (!key) {
        res.status(400).json({ error: { message: 'Invalid data source key' } });
        return;
      }
      const parsed = DataSourceSetupBodySchema.safeParse(req.body ?? {});
      if (!parsed.success) {
        res.status(400).json({ error: { message: 'Invalid input', issues: parsed.error.issues } });
        return;
      }
      const result = await setupFocusDataSource(env, db, req.user?.accessToken, key, parsed.data);
      res.json(result);
    } catch (err) {
      if (err instanceof DataSourceSetupError) {
        res
          .status(err.statusCode)
          .json({ error: { message: err.message, step: err.step ?? null } });
        return;
      }
      next(err);
    }
  });

  router.post('/configurations/:providerName/:accountId/run', async (req, res, next) => {
    try {
      const key = parseDataSourceKey(req.params);
      if (!key) {
        res.status(400).json({ error: { message: 'Invalid data source key' } });
        return;
      }
      const result = await runDataSourceJob(env, db, req.user?.accessToken, key);
      res.json(result);
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

function parseDataSourceKey(params: {
  providerName?: string;
  accountId?: string;
}): DataSourceKey | null {
  const parsed = DataSourceKeySchema.safeParse({
    providerName: params.providerName,
    accountId: params.accountId,
  });
  return parsed.success ? parsed.data : null;
}

function accountIdForCreate(providerName: string, accountId: string | undefined): string | null {
  if (accountId?.trim()) return accountId.trim();
  return isDatabricksProvider(providerName) ? DEFAULT_DATABRICKS_ACCOUNT_ID : null;
}

function isRegisteredAwsSource(source: {
  providerName: string;
  config: Record<string, unknown>;
}): boolean {
  if (!isAwsProvider(source.providerName)) return false;
  return ['awsAccountId', 'externalLocationName', 'exportName', 's3Prefix'].every((key) => {
    const value = source.config[key];
    return typeof value === 'string' && value.trim().length > 0;
  });
}

function sameJsonValue(left: unknown, right: unknown): boolean {
  return JSON.stringify(left) === JSON.stringify(right);
}
