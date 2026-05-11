import { Router } from 'express';
import { z } from 'zod';
import { settingsToRecord, type DatabaseClient } from '@finlake/db';
import {
  CATALOG_SETTING_KEY,
  catalogUserGroupFromSettings,
  medallionSchemaNamesFromSettings,
  MEDALLION_SCHEMAS,
  type Env,
} from '@finlake/shared';
import { CatalogServiceError, provisionCatalog } from '../services/catalogs.js';
import { logger } from '../config/logger.js';
import { syncSharedFocusPipeline } from '../services/dataSourceSetup.js';
import { DataSourceSetupError } from '../services/dataSourceErrors.js';

const PrefsBodySchema = z.object({
  currency: z.string().min(3).max(8).optional(),
  defaultWorkspaceId: z.string().nullable().optional(),
  theme: z.enum(['system', 'light', 'dark']).optional(),
  prefs: z.record(z.string(), z.unknown()).optional(),
});

const AppSettingKeySchema = z
  .string()
  .min(1)
  .max(128)
  .regex(/^[A-Za-z0-9_.-]+$/, 'invalid key');

const AppSettingValueSchema = z.string().max(4096);

const AppSettingsBulkBodySchema = z.object({
  settings: z.record(AppSettingKeySchema, AppSettingValueSchema),
  provision: z
    .object({
      createIfMissing: z.boolean().optional(),
    })
    .optional(),
});

const AppSettingSingleBodySchema = z.object({
  value: AppSettingValueSchema,
});

export function appSettingsRouter(db: DatabaseClient, env: Env): Router {
  const router = Router();

  router.get('/', async (_req, res, next) => {
    try {
      const settings = settingsToRecord(await db.repos.appSettings.list());
      res.json({ settings });
    } catch (err) {
      next(err);
    }
  });

  router.put('/', async (req, res, next) => {
    try {
      const parsed = AppSettingsBulkBodySchema.safeParse(req.body);
      if (!parsed.success) {
        res.status(400).json({ error: { message: 'Invalid input', issues: parsed.error.issues } });
        return;
      }

      const previousSettings = settingsToRecord(await db.repos.appSettings.list());
      const newCatalog = parsed.data.settings[CATALOG_SETTING_KEY]?.trim();
      const previousCatalog = previousSettings[CATALOG_SETTING_KEY]?.trim() ?? '';
      const previousSchemas = medallionSchemaNamesFromSettings(previousSettings);

      // Persist settings before provisioning. If provisionCatalog fails below,
      // the catalog name stays saved so the user can retry via "Fix permission"
      // without re-entering it. This is intentional — no rollback on failure.
      for (const [key, value] of Object.entries(parsed.data.settings)) {
        await db.repos.appSettings.upsert(key, value);
      }
      const settings = { ...previousSettings, ...parsed.data.settings };
      const catalog = (settings[CATALOG_SETTING_KEY] ?? '').trim();
      const medallionSchemas = medallionSchemaNamesFromSettings(settings);

      const hasCatalog = catalog.length > 0;
      const shouldProvision = hasCatalog && parsed.data.provision !== undefined;
      const catalogChanged = newCatalog !== undefined && catalog !== previousCatalog;
      const schemaChanged = MEDALLION_SCHEMAS.some(
        (layer) => medallionSchemas[layer] !== previousSchemas[layer],
      );
      if (!hasCatalog || (!catalogChanged && !schemaChanged && !shouldProvision)) {
        res.json({ settings });
        return;
      }

      try {
        const needPipelineSync = catalogChanged || schemaChanged;
        const [provision, enabledSources] = await Promise.all([
          provisionCatalog(env, req.user?.accessToken, catalog, {
            createIfMissing: parsed.data.provision?.createIfMissing,
            schemaNames: medallionSchemas,
            catalogUserGroup: catalogUserGroupFromSettings(settings),
          }),
          needPipelineSync
            ? db.repos.dataSources.list().then((s) => s.filter((src) => src.enabled))
            : Promise.resolve([]),
        ]);
        let pipelineSynced = false;
        if (needPipelineSync && enabledSources.length > 0) {
          await syncSharedFocusPipeline(env, db, enabledSources, {
            catalog,
            silverSchema: medallionSchemas.silver,
            goldSchema: medallionSchemas.gold,
          });
          pipelineSynced = true;
        }
        res.json({ settings, provision, pipelineSynced });
      } catch (err) {
        if (err instanceof CatalogServiceError) {
          logger.warn(
            { err, catalog, status: err.statusCode },
            'provisionCatalog precondition failed; settings persisted without provisioning',
          );
          res.status(err.statusCode).json({ error: { message: err.message }, settings });
          return;
        }
        if (err instanceof DataSourceSetupError) {
          logger.warn(
            { err, catalog, status: err.statusCode },
            'settings persisted and catalog provisioned but shared pipeline refresh failed',
          );
          res.status(err.statusCode).json({ error: { message: err.message }, settings });
          return;
        }
        throw err;
      }
    } catch (err) {
      next(err);
    }
  });

  router.get('/:key', async (req, res, next) => {
    try {
      const keyParse = AppSettingKeySchema.safeParse(req.params.key);
      if (!keyParse.success) {
        res.status(400).json({ error: { message: 'Invalid key' } });
        return;
      }
      const row = await db.repos.appSettings.get(keyParse.data);
      if (!row) {
        res.status(404).json({ error: { message: 'Not found' } });
        return;
      }
      res.json(row);
    } catch (err) {
      next(err);
    }
  });

  router.put('/:key', async (req, res, next) => {
    try {
      const keyParse = AppSettingKeySchema.safeParse(req.params.key);
      if (!keyParse.success) {
        res.status(400).json({ error: { message: 'Invalid key' } });
        return;
      }
      const parsed = AppSettingSingleBodySchema.safeParse(req.body);
      if (!parsed.success) {
        res.status(400).json({ error: { message: 'Invalid input', issues: parsed.error.issues } });
        return;
      }
      const row = await db.repos.appSettings.upsert(keyParse.data, parsed.data.value);
      res.json(row);
    } catch (err) {
      next(err);
    }
  });

  router.delete('/:key', async (req, res, next) => {
    try {
      const keyParse = AppSettingKeySchema.safeParse(req.params.key);
      if (!keyParse.success) {
        res.status(400).json({ error: { message: 'Invalid key' } });
        return;
      }
      await db.repos.appSettings.delete(keyParse.data);
      res.status(204).end();
    } catch (err) {
      next(err);
    }
  });

  return router;
}

export function settingsRouter(db: DatabaseClient): Router {
  const router = Router();

  router.get('/me', async (req, res, next) => {
    try {
      const userId = req.user?.email ?? 'anonymous';
      const value = (await db.repos.userPreferences.get(userId)) ?? {
        userId,
        currency: 'USD',
        defaultWorkspaceId: null,
        theme: 'system',
        prefs: {},
        updatedAt: new Date().toISOString(),
      };
      res.json(value);
    } catch (err) {
      next(err);
    }
  });

  router.put('/me', async (req, res, next) => {
    try {
      const userId = req.user?.email ?? 'anonymous';
      const parsed = PrefsBodySchema.safeParse(req.body);
      if (!parsed.success) {
        res.status(400).json({ error: { message: 'Invalid input', issues: parsed.error.issues } });
        return;
      }
      const existing = (await db.repos.userPreferences.get(userId)) ?? {
        userId,
        currency: 'USD',
        defaultWorkspaceId: null,
        theme: 'system',
        prefs: {},
        updatedAt: new Date().toISOString(),
      };
      const next = await db.repos.userPreferences.upsert({
        ...existing,
        ...parsed.data,
        prefs: parsed.data.prefs ?? existing.prefs,
        defaultWorkspaceId:
          parsed.data.defaultWorkspaceId === undefined
            ? existing.defaultWorkspaceId
            : parsed.data.defaultWorkspaceId,
        userId,
        updatedAt: new Date().toISOString(),
      });
      res.json(next);
    } catch (err) {
      next(err);
    }
  });

  return router;
}
