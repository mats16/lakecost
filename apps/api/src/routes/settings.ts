import { Router } from 'express';
import { z } from 'zod';
import type { DatabaseClient } from '@lakecost/db';

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
  .regex(/^[A-Za-z0-9_.\-]+$/, 'invalid key');

const AppSettingValueSchema = z.string().max(4096);

const AppSettingsBulkBodySchema = z.object({
  settings: z.record(AppSettingKeySchema, AppSettingValueSchema),
});

const AppSettingSingleBodySchema = z.object({
  value: AppSettingValueSchema,
});

export function settingsRouter(db: DatabaseClient): Router {
  const router = Router();

  router.get('/app', async (_req, res, next) => {
    try {
      const rows = await db.repos.appSettings.list();
      const settings: Record<string, string> = {};
      for (const row of rows) settings[row.key] = row.value;
      res.json({ settings });
    } catch (err) {
      next(err);
    }
  });

  router.put('/app', async (req, res, next) => {
    try {
      const parsed = AppSettingsBulkBodySchema.safeParse(req.body);
      if (!parsed.success) {
        res.status(400).json({ error: { message: 'Invalid input', issues: parsed.error.issues } });
        return;
      }
      for (const [key, value] of Object.entries(parsed.data.settings)) {
        await db.repos.appSettings.upsert(key, value);
      }
      const rows = await db.repos.appSettings.list();
      const settings: Record<string, string> = {};
      for (const row of rows) settings[row.key] = row.value;
      res.json({ settings });
    } catch (err) {
      next(err);
    }
  });

  router.get('/app/:key', async (req, res, next) => {
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

  router.put('/app/:key', async (req, res, next) => {
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

  router.delete('/app/:key', async (req, res, next) => {
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
