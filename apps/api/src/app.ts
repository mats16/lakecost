import express from 'express';
import path from 'node:path';
import fs from 'node:fs';
import { pinoHttp } from 'pino-http';
import type { Env } from '@lakecost/shared';
import type { DatabaseClient } from '@lakecost/db';
import { logger } from './config/logger.js';
import { errorHandler } from './middlewares/error.js';
import { oboMiddleware } from './middlewares/obo.js';
import { healthRouter } from './routes/health.js';
import { usageRouter } from './routes/usage.js';
import { budgetsRouter } from './routes/budgets.js';
import { setupRouter } from './routes/setup.js';
import { appSettingsRouter, settingsRouter } from './routes/settings.js';
import { meRouter } from './routes/me.js';
import { dataSourcesRouter } from './routes/dataSources.js';
import { catalogsRouter } from './routes/catalogs.js';

export interface AppDeps {
  env: Env;
  db: DatabaseClient;
}

export async function buildApp({ env, db }: AppDeps): Promise<express.Express> {
  const app = express();
  app.disable('x-powered-by');
  app.use(pinoHttp({ logger }));
  app.use(express.json({ limit: '1mb' }));

  await tryAttachAppKit(app);

  app.use(oboMiddleware);

  app.use('/api/health', healthRouter(db, env));
  app.use('/api/usage', usageRouter(db, env));
  app.use('/api/budgets', budgetsRouter(db));
  app.use('/api/setup', setupRouter(db, env));
  app.use('/api/app-settings', appSettingsRouter(db, env));
  app.use('/api/settings', settingsRouter(db));
  app.use('/api/me', meRouter(env));
  app.use('/api/data-sources', dataSourcesRouter(db, env));
  app.use('/api/catalogs', catalogsRouter(env));

  if (env.NODE_ENV === 'production') {
    const distDir = resolveWebDistDir(env);
    if (distDir && fs.existsSync(distDir)) {
      logger.info({ distDir }, 'Serving SPA from web dist directory');
      app.use(express.static(distDir));
      app.get(/^(?!\/api\/).*/, (_req, res) => {
        res.sendFile(path.join(distDir, 'index.html'));
      });
    } else {
      logger.warn({ distDir }, 'Web dist directory not found; SPA will not be served');
    }
  }

  app.use(errorHandler);
  return app;
}

async function tryAttachAppKit(app: express.Express): Promise<void> {
  try {
    const appkit: unknown = await import('@databricks/appkit').catch(() => undefined);
    if (!appkit || typeof appkit !== 'object') {
      logger.info('@databricks/appkit not available; skipping AppKit middleware');
      return;
    }
    const mod = appkit as { createAppKitServer?: (opts: unknown) => unknown };
    if (typeof mod.createAppKitServer !== 'function') {
      logger.info('@databricks/appkit loaded but createAppKitServer not exported');
      return;
    }
    const server = mod.createAppKitServer({});
    const middleware =
      typeof (server as { middleware?: () => express.RequestHandler }).middleware === 'function'
        ? (server as { middleware: () => express.RequestHandler }).middleware()
        : undefined;
    if (middleware) {
      app.use(middleware);
      logger.info('@databricks/appkit middleware attached');
    }
  } catch (err) {
    logger.warn({ err }, '@databricks/appkit failed to initialize; continuing without it');
  }
}

function resolveWebDistDir(env: Env): string | undefined {
  if (env.WEB_DIST_DIR) {
    return path.isAbsolute(env.WEB_DIST_DIR)
      ? env.WEB_DIST_DIR
      : path.resolve(process.cwd(), env.WEB_DIST_DIR);
  }
  const candidates = [
    path.resolve(process.cwd(), 'apps/web/dist'),
    path.resolve(process.cwd(), '../web/dist'),
  ];
  return candidates.find((p) => fs.existsSync(p));
}
