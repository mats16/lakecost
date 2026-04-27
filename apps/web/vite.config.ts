import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { defineConfig, loadEnv } from 'vite';
import react from '@vitejs/plugin-react';

const here = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(here, '../..');

export default defineConfig(({ mode }) => {
  // Load env from monorepo root (.env, .env.local, .env.[mode], .env.[mode].local).
  // The empty prefix exposes all keys to this config, but Vite still only ships
  // VITE_-prefixed vars to the client bundle.
  const env = loadEnv(mode, repoRoot, '');

  const apiPort = env.DATABRICKS_APP_PORT ?? env.PORT ?? '8080';
  const apiTarget = `http://localhost:${apiPort}`;

  // Headers that Databricks Apps injects in production. In local dev we forge
  // them from .env.local so the API behaves the same way as in the deployed app.
  const databricksDevHeaders: Record<string, string> = {};
  if (env.DATABRICKS_TOKEN) {
    databricksDevHeaders['x-forwarded-access-token'] = env.DATABRICKS_TOKEN;
  }
  if (env.DATABRICKS_USER_EMAIL) {
    databricksDevHeaders['x-forwarded-email'] = env.DATABRICKS_USER_EMAIL;
  }
  if (env.DATABRICKS_USER_ID) {
    databricksDevHeaders['x-forwarded-user'] = env.DATABRICKS_USER_ID;
  }
  if (env.DATABRICKS_USER_NAME) {
    databricksDevHeaders['x-forwarded-preferred-username'] = env.DATABRICKS_USER_NAME;
  }

  return {
    plugins: [react()],
    envDir: repoRoot,
    server: {
      port: 3000,
      proxy: {
        '/api': {
          target: apiTarget,
          changeOrigin: true,
          headers: databricksDevHeaders,
        },
      },
    },
    build: {
      outDir: 'dist',
      sourcemap: true,
    },
  };
});
