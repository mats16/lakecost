import path from 'node:path';
import fs from 'node:fs';
import { EnvSchema, type Env } from '@finlake/shared';

let cached: Env | undefined;

export function loadEnv(): Env {
  if (cached) return cached;
  loadDotenvFiles();
  const parsed = EnvSchema.safeParse(process.env);
  if (!parsed.success) {
    const issues = parsed.error.issues
      .map((i) => `  - ${i.path.join('.')}: ${i.message}`)
      .join('\n');
    throw new Error(`Invalid environment configuration:\n${issues}`);
  }
  cached = parsed.data;
  return cached;
}

let dotenvLoaded = false;

function loadDotenvFiles(): void {
  if (dotenvLoaded) return;
  dotenvLoaded = true;
  const root = findRepoRoot(process.cwd());
  if (!root) return;
  const hadToken = 'DATABRICKS_TOKEN' in process.env;
  // Higher precedence first wins (process.loadEnvFile does not overwrite existing keys
  // that are already set in process.env, so list .env.local before .env).
  for (const name of ['.env.local', '.env']) {
    const file = path.join(root, name);
    if (fs.existsSync(file)) {
      try {
        process.loadEnvFile(file);
      } catch {
        // ignore parse errors and fall back to whatever is already in process.env
      }
    }
  }
  if (!hadToken) {
    // Local frontend-only token used by the Vite proxy to forge OBO headers.
    // Keeping it in the API process confuses Databricks SDK clients that should
    // authenticate with the app service principal.
    delete process.env.DATABRICKS_TOKEN;
  }
}

function findRepoRoot(start: string): string | undefined {
  let dir = start;
  for (let i = 0; i < 8; i++) {
    if (fs.existsSync(path.join(dir, 'turbo.json'))) {
      return dir;
    }
    const parent = path.dirname(dir);
    if (parent === dir) return undefined;
    dir = parent;
  }
  return undefined;
}
