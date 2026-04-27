import path from 'node:path';
import fs from 'node:fs';
import type { Env } from '@lakecost/shared';

export function resolveSqlitePath(env: Env): string {
  if (env.SQLITE_PATH) return env.SQLITE_PATH;

  // Databricks Apps mounts a writable volume at /home/app. Outside of that
  // runtime (local dev, CI), /home/app does not exist — fall back to cwd.
  // We can't use env vars like DATABRICKS_APP_NAME as the signal because
  // they're commonly set in local .env files to identify the deploy target.
  if (fs.existsSync('/home/app')) {
    return '/home/app/data/lakecost.db';
  }

  return path.resolve(process.cwd(), 'data/lakecost.db');
}

export function ensureParentDir(filePath: string): void {
  if (filePath === ':memory:') return;
  const dir = path.dirname(filePath);
  fs.mkdirSync(dir, { recursive: true });
}
