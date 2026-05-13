import { z } from 'zod';

export const DbBackendSchema = z.enum(['lakebase', 'sqlite', 'auto']);
export type DbBackend = z.infer<typeof DbBackendSchema>;

export const EnvSchema = z.object({
  NODE_ENV: z.enum(['development', 'production', 'test']).default('development'),
  PORT: z.coerce.number().int().default(8080),

  WEB_DIST_DIR: z.string().optional(),

  DB_BACKEND: DbBackendSchema.default('auto'),
  SQLITE_PATH: z.string().optional(),

  LAKEBASE_INSTANCE_NAME: z.string().optional(),
  PGHOST: z.string().optional(),
  PGDATABASE: z.string().optional(),
  PGUSER: z.string().optional(),
  PGPORT: z.coerce.number().int().optional(),
  PGSSLMODE: z.string().optional(),
  LAKEBASE_ENDPOINT: z.string().optional(),

  DATABRICKS_HOST: z.string().min(1).optional(),
  DATABRICKS_CLIENT_ID: z.string().optional(),
  DATABRICKS_CLIENT_SECRET: z.string().optional(),
  DATABRICKS_APP_NAME: z.string().optional(),
  DATABRICKS_APP_PORT: z.coerce.number().int().optional(),
  DATABRICKS_WORKSPACE_ID: z.string().optional(),

  SQL_WAREHOUSE_ID: z.string().optional(),
  SQL_API_CACHE_TTL_SEC: z.coerce.number().int().nonnegative().default(300),
  SQL_API_STATEMENT_TTL_SEC: z.coerce.number().int().nonnegative().default(900),
  SQL_API_SUBMIT_RATE_LIMIT_PER_MINUTE: z.coerce.number().int().nonnegative().default(60),

  MIGRATE_ON_BOOT: z
    .enum(['true', 'false'])
    .default('false')
    .transform((v) => v === 'true'),

  LOG_LEVEL: z.enum(['fatal', 'error', 'warn', 'info', 'debug', 'trace']).default('info'),
});

export type Env = z.infer<typeof EnvSchema>;
