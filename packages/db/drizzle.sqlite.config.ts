import { defineConfig } from 'drizzle-kit';

export default defineConfig({
  dialect: 'sqlite',
  schema: './src/schema/sqlite.ts',
  out: './migrations/sqlite',
  dbCredentials: {
    url: process.env.SQLITE_PATH ?? './data/finlake.db',
  },
});
