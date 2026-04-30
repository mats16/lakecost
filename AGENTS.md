# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

FinLake is a FinOps web app deployed as a Databricks App. The runtime is Node.js 22.16, the UI is React 19 (Vite), and the API is Express extended by `@databricks/appkit`. The same Express process serves the SPA in production (`/api/*` is the API; everything else falls through to `index.html`).

## Commands

All commands run from the repo root unless noted:

```sh
npm install                           # install all workspaces (uses npm workspaces, NOT pnpm)
npm run dev                           # turbo runs apps/web (Vite :3000) and apps/api (tsx watch :8080) in parallel
npm run build                         # turbo: shared -> db -> (web, api), respecting project references
npm run typecheck                     # all workspaces
npm run lint
npm test
npm run db:studio                     # drizzle-kit studio against the SQLite db
npm run db:migrate                    # see packages/db (currently bootstraps schema on boot)

# Per-workspace work
npm run dev   --workspace=apps/api
npm run build --workspace=apps/web
npm run typecheck --workspace=@lakecost/db

# Deploying to Databricks Apps (after build)
databricks bundle validate -t prod
databricks bundle deploy   -t prod
```

A few non-obvious quirks:

- `apps/web` typecheck must be `tsc -b` (not `tsc -b --noEmit`) because TS project references forbid disabling emit on referenced composite packages.
- Vite proxies `/api` to `:8080`; without the API running you'll get connection-refused, not 404.
- The `command` in `app.yaml` is `["npm", "run", "start", "--workspace=apps/api"]`. Don't switch to pnpm — Databricks Apps' Node.js runtime doesn't ship pnpm.

## Architecture

### Workspaces

- `apps/web` — Vite + React 19 SPA. Uses `@databricks/appkit-ui` (optional) and `recharts` for charts. TanStack Query for data fetching.
- `apps/api` — Express + AppKit. Hosts `/api/*` routes and serves the SPA in production.
- `packages/shared` — zod schemas (env, usage, budget, setup, health) and the SQL templates for `system.billing.*`. Single source of truth for request/response types — both apps consume this.
- `packages/db` — `DatabaseClient` abstraction with two backends. **`@lakecost/api` never reaches into Drizzle directly**; it goes through `db.repos.*`.

### Database backend abstraction (load-bearing)

The DB layer is the most-edited area and the easiest to break. Three things to know:

1. **`DB_BACKEND` env flag** controls the backend explicitly:
   - `lakebase` — force Lakebase, fail boot if init fails (no fallback)
   - `sqlite` — force SQLite, ignore Lakebase env vars
   - `auto` (default) — try Lakebase if `LAKEBASE_INSTANCE_NAME`/`PGHOST` is set, otherwise fall back to SQLite

   Implemented in `packages/db/src/index.ts`. Don't add new "auto" decision points elsewhere — keep the switch in one place.

2. **`@databricks/lakebase` has no ORM**. It returns a `pg.Pool`-compatible driver with auto OAuth refresh. Drizzle (`drizzle-orm/node-postgres`) sits on top. `LakebaseClient` is currently a stub that throws — Phase 1b work.

3. **SQLite path resolution** (`packages/db/src/paths.ts`):
   - explicit `SQLITE_PATH` wins
   - `/home/app/data/lakecost.db` if the directory `/home/app` exists (Databricks Apps mounts it; only writable path on the runtime)
   - else `<cwd>/data/lakecost.db` (local dev)

   The signal is filesystem presence, not env vars — `DATABRICKS_APP_NAME` and friends are commonly set in local `.env.local` files to identify the deploy target, so they cannot be used to detect "are we running on Databricks Apps".

   `:memory:` is supported for tests. The SQLite client runs idempotent `CREATE TABLE IF NOT EXISTS` at construction; richer migrations would go through drizzle-kit.

The `DatabaseClient` interface exposes `repos` (BudgetsRepo, UserPreferencesRepo, CachedAggregationsRepo, SetupStateRepo). Drizzle's pgTable / sqliteTable APIs differ, so each backend has its own schema file (`schema/pg.ts`, `schema/sqlite.ts`) and its own repo implementations. Type contracts are kept identical via the zod schemas in `@lakecost/shared`.

### Reading Databricks system tables

`apps/api/src/services/statementExecution.ts` is the only path to `system.billing.*`. Important rules baked into the code:

- Always use `pricing.effective_list.default` (customer discounts), not `pricing.default`.
- Always join `list_prices` with the `price_start_time`/`price_end_time` window — see `packages/shared/src/sql/usageDaily.sql.ts`. Joining only on `sku_name` will multiply rows.
- The App Service Principal acquires its own M2M OAuth token (`auth/appServicePrincipal.ts`, `client_credentials` against `/oidc/v1/token`). Do not put user OBO tokens through this code path — system tables are read with the SP token so all users see the same aggregates.
- User OBO tokens arrive via `x-forwarded-access-token` and are surfaced on `req.user` by `middlewares/obo.ts`. Use them for user-scoped operations only.

The `usage` route caches results in `cached_aggregations` (5-minute TTL keyed by hashed range). When changing query shape, also bump the cache key.

### AppKit integration

`apps/api/src/app.ts:tryAttachAppKit` dynamically imports `@databricks/appkit` and attaches its middleware if present. AppKit is **optional** at runtime — when the export shape changes (or the package isn't installed), the API still boots. This is why `@databricks/appkit` and `@databricks/appkit-ui` are in `optionalDependencies`. If you wire concrete AppKit features (auth, OBO consent flows), keep the failure path graceful so local dev without credentials still works.

### Configure / Data sources UX

The `/configure/data-sources` page (modeled on Security Lakehouse's UI) is the customer-facing setup surface. Data source definitions live in `apps/web/src/pages/Configure/dataSourceCatalog.ts`. Each tile maps to one or more setup steps (`SetupStepId`); verification runs through `POST /api/setup/check/:step` which lives in `apps/api/src/services/setupChecks.ts`. The drawer (`DataSourceDrawer.tsx`) is where verify buttons + remediation snippets (SQL/Terraform/CLI) are rendered. Sources with `available: false` show under "Add data source" and only display a "coming soon" panel.

When adding a new data source: add an entry to `dataSourceCatalog.ts`, add a step id to `packages/shared/src/schemas/setup.ts`, implement the check in `setupChecks.ts`, and (if the source has its own UI) add a branch in `DataSourceDrawer.tsx`'s `Configurator`.

### Deployment shape

- `app.yaml` — Databricks Apps runtime manifest. The `valueFrom: warehouse` pattern binds resources declared in `resources/app.yml` to env vars.
- `databricks.yml` + `resources/app.yml` — Databricks Asset Bundle. `databricks bundle deploy` uploads source and creates the app + its bound resources.
- The `app_name` and Lakebase resource are decoupled from the app via the `DB_BACKEND` env flag — leaving Lakebase out of `resources/app.yml` is intentionally supported (SQLite fallback).

### Naming

The app name is **FinLake** (capital F and L) in any user-visible surface — page titles, sidebar, README. The package/identifier `lakecost` (all lowercase) stays in code: workspace names (`@lakecost/*`), Databricks Apps `name:`, Terraform resource names, the SQLite filename, etc. Don't capitalize identifiers.
