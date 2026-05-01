# FinLake - FinOps Lakehouse

A FinOps web app for Databricks, deployed as a Databricks App.

- React 19 + Vite SPA (`apps/web`)
- Express + AppKit API (`apps/api`)
- Drizzle ORM with **Lakebase** (Postgres) or **SQLite** fallback (`packages/db`)
- npm workspaces + Turborepo

## Quick start (local dev)

```sh
# Use the pinned Node version
nvm use   # 22.16

npm install

# Optional: copy the env example and fill in Databricks credentials if you
# want to hit a real workspace. Without these the API still boots and serves
# empty rows for /api/usage/* endpoints. Both the API (loadEnv) and the Vite
# dev proxy read .env.local from the repo root.
cp .env.example .env.local

npm run dev
# Vite dev server: http://localhost:3000  (proxies /api -> :8080)
# Express API:    http://localhost:8080/api/health
```

The default `DB_BACKEND=sqlite` writes to `./data/lakecost.db` locally and
`/home/app/data/lakecost.db` on Databricks Apps.

## Deploying to Databricks Apps

```sh
npm run build
databricks bundle validate -t prod
databricks bundle deploy   -t prod
```

The `app.yaml` runs `npm run start --workspace=apps/api`. Bind a SQL warehouse
in `resources/app.yml` (already templated as `warehouse`) and grant the app
service principal `SELECT` on `system.billing.usage` / `system.billing.list_prices`.

## Database backend selection

`DB_BACKEND` controls which client is used:

| Value            | Behavior                                                                               |
| ---------------- | -------------------------------------------------------------------------------------- |
| `lakebase`       | Force Lakebase. Boot fails if init fails (no fallback).                                |
| `sqlite`         | Force SQLite. `LAKEBASE_*` env vars are ignored.                                       |
| `auto` (default) | Try Lakebase if `LAKEBASE_INSTANCE_NAME` or `PGHOST` is set, else fall back to SQLite. |
