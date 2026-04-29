import type { DatabaseClient } from '@lakecost/db';
import {
  CATALOG_SETTING_KEY,
  FOCUS_VIEW_SCHEMA_DEFAULT,
  focusSourceTables,
  focusViewFqn,
  quoteIdent,
  quotePrincipal,
  tableLeafName,
  validateAccountPricesTable,
  type DataSourcePermissionStep,
  type DataSourcePreflightBody,
  type DataSourcePreflightResult,
  type DataSourceSystemTableGrantsBody,
  type DataSourceSystemTableGrantsResult,
  type Env,
  type FocusSourceTableRef,
} from '@lakecost/shared';
import {
  buildAppExecutor,
  buildAppWorkspaceClient,
  buildUserExecutor,
  type StatementExecutor,
} from './statementExecution.js';
import {
  dryRunPipelineCreate,
  uploadPipelineFile,
  type PipelineScheduleParams,
} from './databricksJobs.js';
import { DataSourceSetupError } from './dataSourceErrors.js';
import { readFocusConfig, resourceLabelBase, workspacePathFor } from './dataSourceSetup.js';
import {
  buildFocusPipelineConfiguration,
  buildFocusPipelineSql,
} from './databricksFocusTransformPipelineSql.js';
import { z } from 'zod';

const RowSchema = z.record(z.unknown());
const SelectOneSchema = z.object({ ok: z.number().optional() });

export async function grantFocusSystemTables(
  env: Env,
  db: DatabaseClient,
  userToken: string | undefined,
  dataSourceId: number,
  body: DataSourceSystemTableGrantsBody,
): Promise<DataSourceSystemTableGrantsResult> {
  const source = await getDatabricksSource(db, dataSourceId);
  return applyFocusSystemTableGrants('grant', env, userToken, dataSourceId, source, body);
}

export async function revokeFocusSystemTables(
  env: Env,
  db: DatabaseClient,
  userToken: string | undefined,
  dataSourceId: number,
): Promise<DataSourceSystemTableGrantsResult> {
  const source = await getDatabricksSource(db, dataSourceId);
  const existing = readFocusConfig(source.config);
  return applyFocusSystemTableGrants('revoke', env, userToken, dataSourceId, source, {
    accountPricesTable: existing.accountPricesTable,
  });
}

async function applyFocusSystemTableGrants(
  mode: 'grant' | 'revoke',
  env: Env,
  userToken: string | undefined,
  dataSourceId: number,
  source: Awaited<ReturnType<typeof getDatabricksSource>>,
  body: DataSourceSystemTableGrantsBody,
): Promise<DataSourceSystemTableGrantsResult> {
  const existing = readFocusConfig(source.config);
  const accountPricesTable = body.accountPricesTable ?? existing.accountPricesTable;
  const tables = resolveSourceTables(accountPricesTable);
  const sp = (env.DATABRICKS_CLIENT_ID ?? '').trim();
  const warnings: string[] = [];
  const steps: DataSourcePermissionStep[] = [];

  if (!sp) {
    warnings.push('DATABRICKS_CLIENT_ID is not set.');
    return {
      dataSourceId,
      servicePrincipalId: null,
      tables: tables.map((t) => t.fqn),
      steps: [
        {
          label: 'App service principal',
          status: 'error',
          message: 'DATABRICKS_CLIENT_ID is not set.',
        },
      ],
      remediationSql: null,
      warnings,
    };
  }

  const executor = buildUserExecutor(env, userToken);
  if (!executor) {
    throw new DataSourceSetupError(
      'OBO access token + DATABRICKS_HOST + SQL_WAREHOUSE_ID required to manage system table grants.',
      400,
    );
  }

  const statements = grantStatements(mode, tables, sp);
  for (const stmt of statements) {
    try {
      await executor.run(stmt.sql, [], z.unknown());
      steps.push({ label: stmt.label, status: 'ok', message: `${mode} succeeded` });
    } catch (err) {
      steps.push({
        label: stmt.label,
        status: 'error',
        message: `${mode.toUpperCase()} failed: ${(err as Error).message}`,
      });
    }
  }

  return {
    dataSourceId,
    servicePrincipalId: sp,
    tables: tables.map((t) => t.fqn),
    steps,
    remediationSql: statements.map((s) => `${s.sql};`).join('\n'),
    warnings,
  };
}

export async function preflightFocusDataSource(
  env: Env,
  db: DatabaseClient,
  dataSourceId: number,
  body: DataSourcePreflightBody,
): Promise<DataSourcePreflightResult> {
  const [source, catalogSetting] = await Promise.all([
    getDatabricksSource(db, dataSourceId),
    db.repos.appSettings.get(CATALOG_SETTING_KEY),
  ]);
  const catalog = (catalogSetting?.value ?? '').trim();
  const existing = readFocusConfig(source.config);
  const accountPricesTable = body.accountPricesTable ?? existing.accountPricesTable;
  const tables = resolveSourceTables(accountPricesTable);
  const tableName = body.tableName ?? tableLeafName(source.tableName);
  const cronExpression = (body.cronExpression ?? existing.cronExpression).trim();
  const timezoneId = (body.timezoneId ?? existing.timezoneId).trim();
  const sp = (env.DATABRICKS_CLIENT_ID ?? '').trim();
  const steps: DataSourcePermissionStep[] = [];
  const warnings: string[] = [];

  for (const [key, value] of [
    ['DATABRICKS_HOST', env.DATABRICKS_HOST],
    ['DATABRICKS_CLIENT_ID', env.DATABRICKS_CLIENT_ID],
    ['DATABRICKS_CLIENT_SECRET', env.DATABRICKS_CLIENT_SECRET],
    ['DATABRICKS_APP_NAME', env.DATABRICKS_APP_NAME],
    ['SQL_WAREHOUSE_ID', env.SQL_WAREHOUSE_ID],
  ] as const) {
    if (typeof value === 'string' && value.trim().length > 0) {
      steps.push({ label: key, status: 'ok', message: 'configured' });
    } else {
      steps.push({ label: key, status: 'error', message: `${key} is not configured.` });
    }
  }
  if (!catalog) {
    steps.push({
      label: 'Target catalog',
      status: 'error',
      message: 'Main catalog is not configured in Configure -> Admin.',
    });
  }
  if (steps.some((s) => s.status === 'error')) {
    return preflightResult(dataSourceId, sp || null, steps, warnings, tables);
  }

  const appClient = buildAppWorkspaceClient(env);
  const appExecutor = buildAppExecutor(env);
  if (!appClient || !appExecutor || !env.DATABRICKS_APP_NAME || !sp) {
    steps.push({
      label: 'App service principal authentication',
      status: 'error',
      message: 'Failed to build app service-principal Databricks clients.',
    });
    return preflightResult(dataSourceId, sp || null, steps, warnings, tables);
  }

  await pushStep(steps, 'App service principal authentication', async () => {
    const me = await appClient.currentUser.me();
    return `Authenticated as ${me.userName ?? me.id ?? sp}`;
  });

  await pushStep(steps, `SQL warehouse ${env.SQL_WAREHOUSE_ID}`, async () => {
    await appExecutor.run('SELECT 1 AS ok', [], SelectOneSchema);
    return 'App service principal can use the SQL warehouse.';
  });

  for (const table of tables) {
    await pushStep(steps, `SELECT ${table.fqn}`, async () => {
      await appExecutor.run(
        `SELECT 1 AS ok FROM ${focusViewFqn(table)} LIMIT 1`,
        [],
        SelectOneSchema,
      );
      return 'readable by app service principal';
    });
  }

  for (const schema of [FOCUS_VIEW_SCHEMA_DEFAULT, 'gold']) {
    await pushStep(steps, `Target schema ${catalog}.${schema}`, async () => {
      await assertSchemaPrivileges(appExecutor, catalog, schema, sp);
      return 'required schema privileges are visible';
    });
  }

  try {
    const pipelineSql = buildFocusPipelineSql({ catalog, table: tableName, accountPricesTable });
    focusViewFqn({ catalog, schema: FOCUS_VIEW_SCHEMA_DEFAULT, table: tableName });
    const workspacePath = workspacePathFor(env.DATABRICKS_APP_NAME, dataSourceId);
    await pushStep(steps, 'Pipeline SQL upload', async () => {
      await uploadPipelineFile(appClient, workspacePath, pipelineSql);
      return 'pipeline source file uploaded by app service principal';
    });
    const labelBase = resourceLabelBase(source);
    const scheduleParams: PipelineScheduleParams = {
      pipelineName: `${labelBase}-pipeline`,
      jobName: `${labelBase}-job`,
      pipelineSql,
      workspacePath,
      catalog,
      schema: FOCUS_VIEW_SCHEMA_DEFAULT,
      configuration: buildFocusPipelineConfiguration(tableName, accountPricesTable),
      cronExpression,
      timezoneId,
      servicePrincipalId: sp,
    };
    await pushStep(steps, 'Pipeline API dry run', async () => {
      await dryRunPipelineCreate(appClient, scheduleParams);
      return 'Pipeline API accepted the dry-run create request.';
    });
  } catch (err) {
    steps.push({
      label: 'Pipeline definition',
      status: 'error',
      message: (err as Error).message,
    });
  }

  return preflightResult(dataSourceId, sp, steps, warnings, tables);
}

export function assertPreflightOk(result: DataSourcePreflightResult): void {
  if (result.ok) return;
  const failed = result.steps.find((s) => s.status === 'error');
  throw new DataSourceSetupError(
    failed
      ? `Preflight failed at "${failed.label}": ${failed.message}`
      : 'Preflight failed. Review the preflight step output.',
    400,
  );
}

function preflightResult(
  dataSourceId: number,
  sp: string | null,
  steps: DataSourcePermissionStep[],
  warnings: string[],
  tables: FocusSourceTableRef[],
): DataSourcePreflightResult {
  return {
    dataSourceId,
    servicePrincipalId: sp,
    ok: steps.every((s) => s.status === 'ok'),
    steps,
    remediationSql: sp ? renderGrantSql('grant', tables, sp) : null,
    warnings,
  };
}

async function getDatabricksSource(db: DatabaseClient, dataSourceId: number) {
  const source = await db.repos.dataSources.get(dataSourceId);
  if (!source) throw new DataSourceSetupError('Data source not found', 404);
  if (source.providerName !== 'Databricks') {
    throw new DataSourceSetupError(
      `Setup is only supported for providerName='Databricks' (got '${source.providerName}')`,
      400,
    );
  }
  return source;
}

function resolveSourceTables(accountPricesTable: string): FocusSourceTableRef[] {
  try {
    validateAccountPricesTable(accountPricesTable);
    return focusSourceTables(accountPricesTable);
  } catch (err) {
    throw new DataSourceSetupError((err as Error).message, 400);
  }
}

function grantStatements(mode: 'grant' | 'revoke', tables: FocusSourceTableRef[], sp: string) {
  const principal = quotePrincipal(sp);
  const catalogSeen = new Set<string>();
  const schemaSeen = new Set<string>();
  const stmts: Array<{ label: string; sql: string }> = [];

  for (const table of tables) {
    if (!catalogSeen.has(table.catalog)) {
      catalogSeen.add(table.catalog);
      stmts.push({
        label: `${mode.toUpperCase()} USE CATALOG ${table.catalog}`,
        sql:
          mode === 'grant'
            ? `GRANT USE CATALOG ON CATALOG ${quoteIdent(table.catalog)} TO ${principal}`
            : `REVOKE USE CATALOG ON CATALOG ${quoteIdent(table.catalog)} FROM ${principal}`,
      });
    }
    const schemaKey = `${table.catalog}.${table.schema}`;
    if (!schemaSeen.has(schemaKey)) {
      schemaSeen.add(schemaKey);
      stmts.push({
        label: `${mode.toUpperCase()} USE SCHEMA ${schemaKey}`,
        sql:
          mode === 'grant'
            ? `GRANT USE SCHEMA ON SCHEMA ${quoteIdent(table.catalog)}.${quoteIdent(table.schema)} TO ${principal}`
            : `REVOKE USE SCHEMA ON SCHEMA ${quoteIdent(table.catalog)}.${quoteIdent(table.schema)} FROM ${principal}`,
      });
    }
    stmts.push({
      label: `${mode.toUpperCase()} SELECT ${table.fqn}`,
      sql:
        mode === 'grant'
          ? `GRANT SELECT ON TABLE ${focusViewFqn(table)} TO ${principal}`
          : `REVOKE SELECT ON TABLE ${focusViewFqn(table)} FROM ${principal}`,
    });
  }
  return mode === 'grant' ? stmts : stmts.reverse();
}

function renderGrantSql(
  mode: 'grant' | 'revoke',
  tables: FocusSourceTableRef[],
  sp: string,
): string {
  return grantStatements(mode, tables, sp)
    .map((s) => `${s.sql};`)
    .join('\n');
}

async function pushStep(
  steps: DataSourcePermissionStep[],
  label: string,
  fn: () => Promise<string>,
): Promise<void> {
  try {
    steps.push({ label, status: 'ok', message: await fn() });
  } catch (err) {
    steps.push({ label, status: 'error', message: (err as Error).message });
  }
}

async function assertSchemaPrivileges(
  executor: StatementExecutor,
  catalog: string,
  schema: string,
  principal: string,
): Promise<void> {
  const rows = await executor.run(
    `SHOW GRANTS ON SCHEMA ${quoteIdent(catalog)}.${quoteIdent(schema)}`,
    [],
    RowSchema,
  );
  const privileges = new Set(
    rows
      .filter((row) => principalMatches(row, principal))
      .map(extractPrivilege)
      .filter((p): p is string => p !== null),
  );
  const missing = ['USE SCHEMA', 'SELECT', 'CREATE TABLE'].filter(
    (p) => !privileges.has(p) && !privileges.has('ALL PRIVILEGES'),
  );
  if (missing.length > 0) {
    throw new Error(`Missing privileges on ${catalog}.${schema}: ${missing.join(', ')}`);
  }
}

function extractPrivilege(row: Record<string, unknown>): string | null {
  const raw =
    row.actionType ?? row.privilegeType ?? row.privilege ?? row.action ?? row.permission ?? null;
  return typeof raw === 'string' ? raw.toUpperCase().replace(/_/g, ' ') : null;
}

function principalMatches(row: Record<string, unknown>, principal: string): boolean {
  const raw = row.principal ?? row.grantee ?? row.principalName ?? row.principal_name ?? null;
  return typeof raw === 'string' && raw === principal;
}
