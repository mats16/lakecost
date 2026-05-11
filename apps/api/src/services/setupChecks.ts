import type { Env, SetupCheckResult, SetupStepId } from '@finlake/shared';
import { buildUserExecutor } from './statementExecution.js';
import { z } from 'zod';

export async function runSetupCheck(
  step: SetupStepId,
  env: Env,
  input: Record<string, unknown>,
  userToken?: string,
): Promise<SetupCheckResult> {
  const checkedAt = new Date().toISOString();

  switch (step) {
    case 'systemTables':
      return await checkSystemTables(env, checkedAt, userToken);
    case 'permissions':
      return await checkPermissions(env, checkedAt, userToken);
    case 'awsCur':
      return checkAwsCur(input, checkedAt);
    case 'azureExport':
      return checkAzureExport(input, checkedAt);
    case 'tagging':
      return checkTagging(checkedAt);
    default:
      return {
        step,
        status: 'unknown',
        message: `Unknown setup step: ${step}`,
        checkedAt,
      };
  }
}

async function checkSystemTables(
  env: Env,
  checkedAt: string,
  userToken?: string,
): Promise<SetupCheckResult> {
  const executor = buildUserExecutor(env, userToken);
  if (!executor) {
    return notConfigured('systemTables', checkedAt, userToken);
  }
  try {
    const rows = await executor.run(
      'SHOW SCHEMAS IN system',
      [],
      z.object({ databaseName: z.string().optional(), schemaName: z.string().optional() }),
    );
    const names = rows.map((r) => r.databaseName ?? r.schemaName ?? '').filter(Boolean);
    const required = ['billing'];
    const missing = required.filter((s) => !names.includes(s));
    if (missing.length > 0) {
      return {
        step: 'systemTables',
        status: 'error',
        message: `Required system schemas not enabled: ${missing.join(', ')}`,
        details: { enabled: names, missing },
        remediation: {
          terraform: missing
            .map((s) => `resource "databricks_system_schema" "${s}" {\n  schema = "${s}"\n}`)
            .join('\n\n'),
          cli: missing
            .map((s) => `databricks account metastores systemschemas enable <metastore-id> ${s}`)
            .join('\n'),
        },
        checkedAt,
      };
    }
    return {
      step: 'systemTables',
      status: 'ok',
      message: 'Required system schemas are enabled',
      details: { enabled: names },
      checkedAt,
    };
  } catch (err) {
    return {
      step: 'systemTables',
      status: 'error',
      message: `Failed to query system schemas: ${(err as Error).message}`,
      checkedAt,
    };
  }
}

async function checkPermissions(
  env: Env,
  checkedAt: string,
  userToken?: string,
): Promise<SetupCheckResult> {
  const executor = buildUserExecutor(env, userToken);
  if (!executor) {
    return notConfigured('permissions', checkedAt, userToken);
  }
  const ucGrantSql = [
    '-- Unity Catalog grants (system.billing read access) for the calling user',
    'GRANT USE CATALOG ON CATALOG system                      TO `<your-user-or-group>`;',
    'GRANT USE SCHEMA  ON SCHEMA  system.billing              TO `<your-user-or-group>`;',
    'GRANT SELECT      ON TABLE   system.billing.usage        TO `<your-user-or-group>`;',
    'GRANT SELECT      ON TABLE   system.billing.list_prices  TO `<your-user-or-group>`;',
  ].join('\n');
  const warehouseId = env.SQL_WAREHOUSE_ID ?? '<warehouse-id>';
  const warehouseGrantCli = `# Workspace-level: SQL Warehouse "Can use" permission
databricks permissions set sql/warehouses ${warehouseId} \\
  --json '{"access_control_list":[{"user_name":"<your-user-or-group>","permission_level":"CAN_USE"}]}'`;
  const warehouseTerraform = `resource "databricks_permissions" "finlake_warehouse" {
  sql_endpoint_id = "${warehouseId}"
  access_control {
    user_name        = "<your-user-or-group>"
    permission_level = "CAN_USE"
  }
}`;
  try {
    await executor.run(
      'SELECT count(*) AS n FROM system.billing.usage LIMIT 1',
      [],
      z.object({ n: z.number() }),
    );
    return {
      step: 'permissions',
      status: 'ok',
      message: 'Caller can read system.billing.usage',
      checkedAt,
    };
  } catch (err) {
    const message = (err as Error).message;
    const isWarehousePermDenied =
      /not authorized to use this warehouse|PERMISSION_DENIED.*warehouse/i.test(message);
    return {
      step: 'permissions',
      status: 'error',
      message: isWarehousePermDenied
        ? `Cannot use SQL Warehouse ${warehouseId}: ${message}`
        : `Cannot read system.billing.usage: ${message}`,
      remediation: {
        sql: ucGrantSql,
        cli: warehouseGrantCli,
        terraform: warehouseTerraform,
      },
      checkedAt,
    };
  }
}

function checkAwsCur(input: Record<string, unknown>, checkedAt: string): SetupCheckResult {
  const bucket = typeof input.bucket === 'string' ? input.bucket : undefined;
  if (!bucket) {
    return {
      step: 'awsCur',
      status: 'warning',
      message: 'AWS CUR bucket not provided yet',
      remediation: {
        terraform: `resource "aws_cur_report_definition" "finlake" {
  report_name                = "finlake-cur"
  time_unit                  = "DAILY"
  format                     = "Parquet"
  compression                = "Parquet"
  additional_schema_elements = ["RESOURCES"]
  s3_bucket                  = "<your-bucket>"
  s3_region                  = "us-east-1"
  s3_prefix                  = "cur/finlake"
  refresh_closed_reports     = true
  report_versioning          = "OVERWRITE_REPORT"
}`,
        cli: 'aws cur put-report-definition --report-definition file://cur.json',
      },
      checkedAt,
    };
  }
  return {
    step: 'awsCur',
    status: 'ok',
    message: `Marked CUR bucket: ${bucket}. Validate the manifest exists in s3://${bucket}/cur/finlake/`,
    details: { bucket },
    checkedAt,
  };
}

function checkAzureExport(input: Record<string, unknown>, checkedAt: string): SetupCheckResult {
  const storageAccount =
    typeof input.storageAccount === 'string' ? input.storageAccount : undefined;
  if (!storageAccount) {
    return {
      step: 'azureExport',
      status: 'warning',
      message: 'Azure Cost Management Export not configured',
      remediation: {
        cli: 'az costmanagement export create --name finlake-export --scope <subscription> --storage-account <name> --container <container> --root-folder-path <path>',
      },
      checkedAt,
    };
  }
  return {
    step: 'azureExport',
    status: 'ok',
    message: `Marked storage account: ${storageAccount}`,
    details: { storageAccount },
    checkedAt,
  };
}

function checkTagging(checkedAt: string): SetupCheckResult {
  return {
    step: 'tagging',
    status: 'warning',
    message: 'Configure recommended cost-attribution tags',
    details: {
      recommendedKeys: ['team', 'cost_center', 'project', 'environment'],
    },
    remediation: {
      sql: `-- Apply via Compute Policy (admin console > Compute > Policies)
{
  "custom_tags.team":         { "type": "fixed", "value": "{user.team}" },
  "custom_tags.cost_center":  { "type": "regex", "pattern": "^CC-[0-9]{4}$" },
  "custom_tags.project":      { "type": "unlimited" }
}`,
    },
    checkedAt,
  };
}

function notConfigured(
  step: SetupStepId,
  checkedAt: string,
  userToken: string | undefined,
): SetupCheckResult {
  return {
    step,
    status: 'unknown',
    message: notConfiguredMessage(userToken),
    checkedAt,
  };
}

function notConfiguredMessage(userToken: string | undefined): string {
  if (!userToken) {
    return 'Missing OBO access token. Run behind a proxy that forwards the `x-forwarded-access-token` header (Databricks Apps, or `databricks apps run-local`).';
  }
  return 'Databricks workspace credentials not configured (DATABRICKS_HOST, SQL_WAREHOUSE_ID).';
}
