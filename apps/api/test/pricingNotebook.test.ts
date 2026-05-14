import assert from 'node:assert/strict';
import test from 'node:test';

import type { DatabaseClient } from '@finlake/db';
import { CATALOG_SETTING_KEY, type Env, type PricingData } from '@finlake/shared';
import {
  deletePricingNotebookData,
  ensurePricingDataForId,
  pricingNotebookState,
  pricingNotebookStateById,
  pricingNotebookWorkspacePath,
} from '../src/services/pricingNotebook.js';
import {
  getDatabricksRunLink,
  submitManagedNotebookRunById,
} from '../src/services/notebookRuns.js';
import { DataSourceSetupError } from '../src/services/dataSourceErrors.js';
import type { WorkspaceClient } from '../src/services/statementExecution.js';

const UPDATED_AT = '2026-05-14T00:00:00.000Z';
const APP_NAME = 'finlake-dev';
const PRICING_NOTEBOOK_WORKSPACE_PATH = pricingNotebookWorkspacePath(APP_NAME);
const EC2_SOURCE_URL =
  'https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonEC2/current/index.csv';
const RDS_SOURCE_URL =
  'https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonRDS/current/index.csv';
const EC2_VOLUME_PATH = '/Volumes/finops/ingest/downloads/pricing_aws_ec2.csv';
const RDS_VOLUME_PATH = '/Volumes/finops/ingest/downloads/pricing_aws_rds.csv';
const EC2_RAW_TABLE = 'finops.ingest.pricing_aws_ec2';
const RDS_RAW_TABLE = 'finops.ingest.pricing_aws_rds';

type PricingDataInput = Omit<
  PricingData,
  'runId' | 'runStatus' | 'runUrl' | 'runStartedAt' | 'runFinishedAt' | 'runCheckedAt' | 'updatedAt'
> &
  Partial<
    Pick<
      PricingData,
      | 'runId'
      | 'runStatus'
      | 'runUrl'
      | 'runStartedAt'
      | 'runFinishedAt'
      | 'runCheckedAt'
      | 'updatedAt'
    >
  >;

function pricingDataRow(input: PricingDataInput): PricingData {
  return {
    ...input,
    runId: input.runId ?? null,
    runStatus: input.runStatus ?? 'not_started',
    runUrl: input.runUrl ?? null,
    runStartedAt: input.runStartedAt ?? null,
    runFinishedAt: input.runFinishedAt ?? null,
    runCheckedAt: input.runCheckedAt ?? null,
    updatedAt: input.updatedAt ?? UPDATED_AT,
  };
}

function createFakeDb(initial: PricingDataInput[] = []) {
  const stored = new Map(initial.map((row) => [row.id, pricingDataRow(row)]));
  const db = {
    backend: 'sqlite',
    repos: {
      appSettings: {
        async list() {
          return [{ key: CATALOG_SETTING_KEY, value: 'finops', updatedAt: UPDATED_AT }];
        },
      },
      pricingData: {
        async get(provider: string, service: string) {
          return (
            Array.from(stored.values()).find(
              (row) => row.provider === provider && row.service === service,
            ) ?? null
          );
        },
        async getById(id: string) {
          return stored.get(id) ?? null;
        },
        async getByNotebookId(notebookId: string) {
          return Array.from(stored.values()).find((row) => row.notebookId === notebookId) ?? null;
        },
        async upsert(input: Omit<PricingData, 'updatedAt'>) {
          const row = { ...input, updatedAt: UPDATED_AT };
          stored.set(row.id, row);
          return row;
        },
        async updateRun(
          id: string,
          patch: Pick<
            PricingData,
            'runId' | 'runStatus' | 'runUrl' | 'runStartedAt' | 'runFinishedAt' | 'runCheckedAt'
          >,
        ) {
          const current = stored.get(id);
          if (!current) return null;
          const next = { ...current, ...patch, updatedAt: UPDATED_AT };
          stored.set(id, next);
          return next;
        },
        async deleteById(id: string) {
          return stored.delete(id);
        },
      },
    },
  } as unknown as DatabaseClient;
  return {
    db,
    stored: (id: string) => stored.get(id) ?? null,
    storedItems: () => Array.from(stored.values()),
  };
}

test('pricingNotebookWorkspacePath stores pricing notebooks under the pricing directory', () => {
  assert.equal(
    pricingNotebookWorkspacePath(APP_NAME),
    `/Workspace/Shared/${APP_NAME}/pricing/pricing_ingest_aws.ipynb`,
  );
});

test('pricingNotebookState returns AWS EC2 and RDS defaults', async () => {
  const fake = createFakeDb();

  const result = await pricingNotebookState(fake.db, {} as Env);

  assert.equal(result.items.length, 2);
  assert.deepEqual(
    result.items.map((item) => item.id),
    ['aws_ec2', 'aws_rds'],
  );
  assert.equal(result.items[0]?.service, 'AmazonEC2');
  assert.equal(result.items[0]?.table, null);
  assert.equal(result.items[0]?.rawDataTable, null);
  assert.equal(result.items[0]?.rawDataPath, null);
  assert.equal(result.items[0]?.runStatus, 'not_started');
  assert.equal(result.items[0]?.runId, null);
  assert.deepEqual(result.items[0]?.metadata, { source: EC2_SOURCE_URL });
  assert.equal(result.items[1]?.service, 'AmazonRDS');
  assert.equal(result.items[1]?.table, null);
  assert.equal(result.items[1]?.rawDataTable, null);
  assert.equal(result.items[1]?.rawDataPath, null);
  assert.equal(result.items[1]?.runStatus, 'not_started');
  assert.equal(result.items[1]?.runId, null);
  assert.deepEqual(result.items[1]?.metadata, { source: RDS_SOURCE_URL });
});

test('ensurePricingDataForId stores AWS EC2 pricing metadata with service principal client', async () => {
  const fake = createFakeDb();
  const workspaceClient = {
    workspace: {
      async getStatus({ path }: { path: string }) {
        assert.equal(path, PRICING_NOTEBOOK_WORKSPACE_PATH);
        return { object_id: 12345 };
      },
    },
  } as unknown as WorkspaceClient;

  const result = await ensurePricingDataForId(
    {
      DATABRICKS_APP_NAME: APP_NAME,
      DATABRICKS_HOST: 'https://example.cloud.databricks.com',
    } as Env,
    fake.db,
    'aws_ec2',
    {
      workspaceClient,
      async uploadNotebook(wc, workspacePath) {
        assert.equal(wc, workspaceClient);
        assert.equal(workspacePath, PRICING_NOTEBOOK_WORKSPACE_PATH);
        return workspacePath;
      },
    },
  );

  assert.equal(result.provider, 'AWS');
  assert.equal(result.service, 'AmazonEC2');
  assert.equal(result.id, 'aws_ec2');
  assert.equal(result.table, 'finops.pricing.aws_ec2');
  assert.equal(result.rawDataTable, EC2_RAW_TABLE);
  assert.equal(result.rawDataPath, EC2_VOLUME_PATH);
  assert.equal(result.notebookPath, PRICING_NOTEBOOK_WORKSPACE_PATH);
  assert.equal(result.notebookId, '12345');
  assert.deepEqual(result.metadata, {
    source: EC2_SOURCE_URL,
  });

  assert.deepEqual(fake.stored('aws_ec2'), {
    provider: 'AWS',
    service: 'AmazonEC2',
    id: 'aws_ec2',
    table: 'finops.pricing.aws_ec2',
    rawDataTable: EC2_RAW_TABLE,
    rawDataPath: EC2_VOLUME_PATH,
    notebookPath: PRICING_NOTEBOOK_WORKSPACE_PATH,
    notebookId: '12345',
    metadata: { source: EC2_SOURCE_URL },
    runId: null,
    runStatus: 'not_started',
    runUrl: null,
    runStartedAt: null,
    runFinishedAt: null,
    runCheckedAt: null,
    updatedAt: UPDATED_AT,
  });
  assert.equal(fake.stored('aws_rds'), null);
});

test('ensurePricingDataForId stores AWS RDS pricing metadata in pricing_data', async () => {
  const fake = createFakeDb();
  const workspaceClient = {
    workspace: {
      async getStatus({ path }: { path: string }) {
        assert.equal(path, PRICING_NOTEBOOK_WORKSPACE_PATH);
        return { object_id: 12345 };
      },
    },
  } as unknown as WorkspaceClient;

  const result = await ensurePricingDataForId(
    {
      DATABRICKS_APP_NAME: APP_NAME,
      DATABRICKS_HOST: 'https://example.cloud.databricks.com',
    } as Env,
    fake.db,
    'aws_rds',
    {
      workspaceClient,
      async uploadNotebook(wc, workspacePath) {
        assert.equal(wc, workspaceClient);
        assert.equal(workspacePath, PRICING_NOTEBOOK_WORKSPACE_PATH);
        return workspacePath;
      },
    },
  );

  assert.equal(result.provider, 'AWS');
  assert.equal(result.service, 'AmazonRDS');
  assert.equal(result.id, 'aws_rds');
  assert.equal(result.table, 'finops.pricing.aws_rds');
  assert.equal(result.rawDataTable, RDS_RAW_TABLE);
  assert.equal(result.rawDataPath, RDS_VOLUME_PATH);
  assert.equal(result.notebookPath, PRICING_NOTEBOOK_WORKSPACE_PATH);
  assert.equal(result.notebookId, '12345');
  assert.deepEqual(result.metadata, {
    source: RDS_SOURCE_URL,
  });
  assert.equal(fake.stored('aws_ec2'), null);
});

test('getDatabricksRunLink resolves job run URL on demand', async () => {
  let getRunQuery: unknown;
  const workspaceClient = {
    apiClient: {
      async request(options: { path: string; method: string; query?: unknown }) {
        assert.equal(options.path, '/api/2.2/jobs/runs/get');
        assert.equal(options.method, 'GET');
        getRunQuery = options.query;
        return { job_id: 54321 };
      },
    },
  } as unknown as WorkspaceClient;

  const result = await getDatabricksRunLink(
    { DATABRICKS_HOST: 'https://example.cloud.databricks.com' } as Env,
    67890,
    { workspaceClient },
  );

  assert.deepEqual(getRunQuery, { run_id: 67890 });
  assert.deepEqual(result, {
    jobId: 54321,
    runId: 67890,
    runUrl: 'https://example.cloud.databricks.com/jobs/54321/runs/67890',
  });
});

test('pricingNotebookState refreshes active run status with service principal client', async () => {
  const fake = createFakeDb([
    {
      provider: 'AWS',
      service: 'AmazonEC2',
      id: 'aws_ec2',
      table: 'finops.pricing.aws_ec2',
      rawDataTable: EC2_RAW_TABLE,
      rawDataPath: EC2_VOLUME_PATH,
      notebookPath: PRICING_NOTEBOOK_WORKSPACE_PATH,
      notebookId: '12345',
      metadata: { source: EC2_SOURCE_URL },
      runId: 67890,
      runStatus: 'running',
    },
  ]);
  let getRunQuery: unknown;
  const workspaceClient = {
    apiClient: {
      async request(options: { path: string; method: string; query?: unknown }) {
        assert.equal(options.path, '/api/2.2/jobs/runs/get');
        assert.equal(options.method, 'GET');
        getRunQuery = options.query;
        return {
          job_id: 54321,
          run_page_url: 'https://example.cloud.databricks.com/jobs/54321/runs/67890',
          state: { life_cycle_state: 'TERMINATED', result_state: 'SUCCESS' },
          start_time: 1_777_000_000_000,
          end_time: 1_777_000_120_000,
        };
      },
    },
  } as unknown as WorkspaceClient;

  const result = await pricingNotebookStateById(
    fake.db,
    { DATABRICKS_HOST: 'https://example.cloud.databricks.com' } as Env,
    'aws_ec2',
    { workspaceClient },
  );

  assert.deepEqual(getRunQuery, { run_id: 67890 });
  assert.equal(result.runStatus, 'succeeded');
  assert.equal(result.runUrl, 'https://example.cloud.databricks.com/jobs/54321/runs/67890');
  assert.equal(fake.stored('aws_ec2')?.runStatus, 'succeeded');
});

test('deletePricingNotebookData drops Unity Catalog table and deletes pricing_data record', async () => {
  const fake = createFakeDb([
    {
      provider: 'AWS',
      service: 'AmazonEC2',
      id: 'aws_ec2',
      table: 'finops.pricing.aws_ec2',
      rawDataTable: EC2_RAW_TABLE,
      rawDataPath: EC2_VOLUME_PATH,
      notebookPath: PRICING_NOTEBOOK_WORKSPACE_PATH,
      notebookId: '12345',
      metadata: { source: EC2_SOURCE_URL },
    },
  ]);
  let sqlText: string | null = null;
  const executor = {
    async run(query: string) {
      sqlText = query;
      return [];
    },
  };

  const result = await deletePricingNotebookData(
    { DATABRICKS_HOST: 'https://example.cloud.databricks.com' } as Env,
    fake.db,
    'obo-token',
    'aws_ec2',
    { executor: executor as never },
  );

  assert.equal(sqlText, 'DROP TABLE IF EXISTS `finops`.`pricing`.`aws_ec2`');
  assert.deepEqual(result, {
    id: 'aws_ec2',
    table: 'finops.pricing.aws_ec2',
    droppedTable: true,
    deletedPricingData: true,
  });
  assert.equal(fake.stored('aws_ec2'), null);
});

test('deletePricingNotebookData requires OBO token when dropping Unity Catalog table', async () => {
  const fake = createFakeDb([
    {
      provider: 'AWS',
      service: 'AmazonEC2',
      id: 'aws_ec2',
      table: 'finops.pricing.aws_ec2',
      rawDataTable: EC2_RAW_TABLE,
      rawDataPath: EC2_VOLUME_PATH,
      notebookPath: PRICING_NOTEBOOK_WORKSPACE_PATH,
      notebookId: '12345',
      metadata: { source: EC2_SOURCE_URL },
    },
  ]);

  try {
    await deletePricingNotebookData(
      {
        DATABRICKS_HOST: 'https://example.cloud.databricks.com',
        SQL_WAREHOUSE_ID: 'warehouse-1',
      } as Env,
      fake.db,
      undefined,
      'aws_ec2',
    );
    assert.fail('Expected deletePricingNotebookData to reject without OBO token');
  } catch (err) {
    assert.ok(err instanceof DataSourceSetupError);
    assert.equal(err.statusCode, 401);
    assert.equal(fake.stored('aws_ec2')?.table, 'finops.pricing.aws_ec2');
  }
});

test('submitManagedNotebookRunById submits an RDS run with service-specific parameters', async () => {
  const fake = createFakeDb([
    {
      provider: 'AWS',
      service: 'AmazonRDS',
      id: 'aws_rds',
      table: 'finops.pricing.aws_rds',
      rawDataTable: RDS_RAW_TABLE,
      rawDataPath: RDS_VOLUME_PATH,
      notebookPath: PRICING_NOTEBOOK_WORKSPACE_PATH,
      notebookId: '12345',
      metadata: { source: RDS_SOURCE_URL },
      updatedAt: UPDATED_AT,
    },
  ]);
  let payload: unknown;
  const workspaceClient = {
    apiClient: {
      async request(options: { path: string; method: string; payload?: unknown }) {
        assert.equal(options.path, '/api/2.2/jobs/runs/submit');
        assert.equal(options.method, 'POST');
        payload = options.payload;
        return { run_id: 67890 };
      },
    },
  } as unknown as WorkspaceClient;

  const result = await submitManagedNotebookRunById(
    { DATABRICKS_HOST: 'https://example.cloud.databricks.com' } as Env,
    fake.db,
    'aws_rds',
    { workspaceClient },
  );

  assert.deepEqual(result, {
    provider: 'AWS',
    service: 'AmazonRDS',
    id: 'aws_rds',
    runId: 67890,
    runStatus: 'pending',
    runUrl: null,
  });
  assert.equal(fake.stored('aws_rds')?.runId, 67890);
  assert.equal(fake.stored('aws_rds')?.runStatus, 'pending');
  const submitted = payload as { run_name?: string };
  assert.match(submitted.run_name ?? '', /^aws_rds-\d+$/);
  assert.deepEqual(payload, {
    run_name: submitted.run_name,
    performance_target: 'PERFORMANCE_OPTIMIZED',
    environments: [
      {
        environment_key: 'pricing_serverless',
        spec: {
          environment_version: '4',
        },
      },
    ],
    tasks: [
      {
        task_key: 'aws_rds',
        environment_key: 'pricing_serverless',
        notebook_task: {
          notebook_path: PRICING_NOTEBOOK_WORKSPACE_PATH,
          source: 'WORKSPACE',
          base_parameters: {
            source_url: RDS_SOURCE_URL,
            volume_path: RDS_VOLUME_PATH,
            raw_table: RDS_RAW_TABLE,
            target_table: 'finops.pricing.aws_rds',
            aws_service_code: 'AmazonRDS',
          },
        },
      },
    ],
  });
});

test('submitManagedNotebookRunById prepares missing pricing metadata with service principal client', async () => {
  const fake = createFakeDb();
  let importedPath: string | null = null;
  let payload: unknown;
  const workspaceClient = {
    workspace: {
      async mkdirs() {},
      async import({ path }: { path: string }) {
        importedPath = path;
      },
      async getStatus({ path }: { path: string }) {
        assert.equal(path, PRICING_NOTEBOOK_WORKSPACE_PATH);
        return { object_id: 12345 };
      },
    },
    apiClient: {
      async request(options: { path: string; method: string; payload?: unknown }) {
        assert.equal(options.path, '/api/2.2/jobs/runs/submit');
        assert.equal(options.method, 'POST');
        payload = options.payload;
        return { run_id: 67890 };
      },
    },
  } as unknown as WorkspaceClient;

  await submitManagedNotebookRunById(
    {
      DATABRICKS_APP_NAME: APP_NAME,
      DATABRICKS_HOST: 'https://example.cloud.databricks.com',
    } as Env,
    fake.db,
    'aws_ec2',
    { workspaceClient },
  );

  assert.equal(importedPath, PRICING_NOTEBOOK_WORKSPACE_PATH);
  assert.equal(fake.stored('aws_ec2')?.notebookId, '12345');
  assert.deepEqual(
    (payload as { tasks: Array<{ task_key: string }> }).tasks[0]?.task_key,
    'aws_ec2',
  );
});
