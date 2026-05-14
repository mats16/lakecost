import assert from 'node:assert/strict';
import test from 'node:test';

import type { DatabaseClient } from '@finlake/db';
import {
  CATALOG_SETTING_KEY,
  type Env,
  type PricingData,
  type PricingNotebookSetupResult,
} from '@finlake/shared';
import {
  pricingNotebookWorkspacePath,
  setupPricingNotebookWithDeps,
} from '../src/services/pricingNotebook.js';
import { runManagedNotebookById } from '../src/services/notebookRuns.js';
import type { WorkspaceClient } from '../src/services/statementExecution.js';

const UPDATED_AT = '2026-05-14T00:00:00.000Z';
const APP_NAME = 'finlake-dev';
const PRICING_NOTEBOOK_WORKSPACE_PATH = pricingNotebookWorkspacePath(APP_NAME);
const SOURCE_URL =
  'https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonEC2/current/index.csv';
const VOLUME_PATH = '/Volumes/finops/ingest/downloads/pricing_aws_ec2.csv';
const RAW_TABLE = 'finops.ingest.pricing_aws_ec2';

function createFakeDb(initial: PricingData | null = null) {
  let stored: PricingData | null = initial;
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
          return stored?.provider === provider && stored?.service === service ? stored : null;
        },
        async getByNotebookId(notebookId: string) {
          return stored?.notebookId === notebookId ? stored : null;
        },
        async upsert(input: Omit<PricingData, 'updatedAt'>) {
          stored = { ...input, updatedAt: UPDATED_AT };
          return stored;
        },
      },
    },
  } as unknown as DatabaseClient;
  return {
    db,
    stored: () => stored,
  };
}

test('setupPricingNotebook stores AWS EC2 pricing metadata in pricing_data', async () => {
  const fake = createFakeDb();
  const workspaceClient = {
    workspace: {
      async getStatus({ path }: { path: string }) {
        assert.equal(path, PRICING_NOTEBOOK_WORKSPACE_PATH);
        return { object_id: 12345 };
      },
    },
  } as unknown as WorkspaceClient;

  const result: PricingNotebookSetupResult = await setupPricingNotebookWithDeps(
    {
      DATABRICKS_APP_NAME: APP_NAME,
      DATABRICKS_HOST: 'https://example.cloud.databricks.com',
    } as Env,
    fake.db,
    'obo-token',
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
  assert.equal(result.slug, 'aws_ec2');
  assert.equal(result.table, 'finops.pricing.aws_ec2');
  assert.equal(result.rawDataTable, RAW_TABLE);
  assert.equal(result.rawDataPath, VOLUME_PATH);
  assert.equal(result.notebookWorkspacePath, PRICING_NOTEBOOK_WORKSPACE_PATH);
  assert.equal(result.notebookId, '12345');
  assert.deepEqual(result.metadata, {
    source: SOURCE_URL,
  });

  assert.deepEqual(fake.stored(), {
    provider: 'AWS',
    service: 'AmazonEC2',
    slug: 'aws_ec2',
    table: 'finops.pricing.aws_ec2',
    rawDataTable: RAW_TABLE,
    rawDataPath: VOLUME_PATH,
    notebookPath: PRICING_NOTEBOOK_WORKSPACE_PATH,
    notebookId: '12345',
    metadata: { source: SOURCE_URL },
    updatedAt: UPDATED_AT,
  });
});

test('runManagedNotebookById submits a serverless one-time run with pricing parameters', async () => {
  const fake = createFakeDb({
    provider: 'AWS',
    service: 'AmazonEC2',
    slug: 'aws_ec2',
    table: 'finops.pricing.aws_ec2',
    rawDataTable: RAW_TABLE,
    rawDataPath: VOLUME_PATH,
    notebookPath: PRICING_NOTEBOOK_WORKSPACE_PATH,
    notebookId: '12345',
    metadata: { source: SOURCE_URL },
    updatedAt: UPDATED_AT,
  });
  let payload: unknown;
  let getRunQuery: unknown;
  const workspaceClient = {
    apiClient: {
      async request(options: { path: string; method: string; payload?: unknown; query?: unknown }) {
        if (options.path === '/api/2.2/jobs/runs/submit') {
          assert.equal(options.method, 'POST');
          payload = options.payload;
          return { run_id: 67890 };
        }
        assert.equal(options.path, '/api/2.2/jobs/runs/get');
        assert.equal(options.method, 'GET');
        getRunQuery = options.query;
        return { job_id: 54321 };
      },
    },
  } as unknown as WorkspaceClient;

  const result = await runManagedNotebookById(
    { DATABRICKS_HOST: 'https://example.cloud.databricks.com' } as Env,
    fake.db,
    'obo-token',
    '12345',
    { workspaceClient },
  );

  assert.deepEqual(result, {
    provider: 'AWS',
    service: 'AmazonEC2',
    slug: 'aws_ec2',
    jobId: 54321,
    runId: 67890,
    runUrl: 'https://example.cloud.databricks.com/jobs/54321/runs/67890',
  });
  assert.deepEqual(getRunQuery, { run_id: 67890 });
  const submitted = payload as { run_name?: string };
  assert.match(submitted.run_name ?? '', /^aws_ec2-\d+$/);
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
        task_key: 'aws_ec2',
        environment_key: 'pricing_serverless',
        notebook_task: {
          notebook_path: PRICING_NOTEBOOK_WORKSPACE_PATH,
          source: 'WORKSPACE',
          base_parameters: {
            volume_path: VOLUME_PATH,
            raw_table: RAW_TABLE,
            target_table: 'finops.pricing.aws_ec2',
          },
        },
      },
    ],
  });
});
