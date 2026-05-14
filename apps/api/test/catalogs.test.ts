import assert from 'node:assert/strict';
import test from 'node:test';

import type { Env } from '@finlake/shared';
import { provisionCatalogWithDeps } from '../src/services/catalogs.js';
import type { StatementExecutor } from '../src/services/statementExecution.js';

class FakeExecutor {
  readonly sql: string[] = [];

  async run(sqlText: string): Promise<unknown[]> {
    this.sql.push(sqlText);
    return [];
  }
}

const env = {
  DATABRICKS_CLIENT_ID: 'sp-123',
} as Env;

test('provisionCatalog creates pricing schema, downloads volume, and grants', async () => {
  const executor = new FakeExecutor();
  const result = await provisionCatalogWithDeps(
    env,
    'finops',
    {},
    {
      executor: executor as unknown as StatementExecutor,
    },
  );

  assert.equal(result.pricingSchemaEnsured, 'ensured');
  assert.equal(result.downloadsVolumeEnsured, 'ensured');
  assert.equal(result.grants.pricingSchema, 'granted');
  assert.equal(result.grants.downloadsVolume, 'granted');
  assert.equal(result.grants.usersDownloadsVolume, 'granted');

  assert.ok(executor.sql.includes('CREATE SCHEMA IF NOT EXISTS `finops`.`pricing`'));
  assert.ok(executor.sql.includes('CREATE VOLUME IF NOT EXISTS `finops`.`ingest`.`downloads`'));
  assert.ok(
    executor.sql.includes(
      'GRANT USE SCHEMA, SELECT, CREATE TABLE ON SCHEMA `finops`.`pricing` TO `sp-123`',
    ),
  );
  assert.ok(
    executor.sql.includes(
      'GRANT READ VOLUME, WRITE VOLUME ON VOLUME `finops`.`ingest`.`downloads` TO `sp-123`',
    ),
  );
  assert.ok(
    executor.sql.includes(
      'GRANT READ VOLUME ON VOLUME `finops`.`ingest`.`downloads` TO `account users`',
    ),
  );
});
