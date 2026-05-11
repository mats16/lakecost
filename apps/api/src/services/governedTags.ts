import {
  CostExplorerClient,
  ListCostAllocationTagsCommand,
  UpdateCostAllocationTagsStatusCommand,
  type CostAllocationTag,
  type UpdateCostAllocationTagsStatusError,
} from '@aws-sdk/client-cost-explorer';
import {
  GOVERNED_TAG_DEFINITIONS,
  roleNameFromArn,
  type Env,
  type GovernedTagAwsAccount,
  type GovernedTagAwsStatus,
  type GovernedTagDatabricksStatus,
  type GovernedTagRow,
  type GovernedTagSyncAccountResult,
  type GovernedTagSyncBody,
  type GovernedTagSyncResult,
  type GovernedTagSyncTagResult,
  type GovernedTagsResponse,
} from '@finlake/shared';
import { logger } from '../config/logger.js';
import {
  AWS_BCM_REGION,
  AWS_SERVICE_ROLE_NAME,
  generateAwsTemporaryCredentials,
} from './awsCredentials.js';
import { requireAppWorkspaceClient } from './servicePrincipalIdentity.js';
import { listAccessibleServiceCredentials } from './serviceCredentials.js';
import { WorkspaceServiceError } from './workspaceClientErrors.js';

interface TagPolicyLike {
  id?: string;
  tag_key: string;
  description?: string;
  update_time?: string;
}

export class GovernedTagsServiceError extends WorkspaceServiceError {}

export async function listGovernedTags(env: Env): Promise<GovernedTagsResponse> {
  const warnings: string[] = [];
  const [databricksByKey, awsAccounts] = await Promise.all([
    listDatabricksTagPolicies(env, warnings),
    listFinLakeAwsAccounts(env, warnings),
  ]);
  const awsByAccount = new Map<string, Map<string, GovernedTagAwsStatus>>();

  await Promise.all(
    awsAccounts.map(async (account) => {
      try {
        awsByAccount.set(account.awsAccountId, await listAwsCostAllocationTags(env, account));
      } catch (err) {
        const message = (err as Error).message;
        warnings.push(`AWS account ${account.awsAccountId}: ${message}`);
        awsByAccount.set(account.awsAccountId, awsErrorStatuses(account.awsAccountId, message));
      }
    }),
  );

  return {
    items: GOVERNED_TAG_DEFINITIONS.map((definition) => ({
      definition: { ...definition, allowedValues: [...definition.allowedValues] },
      databricks: databricksStatusFor(definition.key, databricksByKey),
      aws: awsAccounts.map((account) => {
        const statuses = awsByAccount.get(account.awsAccountId);
        return (
          statuses?.get(definition.key) ?? {
            accountId: account.awsAccountId,
            status: 'NotFound',
            lastUpdatedDate: null,
            lastUsedDate: null,
            message: null,
          }
        );
      }),
    })) satisfies GovernedTagRow[],
    awsAccounts,
    warnings,
    generatedAt: new Date().toISOString(),
  };
}

export async function syncGovernedTags(
  env: Env,
  body: GovernedTagSyncBody,
): Promise<GovernedTagSyncResult> {
  if (body.platform === 'databricks') {
    return syncDatabricksGovernedTags(env, body.tagKey);
  }
  return syncAwsCostAllocationTags(env, body.awsAccountId, body.tagKey);
}

async function syncDatabricksGovernedTags(
  env: Env,
  tagKey?: string,
): Promise<GovernedTagSyncResult> {
  const syncedAt = new Date().toISOString();
  const definitions = tagKey
    ? GOVERNED_TAG_DEFINITIONS.filter((definition) => definition.key === tagKey)
    : GOVERNED_TAG_DEFINITIONS;
  if (definitions.length === 0) {
    throw new GovernedTagsServiceError(`Unknown governed tag: ${tagKey}`, 400);
  }
  let wc: ReturnType<typeof requireAppWorkspaceClient>;
  try {
    wc = requireAppWorkspaceClient(env, GovernedTagsServiceError);
  } catch (err) {
    return {
      platform: 'databricks',
      syncedAt,
      tags: definitions.map((definition) => ({
        key: definition.key,
        status: 'failed',
        message: (err as Error).message,
      })),
      awsAccounts: [],
    };
  }

  let existing: Map<string, TagPolicyLike>;
  try {
    existing = await listDatabricksTagPolicies(env, [], { throwOnError: true });
  } catch (err) {
    logger.error({ err }, 'Failed to list existing tag policies before sync');
    return {
      platform: 'databricks',
      syncedAt,
      tags: definitions.map((definition) => ({
        key: definition.key,
        status: 'failed',
        message: `Cannot determine existing policies: ${(err as Error).message}`,
      })),
      awsAccounts: [],
    };
  }
  const tags: GovernedTagSyncTagResult[] = [];
  for (const definition of definitions) {
    const tagPolicy = {
      tag_key: definition.key,
      description: definition.description,
      values:
        definition.allowedValues.length > 0
          ? definition.allowedValues.map((name) => ({ name }))
          : undefined,
    };
    try {
      if (existing.has(definition.key)) {
        await wc.tagPolicies.updateTagPolicy({
          tag_key: definition.key,
          tag_policy: tagPolicy,
          update_mask: definition.allowedValues.length > 0 ? 'description,values' : 'description',
        });
        tags.push({ key: definition.key, status: 'synced', message: 'Updated tag policy' });
      } else {
        await wc.tagPolicies.createTagPolicy({ tag_policy: tagPolicy });
        tags.push({ key: definition.key, status: 'synced', message: 'Created tag policy' });
      }
    } catch (err) {
      logger.error({ err, tagKey: definition.key }, 'Databricks governed tag sync failed');
      tags.push({ key: definition.key, status: 'failed', message: (err as Error).message });
    }
  }

  return { platform: 'databricks', syncedAt, tags, awsAccounts: [] };
}

async function syncAwsCostAllocationTags(
  env: Env,
  awsAccountId?: string,
  tagKey?: string,
): Promise<GovernedTagSyncResult> {
  const syncedAt = new Date().toISOString();
  const definitions = tagKey
    ? GOVERNED_TAG_DEFINITIONS.filter((definition) => definition.key === tagKey)
    : GOVERNED_TAG_DEFINITIONS;
  if (definitions.length === 0) {
    throw new GovernedTagsServiceError(`Unknown governed tag: ${tagKey}`, 400);
  }
  const warnings: string[] = [];
  const allAccounts = await listFinLakeAwsAccounts(env, warnings);
  const accounts = awsAccountId
    ? allAccounts.filter((account) => account.awsAccountId === awsAccountId)
    : allAccounts;

  if (awsAccountId && accounts.length === 0) {
    throw new GovernedTagsServiceError(
      `FinLakeServiceRole service credential not found for AWS account ${awsAccountId}`,
      400,
    );
  }
  if (accounts.length === 0) {
    throw new GovernedTagsServiceError('No FinLakeServiceRole service credentials found', 400);
  }

  const accountResults: GovernedTagSyncAccountResult[] = [];
  const byTag = new Map<string, GovernedTagSyncTagResult>();
  for (const definition of definitions) {
    byTag.set(definition.key, { key: definition.key, status: 'skipped', message: null });
  }

  await Promise.all(
    accounts.map(async (account) => {
      try {
        const credential = await generateAwsTemporaryCredentials(
          env,
          account.credentialName,
          GovernedTagsServiceError,
        );
        const client = new CostExplorerClient({
          region: AWS_BCM_REGION,
          credentials: credential,
        });
        const result = await client.send(
          new UpdateCostAllocationTagsStatusCommand({
            CostAllocationTagsStatus: definitions.map((definition) => ({
              TagKey: definition.key,
              Status: 'Active',
            })),
          }),
        );
        const errors = new Map((result.Errors ?? []).map((err) => [err.TagKey, err]));
        for (const definition of definitions) {
          const error = errors.get(definition.key);
          mergeTagSyncResult(byTag, definition.key, error);
        }
        accountResults.push({
          awsAccountId: account.awsAccountId,
          credentialName: account.credentialName,
          status: errors.size > 0 ? 'failed' : 'synced',
          message: errors.size > 0 ? `${errors.size} tag(s) failed to sync` : null,
        });
      } catch (err) {
        logger.error(
          { err, awsAccountId: account.awsAccountId },
          'AWS cost allocation tag sync failed',
        );
        for (const definition of definitions) {
          mergeTagSyncResult(byTag, definition.key, undefined, (err as Error).message);
        }
        accountResults.push({
          awsAccountId: account.awsAccountId,
          credentialName: account.credentialName,
          status: 'failed',
          message: (err as Error).message,
        });
      }
    }),
  );

  return {
    platform: 'aws',
    syncedAt,
    tags: [...byTag.values()],
    awsAccounts: accountResults.sort((a, b) => a.awsAccountId.localeCompare(b.awsAccountId)),
  };
}

async function listDatabricksTagPolicies(
  env: Env,
  warnings: string[],
  options?: { throwOnError?: boolean },
): Promise<Map<string, TagPolicyLike>> {
  try {
    const wc = requireAppWorkspaceClient(env, GovernedTagsServiceError);
    const policies = new Map<string, TagPolicyLike>();
    for await (const policy of wc.tagPolicies.listTagPolicies({ page_size: 1000 })) {
      const item = policy as TagPolicyLike;
      if (item.tag_key) policies.set(item.tag_key, item);
    }
    return policies;
  } catch (err) {
    if (options?.throwOnError) {
      throw err;
    }
    logger.warn({ err }, 'Databricks governed tag policy list failed');
    warnings.push(`Databricks governed tags: ${(err as Error).message}`);
    return new Map();
  }
}

async function listFinLakeAwsAccounts(
  env: Env,
  warnings: string[],
): Promise<GovernedTagAwsAccount[]> {
  try {
    const credentials = await listAccessibleServiceCredentials(env);
    const byAccount = new Map<string, GovernedTagAwsAccount>();
    for (const credential of credentials) {
      if (
        credential.awsAccountId &&
        roleNameFromArn(credential.roleArn) === AWS_SERVICE_ROLE_NAME &&
        !byAccount.has(credential.awsAccountId)
      ) {
        byAccount.set(credential.awsAccountId, {
          awsAccountId: credential.awsAccountId,
          credentialName: credential.name,
        });
      }
    }
    return [...byAccount.values()].sort((a, b) => a.awsAccountId.localeCompare(b.awsAccountId));
  } catch (err) {
    logger.warn({ err }, 'FinLakeServiceRole credential list failed');
    warnings.push(`FinLakeServiceRole credentials: ${(err as Error).message}`);
    return [];
  }
}

async function listAwsCostAllocationTags(
  env: Env,
  account: GovernedTagAwsAccount,
): Promise<Map<string, GovernedTagAwsStatus>> {
  const credential = await generateAwsTemporaryCredentials(
    env,
    account.credentialName,
    GovernedTagsServiceError,
  );
  const client = new CostExplorerClient({
    region: AWS_BCM_REGION,
    credentials: credential,
  });
  const keys = GOVERNED_TAG_DEFINITIONS.map((definition) => definition.key);
  const collected: CostAllocationTag[] = [];
  let NextToken: string | undefined;
  do {
    const page = await client.send(
      new ListCostAllocationTagsCommand({
        TagKeys: keys,
        Type: 'UserDefined',
        MaxResults: 1000,
        NextToken,
      }),
    );
    collected.push(...(page.CostAllocationTags ?? []));
    NextToken = page.NextToken;
  } while (NextToken);

  const byKey = new Map<string, GovernedTagAwsStatus>();
  for (const tag of collected) {
    if (!tag.TagKey) continue;
    byKey.set(tag.TagKey, {
      accountId: account.awsAccountId,
      status: tag.Status === 'Active' ? 'Active' : 'Inactive',
      lastUpdatedDate: tag.LastUpdatedDate ?? null,
      lastUsedDate: tag.LastUsedDate ?? null,
      message: null,
    });
  }
  for (const key of keys) {
    if (!byKey.has(key)) {
      byKey.set(key, {
        accountId: account.awsAccountId,
        status: 'NotFound',
        lastUpdatedDate: null,
        lastUsedDate: null,
        message: 'Tag key has not appeared in AWS billing data yet',
      });
    }
  }
  return byKey;
}

function databricksStatusFor(
  key: string,
  policies: Map<string, TagPolicyLike>,
): GovernedTagDatabricksStatus {
  const policy = policies.get(key);
  if (!policy) {
    return { status: 'missing', policyId: null, updatedAt: null, message: null };
  }
  return {
    status: 'governed',
    policyId: policy.id ?? null,
    updatedAt: policy.update_time ?? null,
    message: null,
  };
}

function awsErrorStatuses(accountId: string, message: string): Map<string, GovernedTagAwsStatus> {
  return new Map(
    GOVERNED_TAG_DEFINITIONS.map((definition) => [
      definition.key,
      {
        accountId,
        status: 'Error',
        lastUpdatedDate: null,
        lastUsedDate: null,
        message,
      },
    ]),
  );
}

function mergeTagSyncResult(
  byTag: Map<string, GovernedTagSyncTagResult>,
  key: string,
  error?: UpdateCostAllocationTagsStatusError,
  accountError?: string,
): void {
  if (error || accountError) {
    byTag.set(key, {
      key,
      status: 'failed',
      message: error
        ? `${error.Code ?? 'Error'}: ${error.Message ?? 'Sync failed'}`
        : (accountError ?? null),
    });
    return;
  }
  const existing = byTag.get(key);
  if (existing?.status !== 'failed') {
    byTag.set(key, { key, status: 'synced', message: 'Activated AWS cost allocation tag' });
  }
}
