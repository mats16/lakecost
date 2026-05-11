import type { Env, StorageCredentialCreateBody, StorageCredentialSummary } from '@finlake/shared';
import { logger } from '../config/logger.js';
import { buildUserWorkspaceClient } from './statementExecution.js';
import {
  isOwnedByCurrentServicePrincipal,
  requireAppWorkspaceClient,
  resolveOwnerAliases,
} from './servicePrincipalIdentity.js';
import { WorkspaceServiceError, isPermissionDenied } from './workspaceClientErrors.js';

export interface StorageCredentialInfoLike {
  name?: string;
  aws_iam_role?: {
    external_id?: string;
    role_arn?: string;
    unity_catalog_iam_arn?: string;
  };
  read_only?: boolean;
  comment?: string;
  owner?: string;
}

export class StorageCredentialServiceError extends WorkspaceServiceError {}

export async function listAccessibleStorageCredentials(
  env: Env,
  userToken: string | undefined,
): Promise<StorageCredentialSummary[]> {
  if (!userToken) throw new StorageCredentialServiceError('OBO access token required', 401);
  const wc = buildUserWorkspaceClient(env, userToken);
  if (!wc) throw new StorageCredentialServiceError('DATABRICKS_HOST not configured', 500);
  return collectStorageCredentials(wc, (cred) => {
    // User-accessible list requires an AWS IAM role
    return !!cred.aws_iam_role?.role_arn;
  });
}

export async function listServicePrincipalStorageCredentials(
  env: Env,
): Promise<StorageCredentialSummary[]> {
  const wc = requireAppWorkspaceClient(env, StorageCredentialServiceError);
  const ownerAliases = await resolveOwnerAliases(wc, env, StorageCredentialServiceError);
  return collectStorageCredentials(wc, (cred) =>
    isOwnedByCurrentServicePrincipal(cred.owner, ownerAliases),
  );
}

async function collectStorageCredentials(
  wc: ReturnType<typeof buildUserWorkspaceClient> & {},
  filter?: (cred: StorageCredentialInfoLike) => boolean,
): Promise<StorageCredentialSummary[]> {
  const collected: StorageCredentialSummary[] = [];
  try {
    for await (const item of wc.storageCredentials.list({
      max_results: 0,
    })) {
      const cred = item as StorageCredentialInfoLike;
      if (!cred.name) continue;
      if (filter && !filter(cred)) continue;
      collected.push(toStorageCredentialSummary(cred));
    }
  } catch (err) {
    logger.error({ err }, 'wc.storageCredentials.list failed');
    throw new StorageCredentialServiceError(
      `Failed to list storage credentials: ${(err as Error).message}`,
      isPermissionDenied(err) ? 403 : 502,
    );
  }
  collected.sort((a, b) => {
    const accountCmp = (a.awsAccountId ?? '').localeCompare(b.awsAccountId ?? '');
    return accountCmp || a.name.localeCompare(b.name);
  });
  return collected;
}

export async function createAwsStorageCredential(
  env: Env,
  input: StorageCredentialCreateBody,
): Promise<StorageCredentialSummary> {
  const wc = requireAppWorkspaceClient(env, StorageCredentialServiceError);
  const roleArn = `arn:aws:iam::${input.awsAccountId}:role/${input.roleName}`;

  try {
    const created = await wc.credentials.createCredential({
      name: input.name,
      purpose: 'STORAGE',
      aws_iam_role: { role_arn: roleArn },
      read_only: input.readOnly ?? false,
      comment: input.comment?.trim() || 'FinLake AWS storage credential',
      skip_validation: true,
    });
    return toStorageCredentialSummary(created as StorageCredentialInfoLike, input.name, roleArn);
  } catch (err) {
    logger.error({ err }, 'wc.storageCredentials.create failed');
    throw new StorageCredentialServiceError(
      `Failed to create storage credential: ${(err as Error).message}`,
      isPermissionDenied(err) ? 403 : 502,
    );
  }
}

export function toStorageCredentialSummary(
  cred: StorageCredentialInfoLike,
  fallbackName?: string,
  fallbackRoleArn?: string,
): StorageCredentialSummary {
  const roleArn = cred.aws_iam_role?.role_arn ?? fallbackRoleArn ?? null;
  return {
    name: cred.name ?? fallbackName ?? '',
    awsAccountId: roleArn ? awsAccountIdFromRoleArn(roleArn) : null,
    roleArn,
    externalId: cred.aws_iam_role?.external_id ?? null,
    unityCatalogIamArn: cred.aws_iam_role?.unity_catalog_iam_arn ?? null,
    readOnly: cred.read_only ?? null,
    comment: cred.comment ?? null,
  };
}

export function awsAccountIdFromRoleArn(roleArn: string): string | null {
  const match = /^arn:aws(?:-[a-z]+)*:iam::(\d{12}):role\/.+/.exec(roleArn);
  return match?.[1] ?? null;
}
