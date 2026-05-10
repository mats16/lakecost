import type { Env, ServiceCredentialCreateBody, ServiceCredentialSummary } from '@lakecost/shared';
import { logger } from '../config/logger.js';
import {
  isOwnedByCurrentServicePrincipal,
  requireAppWorkspaceClient,
  resolveOwnerAliases,
} from './servicePrincipalIdentity.js';
import { awsAccountIdFromRoleArn } from './storageCredentials.js';
import { WorkspaceServiceError, isPermissionDenied } from './workspaceClientErrors.js';

interface CredentialInfoLike {
  name?: string;
  aws_iam_role?: {
    external_id?: string;
    role_arn?: string;
    unity_catalog_iam_arn?: string;
  };
  owner?: string;
  created_at?: number;
  comment?: string;
  purpose?: string;
}

export class ServiceCredentialServiceError extends WorkspaceServiceError {}

export async function listAccessibleServiceCredentials(
  env: Env,
): Promise<ServiceCredentialSummary[]> {
  const wc = requireAppWorkspaceClient(env, ServiceCredentialServiceError);
  const ownerAliases = await resolveOwnerAliases(wc, env, ServiceCredentialServiceError);

  const collected: ServiceCredentialSummary[] = [];
  try {
    for await (const item of wc.credentials.listCredentials({
      max_results: 0,
      purpose: 'SERVICE',
    })) {
      const credential = item as CredentialInfoLike;
      if (!credential.name) continue;
      if (!isOwnedByCurrentServicePrincipal(credential.owner, ownerAliases)) continue;
      collected.push(toSummary(credential));
    }
  } catch (err) {
    logger.error({ err }, 'wc.credentials.listCredentials failed');
    throw new ServiceCredentialServiceError(
      `Failed to list service credentials: ${(err as Error).message}`,
      isPermissionDenied(err) ? 403 : 502,
    );
  }

  collected.sort((a, b) => {
    const accountCmp = (a.awsAccountId ?? '').localeCompare(b.awsAccountId ?? '');
    return accountCmp || a.name.localeCompare(b.name);
  });
  return collected;
}

export async function createAwsServiceCredential(
  env: Env,
  input: ServiceCredentialCreateBody,
): Promise<ServiceCredentialSummary> {
  const wc = requireAppWorkspaceClient(env, ServiceCredentialServiceError);

  const roleArn = `arn:aws:iam::${input.awsAccountId}:role/${input.roleName}`;
  try {
    const created = await wc.credentials.createCredential({
      name: input.name,
      purpose: 'SERVICE',
      aws_iam_role: { role_arn: roleArn },
      comment:
        input.comment?.trim() ||
        'FinLake AWS service credential for data exports and cost allocation tags',
      skip_validation: true,
    });
    return toSummary(created as CredentialInfoLike, input.name);
  } catch (err) {
    logger.error({ err }, 'wc.credentials.createCredential failed');
    throw new ServiceCredentialServiceError(
      `Failed to create service credential: ${(err as Error).message}`,
      isPermissionDenied(err) ? 403 : 502,
    );
  }
}

export async function deleteCredential(env: Env, name: string): Promise<void> {
  const wc = requireAppWorkspaceClient(env, ServiceCredentialServiceError);
  try {
    await wc.credentials.deleteCredential({ name_arg: name });
  } catch (err) {
    logger.error({ err }, 'wc.credentials.deleteCredential failed');
    throw new ServiceCredentialServiceError(
      `Failed to delete credential: ${(err as Error).message}`,
      isPermissionDenied(err) ? 403 : 502,
    );
  }
}

function toSummary(
  credential: CredentialInfoLike,
  fallbackName?: string,
): ServiceCredentialSummary {
  const roleArn = credential.aws_iam_role?.role_arn ?? null;
  return {
    name: credential.name ?? fallbackName ?? '',
    awsAccountId: roleArn ? awsAccountIdFromRoleArn(roleArn) : null,
    roleArn,
    externalId: credential.aws_iam_role?.external_id ?? null,
    unityCatalogIamArn: credential.aws_iam_role?.unity_catalog_iam_arn ?? null,
    owner: credential.owner ?? null,
    createdAt: credential.created_at ?? null,
    comment: credential.comment ?? null,
  };
}
