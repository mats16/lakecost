import type { Env, StorageCredentialSummary } from '@lakecost/shared';
import { logger } from '../config/logger.js';
import { buildUserWorkspaceClient } from './statementExecution.js';
import { WorkspaceServiceError, isPermissionDenied } from './workspaceClientErrors.js';

interface StorageCredentialInfoLike {
  name?: string;
  aws_iam_role?: {
    role_arn?: string;
  };
  read_only?: boolean;
  comment?: string;
}

export class StorageCredentialServiceError extends WorkspaceServiceError {}

export async function listAccessibleStorageCredentials(
  env: Env,
  userToken: string | undefined,
): Promise<StorageCredentialSummary[]> {
  if (!userToken) throw new StorageCredentialServiceError('OBO access token required', 401);
  const wc = buildUserWorkspaceClient(env, userToken);
  if (!wc) throw new StorageCredentialServiceError('DATABRICKS_HOST not configured', 500);

  const collected: StorageCredentialSummary[] = [];
  try {
    for await (const item of wc.storageCredentials.list({
      max_results: 0, // 0 = use paginated mode per SDK docs
    })) {
      const cred = item as StorageCredentialInfoLike;
      const roleArn = cred.aws_iam_role?.role_arn ?? null;
      if (!cred.name || !roleArn) continue;
      collected.push({
        name: cred.name,
        awsAccountId: awsAccountIdFromRoleArn(roleArn),
        roleArn,
        readOnly: cred.read_only ?? null,
        comment: cred.comment ?? null,
      });
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

function awsAccountIdFromRoleArn(roleArn: string): string | null {
  const match = /^arn:aws(?:-[a-z]+)*:iam::(\d{12}):role\/.+/.exec(roleArn);
  return match?.[1] ?? null;
}
