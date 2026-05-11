import type { Env, StorageCredentialSummary } from '@finlake/shared';
import { logger } from '../config/logger.js';
import { sleep } from '../utils/sleep.js';
import { requireAppWorkspaceClient } from './servicePrincipalIdentity.js';
import { type WorkspaceClient } from './statementExecution.js';
import { WorkspaceServiceError, isPermissionDenied } from './workspaceClientErrors.js';

export class AwsStorageCredentialValidationError extends WorkspaceServiceError {}

type ValidationResult = NonNullable<
  Awaited<ReturnType<WorkspaceClient['storageCredentials']['validate']>>['results']
>[number];

const VALIDATION_RETRY_DELAYS_MS = [1_000, 2_000, 4_000, 8_000, 15_000] as const;

export async function validateAwsStorageCredentialWithRetry(
  env: Env,
  storageCredential: StorageCredentialSummary,
  s3Url: string,
): Promise<void> {
  const wc = requireAppWorkspaceClient(env, AwsStorageCredentialValidationError);

  let lastError: Error | null = null;
  for (let attempt = 0; attempt <= VALIDATION_RETRY_DELAYS_MS.length; attempt += 1) {
    try {
      await validateAwsStorageCredentialOnce(wc, storageCredential, s3Url);
      return;
    } catch (err) {
      lastError = err as Error;
      if (attempt >= VALIDATION_RETRY_DELAYS_MS.length) break;
      await sleep(VALIDATION_RETRY_DELAYS_MS[attempt]!);
    }
  }

  throw new AwsStorageCredentialValidationError(
    formatStorageCredentialValidationError(storageCredential, s3Url, lastError),
    lastError instanceof AwsStorageCredentialValidationError
      ? lastError.statusCode
      : isPermissionDenied(lastError)
        ? 403
        : 409,
  );
}

async function validateAwsStorageCredentialOnce(
  wc: WorkspaceClient,
  storageCredential: StorageCredentialSummary,
  s3Url: string,
): Promise<void> {
  try {
    const result = await wc.storageCredentials.validate({
      storage_credential_name: storageCredential.name,
      url: s3Url,
      read_only: true,
    });
    const failureSummary = summarizeStorageCredentialValidationResults(result.results ?? []);
    if (failureSummary) {
      throw new AwsStorageCredentialValidationError(failureSummary, 409);
    }
  } catch (err) {
    if (err instanceof AwsStorageCredentialValidationError) throw err;
    logger.error(
      { err, storageCredentialName: storageCredential.name, s3Url },
      'storageCredentials.validate failed',
    );
    throw new AwsStorageCredentialValidationError(
      `Databricks Storage Credential validation failed: ${(err as Error).message}`,
      isPermissionDenied(err) ? 403 : 409,
    );
  }
}

export function summarizeStorageCredentialValidationResults(
  results: Pick<ValidationResult, 'operation' | 'result' | 'message'>[],
): string | null {
  const failures = results.filter((item) => item.result === 'FAIL');
  if (failures.length === 0) return null;
  return failures
    .map((item) => {
      const operation = item.operation ?? 'UNKNOWN';
      const message = item.message?.trim() || 'Databricks returned FAIL without a message.';
      return `${operation}: ${message}`;
    })
    .join('; ');
}

function formatStorageCredentialValidationError(
  storageCredential: StorageCredentialSummary,
  s3Url: string,
  lastError: Error | null,
): string {
  const details = [
    `Storage Credential: ${storageCredential.name}`,
    `IAM role ARN: ${storageCredential.roleArn ?? 'unknown'}`,
    `Unity Catalog IAM ARN: ${storageCredential.unityCatalogIamArn ?? 'unknown'}`,
    `External ID: ${storageCredential.externalId ?? 'unknown'}`,
    `S3 URL: ${s3Url}`,
    `Validation result: ${lastError?.message ?? 'unknown validation failure'}`,
  ];
  return [
    'AWS Storage Credential validation failed. Unity Catalog could not validate access to the S3 location.',
    'Update the AWS IAM role trust policy with the current Unity Catalog IAM ARN and External ID, then retry.',
    details.join(' | '),
  ].join(' ');
}
