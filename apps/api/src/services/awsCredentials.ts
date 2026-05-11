import type { Env } from '@finlake/shared';
import { logger } from '../config/logger.js';
import { requireAppWorkspaceClient } from './servicePrincipalIdentity.js';
import { WorkspaceServiceError, isPermissionDenied } from './workspaceClientErrors.js';

export const AWS_BCM_REGION = 'us-east-1';
export const AWS_SERVICE_ROLE_NAME = 'FinLakeServiceRole';

export interface AwsTemporaryCredentials {
  accessKeyId: string;
  secretAccessKey: string;
  sessionToken: string;
}

export async function generateAwsTemporaryCredentials(
  env: Env,
  credentialName: string,
  ErrorClass: new (
    message: string,
    statusCode: number,
  ) => WorkspaceServiceError = WorkspaceServiceError,
): Promise<AwsTemporaryCredentials> {
  const wc = requireAppWorkspaceClient(env, ErrorClass);
  try {
    const res = await wc.credentials.generateTemporaryServiceCredential({
      credential_name: credentialName,
    });
    const aws = res.aws_temp_credentials;
    if (!aws?.access_key_id || !aws.secret_access_key || !aws.session_token) {
      throw new Error('Databricks did not return AWS temporary credentials');
    }
    return {
      accessKeyId: aws.access_key_id,
      secretAccessKey: aws.secret_access_key,
      sessionToken: aws.session_token,
    };
  } catch (err) {
    logger.error({ err, credentialName }, 'generateTemporaryServiceCredential failed');
    throw new ErrorClass(
      `Failed to generate temporary AWS credentials from ${credentialName}: ${(err as Error).message}`,
      isPermissionDenied(err) ? 403 : 502,
    );
  }
}
