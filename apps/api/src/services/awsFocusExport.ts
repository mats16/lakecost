import {
  BCMDataExportsClient,
  CreateExportCommand,
  GetExportCommand,
  ListExportsCommand,
} from '@aws-sdk/client-bcm-data-exports';
import {
  CreateRoleCommand,
  GetPolicyCommand,
  GetRoleCommand,
  IAMClient,
  PutRolePolicyCommand as PutIamRolePolicyCommand,
  UpdateAssumeRolePolicyCommand,
} from '@aws-sdk/client-iam';
import {
  CreateBucketCommand,
  GetBucketPolicyCommand,
  HeadBucketCommand,
  PutBucketPolicyCommand,
  S3Client,
} from '@aws-sdk/client-s3';
import {
  externalLocationNameForBucket,
  roleNameFromArn,
  storageCredentialNameForBucket,
  ucNameSuffixFromBucket,
  type AwsFocusExportCreateBody,
  type AwsFocusExportCreateResponse,
  type Env,
  type ExternalLocationSummary,
  type ServiceCredentialSummary,
  type StorageCredentialSummary,
} from '@finlake/shared';
import { logger } from '../config/logger.js';
import { sleep } from '../utils/sleep.js';
import {
  createExternalLocation,
  listServicePrincipalExternalLocations,
  updateExternalLocation,
} from './externalLocations.js';
import {
  awsAccountIdFromRoleArn,
  createAwsStorageCredential,
  listServicePrincipalStorageCredentials,
  toStorageCredentialSummary,
  type StorageCredentialInfoLike,
} from './storageCredentials.js';
import { WorkspaceServiceError, isPermissionDenied } from './workspaceClientErrors.js';
import {
  listAccessibleServiceCredentials,
  ServiceCredentialServiceError,
} from './serviceCredentials.js';
import { requireAppWorkspaceClient } from './servicePrincipalIdentity.js';

const AWS_FOCUS_12_QUERY_STATEMENT =
  'SELECT AvailabilityZone, BilledCost, BillingAccountId, BillingAccountName, BillingAccountType, BillingCurrency, BillingPeriodEnd, BillingPeriodStart, CapacityReservationId, CapacityReservationStatus, ChargeCategory, ChargeClass, ChargeDescription, ChargeFrequency, ChargePeriodEnd, ChargePeriodStart, CommitmentDiscountCategory, CommitmentDiscountId, CommitmentDiscountName, CommitmentDiscountQuantity, CommitmentDiscountStatus, CommitmentDiscountType, CommitmentDiscountUnit, ConsumedQuantity, ConsumedUnit, ContractedCost, ContractedUnitPrice, EffectiveCost, InvoiceId, InvoiceIssuerName, ListCost, ListUnitPrice, PricingCategory, PricingCurrency, PricingCurrencyContractedUnitPrice, PricingCurrencyEffectiveCost, PricingCurrencyListUnitPrice, PricingQuantity, PricingUnit, ProviderName, PublisherName, RegionId, RegionName, ResourceId, ResourceName, ResourceType, ServiceCategory, ServiceName, ServiceSubcategory, SkuId, SkuMeter, SkuPriceDetails, SkuPriceId, SubAccountId, SubAccountName, SubAccountType, Tags, x_Discounts, x_Operation, x_ServiceCode FROM FOCUS_1_2_AWS';
const AWS_BCM_REGION = 'us-east-1';
const AWS_EXPORT_BUCKET_POLICY_SID = 'EnableAWSDataExportsToWriteToS3AndCheckPolicy';
const AWS_SERVICE_ROLE_NAME = 'FinLakeServiceRole';
const AWS_STORAGE_ROLE_NAME = 'FinLakeStorageRole';
const AWS_STORAGE_POLICY_NAME = 'FinLakeStorageAccess';
const AWS_STORAGE_ROLE_BOUNDARY_POLICY_NAME = 'FinLakeStorageRoleBoundary';

interface AwsTemporaryCredentials {
  accessKeyId: string;
  secretAccessKey: string;
  sessionToken: string;
}

type ResourceStatus = 'created' | 'skipped';

interface ResourceResult<T> {
  value: T;
  status: ResourceStatus;
}

interface AwsDataExportDestination {
  arn: string;
  bucket?: string;
  prefix?: string;
}

export class AwsFocusExportServiceError extends WorkspaceServiceError {}

export async function createAwsFocusExportResources(
  env: Env,
  input: AwsFocusExportCreateBody,
): Promise<AwsFocusExportCreateResponse> {
  const serviceCredential = await findFinLakeServiceCredential(env, input.awsAccountId);
  const credentials = await generateAwsTemporaryCredentials(env, serviceCredential.name);

  const s3Client = new S3Client({
    region: AWS_BCM_REGION,
    credentials,
  });

  let bucketStatus: ResourceStatus;
  if (input.createBucketIfMissing ?? true) {
    bucketStatus = await ensureAwsBucketExists({ client: s3Client, bucket: input.s3Bucket });
  } else {
    await headAwsBucket({ client: s3Client, bucket: input.s3Bucket });
    bucketStatus = 'skipped';
  }

  await upsertAwsDataExportBucketPolicy({
    client: s3Client,
    bucket: input.s3Bucket,
    accountId: input.awsAccountId,
  });

  const storageCredentialResult = await ensureFinLakeStorageCredential(
    env,
    input.awsAccountId,
    input.s3Bucket,
  );
  const storageCredential = storageCredentialResult.value;
  const storageRoleResult = await ensureAwsStorageRole({
    credentials,
    storageCredential,
    bucket: input.s3Bucket,
  });
  const storageRoleArn = storageRoleResult.value;
  const externalLocationResult = await ensureExternalLocation(env, {
    name: externalLocationNameForBucket(input.s3Bucket),
    url: `s3://${input.s3Bucket}`,
    storageCredentialName: storageCredential.name,
  });
  const externalLocation = externalLocationResult.value;
  const dataExportResult = await ensureAwsDataExport({
    credentials,
    bucket: input.s3Bucket,
    prefix: input.s3Prefix,
    exportName: input.exportName,
  });
  const exportArn = dataExportResult.value;

  return {
    exportArn,
    storageRoleArn,
    storageCredential,
    externalLocation,
    resourceStatuses: {
      bucket: bucketStatus,
      storageCredential: storageCredentialResult.status,
      storageRole: storageRoleResult.status,
      externalLocation: externalLocationResult.status,
      dataExport: dataExportResult.status,
    },
  };
}

async function findFinLakeServiceCredential(
  env: Env,
  awsAccountId: string,
): Promise<ServiceCredentialSummary> {
  let credentials: ServiceCredentialSummary[];
  try {
    credentials = await listAccessibleServiceCredentials(env);
  } catch (err) {
    if (err instanceof ServiceCredentialServiceError) throw err;
    throw new AwsFocusExportServiceError(
      `Failed to list service credentials: ${(err as Error).message}`,
      isPermissionDenied(err) ? 403 : 502,
    );
  }

  const credential = credentials.find(
    (cred) =>
      cred.awsAccountId === awsAccountId && roleNameFromArn(cred.roleArn) === AWS_SERVICE_ROLE_NAME,
  );
  if (!credential) {
    throw new AwsFocusExportServiceError(
      `FinLakeServiceRole service credential not found for AWS account ${awsAccountId}`,
      400,
    );
  }
  return credential;
}

async function generateAwsTemporaryCredentials(
  env: Env,
  credentialName: string,
): Promise<AwsTemporaryCredentials> {
  const wc = requireAppWorkspaceClient(env, AwsFocusExportServiceError);
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
    throw new AwsFocusExportServiceError(
      `Failed to generate temporary AWS credentials from ${credentialName}: ${(err as Error).message}`,
      isPermissionDenied(err) ? 403 : 502,
    );
  }
}

async function ensureFinLakeStorageCredential(
  env: Env,
  awsAccountId: string,
  bucket: string,
): Promise<ResourceResult<StorageCredentialSummary>> {
  const targetName = storageCredentialNameForBucket(bucket);
  const storageRoleArn = `arn:aws:iam::${awsAccountId}:role/${AWS_STORAGE_ROLE_NAME}`;
  const existing = await listServicePrincipalStorageCredentials(env);
  const byName = existing.find((cred) => cred.name === targetName);
  if (byName) {
    if (byName.roleArn === storageRoleArn) return { value: byName, status: 'skipped' };
    const updated = await updateAwsStorageCredentialRole(env, {
      name: targetName,
      roleArn: storageRoleArn,
    });
    return { value: updated, status: 'created' };
  }

  const created = await createAwsStorageCredential(env, {
    purpose: 'STORAGE',
    name: targetName,
    awsAccountId,
    roleName: AWS_STORAGE_ROLE_NAME,
    readOnly: true,
    comment: 'Registered by FinLake using auto-created FinLakeStorageRole',
  });
  return { value: created, status: 'created' };
}

async function updateAwsStorageCredentialRole(
  env: Env,
  input: { name: string; roleArn: string },
): Promise<StorageCredentialSummary> {
  const wc = requireAppWorkspaceClient(env, AwsFocusExportServiceError);
  try {
    const updated = await wc.credentials.updateCredential({
      name_arg: input.name,
      aws_iam_role: { role_arn: input.roleArn },
      read_only: true,
      comment: 'Registered by FinLake using auto-created FinLakeStorageRole',
      skip_validation: true,
    });
    return toStorageCredentialSummary(
      updated as StorageCredentialInfoLike,
      input.name,
      input.roleArn,
    );
  } catch (err) {
    logger.error({ err, credentialName: input.name }, 'wc.credentials.updateCredential failed');
    throw new AwsFocusExportServiceError(
      `Failed to update Storage Credential ${input.name}: ${(err as Error).message}`,
      isPermissionDenied(err) ? 403 : 502,
    );
  }
}

async function ensureAwsStorageRole({
  credentials,
  storageCredential,
  bucket,
}: {
  credentials: AwsTemporaryCredentials;
  storageCredential: StorageCredentialSummary;
  bucket: string;
}): Promise<ResourceResult<string>> {
  const roleArn = storageCredential.roleArn;
  const externalId = storageCredential.externalId;
  const unityCatalogIamArn = storageCredential.unityCatalogIamArn;
  if (!roleArn || !externalId || !unityCatalogIamArn) {
    throw new AwsFocusExportServiceError(
      `Storage Credential ${storageCredential.name} did not return the AWS role ARN, External ID, or Unity Catalog IAM ARN`,
      502,
    );
  }

  const roleName = roleNameFromArn(roleArn);
  if (roleName !== AWS_STORAGE_ROLE_NAME) {
    throw new AwsFocusExportServiceError(
      `Storage Credential ${storageCredential.name} must reference ${AWS_STORAGE_ROLE_NAME}, but it references ${roleArn}`,
      409,
    );
  }

  const client = new IAMClient({
    region: AWS_BCM_REGION,
    credentials,
  });
  const trustPolicy = JSON.stringify(
    storageRoleTrustPolicy(roleArn, unityCatalogIamArn, externalId, { includeSelfPrincipal: true }),
  );
  const initialTrustPolicy = JSON.stringify(
    storageRoleTrustPolicy(roleArn, unityCatalogIamArn, externalId, {
      includeSelfPrincipal: false,
    }),
  );
  const accountId = awsAccountIdFromRoleArn(roleArn);
  if (!accountId) {
    throw new AwsFocusExportServiceError(`Invalid Storage Credential role ARN: ${roleArn}`, 502);
  }
  const boundaryPolicyArn = `arn:aws:iam::${accountId}:policy/${AWS_STORAGE_ROLE_BOUNDARY_POLICY_NAME}`;

  let roleStatus: ResourceStatus;
  try {
    await client.send(new GetRoleCommand({ RoleName: AWS_STORAGE_ROLE_NAME }));
    await client.send(
      new UpdateAssumeRolePolicyCommand({
        RoleName: AWS_STORAGE_ROLE_NAME,
        PolicyDocument: trustPolicy,
      }),
    );
    roleStatus = 'skipped';
  } catch (err) {
    if (!isNoSuchEntityError(err)) {
      logger.error(
        { err, roleName: AWS_STORAGE_ROLE_NAME },
        'GetRole/UpdateAssumeRolePolicy failed',
      );
      throw new AwsFocusExportServiceError(
        `Failed to update AWS IAM role ${AWS_STORAGE_ROLE_NAME}: ${(err as Error).message}`,
        502,
      );
    }
    await requireStorageRoleBoundaryPolicy(client, boundaryPolicyArn);
    try {
      await client.send(
        new CreateRoleCommand({
          RoleName: AWS_STORAGE_ROLE_NAME,
          AssumeRolePolicyDocument: initialTrustPolicy,
          Description: 'FinLake storage role for Unity Catalog external locations',
          PermissionsBoundary: boundaryPolicyArn,
          Tags: [{ Key: 'ManagedBy', Value: 'FinLake' }],
        }),
      );
      await updateAssumeRolePolicyWithRetry(client, trustPolicy);
      roleStatus = 'created';
    } catch (createErr) {
      logger.error({ err: createErr, roleName: AWS_STORAGE_ROLE_NAME }, 'CreateRole failed');
      throw new AwsFocusExportServiceError(
        `Failed to create AWS IAM role ${AWS_STORAGE_ROLE_NAME}: ${(createErr as Error).message}`,
        502,
      );
    }
  }

  try {
    await client.send(
      new PutIamRolePolicyCommand({
        RoleName: AWS_STORAGE_ROLE_NAME,
        PolicyName: AWS_STORAGE_POLICY_NAME,
        PolicyDocument: JSON.stringify(storageRolePermissionPolicy(bucket)),
      }),
    );
  } catch (err) {
    logger.error({ err, roleName: AWS_STORAGE_ROLE_NAME }, 'PutRolePolicy failed');
    throw new AwsFocusExportServiceError(
      `Failed to attach AWS IAM role policy ${AWS_STORAGE_POLICY_NAME}: ${(err as Error).message}`,
      502,
    );
  }

  return { value: roleArn, status: roleStatus };
}

const IAM_RETRY_DELAYS_MS = [1_000, 2_000, 4_000, 8_000, 15_000] as const;

async function updateAssumeRolePolicyWithRetry(
  client: IAMClient,
  policyDocument: string,
): Promise<void> {
  for (let attempt = 0; attempt <= IAM_RETRY_DELAYS_MS.length; attempt += 1) {
    try {
      await client.send(
        new UpdateAssumeRolePolicyCommand({
          RoleName: AWS_STORAGE_ROLE_NAME,
          PolicyDocument: policyDocument,
        }),
      );
      return;
    } catch (err) {
      if (attempt >= IAM_RETRY_DELAYS_MS.length || !isInvalidPrincipalError(err)) throw err;
      await sleep(IAM_RETRY_DELAYS_MS[attempt]!);
    }
  }
}

async function requireStorageRoleBoundaryPolicy(
  client: IAMClient,
  policyArn: string,
): Promise<void> {
  try {
    await client.send(new GetPolicyCommand({ PolicyArn: policyArn }));
    return;
  } catch (err) {
    if (!isNoSuchEntityError(err)) {
      logger.error({ err, policyArn }, 'GetPolicy failed');
      throw new AwsFocusExportServiceError(
        `Failed to read AWS IAM policy ${AWS_STORAGE_ROLE_BOUNDARY_POLICY_NAME}: ${(err as Error).message}`,
        502,
      );
    }
    throw new AwsFocusExportServiceError(
      `AWS IAM policy ${AWS_STORAGE_ROLE_BOUNDARY_POLICY_NAME} must be created during FinLakeServiceRole setup before creating ${AWS_STORAGE_ROLE_NAME}`,
      400,
    );
  }
}

async function ensureExternalLocation(
  env: Env,
  input: { name: string; url: string; storageCredentialName: string },
): Promise<ResourceResult<ExternalLocationSummary>> {
  const existing = await listServicePrincipalExternalLocations(env);
  const byUrl = existing.find((loc) => normalizeS3Url(loc.url) === normalizeS3Url(input.url));
  if (byUrl) {
    if (
      byUrl.name === input.name &&
      byUrl.credentialName === input.storageCredentialName &&
      byUrl.readOnly === true
    ) {
      return { value: byUrl, status: 'skipped' };
    }
    const updated = await updateExternalLocation(env, {
      currentName: byUrl.name,
      newName: input.name,
      name: input.name,
      url: input.url,
      credentialName: input.storageCredentialName,
      readOnly: true,
      comment: 'Registered by FinLake using FinLakeStorageRole',
    });
    return { value: updated, status: 'created' };
  }

  const byName = existing.find((loc) => loc.name === input.name);
  if (byName) {
    if (
      normalizeS3Url(byName.url) === normalizeS3Url(input.url) &&
      byName.credentialName === input.storageCredentialName &&
      byName.readOnly === true
    ) {
      return { value: byName, status: 'skipped' };
    }
    const updated = await updateExternalLocation(env, {
      currentName: byName.name,
      name: input.name,
      url: input.url,
      credentialName: input.storageCredentialName,
      readOnly: true,
      comment: 'Registered by FinLake using FinLakeStorageRole',
    });
    return { value: updated, status: 'created' };
  }

  const created = await createExternalLocation(env, {
    name: input.name,
    url: input.url,
    credentialName: input.storageCredentialName,
    readOnly: true,
    comment: 'Registered by FinLake using FinLakeStorageRole',
  });
  return { value: created, status: 'created' };
}

async function ensureAwsDataExport({
  credentials,
  bucket,
  prefix,
  exportName,
}: {
  credentials: AwsTemporaryCredentials;
  bucket: string;
  prefix: string;
  exportName: string;
}): Promise<ResourceResult<string>> {
  const client = new BCMDataExportsClient({
    region: AWS_BCM_REGION,
    credentials,
  });

  try {
    const existing = await findAwsDataExport(client, exportName);
    if (existing) {
      assertAwsDataExportDestinationMatches(existing, { bucket, prefix, exportName });
      return { value: existing.arn, status: 'skipped' };
    }

    const res = await client.send(
      new CreateExportCommand({
        Export: {
          Name: exportName,
          Description: 'FOCUS 1.2 billing export',
          DataQuery: {
            QueryStatement: AWS_FOCUS_12_QUERY_STATEMENT,
            TableConfigurations: {
              FOCUS_1_2_AWS: {
                TIME_GRANULARITY: 'DAILY',
              },
            },
          },
          DestinationConfigurations: {
            S3Destination: {
              S3Bucket: bucket,
              S3Prefix: prefix,
              S3Region: AWS_BCM_REGION,
              S3OutputConfigurations: {
                Format: 'PARQUET',
                Compression: 'PARQUET',
                OutputType: 'CUSTOM',
                Overwrite: 'OVERWRITE_REPORT',
              },
            },
          },
          RefreshCadence: {
            Frequency: 'SYNCHRONOUS',
          },
        },
        ResourceTags: [{ Key: 'Environment', Value: 'production' }],
      }),
    );
    return { value: res.ExportArn ?? '', status: 'created' };
  } catch (err) {
    logger.error({ err, bucket, prefix, exportName }, 'CreateExport failed');
    throw new AwsFocusExportServiceError(
      `Failed to create AWS Data Export: ${(err as Error).message}`,
      502,
    );
  }
}

async function findAwsDataExport(
  client: BCMDataExportsClient,
  exportName: string,
): Promise<AwsDataExportDestination | null> {
  let nextToken: string | undefined;
  do {
    const res = await client.send(
      new ListExportsCommand({
        MaxResults: 100,
        NextToken: nextToken,
      }),
    );
    const match = res.Exports?.find((item) => item.ExportName === exportName);
    if (match?.ExportArn) {
      const detail = await client.send(new GetExportCommand({ ExportArn: match.ExportArn }));
      const destination = detail.Export?.DestinationConfigurations?.S3Destination;
      return {
        arn: match.ExportArn,
        bucket: destination?.S3Bucket,
        prefix: destination?.S3Prefix,
      };
    }
    nextToken = res.NextToken;
  } while (nextToken);

  return null;
}

function assertAwsDataExportDestinationMatches(
  existing: AwsDataExportDestination,
  expected: { bucket: string; prefix: string; exportName: string },
) {
  const existingPrefix = normalizeAwsS3Prefix(existing.prefix);
  const expectedPrefix = normalizeAwsS3Prefix(expected.prefix);
  if (existing.bucket === expected.bucket && existingPrefix === expectedPrefix) return;

  throw new AwsFocusExportServiceError(
    `Existing AWS Data Export ${expected.exportName} points to ${formatS3Uri(existing.bucket, existing.prefix)}, but FinLake requested ${formatS3Uri(expected.bucket, expected.prefix)}. Use a different export name or update the existing Data Export destination before retrying.`,
    409,
  );
}

function normalizeAwsS3Prefix(prefix: string | undefined): string {
  return (prefix ?? '')
    .trim()
    .replace(/^\/+|\/+$/g, '')
    .replace(/\/+/g, '/');
}

function formatS3Uri(bucket: string | undefined, prefix: string | undefined): string {
  if (!bucket) return 'unknown S3 destination';
  const normalizedPrefix = normalizeAwsS3Prefix(prefix);
  return normalizedPrefix ? `s3://${bucket}/${normalizedPrefix}` : `s3://${bucket}`;
}

async function headAwsBucket({ client, bucket }: { client: S3Client; bucket: string }) {
  try {
    await client.send(new HeadBucketCommand({ Bucket: bucket }));
  } catch (err) {
    logger.error({ err, bucket }, 'HeadBucket failed');
    throw new AwsFocusExportServiceError(
      `Failed to access S3 bucket ${bucket}: ${(err as Error).message}`,
      isMissingBucketError(err) ? 404 : 502,
    );
  }
}

async function ensureAwsBucketExists({
  client,
  bucket,
}: {
  client: S3Client;
  bucket: string;
}): Promise<ResourceStatus> {
  try {
    await client.send(new HeadBucketCommand({ Bucket: bucket }));
    return 'skipped';
  } catch (err) {
    if (!isMissingBucketError(err)) {
      logger.error({ err, bucket }, 'HeadBucket failed');
      throw new AwsFocusExportServiceError(
        `Failed to access S3 bucket ${bucket}: ${(err as Error).message}`,
        502,
      );
    }
  }

  try {
    await client.send(new CreateBucketCommand({ Bucket: bucket }));
    return 'created';
  } catch (err) {
    logger.error({ err, bucket }, 'CreateBucket failed');
    throw new AwsFocusExportServiceError(
      `Failed to create S3 bucket ${bucket}: ${(err as Error).message}`,
      502,
    );
  }
}

async function upsertAwsDataExportBucketPolicy({
  client,
  bucket,
  accountId,
}: {
  client: S3Client;
  bucket: string;
  accountId: string;
}) {
  let currentPolicy: string | undefined;
  try {
    const res = await client.send(new GetBucketPolicyCommand({ Bucket: bucket }));
    currentPolicy = res.Policy;
  } catch (err) {
    if (!isMissingBucketPolicyError(err)) {
      logger.error({ err, bucket }, 'GetBucketPolicy failed');
      throw new AwsFocusExportServiceError(
        `Failed to read S3 bucket policy for ${bucket}: ${(err as Error).message}`,
        502,
      );
    }
  }

  try {
    await client.send(
      new PutBucketPolicyCommand({
        Bucket: bucket,
        Policy: mergeAwsDataExportBucketPolicy(currentPolicy, bucket, accountId),
      }),
    );
  } catch (err) {
    logger.error({ err, bucket }, 'PutBucketPolicy failed');
    throw new AwsFocusExportServiceError(
      `Failed to update S3 bucket policy for ${bucket}: ${(err as Error).message}`,
      502,
    );
  }
}

function awsDataExportBucketPolicyStatement(bucket: string, accountId: string) {
  return {
    Sid: AWS_EXPORT_BUCKET_POLICY_SID,
    Effect: 'Allow',
    Principal: {
      Service: ['billingreports.amazonaws.com', 'bcm-data-exports.amazonaws.com'],
    },
    Action: ['s3:PutObject', 's3:GetBucketPolicy'],
    Resource: [`arn:aws:s3:::${bucket}`, `arn:aws:s3:::${bucket}/*`],
    Condition: {
      StringLike: {
        'aws:SourceArn': [
          `arn:aws:cur:${AWS_BCM_REGION}:${accountId}:definition/*`,
          `arn:aws:bcm-data-exports:${AWS_BCM_REGION}:${accountId}:export/*`,
        ],
        'aws:SourceAccount': accountId,
      },
    },
  };
}

export function mergeAwsDataExportBucketPolicy(
  policyText: string | undefined,
  bucket: string,
  accountId: string,
): string {
  const nextStatement = awsDataExportBucketPolicyStatement(bucket, accountId);
  const existingPolicy = policyText ? JSON.parse(policyText) : {};
  if (!isRecord(existingPolicy)) throw new Error('S3 bucket policy must be a JSON object.');

  const rawStatements = existingPolicy.Statement;
  const statements = Array.isArray(rawStatements)
    ? rawStatements
    : rawStatements
      ? [rawStatements]
      : [];

  return JSON.stringify(
    {
      ...existingPolicy,
      Version: typeof existingPolicy.Version === 'string' ? existingPolicy.Version : '2012-10-17',
      Statement: [
        ...statements.filter(
          (statement) => !(isRecord(statement) && statement.Sid === AWS_EXPORT_BUCKET_POLICY_SID),
        ),
        nextStatement,
      ],
    },
    null,
    2,
  );
}

function storageRoleTrustPolicy(
  roleArn: string,
  unityCatalogIamArn: string,
  externalId: string,
  options: { includeSelfPrincipal: boolean },
) {
  return {
    Version: '2012-10-17',
    Statement: [
      {
        Effect: 'Allow',
        Principal: {
          AWS: options.includeSelfPrincipal ? [unityCatalogIamArn, roleArn] : unityCatalogIamArn,
        },
        Action: 'sts:AssumeRole',
        Condition: {
          StringEquals: {
            'sts:ExternalId': externalId,
          },
        },
      },
    ],
  };
}

function storageRolePermissionPolicy(bucket: string) {
  return {
    Version: '2012-10-17',
    Statement: [
      {
        Sid: 'ListStorageBucket',
        Effect: 'Allow',
        Action: ['s3:GetBucketLocation', 's3:ListBucket'],
        Resource: `arn:aws:s3:::${bucket}`,
      },
      {
        Sid: 'AccessStorageObjects',
        Effect: 'Allow',
        Action: [
          's3:GetObject',
          's3:PutObject',
          's3:DeleteObject',
          's3:AbortMultipartUpload',
          's3:ListMultipartUploadParts',
        ],
        Resource: `arn:aws:s3:::${bucket}/*`,
      },
    ],
  };
}

function normalizeS3Url(url: string | null | undefined): string | null {
  const trimmed = url?.trim().replace(/\/+$/, '');
  return trimmed ? trimmed.toLowerCase() : null;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function isMissingBucketPolicyError(err: unknown): boolean {
  if (!isRecord(err)) return false;
  return (
    err.name === 'NoSuchBucketPolicy' ||
    err.Code === 'NoSuchBucketPolicy' ||
    (isRecord(err.$metadata) && err.$metadata.httpStatusCode === 404)
  );
}

function isInvalidPrincipalError(err: unknown): boolean {
  if (!isRecord(err)) return false;
  const message = typeof err.message === 'string' ? err.message : '';
  return message.includes('Invalid principal in policy');
}

function isMissingBucketError(err: unknown): boolean {
  if (!isRecord(err)) return false;
  return (
    err.name === 'NotFound' ||
    err.name === 'NoSuchBucket' ||
    err.Code === 'NoSuchBucket' ||
    (isRecord(err.$metadata) && err.$metadata.httpStatusCode === 404)
  );
}

function isNoSuchEntityError(err: unknown): boolean {
  if (!isRecord(err)) return false;
  return (
    err.name === 'NoSuchEntity' ||
    err.Code === 'NoSuchEntity' ||
    err.Code === 'NoSuchEntityException' ||
    (isRecord(err.$metadata) && err.$metadata.httpStatusCode === 404)
  );
}
