import { useEffect, useMemo, useState } from 'react';
import { BCMDataExportsClient, CreateExportCommand } from '@aws-sdk/client-bcm-data-exports';
import { GetBucketPolicyCommand, PutBucketPolicyCommand, S3Client } from '@aws-sdk/client-s3';
import {
  useAppSettings,
  useCreateAwsFocusExport,
  useCreateExternalLocation,
  useExternalLocations,
  useCreateDataSource,
  useCreateStorageCredential,
  useMe,
  useRunDataSourceJob,
  useServiceCredentials,
  useSetupDataSource,
  useStorageCredentials,
  useUpdateDataSource,
} from '../../api/hooks';
import {
  CATALOG_SETTING_KEY,
  FOCUS_REFRESH_CRON_DEFAULT,
  FOCUS_REFRESH_TIMEZONE_DEFAULT,
  externalLocationNameForBucket,
  isValidS3BucketName,
  medallionSchemaNamesFromSettings,
  normalizeS3Prefix,
  roleNameFromArn,
  s3BucketFromUrl,
  s3ExportPath,
  storageCredentialNameForBucket,
  tableLeafName,
  ucNameSuffixFromBucket,
  unquotedFqn,
  type DataSource,
  type DataSourceCreateBody,
  type DataSourceSetupResult,
  type ExternalLocationSummary,
  type StorageCredentialSummary,
} from '@lakecost/shared';
import { messageOf } from './utils';

const AWS_FOCUS_12_QUERY_STATEMENT =
  'SELECT AvailabilityZone, BilledCost, BillingAccountId, BillingAccountName, BillingAccountType, BillingCurrency, BillingPeriodEnd, BillingPeriodStart, CapacityReservationId, CapacityReservationStatus, ChargeCategory, ChargeClass, ChargeDescription, ChargeFrequency, ChargePeriodEnd, ChargePeriodStart, CommitmentDiscountCategory, CommitmentDiscountId, CommitmentDiscountName, CommitmentDiscountQuantity, CommitmentDiscountStatus, CommitmentDiscountType, CommitmentDiscountUnit, ConsumedQuantity, ConsumedUnit, ContractedCost, ContractedUnitPrice, EffectiveCost, InvoiceId, InvoiceIssuerName, ListCost, ListUnitPrice, PricingCategory, PricingCurrency, PricingCurrencyContractedUnitPrice, PricingCurrencyEffectiveCost, PricingCurrencyListUnitPrice, PricingQuantity, PricingUnit, ProviderName, PublisherName, RegionId, RegionName, ResourceId, ResourceName, ResourceType, ServiceCategory, ServiceName, ServiceSubcategory, SkuId, SkuMeter, SkuPriceDetails, SkuPriceId, SubAccountId, SubAccountName, SubAccountType, Tags, x_Discounts, x_Operation, x_ServiceCode FROM FOCUS_1_2_AWS';
const AWS_BCM_REGION = 'us-east-1';
const AWS_EXPORT_NAME_DEFAULT = 'finlake-focus-1-2';
const AWS_EXPORT_PREFIX_DEFAULT = 'bcm-data-export';
const AWS_EXPORT_BUCKET_POLICY_SID = 'EnableAWSDataExportsToWriteToS3AndCheckPolicy';
const AWS_SERVICE_ROLE_NAME = 'FinLakeServiceRole';
const AWS_STORAGE_ROLE_NAME = 'FinLakeStorageRole';
const AWS_BCM_DATA_EXPORTS_URL =
  'https://us-east-1.console.aws.amazon.com/costmanagement/home#/bcm-data-exports';
const S3_PREFIX_PREVIEW_PLACEHOLDER = '{prefix}';
const EXPORT_NAME_PREVIEW_PLACEHOLDER = '{export_name}';

type AwsSetupMode = 'existing' | 'create';
type CreateResourceStepId =
  | 'bucket'
  | 'storageCredential'
  | 'storageRole'
  | 'externalLocation'
  | 'dataExport'
  | 'lakeflowJob';
type CreateResourceStepStatus = 'idle' | 'pending' | 'done' | 'skipped' | 'error';

export interface CreateResourceStep {
  id: CreateResourceStepId;
  status: CreateResourceStepStatus;
  detail: string | null;
  href: string | null;
}

const CREATE_RESOURCE_STEP_IDS: CreateResourceStepId[] = [
  'bucket',
  'storageCredential',
  'storageRole',
  'externalLocation',
  'dataExport',
  'lakeflowJob',
];

export type AwsFocusDraft = Pick<
  DataSourceCreateBody,
  'templateId' | 'name' | 'providerName' | 'tableName'
>;

function configString(config: Record<string, unknown>, key: string): string {
  const value = config[key];
  return typeof value === 'string' ? value : '';
}

function s3PrefixFromUrl(url: string): string {
  const match = /^s3:\/\/[^/]+\/?(.*)$/i.exec(url.trim());
  return normalizeS3Prefix(match?.[1] ?? '');
}

function joinS3Prefixes(basePrefix: string, suffixPrefix: string): string {
  return [normalizeS3Prefix(basePrefix), normalizeS3Prefix(suffixPrefix)].filter(Boolean).join('/');
}

function stripBasePrefix(prefix: string, basePrefix: string): string {
  const normalizedPrefix = normalizeS3Prefix(prefix);
  const normalizedBase = normalizeS3Prefix(basePrefix);
  if (!normalizedBase) return normalizedPrefix;
  if (normalizedPrefix === normalizedBase) return '';
  const baseWithSlash = `${normalizedBase}/`;
  return normalizedPrefix.startsWith(baseWithSlash)
    ? normalizedPrefix.slice(baseWithSlash.length)
    : normalizedPrefix;
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

function defaultAwsBucketName(accountId: string): string {
  return `finlake-${accountId}`;
}

function defaultAwsExternalLocationUrl(bucket: string): string | null {
  const trimmed = bucket.trim();
  return trimmed ? `s3://${trimmed}` : null;
}

function initialCreateResourceSteps(): CreateResourceStep[] {
  return CREATE_RESOURCE_STEP_IDS.map((id) => ({ id, status: 'idle', detail: null, href: null }));
}

function updateCreateResourceSteps(
  steps: CreateResourceStep[],
  updates: Partial<Record<CreateResourceStepId, Partial<Omit<CreateResourceStep, 'id'>>>>,
): CreateResourceStep[] {
  return steps.map((step) => ({ ...step, ...(updates[step.id] ?? {}) }));
}

function failedCreateResourceStepFromMessage(
  message: string | null,
  steps: CreateResourceStep[],
): CreateResourceStepId | null {
  const lower = message?.toLowerCase() ?? '';
  if (
    lower.includes('data export') ||
    lower.includes('bcm-data-exports') ||
    lower.includes('cur:')
  ) {
    return 'dataExport';
  }
  if (
    lower.includes('storage credential validation') ||
    lower.includes('trust policy') ||
    lower.includes('external id') ||
    lower.includes('unity catalog iam arn') ||
    lower.includes('assumerole') ||
    lower.includes('assume role')
  ) {
    return 'storageRole';
  }
  if (
    lower.includes('data source') ||
    lower.includes('job') ||
    lower.includes('pipeline') ||
    lower.includes('invalid input')
  ) {
    return 'lakeflowJob';
  }
  if (lower.includes('external location')) return 'externalLocation';
  if (lower.includes('storage credential')) return 'storageCredential';
  if (lower.includes('iam role') || lower.includes('finlakestoragerole')) return 'storageRole';
  if (lower.includes('bucket') || lower.includes('s3')) return 'bucket';
  return (
    steps.find((step) => step.status === 'pending')?.id ??
    steps.find((step) => step.status === 'idle')?.id ??
    'dataExport'
  );
}

function apiResourceStatusToStepStatus(status: 'created' | 'skipped'): CreateResourceStepStatus {
  return status === 'skipped' ? 'skipped' : 'done';
}

function awsS3BucketUrl(bucket: string): string {
  return `https://s3.console.aws.amazon.com/s3/buckets/${encodeURIComponent(bucket)}?region=${AWS_BCM_REGION}&bucketType=general&tab=objects`;
}

function awsIamRoleUrl(roleArn: string): string | null {
  const roleName = roleNameFromArn(roleArn);
  return roleName
    ? `https://console.aws.amazon.com/iam/home#/roles/details/${encodeURIComponent(roleName)}`
    : null;
}

function databricksStorageCredentialUrl(workspaceUrl: string | null, name: string): string | null {
  return workspaceUrl ? `${workspaceUrl}/explore/credentials/${encodeURIComponent(name)}` : null;
}

function databricksExternalLocationUrl(workspaceUrl: string | null, name: string): string | null {
  return workspaceUrl ? `${workspaceUrl}/explore/locations/${encodeURIComponent(name)}` : null;
}

function databricksJobUrl(workspaceUrl: string | null, jobId: number): string | null {
  return workspaceUrl ? `${workspaceUrl}/jobs/${jobId}` : null;
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

function mergeAwsDataExportBucketPolicy(
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
    if (!isMissingBucketPolicyError(err)) throw err;
  }

  await client.send(
    new PutBucketPolicyCommand({
      Bucket: bucket,
      Policy: mergeAwsDataExportBucketPolicy(currentPolicy, bucket, accountId),
    }),
  );
}

interface UseAwsFocusFormOptions {
  draft?: AwsFocusDraft;
  onCreated?: (row: DataSource) => void;
}

export function useAwsFocusForm(row: DataSource | null, options: UseAwsFocusFormOptions = {}) {
  const storageCredentials = useStorageCredentials();
  const serviceCredentials = useServiceCredentials();
  const locations = useExternalLocations();
  const createLocation = useCreateExternalLocation();
  const createStorageCredential = useCreateStorageCredential();
  const createAwsFocusExport = useCreateAwsFocusExport();
  const createDs = useCreateDataSource();
  const updateDs = useUpdateDataSource();
  const me = useMe();
  const settings = useAppSettings();
  const setupDs = useSetupDataSource();
  const runJob = useRunDataSourceJob();

  // --- Remote (server) values ---
  const remoteConfig = row?.config ?? {};
  const remoteAwsAccountId = row?.billingAccountId ?? configString(remoteConfig, 'awsAccountId');
  const remoteExternalLocationName = configString(remoteConfig, 'externalLocationName');
  const remoteExternalLocationUrl = configString(remoteConfig, 'externalLocationUrl');
  const remoteExportName = configString(remoteConfig, 'exportName');
  const remoteS3Bucket = configString(remoteConfig, 's3Bucket');
  const remoteS3Prefix = configString(remoteConfig, 's3Prefix');
  const remoteCatalog = settings.data?.settings[CATALOG_SETTING_KEY] ?? '';
  const silverSchema = medallionSchemaNamesFromSettings(settings.data?.settings ?? {}).silver;
  const remoteCron = configString(remoteConfig, 'cronExpression') || FOCUS_REFRESH_CRON_DEFAULT;
  const remoteTz = configString(remoteConfig, 'timezoneId') || FOCUS_REFRESH_TIMEZONE_DEFAULT;

  // --- Local form state ---
  const [awsAccountId, setAwsAccountId] = useState(remoteAwsAccountId);
  const [externalLocationName, setExternalLocationName] = useState(remoteExternalLocationName);
  const [accessKeyId, setAccessKeyId] = useState('');
  const [secretAccessKey, setSecretAccessKey] = useState('');
  const [sessionToken, setSessionToken] = useState('');
  const [exportName, setExportName] = useState(remoteExportName || AWS_EXPORT_NAME_DEFAULT);
  const [createBucketName, setCreateBucketName] = useState(
    remoteS3Bucket || (remoteAwsAccountId ? defaultAwsBucketName(remoteAwsAccountId) : ''),
  );
  const [createBucketIfMissing, setCreateBucketIfMissing] = useState(true);
  const [s3Prefix, setS3Prefix] = useState(normalizeS3Prefix(remoteS3Prefix));
  const [tableName, setTableName] = useState(
    tableLeafName(row?.tableName ?? options.draft?.tableName ?? 'aws_billing'),
  );
  const [cron, setCron] = useState(remoteCron);
  const [timezone, setTimezone] = useState(remoteTz);
  const [result, setResult] = useState<DataSourceSetupResult | null>(null);
  const [exportArn, setExportArn] = useState(configString(remoteConfig, 'exportArn'));
  const [exportError, setExportError] = useState<string | null>(null);
  const [creatingExport, setCreatingExport] = useState(false);
  const [exportModalOpen, setExportModalOpen] = useState(false);
  const [savedAt, setSavedAt] = useState<number | null>(null);
  const [setupMode, setSetupMode] = useState<AwsSetupMode>('create');
  const [createResourceSteps, setCreateResourceSteps] = useState<CreateResourceStep[]>(
    initialCreateResourceSteps,
  );
  const [createProgressModalOpen, setCreateProgressModalOpen] = useState(false);
  const [createdRowAfterProgress, setCreatedRowAfterProgress] = useState<DataSource | null>(null);

  // --- Sync local state when server data changes ---
  useEffect(() => setAwsAccountId(remoteAwsAccountId), [remoteAwsAccountId]);
  useEffect(
    () => setExternalLocationName(remoteExternalLocationName),
    [remoteExternalLocationName],
  );
  useEffect(() => setExportName(remoteExportName || AWS_EXPORT_NAME_DEFAULT), [remoteExportName]);
  useEffect(
    () =>
      setCreateBucketName(
        remoteS3Bucket || (remoteAwsAccountId ? defaultAwsBucketName(remoteAwsAccountId) : ''),
      ),
    [remoteAwsAccountId, remoteS3Bucket],
  );
  useEffect(
    () => setTableName(tableLeafName(row?.tableName ?? options.draft?.tableName ?? 'aws_billing')),
    [options.draft?.tableName, row?.tableName],
  );
  useEffect(() => setCron(remoteCron), [remoteCron]);
  useEffect(() => setTimezone(remoteTz), [remoteTz]);

  // --- Derived credential / location lists ---
  const awsCredentials = useMemo(
    () =>
      (storageCredentials.data?.storageCredentials ?? []).filter(
        (cred): cred is StorageCredentialSummary & { awsAccountId: string } =>
          typeof cred.awsAccountId === 'string',
      ),
    [storageCredentials.data],
  );
  const existingAccountOptions = useMemo(
    () => Array.from(new Set(awsCredentials.map((cred) => cred.awsAccountId))).sort(),
    [awsCredentials],
  );
  const serviceAccountOptions = useMemo(
    () =>
      Array.from(
        new Set(
          (serviceCredentials.data?.serviceCredentials ?? [])
            .filter((cred) => roleNameFromArn(cred.roleArn) === AWS_SERVICE_ROLE_NAME)
            .map((cred) => cred.awsAccountId)
            .filter((accountId): accountId is string => typeof accountId === 'string'),
        ),
      ).sort(),
    [serviceCredentials.data],
  );
  const serviceStorageCredentials = useMemo(
    () =>
      (serviceCredentials.data?.storageCredentials ?? []).filter(
        (cred): cred is StorageCredentialSummary & { awsAccountId: string } =>
          typeof cred.awsAccountId === 'string' &&
          roleNameFromArn(cred.roleArn) === AWS_STORAGE_ROLE_NAME,
      ),
    [serviceCredentials.data],
  );
  const accountOptions = setupMode === 'create' ? serviceAccountOptions : existingAccountOptions;
  const credentialNamesForAccount = useMemo(() => {
    const names = new Set<string>();
    for (const cred of awsCredentials) {
      if (cred.awsAccountId === awsAccountId) names.add(cred.name);
    }
    return names;
  }, [awsCredentials, awsAccountId]);
  const allLocations = locations.data?.externalLocations ?? [];
  const linkedLocations = useMemo(
    () =>
      allLocations.filter(
        (loc) =>
          loc.url?.toLowerCase().startsWith('s3://') &&
          loc.credentialName &&
          credentialNamesForAccount.has(loc.credentialName),
      ),
    [allLocations, credentialNamesForAccount],
  );
  const createModeStorageCredential =
    serviceStorageCredentials.find((cred) => cred.awsAccountId === awsAccountId) ?? null;
  const normalizedCreateBucketName = createBucketName.trim();
  const createBucketNameValid =
    normalizedCreateBucketName.length > 0 && isValidS3BucketName(normalizedCreateBucketName);
  const createModeExternalLocationUrl = createBucketNameValid
    ? defaultAwsExternalLocationUrl(normalizedCreateBucketName)
    : null;
  const createModeExistingLocation =
    createModeExternalLocationUrl && awsAccountId
      ? (allLocations.find((loc) => loc.url === createModeExternalLocationUrl) ?? null)
      : null;
  const createModeExternalLocationName =
    createModeExistingLocation?.name ??
    (normalizedCreateBucketName ? externalLocationNameForBucket(normalizedCreateBucketName) : '');
  const selectedLocation: ExternalLocationSummary | null =
    setupMode === 'create' && awsAccountId
      ? {
          name: externalLocationName || createModeExternalLocationName,
          url: createModeExternalLocationUrl,
          credentialName: createModeStorageCredential?.name ?? null,
          readOnly: true,
          comment: null,
        }
      : (allLocations.find((loc) => loc.name === externalLocationName) ??
        (externalLocationName
          ? ({ name: externalLocationName } as ExternalLocationSummary)
          : null));
  const locationOptions = useMemo(() => {
    if (!selectedLocation || linkedLocations.some((loc) => loc.name === selectedLocation.name)) {
      return linkedLocations;
    }
    return [selectedLocation, ...linkedLocations];
  }, [linkedLocations, selectedLocation]);
  const selectedS3Url = selectedLocation?.url ?? (remoteExternalLocationUrl || null);
  const selectedS3Bucket = selectedS3Url ? s3BucketFromUrl(selectedS3Url) : null;
  const selectedS3BasePrefix = selectedS3Url ? s3PrefixFromUrl(selectedS3Url) : '';
  const normalizedS3Prefix = normalizeS3Prefix(s3Prefix);
  const effectiveS3Prefix = joinS3Prefixes(selectedS3BasePrefix, normalizedS3Prefix);
  const registered =
    Boolean(row) &&
    Boolean(remoteAwsAccountId) &&
    Boolean(remoteExternalLocationName) &&
    Boolean(remoteExportName) &&
    Boolean(remoteS3Prefix);

  useEffect(() => {
    if (registered) return;
    if (awsAccountId && accountOptions.includes(awsAccountId)) return;
    const nextAccountId = accountOptions[0] ?? '';
    setAwsAccountId(nextAccountId);
    if (setupMode === 'create') {
      setCreateBucketName(nextAccountId ? defaultAwsBucketName(nextAccountId) : '');
    }
    setExternalLocationName('');
  }, [accountOptions, awsAccountId, registered, setupMode]);

  useEffect(() => {
    if (registered || setupMode !== 'create' || !awsAccountId || !createModeExternalLocationName) {
      return;
    }
    setExternalLocationName(createModeExternalLocationName);
  }, [awsAccountId, createModeExternalLocationName, registered, setupMode]);

  useEffect(() => {
    if (!registered && setupMode === 'create') return;
    setS3Prefix(stripBasePrefix(remoteS3Prefix, selectedS3BasePrefix));
  }, [registered, remoteS3Prefix, selectedS3BasePrefix, setupMode]);

  useEffect(() => {
    if (registered || setupMode !== 'create') return;
    if (!exportName.trim()) setExportName(AWS_EXPORT_NAME_DEFAULT);
    if (!s3Prefix.trim()) setS3Prefix(AWS_EXPORT_PREFIX_DEFAULT);
  }, [exportName, registered, s3Prefix, setupMode]);

  const exportDestinationPreview = selectedS3Bucket
    ? s3ExportPath(
        selectedS3Bucket,
        effectiveS3Prefix || S3_PREFIX_PREVIEW_PLACEHOLDER,
        exportName || EXPORT_NAME_PREVIEW_PLACEHOLDER,
      )
    : null;

  // --- Flags ---
  const dirty =
    !row ||
    awsAccountId !== remoteAwsAccountId ||
    externalLocationName !== remoteExternalLocationName ||
    (setupMode === 'create' && normalizedCreateBucketName !== remoteS3Bucket) ||
    exportName !== remoteExportName ||
    effectiveS3Prefix !== remoteS3Prefix;
  const loadingInputs =
    storageCredentials.isLoading ||
    locations.isLoading ||
    (setupMode === 'create' && serviceCredentials.isLoading);
  const savePending = createDs.isPending || updateDs.isPending || setupDs.isPending;
  const saveDisabled =
    registered ||
    savePending ||
    !awsAccountId ||
    !externalLocationName ||
    !exportName ||
    !effectiveS3Prefix ||
    !selectedS3Bucket ||
    !dirty;
  const jobId = result?.jobId ?? row?.jobId ?? null;
  const pipelineId = result?.pipelineId ?? row?.pipelineId ?? null;
  const workspaceUrl = me.data?.workspaceUrl ?? null;
  const fqn = remoteCatalog
    ? unquotedFqn(remoteCatalog, silverSchema, tableName)
    : `${silverSchema}.${tableName}`;
  const hadScheduleBeforeSetup = row?.jobId !== null && row?.jobId !== undefined;
  const setupDisabled =
    !row ||
    setupDs.isPending ||
    !remoteCatalog ||
    !selectedS3Url ||
    !tableName ||
    !cron ||
    !timezone;
  const createExportDisabled =
    creatingExport ||
    createAwsFocusExport.isPending ||
    savePending ||
    registered ||
    !awsAccountId ||
    (setupMode === 'create' && !createBucketNameValid) ||
    !selectedS3Bucket ||
    (setupMode === 'existing' && (!accessKeyId || !secretAccessKey)) ||
    !exportName ||
    !effectiveS3Prefix;
  const errorMessage =
    messageOf(storageCredentials.error) ??
    messageOf(serviceCredentials.error) ??
    messageOf(locations.error) ??
    messageOf(createLocation.error) ??
    messageOf(createStorageCredential.error) ??
    messageOf(createAwsFocusExport.error) ??
    messageOf(createDs.error) ??
    messageOf(updateDs.error) ??
    messageOf(setupDs.error);

  // --- Actions ---
  const onSetupModeChange = (value: AwsSetupMode) => {
    updateDs.reset();
    createLocation.reset();
    createStorageCredential.reset();
    createAwsFocusExport.reset();
    setSavedAt(null);
    setCreateResourceSteps(initialCreateResourceSteps());
    setCreateProgressModalOpen(false);
    setSetupMode(value);
    if (!registered) {
      setAwsAccountId('');
      setExternalLocationName('');
      setCreateBucketName('');
      if (!exportName.trim()) setExportName(AWS_EXPORT_NAME_DEFAULT);
    }
  };

  const onAccountChange = (value: string) => {
    updateDs.reset();
    createStorageCredential.reset();
    createAwsFocusExport.reset();
    setSavedAt(null);
    setCreateResourceSteps(initialCreateResourceSteps());
    setCreateProgressModalOpen(false);
    setAwsAccountId(value);
    if (setupMode === 'create') {
      const nextBucket = defaultAwsBucketName(value);
      setCreateBucketName(nextBucket);
      setExternalLocationName(externalLocationNameForBucket(nextBucket));
      if (!exportName.trim()) setExportName(AWS_EXPORT_NAME_DEFAULT);
      if (!s3Prefix.trim()) setS3Prefix(AWS_EXPORT_PREFIX_DEFAULT);
    } else {
      setExternalLocationName('');
      if (!exportName.trim()) setExportName(AWS_EXPORT_NAME_DEFAULT);
    }
  };

  const onCreateBucketNameChange = (value: string) => {
    updateDs.reset();
    createLocation.reset();
    createStorageCredential.reset();
    createAwsFocusExport.reset();
    setSavedAt(null);
    setCreateResourceSteps(initialCreateResourceSteps());
    setCreateProgressModalOpen(false);
    setCreateBucketName(value);
    const trimmed = value.trim();
    setExternalLocationName(awsAccountId && trimmed ? externalLocationNameForBucket(trimmed) : '');
  };

  const onLocationChange = (value: string) => {
    updateDs.reset();
    setSavedAt(null);
    setCreateResourceSteps(initialCreateResourceSteps());
    setCreateProgressModalOpen(false);
    setExternalLocationName(value);
    if (!exportName.trim()) setExportName(AWS_EXPORT_NAME_DEFAULT);
  };

  const buildConfig = (
    overrides?: Record<string, unknown>,
    selection?: {
      externalLocationName: string;
      externalLocationUrl: string | null;
      storageCredentialName: string | null;
      s3Bucket: string | null;
      s3Prefix: string;
    },
  ) => {
    const selected = allLocations.find((loc) => loc.name === externalLocationName);
    return {
      ...remoteConfig,
      awsAccountId,
      externalLocationName: selection?.externalLocationName ?? externalLocationName,
      externalLocationUrl: selection?.externalLocationUrl ?? selected?.url ?? selectedS3Url,
      storageCredentialName:
        selection?.storageCredentialName ??
        selected?.credentialName ??
        selectedLocation?.credentialName ??
        null,
      s3Bucket: selection?.s3Bucket ?? selectedS3Bucket,
      exportName,
      s3Prefix: selection?.s3Prefix ?? effectiveS3Prefix,
      s3Region: AWS_BCM_REGION,
      ...overrides,
    };
  };

  const persistConfig = async (
    config: Record<string, unknown>,
    optionsOverride?: { deferCreatedCallback?: boolean; skipCreatedCallback?: boolean },
  ): Promise<DataSource | null> => {
    if (row) {
      const updated = await updateDs.mutateAsync({
        id: row.id,
        body: { billingAccountId: awsAccountId, config },
      });
      setSavedAt(Date.now());
      return updated;
    } else if (options.draft) {
      const created = await createDs.mutateAsync({
        ...options.draft,
        billingAccountId: awsAccountId,
        enabled: false,
        config,
      });
      if (optionsOverride?.deferCreatedCallback) {
        setCreatedRowAfterProgress(created);
      } else if (optionsOverride?.skipCreatedCallback) {
        // Caller will notify the parent after finishing follow-up work such as Job setup.
      } else {
        options.onCreated?.(created);
      }
      setSavedAt(Date.now());
      return created;
    }
    setSavedAt(Date.now());
    return null;
  };

  const onSave = async () => {
    const savedSource = await persistConfig(buildConfig(exportArn ? { exportArn } : {}), {
      skipCreatedCallback: true,
    });
    const dataSourceId = savedSource?.id ?? row?.id;
    if (dataSourceId) await setupDataSourceJob(dataSourceId);
    if (savedSource && !row) options.onCreated?.(savedSource);
  };

  const saveExportArn = async (
    nextExportArn: string,
    selection?: Parameters<typeof buildConfig>[1],
    optionsOverride?: { deferCreatedCallback?: boolean; skipCreatedCallback?: boolean },
  ): Promise<DataSource | null> => {
    return persistConfig(
      buildConfig(
        {
          exportArn: nextExportArn,
          exportCreatedAt: new Date().toISOString(),
        },
        selection,
      ),
      optionsOverride,
    );
  };

  const setupDataSourceJob = async (dataSourceId: number) => {
    const r = await setupDs.mutateAsync({
      id: dataSourceId,
      body: {
        tableName,
        cronExpression: cron,
        timezoneId: timezone,
      },
    });
    setResult(r);
    return r;
  };

  const ensureExternalLocationForCreate = async (): Promise<ExternalLocationSummary | null> => {
    if (setupMode !== 'create') return selectedLocation;
    if (!awsAccountId || !createModeExternalLocationUrl) return null;
    if (!createBucketNameValid) {
      throw new Error('S3 bucket name is invalid.');
    }
    const storageCredential =
      createModeStorageCredential ??
      (
        await createStorageCredential.mutateAsync({
          purpose: 'STORAGE',
          name: storageCredentialNameForBucket(normalizedCreateBucketName),
          awsAccountId,
          roleName: AWS_STORAGE_ROLE_NAME,
          readOnly: true,
        })
      ).storageCredential;
    if (createModeExistingLocation) {
      setExternalLocationName(createModeExistingLocation.name);
      return createModeExistingLocation;
    }

    const res = await createLocation.mutateAsync({
      name: createModeExternalLocationName,
      url: createModeExternalLocationUrl,
      credentialName: storageCredential.name,
      readOnly: true,
    });
    setExternalLocationName(res.externalLocation.name);
    return res.externalLocation;
  };

  const onCreateExport = async () => {
    if (!selectedS3Bucket || !awsAccountId) return;
    setCreatingExport(true);
    setExportError(null);
    try {
      if (setupMode === 'create') {
        const targetStorageCredentialName = `db_s3_credential_${ucNameSuffixFromBucket(selectedS3Bucket) || 'bucket'}`;
        const targetStorageRoleArn = `arn:aws:iam::${awsAccountId}:role/FinLakeStorageRole`;
        const targetExternalLocationName = externalLocationName || createModeExternalLocationName;
        setCreateProgressModalOpen(true);
        setCreateResourceSteps(
          updateCreateResourceSteps(initialCreateResourceSteps(), {
            bucket: {
              status: 'pending',
              detail: selectedS3Bucket,
              href: awsS3BucketUrl(selectedS3Bucket),
            },
            storageCredential: {
              status: 'pending',
              detail: targetStorageCredentialName,
              href: databricksStorageCredentialUrl(workspaceUrl, targetStorageCredentialName),
            },
            storageRole: {
              status: 'pending',
              detail: targetStorageRoleArn,
              href: awsIamRoleUrl(targetStorageRoleArn),
            },
            externalLocation: {
              status: 'pending',
              detail: targetExternalLocationName,
              href: databricksExternalLocationUrl(workspaceUrl, targetExternalLocationName),
            },
            dataExport: {
              status: 'pending',
              detail: exportName,
              href: AWS_BCM_DATA_EXPORTS_URL,
            },
            lakeflowJob: {
              status: 'idle',
              detail: null,
              href: null,
            },
          }),
        );
        const res = await createAwsFocusExport.mutateAsync({
          awsAccountId,
          s3Bucket: selectedS3Bucket,
          createBucketIfMissing,
          s3Prefix: effectiveS3Prefix,
          exportName,
          externalLocationName: externalLocationName || createModeExternalLocationName,
        });
        setExternalLocationName(res.externalLocation.name);
        setExportArn(res.exportArn);
        setCreateResourceSteps((steps) =>
          updateCreateResourceSteps(steps, {
            bucket: {
              status: apiResourceStatusToStepStatus(res.resourceStatuses.bucket),
              detail: selectedS3Bucket,
              href: awsS3BucketUrl(selectedS3Bucket),
            },
            storageCredential: {
              status: apiResourceStatusToStepStatus(res.resourceStatuses.storageCredential),
              detail: res.storageCredential.name,
              href: databricksStorageCredentialUrl(workspaceUrl, res.storageCredential.name),
            },
            storageRole: {
              status: apiResourceStatusToStepStatus(res.resourceStatuses.storageRole),
              detail: res.storageRoleArn,
              href: awsIamRoleUrl(res.storageRoleArn),
            },
            externalLocation: {
              status: apiResourceStatusToStepStatus(res.resourceStatuses.externalLocation),
              detail: res.externalLocation.name,
              href: databricksExternalLocationUrl(workspaceUrl, res.externalLocation.name),
            },
            dataExport: {
              status: apiResourceStatusToStepStatus(res.resourceStatuses.dataExport),
              detail: res.exportArn || exportName,
              href: AWS_BCM_DATA_EXPORTS_URL,
            },
            lakeflowJob: {
              status: 'pending',
              detail: null,
              href: null,
            },
          }),
        );
        const savedSource = await saveExportArn(
          res.exportArn,
          {
            externalLocationName: res.externalLocation.name,
            externalLocationUrl: res.externalLocation.url,
            storageCredentialName: res.storageCredential.name,
            s3Bucket: selectedS3Bucket,
            s3Prefix: effectiveS3Prefix,
          },
          {
            deferCreatedCallback: true,
          },
        );
        const dataSourceId = savedSource?.id ?? row?.id;
        if (dataSourceId) {
          const setupResult = await setupDataSourceJob(dataSourceId);
          setCreateResourceSteps((steps) =>
            updateCreateResourceSteps(steps, {
              lakeflowJob: {
                status: 'done',
                detail: `#${setupResult.jobId}`,
                href: databricksJobUrl(workspaceUrl, setupResult.jobId),
              },
            }),
          );
        }
        return;
      }

      const credentials = {
        accessKeyId,
        secretAccessKey,
        sessionToken: sessionToken.trim() || undefined,
      };
      const s3Client = new S3Client({
        region: AWS_BCM_REGION,
        credentials,
      });
      const ensuredLocation = await ensureExternalLocationForCreate();
      const targetS3Url = ensuredLocation?.url ?? selectedS3Url;
      const targetS3Bucket = targetS3Url ? s3BucketFromUrl(targetS3Url) : selectedS3Bucket;
      const targetS3BasePrefix = targetS3Url ? s3PrefixFromUrl(targetS3Url) : selectedS3BasePrefix;
      const targetS3Prefix = joinS3Prefixes(targetS3BasePrefix, normalizedS3Prefix);
      if (!targetS3Bucket) throw new Error('S3 bucket is required.');

      await upsertAwsDataExportBucketPolicy({
        client: s3Client,
        bucket: targetS3Bucket,
        accountId: awsAccountId,
      });

      const client = new BCMDataExportsClient({
        region: AWS_BCM_REGION,
        credentials,
      });
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
                S3Bucket: targetS3Bucket,
                S3Prefix: targetS3Prefix,
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
      const nextExportArn = res.ExportArn ?? '';
      setExportArn(nextExportArn);
      await saveExportArn(nextExportArn, {
        externalLocationName: ensuredLocation?.name ?? externalLocationName,
        externalLocationUrl: targetS3Url,
        storageCredentialName:
          ensuredLocation?.credentialName ?? selectedLocation?.credentialName ?? null,
        s3Bucket: targetS3Bucket,
        s3Prefix: targetS3Prefix,
      });
    } catch (err) {
      const nextExportError = messageOf(err) ?? String(err);
      if (setupMode === 'create') {
        setCreateResourceSteps((steps) => {
          const nextErrorStep = failedCreateResourceStepFromMessage(nextExportError, steps);
          if (!nextErrorStep) return steps;
          return updateCreateResourceSteps(steps, {
            [nextErrorStep]: { status: 'error' },
          });
        });
      }
      setExportError(nextExportError);
    } finally {
      setCreatingExport(false);
    }
  };

  const onSetup = async () => {
    if (!row) return;
    await setupDataSourceJob(row.id);
  };

  const onRunJob = async () => {
    if (!row) return;
    await runJob.mutateAsync(row.id);
  };

  const openExportModal = (open: boolean) => {
    if (open && !exportName.trim()) setExportName(AWS_EXPORT_NAME_DEFAULT);
    setExportModalOpen(open);
  };

  const closeCreateProgressModal = () => {
    setCreateProgressModalOpen(false);
    if (createdRowAfterProgress) {
      options.onCreated?.(createdRowAfterProgress);
      setCreatedRowAfterProgress(null);
    }
  };

  return {
    // Source form state
    setupMode,
    awsAccountId,
    externalLocationName,
    createBucketName,
    setCreateBucketName: onCreateBucketNameChange,
    createBucketIfMissing,
    setCreateBucketIfMissing,
    s3Prefix,
    setS3Prefix,
    exportName,
    setExportName,
    effectiveS3Prefix,

    // Source form derived
    accountOptions,
    linkedLocations,
    locationOptions,
    selectedS3Url,
    selectedS3Bucket,
    selectedS3BasePrefix,
    exportDestinationPreview,
    registered,
    persisted: Boolean(row),
    dirty,
    loadingInputs,
    saveDisabled,
    errorMessage,
    savedAt,
    storageCredentialsLoading: storageCredentials.isLoading,
    serviceCredentialsLoading: serviceCredentials.isLoading,
    createModeStorageCredential,
    serviceAccountOptions,
    savePending,
    createResourceSteps,
    createProgressModalOpen,

    // Source form actions
    onSetupModeChange,
    onAccountChange,
    onLocationChange,
    onSave,
    closeCreateProgressModal,

    // Export panel state
    accessKeyId,
    setAccessKeyId,
    secretAccessKey,
    setSecretAccessKey,
    sessionToken,
    setSessionToken,
    exportModalOpen,
    openExportModal,
    exportArn,
    exportError,
    creatingExport,
    createExportDisabled,
    onCreateExport,

    // Transformation section state
    remoteCatalog,
    tableName,
    setTableName,
    cron,
    setCron,
    timezone,
    setTimezone,
    jobId,
    pipelineId,
    workspaceUrl,
    fqn,
    hadScheduleBeforeSetup,
    setupDisabled,
    result,
    setupDs,
    runJob,
    onSetup,
    onRunJob,
  };
}
