import { useEffect, useMemo, useState } from 'react';
import { BCMDataExportsClient, CreateExportCommand } from '@aws-sdk/client-bcm-data-exports';
import { GetBucketPolicyCommand, PutBucketPolicyCommand, S3Client } from '@aws-sdk/client-s3';
import {
  useAppSettings,
  useExternalLocations,
  useCreateDataSource,
  useMe,
  useRunDataSourceJob,
  useSetupDataSource,
  useStorageCredentials,
  useUpdateDataSource,
} from '../../api/hooks';
import {
  CATALOG_SETTING_KEY,
  FOCUS_REFRESH_CRON_DEFAULT,
  FOCUS_REFRESH_TIMEZONE_DEFAULT,
  medallionSchemaNamesFromSettings,
  normalizeS3Prefix,
  s3BucketFromUrl,
  s3ExportPath,
  tableLeafName,
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
const AWS_EXPORT_BUCKET_POLICY_SID = 'EnableAWSDataExportsToWriteToS3AndCheckPolicy';
const S3_PREFIX_PREVIEW_PLACEHOLDER = '{prefix}';
const EXPORT_NAME_PREVIEW_PLACEHOLDER = '{export_name}';

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
  const locations = useExternalLocations();
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
  const [exportName, setExportName] = useState(remoteExportName);
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

  // --- Sync local state when server data changes ---
  useEffect(() => setAwsAccountId(remoteAwsAccountId), [remoteAwsAccountId]);
  useEffect(
    () => setExternalLocationName(remoteExternalLocationName),
    [remoteExternalLocationName],
  );
  useEffect(() => setExportName(remoteExportName), [remoteExportName]);
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
  const accountOptions = useMemo(
    () => Array.from(new Set(awsCredentials.map((cred) => cred.awsAccountId))).sort(),
    [awsCredentials],
  );
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
  const selectedLocation: ExternalLocationSummary | null =
    allLocations.find((loc) => loc.name === externalLocationName) ??
    (externalLocationName ? ({ name: externalLocationName } as ExternalLocationSummary) : null);
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

  useEffect(
    () => setS3Prefix(stripBasePrefix(remoteS3Prefix, selectedS3BasePrefix)),
    [remoteS3Prefix, selectedS3BasePrefix],
  );

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
    exportName !== remoteExportName ||
    effectiveS3Prefix !== remoteS3Prefix;
  const loadingInputs = storageCredentials.isLoading || locations.isLoading;
  const registered =
    Boolean(row) &&
    Boolean(remoteAwsAccountId) &&
    Boolean(remoteExternalLocationName) &&
    Boolean(remoteExportName) &&
    Boolean(remoteS3Prefix);
  const savePending = createDs.isPending || updateDs.isPending;
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
    savePending ||
    registered ||
    !awsAccountId ||
    !selectedS3Bucket ||
    !accessKeyId ||
    !secretAccessKey ||
    !exportName ||
    !effectiveS3Prefix;
  const errorMessage =
    messageOf(storageCredentials.error) ??
    messageOf(locations.error) ??
    messageOf(createDs.error) ??
    messageOf(updateDs.error);

  // --- Actions ---
  const onAccountChange = (value: string) => {
    updateDs.reset();
    setSavedAt(null);
    setAwsAccountId(value);
    setExternalLocationName('');
  };

  const onLocationChange = (value: string) => {
    updateDs.reset();
    setSavedAt(null);
    setExternalLocationName(value);
  };

  const buildConfig = (overrides?: Record<string, unknown>) => {
    const selected = allLocations.find((loc) => loc.name === externalLocationName);
    return {
      ...remoteConfig,
      awsAccountId,
      externalLocationName,
      externalLocationUrl: selected?.url ?? selectedS3Url,
      storageCredentialName: selected?.credentialName ?? null,
      s3Bucket: selectedS3Bucket,
      exportName,
      s3Prefix: effectiveS3Prefix,
      s3Region: AWS_BCM_REGION,
      ...overrides,
    };
  };

  const onSave = async () => {
    const config = buildConfig(exportArn ? { exportArn } : {});
    if (row) {
      await updateDs.mutateAsync({
        id: row.id,
        body: { billingAccountId: awsAccountId, config },
      });
    } else if (options.draft) {
      const created = await createDs.mutateAsync({
        ...options.draft,
        billingAccountId: awsAccountId,
        enabled: false,
        config,
      });
      options.onCreated?.(created);
    }
    setSavedAt(Date.now());
  };

  const saveExportArn = async (nextExportArn: string) => {
    if (!row) return;
    const config = buildConfig({
      exportArn: nextExportArn,
      exportCreatedAt: new Date().toISOString(),
    });
    await updateDs.mutateAsync({
      id: row.id,
      body: { billingAccountId: awsAccountId, config },
    });
  };

  const onCreateExport = async () => {
    if (!selectedS3Bucket || !awsAccountId) return;
    setCreatingExport(true);
    setExportError(null);
    try {
      const credentials = {
        accessKeyId,
        secretAccessKey,
        sessionToken: sessionToken.trim() || undefined,
      };
      const s3Client = new S3Client({
        region: AWS_BCM_REGION,
        credentials,
      });
      await upsertAwsDataExportBucketPolicy({
        client: s3Client,
        bucket: selectedS3Bucket,
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
                S3Bucket: selectedS3Bucket,
                S3Prefix: effectiveS3Prefix,
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
      await saveExportArn(nextExportArn);
    } catch (err) {
      setExportError(messageOf(err) ?? String(err));
    } finally {
      setCreatingExport(false);
    }
  };

  const onSetup = async () => {
    if (!row) return;
    const r = await setupDs.mutateAsync({
      id: row.id,
      body: {
        tableName,
        cronExpression: cron,
        timezoneId: timezone,
      },
    });
    setResult(r);
  };

  const onRunJob = async () => {
    if (!row) return;
    await runJob.mutateAsync(row.id);
  };

  const openExportModal = (open: boolean) => {
    if (open && !exportName.trim()) setExportName(AWS_EXPORT_NAME_DEFAULT);
    setExportModalOpen(open);
  };

  return {
    // Source form state
    awsAccountId,
    externalLocationName,
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
    savePending,

    // Source form actions
    onAccountChange,
    onLocationChange,
    onSave,

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
    silverSchema,
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
