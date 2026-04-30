import { useEffect, useMemo, useState } from 'react';
import { BCMDataExportsClient, CreateExportCommand } from '@aws-sdk/client-bcm-data-exports';
import {
  useAppSettings,
  useExternalLocations,
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
  FOCUS_VIEW_SCHEMA_DEFAULT,
  normalizeS3Prefix,
  s3BucketFromUrl,
  s3ExportPath,
  tableLeafName,
  unquotedFqn,
  type DataSource,
  type DataSourceSetupResult,
  type ExternalLocationSummary,
  type StorageCredentialSummary,
} from '@lakecost/shared';
import { messageOf } from './utils';

const AWS_FOCUS_12_QUERY_STATEMENT =
  'SELECT AvailabilityZone, BilledCost, BillingAccountId, BillingAccountName, BillingAccountType, BillingCurrency, BillingPeriodEnd, BillingPeriodStart, CapacityReservationId, CapacityReservationStatus, ChargeCategory, ChargeClass, ChargeDescription, ChargeFrequency, ChargePeriodEnd, ChargePeriodStart, CommitmentDiscountCategory, CommitmentDiscountId, CommitmentDiscountName, CommitmentDiscountQuantity, CommitmentDiscountStatus, CommitmentDiscountType, CommitmentDiscountUnit, ConsumedQuantity, ConsumedUnit, ContractedCost, ContractedUnitPrice, EffectiveCost, InvoiceId, InvoiceIssuerName, ListCost, ListUnitPrice, PricingCategory, PricingCurrency, PricingCurrencyContractedUnitPrice, PricingCurrencyEffectiveCost, PricingCurrencyListUnitPrice, PricingQuantity, PricingUnit, ProviderName, PublisherName, RegionId, RegionName, ResourceId, ResourceName, ResourceType, ServiceCategory, ServiceName, ServiceSubcategory, SkuId, SkuMeter, SkuPriceDetails, SkuPriceId, SubAccountId, SubAccountName, SubAccountType, Tags, x_Discounts, x_Operation, x_ServiceCode FROM FOCUS_1_2_AWS';
const AWS_BCM_REGION = 'us-east-1';

function configString(config: Record<string, unknown>, key: string): string {
  const value = config[key];
  return typeof value === 'string' ? value : '';
}

export function useAwsFocusForm(row: DataSource) {
  const storageCredentials = useStorageCredentials();
  const locations = useExternalLocations();
  const updateDs = useUpdateDataSource();
  const me = useMe();
  const settings = useAppSettings();
  const setupDs = useSetupDataSource();
  const runJob = useRunDataSourceJob();

  // --- Remote (server) values ---
  const remoteAwsAccountId = configString(row.config, 'awsAccountId');
  const remoteExternalLocationName = configString(row.config, 'externalLocationName');
  const remoteExportName = configString(row.config, 'exportName');
  const remoteS3Prefix = configString(row.config, 's3Prefix');
  const remoteCatalog = settings.data?.settings[CATALOG_SETTING_KEY] ?? '';
  const remoteCron = configString(row.config, 'cronExpression') || FOCUS_REFRESH_CRON_DEFAULT;
  const remoteTz = configString(row.config, 'timezoneId') || FOCUS_REFRESH_TIMEZONE_DEFAULT;

  // --- Local form state ---
  const [awsAccountId, setAwsAccountId] = useState(remoteAwsAccountId);
  const [externalLocationName, setExternalLocationName] = useState(remoteExternalLocationName);
  const [accessKeyId, setAccessKeyId] = useState('');
  const [secretAccessKey, setSecretAccessKey] = useState('');
  const [sessionToken, setSessionToken] = useState('');
  const [exportName, setExportName] = useState(remoteExportName || 'finlake-focus-1-2');
  const [s3Prefix, setS3Prefix] = useState(normalizeS3Prefix(remoteS3Prefix || 'export'));
  const [tableName, setTableName] = useState(tableLeafName(row.tableName));
  const [cron, setCron] = useState(remoteCron);
  const [timezone, setTimezone] = useState(remoteTz);
  const [result, setResult] = useState<DataSourceSetupResult | null>(null);
  const [exportArn, setExportArn] = useState(configString(row.config, 'exportArn'));
  const [exportError, setExportError] = useState<string | null>(null);
  const [creatingExport, setCreatingExport] = useState(false);
  const [exportPanelOpen, setExportPanelOpen] = useState(false);
  const [savedAt, setSavedAt] = useState<number | null>(null);

  // --- Sync local state when server data changes ---
  useEffect(() => setAwsAccountId(remoteAwsAccountId), [remoteAwsAccountId]);
  useEffect(
    () => setExternalLocationName(remoteExternalLocationName),
    [remoteExternalLocationName],
  );
  useEffect(() => setExportName(remoteExportName || 'finlake-focus-1-2'), [remoteExportName]);
  useEffect(() => setS3Prefix(normalizeS3Prefix(remoteS3Prefix || 'export')), [remoteS3Prefix]);
  useEffect(() => setTableName(tableLeafName(row.tableName)), [row.tableName]);
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
  const selectedS3Url = selectedLocation?.url ?? null;
  const selectedS3Bucket = selectedS3Url ? s3BucketFromUrl(selectedS3Url) : null;
  const normalizedS3Prefix = normalizeS3Prefix(s3Prefix);
  const exportDestinationPreview =
    selectedS3Bucket && exportName && normalizedS3Prefix
      ? s3ExportPath(selectedS3Bucket, normalizedS3Prefix, exportName)
      : null;

  // --- Flags ---
  const dirty =
    awsAccountId !== remoteAwsAccountId ||
    externalLocationName !== remoteExternalLocationName ||
    exportName !== remoteExportName ||
    s3Prefix !== remoteS3Prefix;
  const loadingInputs = storageCredentials.isLoading || locations.isLoading;
  const saveDisabled = updateDs.isPending || !awsAccountId || !externalLocationName || !dirty;
  const jobId = result?.jobId ?? row.jobId;
  const pipelineId = result?.pipelineId ?? row.pipelineId;
  const workspaceUrl = me.data?.workspaceUrl ?? null;
  const fqn = remoteCatalog
    ? unquotedFqn(remoteCatalog, FOCUS_VIEW_SCHEMA_DEFAULT, tableName)
    : `${FOCUS_VIEW_SCHEMA_DEFAULT}.${tableName}`;
  const hadScheduleBeforeSetup = row.jobId !== null;
  const setupDisabled =
    setupDs.isPending || !remoteCatalog || !selectedS3Url || !tableName || !cron || !timezone;
  const createExportDisabled =
    creatingExport ||
    updateDs.isPending ||
    !selectedS3Bucket ||
    !accessKeyId ||
    !secretAccessKey ||
    !exportName ||
    !normalizedS3Prefix;
  const errorMessage =
    messageOf(storageCredentials.error) ?? messageOf(locations.error) ?? messageOf(updateDs.error);

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

  const onSave = async () => {
    const selected = allLocations.find((loc) => loc.name === externalLocationName);
    const storageCredentialName = selected?.credentialName ?? null;
    const s3Bucket = selected?.url ? s3BucketFromUrl(selected.url) : null;
    await updateDs.mutateAsync({
      id: row.id,
      body: {
        config: {
          ...row.config,
          awsAccountId,
          externalLocationName,
          externalLocationUrl: selected?.url ?? null,
          storageCredentialName,
          s3Bucket,
          exportName,
          s3Prefix: normalizedS3Prefix,
          s3Region: AWS_BCM_REGION,
        },
      },
    });
    setSavedAt(Date.now());
  };

  const onCreateExport = async () => {
    if (!selectedS3Bucket) return;
    setCreatingExport(true);
    setExportError(null);
    try {
      const client = new BCMDataExportsClient({
        region: AWS_BCM_REGION,
        credentials: {
          accessKeyId,
          secretAccessKey,
          sessionToken: sessionToken.trim() || undefined,
        },
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
                S3Prefix: normalizedS3Prefix,
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
      await updateDs.mutateAsync({
        id: row.id,
        body: {
          config: {
            ...row.config,
            awsAccountId,
            externalLocationName,
            externalLocationUrl: selectedS3Url,
            storageCredentialName: selectedLocation?.credentialName ?? null,
            s3Bucket: selectedS3Bucket,
            exportName,
            s3Prefix: normalizedS3Prefix,
            s3Region: AWS_BCM_REGION,
            exportArn: nextExportArn,
            exportCreatedAt: new Date().toISOString(),
          },
        },
      });
    } catch (err) {
      setExportError(messageOf(err) ?? String(err));
    } finally {
      setCreatingExport(false);
    }
  };

  const onSetup = async () => {
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
    await runJob.mutateAsync(row.id);
  };

  return {
    // Source form state
    awsAccountId,
    externalLocationName,
    s3Prefix,
    setS3Prefix,
    exportName,
    setExportName,
    normalizedS3Prefix,

    // Source form derived
    accountOptions,
    linkedLocations,
    selectedS3Url,
    exportDestinationPreview,
    dirty,
    loadingInputs,
    saveDisabled,
    errorMessage,
    savedAt,
    storageCredentialsLoading: storageCredentials.isLoading,
    updatePending: updateDs.isPending,

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
    exportPanelOpen,
    setExportPanelOpen,
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
