import { useEffect, useMemo, useState } from 'react';
import { BCMDataExportsClient, CreateExportCommand } from '@aws-sdk/client-bcm-data-exports';
import {
  Alert,
  AlertDescription,
  Button,
  Card,
  CardContent,
  CardHeader,
  CardTitle,
  Input,
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
  Sheet,
  SheetContent,
  SheetHeader,
  SheetTitle,
  Spinner,
} from '@databricks/appkit-ui/react';
import { ChevronDown, ExternalLink, Info } from 'lucide-react';
import {
  useAppSettings,
  useDataSource,
  useDeleteDataSource,
  useExternalLocations,
  useMe,
  useRunDataSourceJob,
  useSetupDataSource,
  useStorageCredentials,
  useUpdateDataSource,
} from '../../api/hooks';
import {
  ACCOUNT_PRICES_DEFAULT,
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
import { useI18n } from '../../i18n';
import { messageOf } from './utils';
import { displayNameForRow, findTemplateForRow } from './dataSourceCatalog';

interface Props {
  dataSourceId: number | null;
  onClose: () => void;
}

const AWS_FOCUS_12_QUERY_STATEMENT =
  'SELECT AvailabilityZone, BilledCost, BillingAccountId, BillingAccountName, BillingAccountType, BillingCurrency, BillingPeriodEnd, BillingPeriodStart, CapacityReservationId, CapacityReservationStatus, ChargeCategory, ChargeClass, ChargeDescription, ChargeFrequency, ChargePeriodEnd, ChargePeriodStart, CommitmentDiscountCategory, CommitmentDiscountId, CommitmentDiscountName, CommitmentDiscountQuantity, CommitmentDiscountStatus, CommitmentDiscountType, CommitmentDiscountUnit, ConsumedQuantity, ConsumedUnit, ContractedCost, ContractedUnitPrice, EffectiveCost, InvoiceId, InvoiceIssuerName, ListCost, ListUnitPrice, PricingCategory, PricingCurrency, PricingCurrencyContractedUnitPrice, PricingCurrencyEffectiveCost, PricingCurrencyListUnitPrice, PricingQuantity, PricingUnit, ProviderName, PublisherName, RegionId, RegionName, ResourceId, ResourceName, ResourceType, ServiceCategory, ServiceName, ServiceSubcategory, SkuId, SkuMeter, SkuPriceDetails, SkuPriceId, SubAccountId, SubAccountName, SubAccountType, Tags, x_Discounts, x_Operation, x_ServiceCode FROM FOCUS_1_2_AWS';
const AWS_BCM_REGION = 'us-east-1';

function catalogTableUrl(workspaceUrl: string, fqn: string): string {
  return `${workspaceUrl}/explore/data/${fqn.split('.').map(encodeURIComponent).join('/')}`;
}

export function DataSourceDrawer({ dataSourceId, onClose }: Props) {
  const { t } = useI18n();
  const ds = useDataSource(dataSourceId ?? undefined);
  const isOpen = dataSourceId !== null;
  const row = ds.data;

  return (
    <Sheet open={isOpen} onOpenChange={(open) => (open ? null : onClose())}>
      <SheetContent
        side="right"
        className="w-full max-w-(--container-md) sm:max-w-xl"
        onOpenAutoFocus={(e) => e.preventDefault()}
      >
        <SheetHeader>
          <SheetTitle>
            {row
              ? (() => {
                  const tpl = findTemplateForRow(row);
                  return tpl ? displayNameForRow(row, tpl) : row.name;
                })()
              : t('common.loading')}
          </SheetTitle>
        </SheetHeader>
        <div className="flex flex-col gap-4 overflow-auto px-4 pb-6">
          {row?.providerName === 'Databricks' ? (
            <p className="text-muted-foreground text-sm">
              {t('dataSources.systemTables.focusViewDesc')}
            </p>
          ) : null}
          {row ? <Configurator row={row} onClose={onClose} /> : null}
        </div>
      </SheetContent>
    </Sheet>
  );
}

function Configurator({ row, onClose }: { row: DataSource; onClose: () => void }) {
  const { t } = useI18n();
  const deleteDs = useDeleteDataSource();
  const template = findTemplateForRow(row);

  const onDelete = async () => {
    if (!window.confirm(t('dataSources.confirmDelete', { name: row.name }))) return;
    await deleteDs.mutateAsync(row.id);
    onClose();
  };

  return (
    <>
      {template?.id === 'databricks_focus13' ? (
        <FocusViewSection row={row} />
      ) : template?.id === 'aws' ? (
        <AwsCurSection row={row} />
      ) : (
        <Alert>
          <Info />
          <AlertDescription>{t('dataSources.drawer.notImplemented')}</AlertDescription>
        </Alert>
      )}

      <div className="flex justify-end pt-2">
        <Button
          type="button"
          variant="destructive"
          disabled={deleteDs.isPending}
          onClick={onDelete}
        >
          {deleteDs.isPending ? <Spinner /> : null}
          {t('dataSources.delete')}
        </Button>
      </div>
    </>
  );
}

function configString(config: Record<string, unknown>, key: string): string {
  const value = config[key];
  return typeof value === 'string' ? value : '';
}

function AwsCurSection({ row }: { row: DataSource }) {
  const { t } = useI18n();
  const storageCredentials = useStorageCredentials();
  const locations = useExternalLocations();
  const updateDs = useUpdateDataSource();
  const me = useMe();
  const settings = useAppSettings();
  const setupDs = useSetupDataSource();
  const runJob = useRunDataSourceJob();

  const remoteAwsAccountId = configString(row.config, 'awsAccountId');
  const remoteExternalLocationName = configString(row.config, 'externalLocationName');
  const remoteExportName = configString(row.config, 'exportName');
  const remoteS3Prefix = configString(row.config, 's3Prefix');
  const remoteCatalog = settings.data?.settings[CATALOG_SETTING_KEY] ?? '';
  const remoteCron = configString(row.config, 'cronExpression') || FOCUS_REFRESH_CRON_DEFAULT;
  const remoteTz = configString(row.config, 'timezoneId') || FOCUS_REFRESH_TIMEZONE_DEFAULT;
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
  const selectedLocation =
    allLocations.find((loc) => loc.name === externalLocationName) ??
    (externalLocationName ? ({ name: externalLocationName } as ExternalLocationSummary) : null);
  const selectedS3Url = selectedLocation?.url ?? null;
  const selectedS3Bucket = selectedS3Url ? s3BucketFromUrl(selectedS3Url) : null;
  const normalizedS3Prefix = normalizeS3Prefix(s3Prefix);
  const exportDestinationPreview =
    selectedS3Bucket && exportName && normalizedS3Prefix
      ? s3ExportPath(selectedS3Bucket, normalizedS3Prefix, exportName)
      : null;
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
      const result = await client.send(
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
      const nextExportArn = result.ExportArn ?? '';
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

  return (
    <>
      <Card>
        <CardHeader>
          <CardTitle className="text-sm">{t('dataSources.awsCur.title')}</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="grid gap-3">
            <label className="grid gap-1 text-xs">
              <span className="text-muted-foreground">{t('dataSources.awsCur.awsAccountId')}</span>
              <Select
                value={awsAccountId}
                onValueChange={(value: string) => {
                  updateDs.reset();
                  setSavedAt(null);
                  setAwsAccountId(value);
                  setExternalLocationName('');
                }}
                disabled={storageCredentials.isLoading || updateDs.isPending}
              >
                <SelectTrigger className="w-full">
                  <SelectValue placeholder={t('dataSources.awsCur.awsAccountIdPlaceholder')} />
                </SelectTrigger>
                <SelectContent>
                  {accountOptions.map((accountId) => (
                    <SelectItem key={accountId} value={accountId}>
                      {accountId}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </label>

            <label className="grid gap-1 text-xs">
              <span className="text-muted-foreground">{t('dataSources.awsCur.s3Url')}</span>
              <Select
                value={externalLocationName}
                onValueChange={(value: string) => {
                  updateDs.reset();
                  setSavedAt(null);
                  setExternalLocationName(value);
                }}
                disabled={!awsAccountId || loadingInputs || updateDs.isPending}
              >
                <SelectTrigger className="w-full">
                  <SelectValue placeholder={t('dataSources.awsCur.s3UrlPlaceholder')} />
                </SelectTrigger>
                <SelectContent>
                  {linkedLocations.map((loc) => (
                    <SelectItem key={loc.name} value={loc.name}>
                      {s3UrlLabel(loc)}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </label>

            {selectedS3Url ? (
              <>
                <div className="grid grid-cols-1 gap-2 sm:grid-cols-2">
                  <label className="grid gap-1 text-xs">
                    <span className="text-muted-foreground">
                      {t('dataSources.awsCur.s3Prefix')}
                    </span>
                    <Input
                      value={s3Prefix}
                      onChange={(e) => setS3Prefix(e.target.value)}
                      onBlur={() => setS3Prefix((value) => normalizeS3Prefix(value))}
                      placeholder="export"
                    />
                  </label>
                  <label className="grid gap-1 text-xs">
                    <span className="text-muted-foreground">
                      {t('dataSources.awsCur.exportName')}
                    </span>
                    <Input value={exportName} onChange={(e) => setExportName(e.target.value)} />
                  </label>
                </div>

                {exportDestinationPreview ? (
                  <div className="text-muted-foreground break-all text-xs">
                    {t('dataSources.awsCur.exportDestination')}:{' '}
                    <span className="text-foreground font-mono">{exportDestinationPreview}</span>
                  </div>
                ) : null}
              </>
            ) : null}

            <div className="flex flex-wrap items-center gap-3">
              <Button
                type="button"
                disabled={saveDisabled}
                onClick={onSave}
                className="bg-(--success) text-(--background) hover:bg-(--success)/90 disabled:bg-muted disabled:text-muted-foreground"
              >
                {updateDs.isPending ? <Spinner /> : null}
                {t('dataSources.awsCur.saveExternalLocation')}
              </Button>
              {savedAt && !dirty && !updateDs.isPending ? (
                <span className="text-muted-foreground text-xs">{t('settings.saved')}</span>
              ) : null}
            </div>

            {selectedS3Url ? (
              <div className="border-border bg-background/35 rounded-md border">
                <button
                  type="button"
                  className="text-foreground hover:bg-muted/30 flex w-full items-center justify-between gap-3 rounded-md px-3 py-2 text-left text-sm font-semibold transition-colors"
                  aria-expanded={exportPanelOpen}
                  aria-controls="aws-cur-export-panel"
                  onClick={() => setExportPanelOpen((open) => !open)}
                >
                  <span>{t('dataSources.awsCur.exportCreateSection')}</span>
                  <ChevronDown
                    className={`text-muted-foreground size-4 shrink-0 transition-transform ${
                      exportPanelOpen ? 'rotate-180' : ''
                    }`}
                    aria-hidden="true"
                  />
                </button>
                {exportPanelOpen ? (
                  <div id="aws-cur-export-panel" className="grid gap-3 border-t px-3 py-3">
                    <div className="grid grid-cols-1 gap-2 sm:grid-cols-2">
                      <label className="grid gap-1 text-xs">
                        <span className="text-muted-foreground">
                          {t('dataSources.awsCur.accessKeyId')}
                        </span>
                        <Input
                          value={accessKeyId}
                          onChange={(e) => setAccessKeyId(e.target.value)}
                          autoComplete="off"
                        />
                      </label>
                      <label className="grid gap-1 text-xs">
                        <span className="text-muted-foreground">
                          {t('dataSources.awsCur.secretAccessKey')}
                        </span>
                        <Input
                          type="password"
                          value={secretAccessKey}
                          onChange={(e) => setSecretAccessKey(e.target.value)}
                          autoComplete="new-password"
                        />
                      </label>
                      <label className="grid gap-1 text-xs sm:col-span-2">
                        <span className="text-muted-foreground">
                          {t('dataSources.awsCur.sessionToken')}
                        </span>
                        <Input
                          type="password"
                          value={sessionToken}
                          onChange={(e) => setSessionToken(e.target.value)}
                          autoComplete="off"
                          placeholder={t('dataSources.awsCur.sessionTokenPlaceholder')}
                        />
                      </label>
                    </div>
                    <div>
                      <Button
                        type="button"
                        variant="secondary"
                        disabled={createExportDisabled}
                        onClick={onCreateExport}
                      >
                        {creatingExport ? <Spinner /> : null}
                        {t('dataSources.awsCur.createExport')}
                      </Button>
                    </div>
                    {exportArn ? (
                      <Alert>
                        <Info />
                        <AlertDescription>
                          {t('dataSources.awsCur.exportCreated', { exportArn })}
                        </AlertDescription>
                      </Alert>
                    ) : null}
                    {exportError ? (
                      <Alert variant="destructive">
                        <Info />
                        <AlertDescription>{exportError}</AlertDescription>
                      </Alert>
                    ) : null}
                  </div>
                ) : null}
              </div>
            ) : null}

            {!storageCredentials.isLoading && accountOptions.length === 0 ? (
              <Alert>
                <Info />
                <AlertDescription>{t('dataSources.awsCur.noStorageCredentials')}</AlertDescription>
              </Alert>
            ) : null}

            {awsAccountId && !loadingInputs && linkedLocations.length === 0 ? (
              <Alert>
                <Info />
                <AlertDescription>
                  {t('dataSources.awsCur.noLinkedExternalLocations')}
                </AlertDescription>
              </Alert>
            ) : null}

            {errorMessage ? (
              <Alert variant="destructive">
                <Info />
                <AlertDescription>{errorMessage}</AlertDescription>
              </Alert>
            ) : null}
          </div>
        </CardContent>
      </Card>

      {selectedS3Url ? (
        <Card>
          <CardHeader>
            <CardTitle className="text-sm">{t('dataSources.systemTables.title')}</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="grid grid-cols-1 gap-2 sm:grid-cols-2">
              <label className="grid gap-1 text-xs">
                <span className="text-muted-foreground">
                  {t('dataSources.systemTables.catalog')}
                </span>
                <Input value={remoteCatalog} disabled placeholder="main" />
              </label>
              <label className="grid gap-1 text-xs">
                <span className="text-muted-foreground">
                  {t('dataSources.systemTables.schema')}
                </span>
                <Input value={FOCUS_VIEW_SCHEMA_DEFAULT} disabled />
              </label>
              <label className="grid gap-1 text-xs sm:col-span-2">
                <span className="text-muted-foreground">
                  {t('dataSources.systemTables.tableName')}
                </span>
                <Input value={tableName} onChange={(e) => setTableName(e.target.value)} />
              </label>
              <label className="grid gap-1 text-xs">
                <span className="text-muted-foreground">{t('dataSources.systemTables.cron')}</span>
                <Input
                  value={cron}
                  onChange={(e) => setCron(e.target.value)}
                  placeholder={FOCUS_REFRESH_CRON_DEFAULT}
                />
              </label>
              <label className="grid gap-1 text-xs">
                <span className="text-muted-foreground">
                  {t('dataSources.systemTables.timezone')}
                </span>
                <Input
                  value={timezone}
                  onChange={(e) => setTimezone(e.target.value)}
                  placeholder={FOCUS_REFRESH_TIMEZONE_DEFAULT}
                />
              </label>
            </div>
            <div className="mt-3 flex flex-wrap items-center gap-2">
              <Button
                type="button"
                disabled={setupDisabled}
                onClick={onSetup}
                className="bg-(--success) text-(--background) hover:bg-(--success)/90 disabled:bg-muted disabled:text-muted-foreground"
              >
                {setupDs.isPending ? <Spinner /> : null}
                {t(
                  hadScheduleBeforeSetup
                    ? 'dataSources.systemTables.updateSchedule'
                    : 'dataSources.systemTables.setupAndSchedule',
                )}
              </Button>
              {jobId !== null ? (
                <Button
                  type="button"
                  variant="secondary"
                  disabled={runJob.isPending}
                  onClick={onRunJob}
                >
                  {runJob.isPending ? <Spinner /> : null}
                  {t('dataSources.systemTables.runJob')}
                </Button>
              ) : null}
            </div>
            <DatabricksResourceLinks
              workspaceUrl={workspaceUrl}
              jobId={jobId}
              pipelineId={pipelineId}
              tableFqn={jobId !== null && remoteCatalog ? fqn : null}
            />
            {!remoteCatalog ? (
              <Alert className="mt-3">
                <Info />
                <AlertDescription>{t('dataSources.systemTables.catalogMissing')}</AlertDescription>
              </Alert>
            ) : null}
            {result ? (
              <Alert className="mt-3">
                <Info />
                <AlertDescription>
                  {t(
                    hadScheduleBeforeSetup
                      ? 'dataSources.systemTables.updateOk'
                      : 'dataSources.systemTables.setupOk',
                    {
                      fqn: result.fqn,
                      jobId: String(result.jobId),
                    },
                  )}
                </AlertDescription>
              </Alert>
            ) : null}
            {runJob.data ? (
              <Alert className="mt-3">
                <Info />
                <AlertDescription>
                  {t('dataSources.systemTables.runOk', {
                    jobId: String(runJob.data.jobId),
                    runId: String(runJob.data.runId),
                  })}
                </AlertDescription>
              </Alert>
            ) : null}
            {setupDs.error ? (
              <Alert className="mt-3" variant="destructive">
                <Info />
                <AlertDescription>{(setupDs.error as Error).message}</AlertDescription>
              </Alert>
            ) : null}
            {runJob.error ? (
              <Alert className="mt-3" variant="destructive">
                <Info />
                <AlertDescription>{(runJob.error as Error).message}</AlertDescription>
              </Alert>
            ) : null}
          </CardContent>
        </Card>
      ) : null}
    </>
  );
}

function s3UrlLabel(location: ExternalLocationSummary): string {
  return location.url ?? location.name;
}

function FocusViewSection({ row }: { row: DataSource }) {
  const { t } = useI18n();
  const me = useMe();
  const settings = useAppSettings();
  const setupDs = useSetupDataSource();
  const runJob = useRunDataSourceJob();

  const remoteCatalog = settings.data?.settings[CATALOG_SETTING_KEY] ?? '';
  const remoteAccountPrices =
    (row.config.accountPricesTable as string | undefined) ?? ACCOUNT_PRICES_DEFAULT;
  const remoteCron =
    (row.config.cronExpression as string | undefined) ?? FOCUS_REFRESH_CRON_DEFAULT;
  const remoteTz = (row.config.timezoneId as string | undefined) ?? FOCUS_REFRESH_TIMEZONE_DEFAULT;

  const [tableName, setTableName] = useState(tableLeafName(row.tableName));
  const [accountPrices, setAccountPrices] = useState(remoteAccountPrices);
  const [cron, setCron] = useState(remoteCron);
  const [timezone, setTimezone] = useState(remoteTz);
  const [result, setResult] = useState<DataSourceSetupResult | null>(null);
  const jobId = result?.jobId ?? row.jobId;
  const pipelineId = result?.pipelineId ?? row.pipelineId;
  const workspaceUrl = me.data?.workspaceUrl ?? null;
  // Use only the persisted row state so the label stays correct after first setup
  const hadScheduleBeforeSetup = row.jobId !== null;

  useEffect(() => setTableName(tableLeafName(row.tableName)), [row.tableName]);
  useEffect(() => setAccountPrices(remoteAccountPrices), [remoteAccountPrices]);
  useEffect(() => setCron(remoteCron), [remoteCron]);
  useEffect(() => setTimezone(remoteTz), [remoteTz]);

  const fqn = remoteCatalog
    ? unquotedFqn(remoteCatalog, FOCUS_VIEW_SCHEMA_DEFAULT, tableName)
    : `${FOCUS_VIEW_SCHEMA_DEFAULT}.${tableName}`;

  const onSetup = async () => {
    const r = await setupDs.mutateAsync({
      id: row.id,
      body: {
        tableName,
        accountPricesTable: accountPrices,
        cronExpression: cron,
        timezoneId: timezone,
      },
    });
    setResult(r);
  };

  const onRunJob = async () => {
    await runJob.mutateAsync(row.id);
  };

  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-sm">{t('dataSources.systemTables.title')}</CardTitle>
      </CardHeader>
      <CardContent>
        <div className="grid grid-cols-1 gap-2 sm:grid-cols-2">
          <label className="grid gap-1 text-xs">
            <span className="text-muted-foreground">{t('dataSources.systemTables.catalog')}</span>
            <Input value={remoteCatalog} disabled placeholder="main" />
          </label>
          <label className="grid gap-1 text-xs">
            <span className="text-muted-foreground">{t('dataSources.systemTables.schema')}</span>
            <Input value={FOCUS_VIEW_SCHEMA_DEFAULT} disabled />
          </label>
          <label className="grid gap-1 text-xs sm:col-span-2">
            <span className="text-muted-foreground">{t('dataSources.systemTables.tableName')}</span>
            <Input value={tableName} onChange={(e) => setTableName(e.target.value)} />
          </label>
          <label className="grid gap-1 text-xs sm:col-span-2">
            <span className="text-muted-foreground">
              {t('dataSources.systemTables.accountPrices')}
            </span>
            <Input
              value={accountPrices}
              onChange={(e) => setAccountPrices(e.target.value)}
              placeholder={ACCOUNT_PRICES_DEFAULT}
            />
          </label>
          <label className="grid gap-1 text-xs">
            <span className="text-muted-foreground">{t('dataSources.systemTables.cron')}</span>
            <Input
              value={cron}
              onChange={(e) => setCron(e.target.value)}
              placeholder={FOCUS_REFRESH_CRON_DEFAULT}
            />
          </label>
          <label className="grid gap-1 text-xs">
            <span className="text-muted-foreground">{t('dataSources.systemTables.timezone')}</span>
            <Input
              value={timezone}
              onChange={(e) => setTimezone(e.target.value)}
              placeholder={FOCUS_REFRESH_TIMEZONE_DEFAULT}
            />
          </label>
        </div>
        <div className="mt-3 flex flex-wrap items-center gap-2">
          <Button
            type="button"
            disabled={setupDs.isPending || !remoteCatalog}
            onClick={onSetup}
            className="bg-(--success) text-(--background) hover:bg-(--success)/90"
          >
            {setupDs.isPending ? <Spinner /> : null}
            {t(
              hadScheduleBeforeSetup
                ? 'dataSources.systemTables.updateSchedule'
                : 'dataSources.systemTables.setupAndSchedule',
            )}
          </Button>
          {jobId !== null ? (
            <Button
              type="button"
              variant="secondary"
              disabled={runJob.isPending}
              onClick={onRunJob}
            >
              {runJob.isPending ? <Spinner /> : null}
              {t('dataSources.systemTables.runJob')}
            </Button>
          ) : null}
        </div>
        <DatabricksResourceLinks
          workspaceUrl={workspaceUrl}
          jobId={jobId}
          pipelineId={pipelineId}
          tableFqn={jobId !== null && remoteCatalog ? fqn : null}
        />
        {!remoteCatalog ? (
          <Alert className="mt-3">
            <Info />
            <AlertDescription>{t('dataSources.systemTables.catalogMissing')}</AlertDescription>
          </Alert>
        ) : null}
        {result ? (
          <Alert className="mt-3">
            <Info />
            <AlertDescription>
              {t(
                hadScheduleBeforeSetup
                  ? 'dataSources.systemTables.updateOk'
                  : 'dataSources.systemTables.setupOk',
                {
                  fqn: result.fqn,
                  jobId: String(result.jobId),
                },
              )}
            </AlertDescription>
          </Alert>
        ) : null}
        {runJob.data ? (
          <Alert className="mt-3">
            <Info />
            <AlertDescription>
              {t('dataSources.systemTables.runOk', {
                jobId: String(runJob.data.jobId),
                runId: String(runJob.data.runId),
              })}
            </AlertDescription>
          </Alert>
        ) : null}
        {setupDs.error ? (
          <Alert className="mt-3" variant="destructive">
            <Info />
            <AlertDescription>{(setupDs.error as Error).message}</AlertDescription>
          </Alert>
        ) : null}
        {runJob.error ? (
          <Alert className="mt-3" variant="destructive">
            <Info />
            <AlertDescription>{(runJob.error as Error).message}</AlertDescription>
          </Alert>
        ) : null}
      </CardContent>
    </Card>
  );
}

function DatabricksResourceLinks({
  workspaceUrl,
  jobId,
  pipelineId,
  tableFqn,
}: {
  workspaceUrl: string | null;
  jobId: number | null;
  pipelineId: string | null;
  tableFqn: string | null;
}) {
  const { t } = useI18n();
  if (jobId === null && !pipelineId && !tableFqn) return null;

  return (
    <div className="border-border bg-background/35 mt-4 rounded-md border p-3">
      <div className="text-muted-foreground mb-2 text-xs font-medium">
        {t('dataSources.systemTables.resourcesTitle')}
      </div>
      <div className="grid grid-cols-1 gap-2">
        {jobId !== null ? (
          <ResourceLink
            label={t('dataSources.systemTables.jobResource')}
            id={String(jobId)}
            href={workspaceUrl ? `${workspaceUrl}/jobs/${jobId}` : null}
          />
        ) : null}
        {pipelineId ? (
          <ResourceLink
            label={t('dataSources.systemTables.pipelineResource')}
            id={pipelineId}
            href={workspaceUrl ? `${workspaceUrl}/pipelines/${pipelineId}` : null}
          />
        ) : null}
        {tableFqn ? (
          <ResourceLink
            label={t('dataSources.systemTables.tableResource')}
            id={tableFqn}
            href={workspaceUrl ? catalogTableUrl(workspaceUrl, tableFqn) : null}
          />
        ) : null}
      </div>
    </div>
  );
}

function ResourceLink({ href, label, id }: { href: string | null; label: string; id: string }) {
  const content = (
    <>
      <span className="text-muted-foreground shrink-0 text-xs font-medium">{label}</span>
      <span className="text-foreground min-w-0 break-all font-mono text-xs" title={id}>
        {id}
      </span>
      {href ? <ExternalLink className="text-primary size-3.5 shrink-0" /> : null}
    </>
  );

  const className =
    'border-border bg-card/70 hover:border-primary focus-visible:border-primary flex min-w-0 items-center gap-2 rounded-md border px-3 py-2 text-left transition-colors';

  if (!href) {
    return <div className={className}>{content}</div>;
  }
  return (
    <a href={href} target="_blank" rel="noreferrer noopener" className={className}>
      {content}
    </a>
  );
}
