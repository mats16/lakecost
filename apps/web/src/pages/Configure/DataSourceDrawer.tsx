import { useEffect, useState } from 'react';
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
  useMe,
  useRunDataSourceJob,
  useSetupDataSource,
} from '../../api/hooks';
import {
  ACCOUNT_PRICES_DEFAULT,
  CATALOG_SETTING_KEY,
  FOCUS_REFRESH_CRON_DEFAULT,
  FOCUS_REFRESH_TIMEZONE_DEFAULT,
  FOCUS_VIEW_SCHEMA_DEFAULT,
  normalizeS3Prefix,
  tableLeafName,
  unquotedFqn,
  type DataSource,
  type DataSourceSetupResult,
  type ExternalLocationSummary,
} from '@lakecost/shared';
import { useI18n } from '../../i18n';
import { displayNameForRow, findTemplateForRow } from './dataSourceCatalog';
import { useAwsFocusForm } from './useAwsFocusForm';

interface Props {
  dataSourceId: number | null;
  onClose: () => void;
}

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
        <AwsFocusSection row={row} />
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

function AwsFocusSection({ row }: { row: DataSource }) {
  const form = useAwsFocusForm(row);
  return (
    <>
      <AwsSourceForm form={form} />
      {form.selectedS3Url ? <AwsTransformationSection form={form} /> : null}
    </>
  );
}

function AwsSourceForm({ form }: { form: ReturnType<typeof useAwsFocusForm> }) {
  const { t } = useI18n();
  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-sm">{t('dataSources.awsCur.title')}</CardTitle>
      </CardHeader>
      <CardContent>
        <div className="grid gap-3">
          <label className="grid gap-1 text-xs">
            <span className="text-muted-foreground">{t('dataSources.awsCur.awsAccountId')}</span>
            <Select
              value={form.awsAccountId}
              onValueChange={form.onAccountChange}
              disabled={form.storageCredentialsLoading || form.updatePending}
            >
              <SelectTrigger className="w-full">
                <SelectValue placeholder={t('dataSources.awsCur.awsAccountIdPlaceholder')} />
              </SelectTrigger>
              <SelectContent>
                {form.accountOptions.map((accountId) => (
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
              value={form.externalLocationName}
              onValueChange={form.onLocationChange}
              disabled={!form.awsAccountId || form.loadingInputs || form.updatePending}
            >
              <SelectTrigger className="w-full">
                <SelectValue placeholder={t('dataSources.awsCur.s3UrlPlaceholder')} />
              </SelectTrigger>
              <SelectContent>
                {form.linkedLocations.map((loc) => (
                  <SelectItem key={loc.name} value={loc.name}>
                    {s3UrlLabel(loc)}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </label>

          {form.selectedS3Url ? (
            <>
              <div className="grid grid-cols-1 gap-2 sm:grid-cols-2">
                <label className="grid gap-1 text-xs">
                  <span className="text-muted-foreground">{t('dataSources.awsCur.s3Prefix')}</span>
                  <Input
                    value={form.s3Prefix}
                    onChange={(e) => form.setS3Prefix(e.target.value)}
                    onBlur={() => form.setS3Prefix((value) => normalizeS3Prefix(value))}
                    placeholder="export"
                  />
                </label>
                <label className="grid gap-1 text-xs">
                  <span className="text-muted-foreground">
                    {t('dataSources.awsCur.exportName')}
                  </span>
                  <Input
                    value={form.exportName}
                    onChange={(e) => form.setExportName(e.target.value)}
                  />
                </label>
              </div>

              {form.exportDestinationPreview ? (
                <div className="text-muted-foreground break-all text-xs">
                  {t('dataSources.awsCur.exportDestination')}:{' '}
                  <span className="text-foreground font-mono">{form.exportDestinationPreview}</span>
                </div>
              ) : null}
            </>
          ) : null}

          <div className="flex flex-wrap items-center gap-3">
            <Button
              type="button"
              disabled={form.saveDisabled}
              onClick={form.onSave}
              className="bg-(--success) text-(--background) hover:bg-(--success)/90 disabled:bg-muted disabled:text-muted-foreground"
            >
              {form.updatePending ? <Spinner /> : null}
              {t('dataSources.awsCur.saveExternalLocation')}
            </Button>
            {form.savedAt && !form.dirty && !form.updatePending ? (
              <span className="text-muted-foreground text-xs">{t('settings.saved')}</span>
            ) : null}
          </div>

          {form.selectedS3Url ? <AwsExportPanel form={form} /> : null}

          {!form.storageCredentialsLoading && form.accountOptions.length === 0 ? (
            <Alert>
              <Info />
              <AlertDescription>{t('dataSources.awsCur.noStorageCredentials')}</AlertDescription>
            </Alert>
          ) : null}

          {form.awsAccountId && !form.loadingInputs && form.linkedLocations.length === 0 ? (
            <Alert>
              <Info />
              <AlertDescription>
                {t('dataSources.awsCur.noLinkedExternalLocations')}
              </AlertDescription>
            </Alert>
          ) : null}

          {form.errorMessage ? (
            <Alert variant="destructive">
              <Info />
              <AlertDescription>{form.errorMessage}</AlertDescription>
            </Alert>
          ) : null}
        </div>
      </CardContent>
    </Card>
  );
}

function AwsExportPanel({ form }: { form: ReturnType<typeof useAwsFocusForm> }) {
  const { t } = useI18n();
  return (
    <div className="border-border bg-background/35 rounded-md border">
      <button
        type="button"
        className="text-foreground hover:bg-muted/30 flex w-full items-center justify-between gap-3 rounded-md px-3 py-2 text-left text-sm font-semibold transition-colors"
        aria-expanded={form.exportPanelOpen}
        aria-controls="aws-export-panel"
        onClick={() => form.setExportPanelOpen((open) => !open)}
      >
        <span>{t('dataSources.awsCur.exportCreateSection')}</span>
        <ChevronDown
          className={`text-muted-foreground size-4 shrink-0 transition-transform ${
            form.exportPanelOpen ? 'rotate-180' : ''
          }`}
          aria-hidden="true"
        />
      </button>
      {form.exportPanelOpen ? (
        <div id="aws-export-panel" className="grid gap-3 border-t px-3 py-3">
          <div className="grid grid-cols-1 gap-2 sm:grid-cols-2">
            <label className="grid gap-1 text-xs">
              <span className="text-muted-foreground">{t('dataSources.awsCur.accessKeyId')}</span>
              <Input
                value={form.accessKeyId}
                onChange={(e) => form.setAccessKeyId(e.target.value)}
                autoComplete="off"
              />
            </label>
            <label className="grid gap-1 text-xs">
              <span className="text-muted-foreground">
                {t('dataSources.awsCur.secretAccessKey')}
              </span>
              <Input
                type="password"
                value={form.secretAccessKey}
                onChange={(e) => form.setSecretAccessKey(e.target.value)}
                autoComplete="new-password"
              />
            </label>
            <label className="grid gap-1 text-xs sm:col-span-2">
              <span className="text-muted-foreground">{t('dataSources.awsCur.sessionToken')}</span>
              <Input
                type="password"
                value={form.sessionToken}
                onChange={(e) => form.setSessionToken(e.target.value)}
                autoComplete="off"
                placeholder={t('dataSources.awsCur.sessionTokenPlaceholder')}
              />
            </label>
          </div>
          <div>
            <Button
              type="button"
              variant="secondary"
              disabled={form.createExportDisabled}
              onClick={form.onCreateExport}
            >
              {form.creatingExport ? <Spinner /> : null}
              {t('dataSources.awsCur.createExport')}
            </Button>
          </div>
          {form.exportArn ? (
            <Alert>
              <Info />
              <AlertDescription>
                {t('dataSources.awsCur.exportCreated', { exportArn: form.exportArn })}
              </AlertDescription>
            </Alert>
          ) : null}
          {form.exportError ? (
            <Alert variant="destructive">
              <Info />
              <AlertDescription>{form.exportError}</AlertDescription>
            </Alert>
          ) : null}
        </div>
      ) : null}
    </div>
  );
}

function AwsTransformationSection({ form }: { form: ReturnType<typeof useAwsFocusForm> }) {
  const { t } = useI18n();
  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-sm">{t('dataSources.systemTables.title')}</CardTitle>
      </CardHeader>
      <CardContent>
        <div className="grid grid-cols-1 gap-2 sm:grid-cols-2">
          <label className="grid gap-1 text-xs">
            <span className="text-muted-foreground">{t('dataSources.systemTables.catalog')}</span>
            <Input value={form.remoteCatalog} disabled placeholder="main" />
          </label>
          <label className="grid gap-1 text-xs">
            <span className="text-muted-foreground">{t('dataSources.systemTables.schema')}</span>
            <Input value={FOCUS_VIEW_SCHEMA_DEFAULT} disabled />
          </label>
          <label className="grid gap-1 text-xs sm:col-span-2">
            <span className="text-muted-foreground">{t('dataSources.systemTables.tableName')}</span>
            <Input value={form.tableName} onChange={(e) => form.setTableName(e.target.value)} />
          </label>
          <label className="grid gap-1 text-xs">
            <span className="text-muted-foreground">{t('dataSources.systemTables.cron')}</span>
            <Input
              value={form.cron}
              onChange={(e) => form.setCron(e.target.value)}
              placeholder={FOCUS_REFRESH_CRON_DEFAULT}
            />
          </label>
          <label className="grid gap-1 text-xs">
            <span className="text-muted-foreground">{t('dataSources.systemTables.timezone')}</span>
            <Input
              value={form.timezone}
              onChange={(e) => form.setTimezone(e.target.value)}
              placeholder={FOCUS_REFRESH_TIMEZONE_DEFAULT}
            />
          </label>
        </div>
        <div className="mt-3 flex flex-wrap items-center gap-2">
          <Button
            type="button"
            disabled={form.setupDisabled}
            onClick={form.onSetup}
            className="bg-(--success) text-(--background) hover:bg-(--success)/90 disabled:bg-muted disabled:text-muted-foreground"
          >
            {form.setupDs.isPending ? <Spinner /> : null}
            {t(
              form.hadScheduleBeforeSetup
                ? 'dataSources.systemTables.updateSchedule'
                : 'dataSources.systemTables.setupAndSchedule',
            )}
          </Button>
          {form.jobId !== null ? (
            <Button
              type="button"
              variant="secondary"
              disabled={form.runJob.isPending}
              onClick={form.onRunJob}
            >
              {form.runJob.isPending ? <Spinner /> : null}
              {t('dataSources.systemTables.runJob')}
            </Button>
          ) : null}
        </div>
        <DatabricksResourceLinks
          workspaceUrl={form.workspaceUrl}
          jobId={form.jobId}
          pipelineId={form.pipelineId}
          tableFqn={form.jobId !== null && form.remoteCatalog ? form.fqn : null}
        />
        {!form.remoteCatalog ? (
          <Alert className="mt-3">
            <Info />
            <AlertDescription>{t('dataSources.systemTables.catalogMissing')}</AlertDescription>
          </Alert>
        ) : null}
        {form.result ? (
          <Alert className="mt-3">
            <Info />
            <AlertDescription>
              {t(
                form.hadScheduleBeforeSetup
                  ? 'dataSources.systemTables.updateOk'
                  : 'dataSources.systemTables.setupOk',
                {
                  fqn: form.result.fqn,
                  jobId: String(form.result.jobId),
                },
              )}
            </AlertDescription>
          </Alert>
        ) : null}
        {form.runJob.data ? (
          <Alert className="mt-3">
            <Info />
            <AlertDescription>
              {t('dataSources.systemTables.runOk', {
                jobId: String(form.runJob.data.jobId),
                runId: String(form.runJob.data.runId),
              })}
            </AlertDescription>
          </Alert>
        ) : null}
        {form.setupDs.error ? (
          <Alert className="mt-3" variant="destructive">
            <Info />
            <AlertDescription>{(form.setupDs.error as Error).message}</AlertDescription>
          </Alert>
        ) : null}
        {form.runJob.error ? (
          <Alert className="mt-3" variant="destructive">
            <Info />
            <AlertDescription>{(form.runJob.error as Error).message}</AlertDescription>
          </Alert>
        ) : null}
      </CardContent>
    </Card>
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
