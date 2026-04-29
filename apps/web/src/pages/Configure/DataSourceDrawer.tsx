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
  Sheet,
  SheetContent,
  SheetHeader,
  SheetTitle,
  Spinner,
} from '@databricks/appkit-ui/react';
import { ExternalLink, Info } from 'lucide-react';
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
  type DataSource,
  type DataSourceSetupResult,
} from '@lakecost/shared';
import { useI18n } from '../../i18n';
import { displayNameForRow, findTemplateForRow } from './dataSourceCatalog';
import { tableLeafName, unquotedFqn } from '@lakecost/shared';

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
              ? findTemplateForRow(row)
                ? displayNameForRow(row, findTemplateForRow(row)!)
                : row.name
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

  const onDelete = async () => {
    if (!window.confirm(t('dataSources.confirmDelete', { name: row.name }))) return;
    await deleteDs.mutateAsync(row.id);
    onClose();
  };

  return (
    <>
      {row.providerName === 'Databricks' ? (
        <FocusViewSection row={row} />
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
