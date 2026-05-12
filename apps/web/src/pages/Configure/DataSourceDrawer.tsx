import { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
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
  cn,
} from '@databricks/appkit-ui/react';
import { Check, ExternalLink, Info, Pencil, X } from 'lucide-react';
import {
  useAppSettings,
  useCreateDataSource,
  useDataSource,
  useDeleteDataSource,
  useMe,
  useRunDataSourceJob,
  useSetupDataSource,
  useUpdateDataSource,
} from '../../api/hooks';
import {
  ACCOUNT_PRICES_DEFAULT,
  CATALOG_SETTING_KEY,
  LAKEFLOW_PIPELINE_SETTING_KEYS,
  medallionSchemaNamesFromSettings,
  normalizeS3Prefix,
  s3BucketFromUrl,
  tableLeafName,
  unquotedFqn,
  type DataSource,
  type DataSourceSetupResult,
  type ExternalLocationSummary,
} from '@finlake/shared';
import { useI18n } from '../../i18n';
import { displayNameForRow, findTemplateById, findTemplateForRow } from './dataSourceCatalog';
import type { DatabricksFocusDraft } from './DataSources';
import { type AwsFocusDraft, useAwsFocusForm } from './useAwsFocusForm';
import { numberSetting } from './utils';

const AWS_BCM_DATA_EXPORTS_URL =
  'https://us-east-1.console.aws.amazon.com/costmanagement/home#/bcm-data-exports';

interface Props {
  dataSourceId: number | null;
  draftAwsSource?: AwsFocusDraft | null;
  draftDatabricksSource?: DatabricksFocusDraft | null;
  onClose: () => void;
  onCreated?: (row: DataSource) => void;
}

type ResourceStepStatus = 'idle' | 'pending' | 'done' | 'skipped' | 'error';

interface ResourceStep {
  id: string;
  status: ResourceStepStatus;
  detail: string | null;
  href: string | null;
}

function catalogTableUrl(workspaceUrl: string, fqn: string): string {
  return `${workspaceUrl}/explore/data/${fqn.split('.').map(encodeURIComponent).join('/')}`;
}

export function DataSourceDrawer({
  dataSourceId,
  draftAwsSource,
  draftDatabricksSource,
  onClose,
  onCreated,
}: Props) {
  const { t } = useI18n();
  const ds = useDataSource(dataSourceId ?? undefined);
  const isOpen = dataSourceId !== null || Boolean(draftAwsSource) || Boolean(draftDatabricksSource);
  const row = ds.data;
  const template = row
    ? findTemplateForRow(row)
    : draftAwsSource
      ? findTemplateById(draftAwsSource.templateId)
      : draftDatabricksSource
        ? findTemplateById(draftDatabricksSource.templateId)
        : undefined;
  const descriptionKey =
    template?.id === 'databricks_focus13'
      ? 'dataSources.systemTables.focusViewDesc'
      : template?.id === 'aws'
        ? 'dataSources.aws.description'
        : null;

  return (
    <Sheet open={isOpen} onOpenChange={(open) => (open ? null : onClose())}>
      <SheetContent
        side="right"
        className="w-full max-w-(--container-md) sm:max-w-xl"
        onOpenAutoFocus={(e) => e.preventDefault()}
      >
        <SheetHeader>
          <DataSourceDrawerTitle
            row={row}
            title={
              row
                ? template
                  ? displayNameForRow(row, template)
                  : row.name
                : draftAwsSource
                  ? (template?.name ?? draftAwsSource.name)
                  : draftDatabricksSource
                    ? (template?.name ?? draftDatabricksSource.name)
                    : t('common.loading')
            }
          />
        </SheetHeader>
        <div className="flex flex-col gap-4 overflow-auto px-4 pb-6">
          {descriptionKey ? (
            <p className="text-muted-foreground text-sm">{t(descriptionKey)}</p>
          ) : null}
          {row ? <Configurator row={row} onClose={onClose} /> : null}
          {!row && draftAwsSource ? (
            <AwsFocusSection row={null} draft={draftAwsSource} onCreated={onCreated} />
          ) : null}
          {!row && draftDatabricksSource ? (
            <FocusViewSection row={null} draft={draftDatabricksSource} onCreated={onCreated} />
          ) : null}
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
          className="warning-action-button"
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

function DataSourceDrawerTitle({ row, title }: { row?: DataSource; title: string }) {
  const { t } = useI18n();
  const updateDs = useUpdateDataSource();
  const [editing, setEditing] = useState(false);
  const [name, setName] = useState(title);
  const trimmedName = name.trim();
  const dirty = trimmedName !== title;

  useEffect(() => {
    if (!editing) setName(title);
  }, [editing, title]);

  const onSave = async () => {
    if (!row || !trimmedName || !dirty) return;
    await updateDs.mutateAsync({ id: row.id, body: { name: trimmedName } });
    setEditing(false);
  };

  const onCancel = () => {
    setName(title);
    setEditing(false);
  };

  if (editing && row) {
    return (
      <div className="grid gap-2">
        <SheetTitle className="sr-only">{title}</SheetTitle>
        <form
          className="flex min-w-0 items-center gap-2 pr-8"
          onSubmit={(event) => {
            event.preventDefault();
            void onSave();
          }}
        >
          <Input
            value={name}
            onChange={(e) => setName(e.target.value)}
            disabled={updateDs.isPending}
            autoFocus
            aria-label={t('dataSources.name.label')}
            className="h-14 min-w-0 text-4xl font-semibold"
          />
          <Button
            type="submit"
            disabled={updateDs.isPending || !trimmedName || !dirty}
            aria-label={t('dataSources.name.save')}
            className="success-action-button size-10 shrink-0 p-0"
          >
            {updateDs.isPending ? <Spinner /> : <Check className="size-4" aria-hidden="true" />}
          </Button>
          <Button
            type="button"
            variant="secondary"
            disabled={updateDs.isPending}
            onClick={onCancel}
            aria-label={t('common.cancel')}
            className="size-10 shrink-0 p-0"
          >
            <X className="size-4" aria-hidden="true" />
          </Button>
        </form>
        {updateDs.error ? (
          <Alert variant="destructive">
            <Info />
            <AlertDescription>{(updateDs.error as Error).message}</AlertDescription>
          </Alert>
        ) : null}
      </div>
    );
  }

  return (
    <SheetTitle>
      <span className="inline-flex min-w-0 items-center gap-2 pr-8 align-middle">
        <span className="truncate">{title}</span>
        {row ? (
          <button
            type="button"
            className="text-muted-foreground hover:text-foreground hover:bg-muted/40 inline-grid size-9 shrink-0 place-items-center rounded-md transition-colors"
            aria-label={t('dataSources.name.edit')}
            onClick={() => setEditing(true)}
          >
            <Pencil className="size-4" aria-hidden="true" />
          </button>
        ) : null}
      </span>
    </SheetTitle>
  );
}

function AwsFocusSection({
  row,
  draft,
  onCreated,
}: {
  row: DataSource | null;
  draft?: AwsFocusDraft;
  onCreated?: (row: DataSource) => void;
}) {
  const form = useAwsFocusForm(row, { draft, onCreated });
  return (
    <>
      <AwsSourceForm form={form} />
      {form.persisted && form.selectedS3Url ? <AwsTransformationSection form={form} /> : null}
    </>
  );
}

function AwsSourceForm({ form }: { form: ReturnType<typeof useAwsFocusForm> }) {
  const { t } = useI18n();
  const credentialsUrl = form.workspaceUrl ? `${form.workspaceUrl}/explore/credentials` : null;
  const serviceCredentialsUrl = '/credentials';
  const externalLocationsUrl = form.workspaceUrl ? `${form.workspaceUrl}/explore/locations` : null;
  return (
    <Card>
      <CardHeader>
        <div className="flex items-center gap-2">
          <CardTitle className="text-sm">{t('dataSources.aws.title')}</CardTitle>
          <a
            href={AWS_BCM_DATA_EXPORTS_URL}
            target="_blank"
            rel="noreferrer"
            aria-label={t('dataSources.aws.openDataExports')}
            className="text-muted-foreground hover:text-foreground inline-flex size-5 items-center justify-center rounded-sm transition-colors"
          >
            <ExternalLink className="size-3.5" aria-hidden="true" />
          </a>
        </div>
      </CardHeader>
      <CardContent>
        <div className="grid gap-3">
          {!form.registered ? (
            <div className="grid gap-1 text-xs">
              <span className="text-muted-foreground">{t('dataSources.aws.setupMode')}</span>
              <Select
                value={form.setupMode}
                onValueChange={(value) => form.onSetupModeChange(value as typeof form.setupMode)}
                disabled={form.savePending || form.creatingExport}
              >
                <SelectTrigger className="w-full">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="create">
                    {t('dataSources.aws.createExternalLocationAndExport')}
                  </SelectItem>
                  <SelectItem value="existing">
                    {t('dataSources.aws.useExistingExternalLocation')}
                  </SelectItem>
                </SelectContent>
              </Select>
            </div>
          ) : null}

          <label className="grid gap-1 text-xs">
            <span className="text-muted-foreground">
              {t('dataSources.aws.awsAccountId')} ({t('dataSources.aws.from')}{' '}
              {form.setupMode === 'create' ? (
                <SourceLabelLink href={serviceCredentialsUrl}>
                  {t('dataSources.aws.finLakeServiceRoleSource')}
                </SourceLabelLink>
              ) : (
                <SourceLabelLink href={credentialsUrl}>
                  {t('dataSources.aws.storageCredentialSource')}
                </SourceLabelLink>
              )}
              )
            </span>
            <Select
              value={form.awsAccountId}
              onValueChange={form.onAccountChange}
              disabled={form.registered || form.loadingInputs || form.savePending}
            >
              <SelectTrigger className="w-full">
                <SelectValue
                  placeholder={
                    form.setupMode === 'create'
                      ? t('dataSources.aws.serviceAccountIdPlaceholder')
                      : t('dataSources.aws.awsAccountIdPlaceholder')
                  }
                />
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

          {form.setupMode === 'existing' ? (
            <label className="grid gap-1 text-xs">
              <span className="text-muted-foreground">
                {t('dataSources.aws.s3Bucket')} ({t('dataSources.aws.from')}{' '}
                <SourceLabelLink href={externalLocationsUrl}>
                  {t('dataSources.aws.externalLocationSource')}
                </SourceLabelLink>
                )
              </span>
              <Select
                value={form.externalLocationName}
                onValueChange={form.onLocationChange}
                disabled={
                  form.registered || !form.awsAccountId || form.loadingInputs || form.savePending
                }
              >
                <SelectTrigger className="w-full">
                  <SelectValue placeholder={t('dataSources.aws.s3UrlPlaceholder')} />
                </SelectTrigger>
                <SelectContent>
                  {form.locationOptions.map((loc) => (
                    <SelectItem key={loc.name} value={loc.name}>
                      {s3BucketLabel(loc)}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </label>
          ) : (
            <div className="grid gap-2">
              <div className="grid grid-cols-1 items-end gap-2 sm:grid-cols-[minmax(0,1fr)_auto]">
                <label className="grid gap-1 text-xs">
                  <span className="text-muted-foreground">{t('dataSources.aws.s3Bucket')}</span>
                  <Input
                    value={form.createBucketName}
                    onChange={(e) => form.setCreateBucketName(e.target.value)}
                    disabled={form.registered || form.savePending || form.creatingExport}
                    placeholder={form.awsAccountId ? `finlake-${form.awsAccountId}` : 'finlake-'}
                  />
                </label>
                {!form.registered ? (
                  <label className="flex min-h-9 items-center gap-2 text-xs sm:pb-2">
                    <input
                      type="checkbox"
                      checked={form.createBucketIfMissing}
                      disabled={form.savePending || form.creatingExport}
                      onChange={(e) => form.setCreateBucketIfMissing(e.target.checked)}
                    />
                    <span className="text-muted-foreground whitespace-nowrap">
                      {t('dataSources.aws.createBucketIfMissing')}
                    </span>
                  </label>
                ) : null}
              </div>
              {form.createBucketName && !form.selectedS3Bucket ? (
                <Alert>
                  <Info />
                  <AlertDescription>{t('dataSources.aws.invalidBucketName')}</AlertDescription>
                </Alert>
              ) : null}
            </div>
          )}

          {form.selectedS3Url ? (
            <>
              <div className="grid grid-cols-1 gap-2 sm:grid-cols-2">
                <label className="grid gap-1 text-xs">
                  <span className="text-muted-foreground">{t('dataSources.aws.s3PathPrefix')}</span>
                  <S3PrefixInput
                    form={form}
                    disabled={form.registered || form.savePending}
                    placeholder="bcm-data-export"
                  />
                </label>
                <label className="grid gap-1 text-xs">
                  <span className="text-muted-foreground">{t('dataSources.aws.exportName')}</span>
                  <Input
                    value={form.exportName}
                    onChange={(e) => form.setExportName(e.target.value)}
                    disabled={form.registered || form.savePending}
                  />
                </label>
              </div>

              {form.exportDestinationPreview ? (
                <div className="text-muted-foreground break-all text-xs">
                  {t('dataSources.aws.exportDestination')}:{' '}
                  <span className="text-foreground font-mono">{form.exportDestinationPreview}</span>
                </div>
              ) : null}
            </>
          ) : null}

          {!form.registered && form.setupMode === 'existing' ? (
            <div className="flex flex-wrap items-center gap-3">
              <Button type="button" disabled={form.saveDisabled} onClick={form.onSave}>
                {form.savePending ? <Spinner /> : null}
                {t('dataSources.aws.saveExternalLocation')}
              </Button>
              {form.savedAt && !form.dirty && !form.savePending ? (
                <span className="text-muted-foreground text-xs">{t('settings.saved')}</span>
              ) : null}
            </div>
          ) : null}

          {!form.registered && form.setupMode === 'create' && form.selectedS3Url ? (
            <div className="grid gap-3">
              <div className="flex flex-wrap items-center gap-3">
                <AwsExportPanel form={form} />
                {form.savedAt && !form.dirty && !form.savePending ? (
                  <span className="text-muted-foreground text-xs">{t('settings.saved')}</span>
                ) : null}
              </div>
              <AwsCreateResourceProgressModal form={form} />
            </div>
          ) : null}

          {!form.registered &&
          form.setupMode === 'existing' &&
          !form.storageCredentialsLoading &&
          form.accountOptions.length === 0 ? (
            <Alert>
              <Info />
              <AlertDescription>{t('dataSources.aws.noStorageCredentials')}</AlertDescription>
            </Alert>
          ) : null}

          {!form.registered &&
          form.setupMode === 'create' &&
          !form.serviceCredentialsLoading &&
          form.serviceAccountOptions.length === 0 ? (
            <Alert>
              <Info />
              <AlertDescription>{t('dataSources.aws.noServiceCredentials')}</AlertDescription>
            </Alert>
          ) : null}

          {!form.registered &&
          form.setupMode === 'existing' &&
          form.awsAccountId &&
          !form.loadingInputs &&
          form.linkedLocations.length === 0 ? (
            <Alert>
              <Info />
              <AlertDescription>{t('dataSources.aws.noLinkedExternalLocations')}</AlertDescription>
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

function SourceLabelLink({ href, children }: { href: string | null; children: React.ReactNode }) {
  const className = 'border-current border-b pb-px';

  if (!href) return <span className={className}>{children}</span>;
  if (href.startsWith('/')) {
    return (
      <Link to={href} className={`hover:text-foreground ${className}`}>
        {children}
      </Link>
    );
  }

  return (
    <a
      href={href}
      target="_blank"
      rel="noreferrer"
      className={`hover:text-foreground ${className}`}
    >
      {children}
    </a>
  );
}

function AwsExportPanel({ form }: { form: ReturnType<typeof useAwsFocusForm> }) {
  const { t } = useI18n();
  if (form.setupMode === 'create') {
    return (
      <Button type="button" disabled={form.createExportDisabled} onClick={form.onCreateExport}>
        {form.creatingExport ? <Spinner /> : null}
        {t('dataSources.aws.createExport')}
      </Button>
    );
  }

  return (
    <>
      <Button type="button" onClick={() => form.openExportModal(true)}>
        {t('dataSources.aws.exportCreateSection')}
      </Button>
      <AwsExportModal form={form} />
    </>
  );
}

function AwsCreateResourceProgressModal({ form }: { form: ReturnType<typeof useAwsFocusForm> }) {
  const { t } = useI18n();

  return (
    <ResourceProgressModal
      open={form.createProgressModalOpen}
      titleId="aws-create-progress-title"
      title={t('dataSources.aws.resourceProgress')}
      steps={form.createResourceSteps}
      stepLabelPrefix="dataSources.aws.resourceSteps"
      statusLabelPrefix="dataSources.aws.resourceStepStatus"
      error={form.exportError}
      closeDisabled={form.creatingExport}
      onClose={form.closeCreateProgressModal}
    />
  );
}

function ResourceProgressModal({
  open,
  titleId,
  title,
  steps,
  stepLabelPrefix,
  statusLabelPrefix,
  error,
  closeDisabled,
  onClose,
}: {
  open: boolean;
  titleId: string;
  title: string;
  steps: ResourceStep[];
  stepLabelPrefix: string;
  statusLabelPrefix: string;
  error?: string | null;
  closeDisabled?: boolean;
  onClose: () => void;
}) {
  const { t } = useI18n();
  if (!open) return null;

  return (
    <div
      className="fixed inset-0 z-[75] flex items-center justify-center bg-black/55 p-4"
      role="presentation"
    >
      <div
        role="dialog"
        aria-modal="true"
        aria-labelledby={titleId}
        className="bg-background border-border grid w-full max-w-xl gap-4 rounded-lg border p-5 shadow-xl"
        onMouseDown={(event) => event.stopPropagation()}
      >
        <div className="flex items-center justify-between gap-3">
          <h3 id={titleId} className="text-base font-semibold">
            {title}
          </h3>
          <button
            type="button"
            className="text-muted-foreground hover:text-foreground hover:bg-muted/40 grid size-8 place-items-center rounded-md transition-colors disabled:opacity-50"
            aria-label={t('common.close')}
            disabled={closeDisabled}
            onClick={onClose}
          >
            <X className="size-4" aria-hidden="true" />
          </button>
        </div>
        <CreateResourceChecklist
          steps={steps}
          stepLabelPrefix={stepLabelPrefix}
          statusLabelPrefix={statusLabelPrefix}
        />
        {error ? (
          <Alert variant="destructive">
            <Info />
            <AlertDescription>{error}</AlertDescription>
          </Alert>
        ) : null}
        {!closeDisabled ? (
          <div className="flex justify-end">
            <Button type="button" variant="secondary" onClick={onClose}>
              {t('common.close')}
            </Button>
          </div>
        ) : null}
      </div>
    </div>
  );
}

function CreateResourceChecklist({
  steps,
  stepLabelPrefix,
  statusLabelPrefix,
}: {
  steps: ResourceStep[];
  stepLabelPrefix: string;
  statusLabelPrefix: string;
}) {
  const { t } = useI18n();

  return (
    <div className="grid gap-2">
      {steps.map((step) => (
        <div key={step.id} className="grid grid-cols-[1rem_minmax(0,1fr)_auto] items-center gap-2">
          <CreateResourceStepIcon status={step.status} />
          <div className="min-w-0">
            <div className="text-sm">{t(`${stepLabelPrefix}.${step.id}`)}</div>
            <CreateResourceStepDetail step={step} />
          </div>
          <span className="text-muted-foreground text-xs">
            {t(`${statusLabelPrefix}.${step.status}`)}
          </span>
        </div>
      ))}
    </div>
  );
}

function CreateResourceStepDetail({ step }: { step: ResourceStep }) {
  if (!step.detail) return null;

  const href =
    step.href && (step.status === 'done' || step.status === 'skipped') ? step.href : null;
  const className =
    'text-muted-foreground inline-flex max-w-full items-center gap-1 truncate font-mono text-xs';

  if (!href) {
    return <div className="text-muted-foreground truncate font-mono text-xs">{step.detail}</div>;
  }

  return (
    <a
      href={href}
      target="_blank"
      rel="noreferrer noopener"
      className={`${className} hover:text-foreground`}
      title={step.detail}
    >
      <span className="truncate">{step.detail}</span>
      <ExternalLink className="size-3 shrink-0" aria-hidden="true" />
    </a>
  );
}

function CreateResourceStepIcon({ status }: { status: ResourceStepStatus }) {
  if (status === 'pending') return <Spinner />;
  if (status === 'done') {
    return (
      <span className="bg-(--success) text-(--background) grid size-4 place-items-center rounded-full">
        <Check className="size-3" aria-hidden="true" />
      </span>
    );
  }
  if (status === 'skipped') {
    return (
      <span className="border-muted-foreground/50 text-muted-foreground grid size-4 place-items-center rounded-full border">
        <Check className="size-3" aria-hidden="true" />
      </span>
    );
  }
  if (status === 'error') {
    return (
      <span className="bg-destructive text-destructive-foreground grid size-4 place-items-center rounded-full">
        <X className="size-3" aria-hidden="true" />
      </span>
    );
  }
  return <span className="border-muted-foreground/40 size-4 rounded-full border" />;
}

function AwsExportModal({ form }: { form: ReturnType<typeof useAwsFocusForm> }) {
  const { t } = useI18n();
  const { exportModalOpen, openExportModal } = form;

  useEffect(() => {
    if (!exportModalOpen) return;
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') openExportModal(false);
    };
    window.addEventListener('keydown', onKeyDown);
    return () => window.removeEventListener('keydown', onKeyDown);
  }, [exportModalOpen, openExportModal]);

  if (!exportModalOpen) return null;

  return (
    <div
      className="fixed inset-0 z-[70] flex items-center justify-center bg-black/55 p-4"
      role="presentation"
      onMouseDown={() => openExportModal(false)}
    >
      <div
        role="dialog"
        aria-modal="true"
        aria-labelledby="aws-export-modal-title"
        className="bg-background border-border grid w-full max-w-lg gap-4 rounded-lg border p-5 shadow-xl"
        onMouseDown={(event) => event.stopPropagation()}
      >
        <div className="flex items-center justify-between gap-3">
          <h3 id="aws-export-modal-title" className="text-base font-semibold">
            {t(
              form.setupMode === 'create'
                ? 'dataSources.aws.createExternalLocationAndExport'
                : 'dataSources.aws.exportCreateSection',
            )}
          </h3>
          <button
            type="button"
            className="text-muted-foreground hover:text-foreground hover:bg-muted/40 grid size-8 place-items-center rounded-md transition-colors"
            aria-label={t('common.close')}
            onClick={() => openExportModal(false)}
          >
            <X className="size-4" aria-hidden="true" />
          </button>
        </div>
        {form.setupMode === 'create' && form.createBucketIfMissing ? (
          <Alert>
            <Info />
            <AlertDescription>
              {t('dataSources.aws.bucketWillBeCreated', {
                bucket: form.selectedS3Bucket ?? '',
              })}
            </AlertDescription>
          </Alert>
        ) : null}
        <div className="grid gap-2">
          <label className="grid gap-1 text-xs">
            <span className="text-muted-foreground">{t('dataSources.aws.exportName')}</span>
            <Input
              value={form.exportName}
              onChange={(e) => form.setExportName(e.target.value)}
              disabled={form.creatingExport}
            />
          </label>
          <div className="grid grid-cols-1 gap-2 sm:grid-cols-2">
            <label className="grid gap-1 text-xs">
              <span className="text-muted-foreground">{t('dataSources.aws.s3Bucket')}</span>
              <Input
                value={
                  form.setupMode === 'create'
                    ? form.createBucketName
                    : (form.selectedS3Bucket ?? '')
                }
                onChange={(e) => form.setCreateBucketName(e.target.value)}
                disabled={form.setupMode !== 'create' || form.creatingExport}
              />
            </label>
            <label className="grid gap-1 text-xs">
              <span className="text-muted-foreground">{t('dataSources.aws.s3PathPrefix')}</span>
              <S3PrefixInput
                form={form}
                disabled={form.creatingExport}
                placeholder="bcm-data-export"
              />
            </label>
          </div>
        </div>
        {form.setupMode === 'existing' ? (
          <>
            <div className="border-border border-t" />
            <div className="grid grid-cols-1 gap-2 sm:grid-cols-2">
              <label className="grid gap-1 text-xs">
                <span className="text-muted-foreground">{t('dataSources.aws.accessKeyId')}</span>
                <Input
                  value={form.accessKeyId}
                  onChange={(e) => form.setAccessKeyId(e.target.value)}
                  autoComplete="off"
                />
              </label>
              <label className="grid gap-1 text-xs">
                <span className="text-muted-foreground">
                  {t('dataSources.aws.secretAccessKey')}
                </span>
                <Input
                  type="password"
                  value={form.secretAccessKey}
                  onChange={(e) => form.setSecretAccessKey(e.target.value)}
                  autoComplete="new-password"
                />
              </label>
              <label className="grid gap-1 text-xs sm:col-span-2">
                <span className="text-muted-foreground">{t('dataSources.aws.sessionToken')}</span>
                <Input
                  type="password"
                  value={form.sessionToken}
                  onChange={(e) => form.setSessionToken(e.target.value)}
                  autoComplete="off"
                  placeholder={t('dataSources.aws.sessionTokenPlaceholder')}
                />
              </label>
            </div>
          </>
        ) : null}
        <p className="text-muted-foreground text-xs">
          {t(
            form.setupMode === 'create'
              ? 'dataSources.aws.serviceCredentialUsed'
              : 'dataSources.aws.credentialsNotSaved',
          )}
        </p>
        <div className="flex justify-end">
          <Button type="button" disabled={form.createExportDisabled} onClick={form.onCreateExport}>
            {form.creatingExport ? <Spinner /> : null}
            {t(
              form.setupMode === 'create'
                ? 'dataSources.aws.createExternalLocationAndExportAction'
                : 'dataSources.aws.createExport',
            )}
          </Button>
        </div>
        {form.exportArn ? (
          <Alert>
            <Info />
            <AlertDescription>
              {t('dataSources.aws.exportCreated', { exportArn: form.exportArn })}
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
    </div>
  );
}

function S3PrefixInput({
  form,
  disabled,
  placeholder,
}: {
  form: ReturnType<typeof useAwsFocusForm>;
  disabled?: boolean;
  placeholder?: string;
}) {
  if (!form.selectedS3BasePrefix) {
    return (
      <Input
        value={form.s3Prefix}
        onChange={(e) => form.setS3Prefix(e.target.value)}
        onBlur={() => form.setS3Prefix((value) => normalizeS3Prefix(value))}
        placeholder={placeholder}
        disabled={disabled}
      />
    );
  }

  return (
    <div
      className={cn(
        'border-input bg-background flex h-9 min-w-0 items-center overflow-hidden rounded-md border text-sm',
        disabled && 'opacity-50',
      )}
    >
      <span className="bg-muted/40 text-muted-foreground border-border max-w-[55%] shrink-0 overflow-hidden border-r px-3 py-2 font-mono text-xs text-ellipsis whitespace-nowrap">
        {form.selectedS3BasePrefix}/
      </span>
      <input
        value={form.s3Prefix}
        onChange={(e) => form.setS3Prefix(e.target.value)}
        onBlur={() => form.setS3Prefix((value) => normalizeS3Prefix(value))}
        placeholder={placeholder}
        disabled={disabled}
        className="placeholder:text-muted-foreground min-w-0 flex-1 bg-transparent px-3 py-2 font-mono text-xs outline-none disabled:cursor-not-allowed"
      />
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
          <label className="grid gap-1 text-xs sm:col-span-2">
            <span className="text-muted-foreground">{t('dataSources.systemTables.tableName')}</span>
            <Input value={form.tableName} disabled />
          </label>
        </div>
        <div className="mt-3 flex flex-wrap items-center gap-2">
          <Button
            type="button"
            disabled={form.setupDisabled}
            onClick={form.onSetup}
            className={form.hadScheduleBeforeSetup ? 'success-action-button' : undefined}
          >
            {form.setupDs.isPending ? <Spinner /> : null}
            {t(
              form.hadScheduleBeforeSetup
                ? 'dataSources.systemTables.updateSchedule'
                : 'dataSources.systemTables.setupAndSchedule',
            )}
          </Button>
          {form.sourceSetup && form.jobId !== null ? (
            <Button
              type="button"
              variant="secondary"
              className="hover:bg-(--secondary-foreground) hover:text-(--secondary)"
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
          tableFqn={form.sourceSetup && form.remoteCatalog ? form.fqn : null}
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

function s3BucketLabel(location: ExternalLocationSummary): string {
  return location.url ? (s3BucketFromUrl(location.url) ?? location.url) : location.name;
}

type DatabricksSetupStepId = 'systemGrants' | 'lakeflowJob';

function systemCatalogPermissionsUrl(workspaceUrl: string | null): string | null {
  return workspaceUrl ? `${workspaceUrl}/explore/data/system?activeTab=permissions` : null;
}

function databricksPipelineUrl(
  workspaceUrl: string | null,
  pipelineId: string | null,
): string | null {
  return workspaceUrl && pipelineId ? `${workspaceUrl}/pipelines/${pipelineId}` : null;
}

function initialDatabricksSetupSteps(): ResourceStep[] {
  return [
    { id: 'systemGrants', status: 'idle', detail: null, href: null },
    { id: 'lakeflowJob', status: 'idle', detail: null, href: null },
  ];
}

function updateDatabricksSetupSteps(
  steps: ResourceStep[],
  updates: Partial<Record<DatabricksSetupStepId, Partial<Omit<ResourceStep, 'id'>>>>,
): ResourceStep[] {
  return steps.map((step) => {
    const update = updates[step.id as DatabricksSetupStepId];
    return update ? { ...step, ...update } : step;
  });
}

function databricksErrorStep(message: string): DatabricksSetupStepId {
  if (/grant|system table|system\.|USE CATALOG|USE SCHEMA|SELECT/i.test(message)) {
    return 'systemGrants';
  }
  return 'lakeflowJob';
}

function FocusViewSection({
  row,
  draft,
  onCreated,
}: {
  row: DataSource | null;
  draft?: DatabricksFocusDraft;
  onCreated?: (row: DataSource) => void;
}) {
  const { t } = useI18n();
  const me = useMe();
  const settings = useAppSettings();
  const createDs = useCreateDataSource();
  const setupDs = useSetupDataSource();
  const runJob = useRunDataSourceJob();

  const remoteCatalog = settings.data?.settings[CATALOG_SETTING_KEY] ?? '';
  const silverSchema = medallionSchemaNamesFromSettings(settings.data?.settings ?? {}).silver;
  const remoteAccountPrices =
    (row?.config.accountPricesTable as string | undefined) ?? ACCOUNT_PRICES_DEFAULT;
  const [tableName, setTableName] = useState(
    tableLeafName(row?.tableName ?? draft?.tableName ?? 'databricks_usage'),
  );
  const [accountPrices, setAccountPrices] = useState(remoteAccountPrices);
  const [result, setResult] = useState<DataSourceSetupResult | null>(null);
  const [setupSteps, setSetupSteps] = useState<ResourceStep[]>(initialDatabricksSetupSteps);
  const [setupProgressModalOpen, setSetupProgressModalOpen] = useState(false);
  const [createdRowAfterSetup, setCreatedRowAfterSetup] = useState<DataSource | null>(null);
  const [lastSetupWasUpdate, setLastSetupWasUpdate] = useState(false);
  const sharedJobId = numberSetting(settings.data?.settings[LAKEFLOW_PIPELINE_SETTING_KEYS.jobId]);
  const jobId = result?.jobId ?? sharedJobId;
  const workspaceUrl = me.data?.workspaceUrl ?? null;
  const isSetup = Boolean(row?.enabled || result);

  useEffect(
    () => setTableName(tableLeafName(row?.tableName ?? draft?.tableName ?? 'databricks_usage')),
    [draft?.tableName, row?.tableName],
  );
  useEffect(() => setAccountPrices(remoteAccountPrices), [remoteAccountPrices]);
  useEffect(() => {
    setSetupSteps(initialDatabricksSetupSteps());
    setSetupProgressModalOpen(false);
    setCreatedRowAfterSetup(null);
    setLastSetupWasUpdate(Boolean(row?.enabled));
  }, [row?.id]);

  const fqn = remoteCatalog
    ? unquotedFqn(remoteCatalog, silverSchema, tableName)
    : `${silverSchema}.${tableName}`;

  const onSetup = async () => {
    setResult(null);
    setLastSetupWasUpdate(Boolean(row?.enabled));
    setSetupProgressModalOpen(true);
    setCreatedRowAfterSetup(null);
    setSetupSteps(
      updateDatabricksSetupSteps(initialDatabricksSetupSteps(), {
        systemGrants: {
          status: 'pending',
          detail: 'system',
          href: systemCatalogPermissionsUrl(workspaceUrl),
        },
        lakeflowJob: { status: 'idle', detail: null },
      }),
    );
    let dataSource = row;
    try {
      if (!dataSource) {
        if (!draft) throw new Error('Missing draft data source configuration.');
        const created = await createDs.mutateAsync({
          templateId: draft.templateId,
          name: draft.name,
          providerName: draft.providerName,
          tableName,
          enabled: false,
          config: {
            accountPricesTable: accountPrices,
          },
        });
        dataSource = created;
        setCreatedRowAfterSetup(created);
      }

      const r = await setupDs.mutateAsync({
        id: dataSource.id,
        body: {
          tableName,
          accountPricesTable: accountPrices,
        },
      });
      setResult(r);
      setSetupSteps((steps) =>
        updateDatabricksSetupSteps(steps, {
          systemGrants: {
            status: 'done',
            detail: 'system',
            href: systemCatalogPermissionsUrl(workspaceUrl),
          },
          lakeflowJob: {
            status: 'done',
            detail: r.pipelineId,
            href: databricksPipelineUrl(workspaceUrl, r.pipelineId),
          },
        }),
      );
    } catch (err) {
      const message = (err as Error).message;
      const serverStep = (err as { step?: string }).step;
      const failed: DatabricksSetupStepId =
        serverStep === 'systemGrants' || serverStep === 'lakeflowJob'
          ? serverStep
          : databricksErrorStep(message);
      setSetupSteps((steps) =>
        updateDatabricksSetupSteps(steps, {
          [failed]: { status: 'error', detail: message },
          ...(failed === 'systemGrants' ? { lakeflowJob: { status: 'idle', detail: null } } : {}),
        }),
      );
    }
  };

  const onRunJob = async () => {
    if (!row) return;
    await runJob.mutateAsync(row.id);
  };
  const setupBusy = setupDs.isPending || createDs.isPending;
  const setupErrorMessage =
    (createDs.error as Error | null)?.message ?? (setupDs.error as Error | null)?.message ?? null;
  const closeSetupProgressModal = () => {
    if (setupBusy) return;
    setSetupProgressModalOpen(false);
    if (createdRowAfterSetup) {
      onCreated?.(createdRowAfterSetup);
      setCreatedRowAfterSetup(null);
    }
  };

  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-sm">{t('dataSources.systemTables.title')}</CardTitle>
      </CardHeader>
      <CardContent>
        <div className="grid grid-cols-1 gap-2 sm:grid-cols-2">
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
        </div>
        <div className="mt-3 flex flex-wrap items-center gap-2">
          <Button
            type="button"
            disabled={setupBusy || !remoteCatalog || tableName.trim() === ''}
            onClick={onSetup}
            className={isSetup ? 'success-action-button' : undefined}
          >
            {setupBusy ? <Spinner /> : null}
            {t(
              isSetup
                ? 'dataSources.systemTables.updateSchedule'
                : 'dataSources.systemTables.setupAndSchedule',
            )}
          </Button>
          {row?.enabled && jobId !== null ? (
            <Button
              type="button"
              variant="secondary"
              className="hover:bg-(--secondary-foreground) hover:text-(--secondary)"
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
          tableFqn={isSetup && remoteCatalog ? fqn : null}
        />
        <DatabricksSetupProgressModal
          open={setupProgressModalOpen}
          steps={setupSteps}
          error={setupErrorMessage}
          closeDisabled={setupBusy}
          onClose={closeSetupProgressModal}
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
                lastSetupWasUpdate
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
        {createDs.error ? (
          <Alert className="mt-3" variant="destructive">
            <Info />
            <AlertDescription>{(createDs.error as Error).message}</AlertDescription>
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

function DatabricksSetupProgressModal({
  open,
  steps,
  error,
  closeDisabled,
  onClose,
}: {
  open: boolean;
  steps: ResourceStep[];
  error: string | null;
  closeDisabled: boolean;
  onClose: () => void;
}) {
  const { t } = useI18n();

  return (
    <ResourceProgressModal
      open={open}
      titleId="databricks-create-progress-title"
      title={t('dataSources.systemTables.resourceProgress')}
      steps={steps}
      stepLabelPrefix="dataSources.systemTables.resourceSteps"
      statusLabelPrefix="dataSources.systemTables.resourceStepStatus"
      error={error}
      closeDisabled={closeDisabled}
      onClose={onClose}
    />
  );
}

function DatabricksResourceLinks({
  workspaceUrl,
  tableFqn,
}: {
  workspaceUrl: string | null;
  tableFqn: string | null;
}) {
  const { t } = useI18n();
  if (!tableFqn) return null;

  return (
    <div className="border-border bg-background/35 mt-4 rounded-md border p-3">
      <div className="text-muted-foreground mb-2 text-xs font-medium">
        {t('dataSources.systemTables.resourcesTitle')}
      </div>
      <div className="grid grid-cols-1 gap-2">
        <ResourceLink
          label={t('dataSources.systemTables.tableResource')}
          id={tableFqn}
          href={workspaceUrl ? catalogTableUrl(workspaceUrl, tableFqn) : null}
        />
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
