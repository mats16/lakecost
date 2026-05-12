import { useEffect, useState } from 'react';
import {
  Alert,
  AlertDescription,
  AlertTitle,
  Button,
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
  FieldGroup,
  Spinner,
} from '@databricks/appkit-ui/react';
import { AlertCircle, CheckCircle2, Circle, Info, Play, Trash2, X } from 'lucide-react';
import { CATALOG_SETTING_KEY } from '@finlake/shared';
import { useI18n } from '../../i18n';
import { useAdminCleanup, useAppSettings, useRunSharedTransformationJob } from '../../api/hooks';
import { messageOf } from './utils';
import { CatalogSettingsForm } from '../../components/CatalogSettingsForm';

export function Catalog() {
  const { t } = useI18n();
  const settings = useAppSettings();
  const runSharedJob = useRunSharedTransformationJob();
  const cleanup = useAdminCleanup();

  const remoteCatalog = settings.data?.settings[CATALOG_SETTING_KEY] ?? '';
  const [pipelineRunModalOpen, setPipelineRunModalOpen] = useState(false);
  const [cleanupModalOpen, setCleanupModalOpen] = useState(false);

  return (
    <>
      <CatalogSettingsForm
        variant="page"
        onSaved={(data, hadPreviousCatalog) => {
          runSharedJob.reset();
          if (hadPreviousCatalog && data.pipelineSynced) {
            setPipelineRunModalOpen(true);
          }
        }}
      />
      <Card className="mt-4">
        <CardHeader>
          <CardTitle>{t('settings.cleanup.heading')}</CardTitle>
          <CardDescription>{t('settings.cleanup.desc')}</CardDescription>
        </CardHeader>
        <CardContent>
          <FieldGroup>
            <Alert variant="destructive">
              <AlertCircle />
              <AlertDescription>{t('settings.cleanup.notice')}</AlertDescription>
            </Alert>
            <div>
              <Button
                type="button"
                className="warning-action-button"
                onClick={() => {
                  cleanup.reset();
                  setCleanupModalOpen(true);
                }}
                disabled={cleanup.isPending}
              >
                {cleanup.isPending ? <Spinner /> : <Trash2 />}
                {t('settings.cleanup.openAction')}
              </Button>
            </div>
            {cleanup.data ? <CleanupResultList cleanup={cleanup.data} /> : null}
            {cleanup.error ? (
              <Alert variant="destructive">
                <AlertCircle />
                <AlertDescription>{messageOf(cleanup.error)}</AlertDescription>
              </Alert>
            ) : null}
          </FieldGroup>
        </CardContent>
      </Card>
      <CleanupConfirmModal
        open={cleanupModalOpen}
        cleanup={cleanup}
        catalogName={remoteCatalog}
        onClose={() => setCleanupModalOpen(false)}
      />
      <PipelineRunPromptModal
        open={pipelineRunModalOpen}
        runJob={runSharedJob}
        onClose={() => {
          setPipelineRunModalOpen(false);
          runSharedJob.reset();
        }}
      />
    </>
  );
}

function CleanupConfirmModal({
  open,
  cleanup,
  catalogName,
  onClose,
}: {
  open: boolean;
  cleanup: ReturnType<typeof useAdminCleanup>;
  catalogName: string;
  onClose: () => void;
}) {
  const { t } = useI18n();
  const [deleteCatalog, setDeleteCatalog] = useState(false);
  const [started, setStarted] = useState(false);

  useEffect(() => {
    if (open) {
      setDeleteCatalog(false);
      setStarted(false);
    }
  }, [open]);

  useEffect(() => {
    if (!open) return;
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape' && !cleanup.isPending) onClose();
    };
    window.addEventListener('keydown', onKeyDown);
    return () => window.removeEventListener('keydown', onKeyDown);
  }, [cleanup.isPending, onClose, open]);

  if (!open) return null;
  const finished = started && !cleanup.isPending && (cleanup.data || cleanup.error);

  return (
    <div
      className="fixed inset-0 z-[80] flex items-center justify-center bg-black/55 p-4"
      role="presentation"
      onMouseDown={() => {
        if (!cleanup.isPending) onClose();
      }}
    >
      <div
        role="dialog"
        aria-modal="true"
        aria-labelledby="cleanup-confirm-title"
        className="bg-background border-border grid w-full max-w-xl gap-4 rounded-lg border p-5 shadow-xl"
        onMouseDown={(event) => event.stopPropagation()}
      >
        <div className="flex items-start justify-between gap-4">
          <div className="min-w-0">
            <h3 id="cleanup-confirm-title" className="text-base font-semibold">
              {started ? t('settings.cleanup.progressTitle') : t('settings.cleanup.modalTitle')}
            </h3>
            <p className="text-muted-foreground mt-1 mb-0 text-sm">
              {started ? t('settings.cleanup.progressDesc') : t('settings.cleanup.modalDesc')}
            </p>
          </div>
          <button
            type="button"
            className="text-muted-foreground hover:text-foreground hover:bg-muted/40 grid size-8 place-items-center rounded-md transition-colors disabled:opacity-50"
            aria-label={t('common.close')}
            onClick={onClose}
            disabled={cleanup.isPending}
          >
            <X className="size-4" aria-hidden="true" />
          </button>
        </div>

        {started ? (
          <>
            <CleanupProgressChecklist
              cleanup={cleanup.data ?? null}
              pending={cleanup.isPending}
              includeCatalog={deleteCatalog}
            />
            {cleanup.error ? (
              <Alert variant="destructive">
                <AlertCircle />
                <AlertDescription>{messageOf(cleanup.error)}</AlertDescription>
              </Alert>
            ) : null}
          </>
        ) : (
          <label className="border-border flex items-start gap-3 rounded-md border p-3 text-sm">
            <input
              type="checkbox"
              checked={deleteCatalog}
              disabled={cleanup.isPending}
              onChange={(event) => setDeleteCatalog(event.target.checked)}
            />
            <span>
              <span className="block font-medium">{t('settings.cleanup.deleteCatalogLabel')}</span>
              <span className="text-muted-foreground block">
                {catalogName
                  ? t('settings.cleanup.deleteCatalogDesc', { catalog: catalogName })
                  : t('settings.cleanup.deleteCatalogNoCatalog')}
              </span>
            </span>
          </label>
        )}

        <div className="flex flex-wrap justify-end gap-2">
          {started ? (
            <Button
              type="button"
              variant="secondary"
              onClick={onClose}
              disabled={cleanup.isPending}
            >
              {finished ? t('common.close') : t('settings.cleanup.runningAction')}
            </Button>
          ) : (
            <>
              <Button
                type="button"
                variant="secondary"
                onClick={onClose}
                disabled={cleanup.isPending}
              >
                {t('common.cancel')}
              </Button>
              <Button
                type="button"
                className="warning-action-button"
                onClick={() => {
                  cleanup.reset();
                  setStarted(true);
                  cleanup.mutate({ deleteCatalog });
                }}
                disabled={cleanup.isPending}
              >
                {cleanup.isPending ? <Spinner /> : <Trash2 />}
                {t('settings.cleanup.confirmAction')}
              </Button>
            </>
          )}
        </div>
      </div>
    </div>
  );
}

const CLEANUP_PROGRESS_ITEMS = [
  'job',
  'pipeline',
  'workspace',
  'genie_space',
  'catalog',
  'database',
] as const;

type CleanupProgressStatus = 'pending' | 'running' | 'deleted' | 'skipped' | 'failed';

function CleanupProgressChecklist({
  cleanup,
  pending,
  includeCatalog,
}: {
  cleanup: NonNullable<ReturnType<typeof useAdminCleanup>['data']> | null;
  pending: boolean;
  includeCatalog: boolean;
}) {
  const { t } = useI18n();
  const rows = CLEANUP_PROGRESS_ITEMS.map((resourceType) => {
    if (cleanup) {
      if (resourceType === 'database') {
        return {
          resourceType,
          status: cleanup.database.status,
          message: cleanup.database.message ?? t('settings.cleanup.databaseCleaned'),
        };
      }
      const result = cleanup.resources.find((item) => item.resourceType === resourceType);
      return {
        resourceType,
        status: result?.status ?? 'skipped',
        message: result?.message ?? null,
      };
    }
    if (resourceType === 'catalog' && !includeCatalog) {
      return {
        resourceType,
        status: 'skipped' as CleanupProgressStatus,
        message: t('settings.cleanup.catalogNotRequested'),
      };
    }
    return {
      resourceType,
      status: pending ? ('running' as CleanupProgressStatus) : ('pending' as CleanupProgressStatus),
      message: pending ? t('settings.cleanup.runningMessage') : null,
    };
  });

  return (
    <div className="border-border rounded-md border p-3">
      <ul className="grid gap-2 text-sm">
        {rows.map((row) => (
          <li key={row.resourceType} className="grid grid-cols-[20px_minmax(0,1fr)] gap-2">
            <span className="mt-0.5">{cleanupStatusIcon(row.status)}</span>
            <span className="min-w-0">
              <span className="flex flex-wrap items-center gap-x-2 gap-y-1">
                <span className="font-medium">
                  {t(`settings.cleanup.resourceTypes.${row.resourceType}`)}
                </span>
                <span className="text-muted-foreground text-xs">
                  {t(`settings.cleanup.status.${row.status}`)}
                </span>
              </span>
              {row.message ? (
                <span className="text-muted-foreground mt-0.5 block text-xs">{row.message}</span>
              ) : null}
            </span>
          </li>
        ))}
      </ul>
    </div>
  );
}

function cleanupStatusIcon(status: CleanupProgressStatus) {
  if (status === 'running') return <Spinner className="size-4" />;
  if (status === 'deleted') return <CheckCircle2 className="size-4 text-(--success)" />;
  if (status === 'failed') return <AlertCircle className="text-destructive size-4" />;
  if (status === 'skipped') return <Info className="text-muted-foreground size-4" />;
  return <Circle className="text-muted-foreground size-4" />;
}

function CleanupResultList({
  cleanup,
}: {
  cleanup: NonNullable<ReturnType<typeof useAdminCleanup>['data']>;
}) {
  const { t } = useI18n();
  const items = [
    ...cleanup.resources,
    {
      resourceType: 'database' as const,
      resourceId: null,
      status: cleanup.database.status,
      message: cleanup.database.message ?? t('settings.cleanup.databaseCleaned'),
    },
  ];

  return (
    <Alert variant={items.some((item) => item.status === 'failed') ? 'destructive' : 'default'}>
      {items.some((item) => item.status === 'failed') ? <AlertCircle /> : <CheckCircle2 />}
      <AlertTitle>{t('settings.cleanup.resultTitle')}</AlertTitle>
      <AlertDescription>
        <ul className="grid gap-1 text-xs">
          {items.map((item) => (
            <li key={`${item.resourceType}:${item.resourceId ?? 'none'}`} className="flex gap-2">
              <span className="font-medium">
                {t(`settings.cleanup.resourceTypes.${item.resourceType}`)}
              </span>
              <span>{t(`settings.cleanup.status.${item.status}`)}</span>
              {item.resourceId ? (
                <span className="text-muted-foreground">{item.resourceId}</span>
              ) : null}
              {item.message ? <span className="text-muted-foreground">{item.message}</span> : null}
            </li>
          ))}
        </ul>
      </AlertDescription>
    </Alert>
  );
}

function PipelineRunPromptModal({
  open,
  runJob,
  onClose,
}: {
  open: boolean;
  runJob: ReturnType<typeof useRunSharedTransformationJob>;
  onClose: () => void;
}) {
  const { t } = useI18n();

  useEffect(() => {
    if (!open) return;
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape' && !runJob.isPending) onClose();
    };
    window.addEventListener('keydown', onKeyDown);
    return () => window.removeEventListener('keydown', onKeyDown);
  }, [onClose, open, runJob.isPending]);

  if (!open) return null;

  return (
    <div
      className="fixed inset-0 z-[80] flex items-center justify-center bg-black/55 p-4"
      role="presentation"
      onMouseDown={() => {
        if (!runJob.isPending) onClose();
      }}
    >
      <div
        role="dialog"
        aria-modal="true"
        aria-labelledby="pipeline-run-prompt-title"
        className="bg-background border-border grid w-full max-w-xl gap-4 rounded-lg border p-5 shadow-xl"
        onMouseDown={(event) => event.stopPropagation()}
      >
        <div className="flex items-start justify-between gap-4">
          <div className="min-w-0">
            <h3 id="pipeline-run-prompt-title" className="text-base font-semibold">
              {t('settings.pipelineChangedTitle')}
            </h3>
            <p className="text-muted-foreground mt-1 mb-0 text-sm">
              {t('settings.pipelineChangedDesc')}
            </p>
          </div>
          <button
            type="button"
            className="text-muted-foreground hover:text-foreground hover:bg-muted/40 grid size-8 place-items-center rounded-md transition-colors disabled:opacity-50"
            aria-label={t('common.close')}
            onClick={onClose}
            disabled={runJob.isPending}
          >
            <X className="size-4" aria-hidden="true" />
          </button>
        </div>

        <Alert>
          <Info />
          <AlertDescription>{t('settings.pipelineChangedNotice')}</AlertDescription>
        </Alert>

        {runJob.data ? (
          <Alert>
            <CheckCircle2 />
            <AlertDescription>
              {t('settings.pipelineRunStarted', {
                jobId: String(runJob.data.jobId),
                runId: String(runJob.data.runId),
              })}
            </AlertDescription>
          </Alert>
        ) : null}

        {runJob.error ? (
          <Alert variant="destructive">
            <AlertCircle />
            <AlertDescription>{messageOf(runJob.error)}</AlertDescription>
          </Alert>
        ) : null}

        <div className="flex flex-wrap justify-end gap-2">
          <Button type="button" variant="secondary" onClick={onClose} disabled={runJob.isPending}>
            {t('common.close')}
          </Button>
          <Button type="button" onClick={() => runJob.mutate()} disabled={runJob.isPending}>
            {runJob.isPending ? <Spinner /> : <Play />}
            {t('settings.runPipelineJob')}
          </Button>
        </div>
      </div>
    </div>
  );
}
