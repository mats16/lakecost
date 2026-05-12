import { useEffect, useMemo, useState, type FormEvent, type ReactElement } from 'react';
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
  Field,
  FieldGroup,
  FieldLabel,
  Input,
  Spinner,
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from '@databricks/appkit-ui/react';
import { CheckCircle2, AlertCircle, Circle, Info, Play, Trash2, X } from 'lucide-react';
import {
  CATALOG_SETTING_KEY,
  CATALOG_USER_GROUP_DEFAULT,
  CATALOG_USER_GROUP_SETTING_KEY,
  IDENT_RE,
  MEDALLION_SCHEMA_DEFAULTS,
  MEDALLION_SCHEMAS,
  MEDALLION_SCHEMA_SETTING_KEYS,
  quoteIdent,
  quotePrincipal,
  schemaGrantPrivileges,
  type ProvisionResult,
} from '@finlake/shared';
import { useI18n } from '../../i18n';
import {
  useAdminCleanup,
  useAppSettings,
  useRunSharedTransformationJob,
  useUpdateAppSettings,
} from '../../api/hooks';
import { messageOf } from './utils';

type Severity = 'success' | 'warning' | 'error';
type MedallionSchemaSettingKey =
  (typeof MEDALLION_SCHEMA_SETTING_KEYS)[keyof typeof MEDALLION_SCHEMA_SETTING_KEYS];

const MEDALLION_SCHEMA_FIELDS = [
  {
    key: MEDALLION_SCHEMA_SETTING_KEYS.bronze,
    defaultValue: MEDALLION_SCHEMA_DEFAULTS.bronze,
    labelKey: 'settings.medallion.bronzeLabel',
  },
  {
    key: MEDALLION_SCHEMA_SETTING_KEYS.silver,
    defaultValue: MEDALLION_SCHEMA_DEFAULTS.silver,
    labelKey: 'settings.medallion.silverLabel',
  },
  {
    key: MEDALLION_SCHEMA_SETTING_KEYS.gold,
    defaultValue: MEDALLION_SCHEMA_DEFAULTS.gold,
    labelKey: 'settings.medallion.goldLabel',
  },
] as const;

const SEVERITY_VARIANT: Record<Severity, 'default' | 'destructive'> = {
  success: 'default',
  warning: 'default',
  error: 'destructive',
};

const SEVERITY_ICON: Record<Severity, () => ReactElement> = {
  success: () => <CheckCircle2 />,
  warning: () => <Info />,
  error: () => <AlertCircle />,
};

const SEVERITY_TITLE_KEY: Record<Severity, string> = {
  success: 'settings.provisionSuccess',
  warning: 'settings.provisionWarning',
  error: 'settings.provisionFailed',
};

const DEFAULT_CATALOG_NAME = 'finops';

export function Catalog() {
  const { t } = useI18n();
  const settings = useAppSettings();
  const updateSettings = useUpdateAppSettings();
  const runSharedJob = useRunSharedTransformationJob();
  const cleanup = useAdminCleanup();

  const remoteCatalog = settings.data?.settings[CATALOG_SETTING_KEY] ?? '';
  const remoteCatalogUserGroup =
    settings.data?.settings[CATALOG_USER_GROUP_SETTING_KEY]?.trim() || CATALOG_USER_GROUP_DEFAULT;
  const remoteMedallionSchemas = useMemo(
    () => medallionSchemaValues(settings.data?.settings),
    [settings.data?.settings],
  );
  const [catalogNameInput, setCatalogNameInput] = useState(remoteCatalog || DEFAULT_CATALOG_NAME);
  const [createIfMissing, setCreateIfMissing] = useState(false);
  const [catalogUserGroup, setCatalogUserGroup] = useState(remoteCatalogUserGroup);
  const [medallionSchemas, setMedallionSchemas] = useState(remoteMedallionSchemas);
  const [savedAt, setSavedAt] = useState<number | null>(null);
  const [pipelineRunModalOpen, setPipelineRunModalOpen] = useState(false);
  const [cleanupModalOpen, setCleanupModalOpen] = useState(false);

  useEffect(() => {
    setCatalogNameInput(remoteCatalog || DEFAULT_CATALOG_NAME);
  }, [remoteCatalog]);

  useEffect(() => {
    setMedallionSchemas(remoteMedallionSchemas);
  }, [remoteMedallionSchemas]);

  useEffect(() => {
    setCatalogUserGroup(remoteCatalogUserGroup);
  }, [remoteCatalogUserGroup]);

  const hasConfiguredCatalog = remoteCatalog.length > 0;
  const catalogName = catalogNameInput.trim();
  const catalogDirty = catalogName !== remoteCatalog;
  const validName = catalogName.length > 0 && IDENT_RE.test(catalogName);
  const saving = updateSettings.isPending;
  const medallionPayload = useMemo(
    () => trimMedallionSchemaValues(medallionSchemas),
    [medallionSchemas],
  );
  const medallionDirty = MEDALLION_SCHEMA_FIELDS.some(({ key, defaultValue }) => {
    const stored = settings.data?.settings[key]?.trim();
    const effective = stored || defaultValue;
    return medallionPayload[key] !== effective;
  });
  const medallionValid = MEDALLION_SCHEMA_FIELDS.every(({ key }) =>
    IDENT_RE.test(medallionPayload[key]),
  );
  const catalogUserGroupPayload = catalogUserGroup.trim();
  const catalogUserGroupDirty = catalogUserGroupPayload !== remoteCatalogUserGroup;
  const catalogUserGroupValid = catalogUserGroupPayload.length > 0;
  const dirty = catalogDirty || medallionDirty || catalogUserGroupDirty;
  const submitDisabled = hasConfiguredCatalog
    ? saving || !validName || !medallionValid || !catalogUserGroupValid
    : !dirty || saving || !validName || !medallionValid || !catalogUserGroupValid;

  const onSubmit = (e: FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    if (!validName) return;
    const shouldPromptPipelineRun = hasConfiguredCatalog && (catalogDirty || medallionDirty);
    runSharedJob.reset();
    updateSettings.reset();
    updateSettings.mutate(
      {
        settings: {
          [CATALOG_SETTING_KEY]: catalogName,
          [CATALOG_USER_GROUP_SETTING_KEY]: catalogUserGroupPayload,
          ...medallionPayload,
        },
        provision: { createIfMissing },
      },
      {
        onSuccess: (data) => {
          setSavedAt(Date.now());
          if (shouldPromptPipelineRun && data.pipelineSynced) setPipelineRunModalOpen(true);
        },
      },
    );
  };

  const errorMessage = messageOf(updateSettings.error);
  const provision = updateSettings.data?.provision ?? null;
  const provisionMessages = useMemo(
    () =>
      provision
        ? buildProvisionMessages(provision, t, medallionPayload, catalogUserGroupPayload)
        : null,
    [catalogUserGroupPayload, medallionPayload, provision, t],
  );

  return (
    <>
      <form onSubmit={onSubmit}>
        <Card>
          <CardHeader>
            <CardTitle>{t('settings.mainCatalogHeading')}</CardTitle>
            <CardDescription>{t('settings.mainCatalogDesc')}</CardDescription>
          </CardHeader>
          <CardContent>
            <FieldGroup>
              <div className="grid max-w-3xl gap-3 md:grid-cols-[160px_minmax(0,1fr)] md:items-center">
                <FieldLabel>{t('settings.catalogNameLabel')}</FieldLabel>
                <div className="grid gap-2 sm:grid-cols-[minmax(0,1fr)_auto] sm:items-center">
                  <Input
                    id="catalog-name"
                    value={catalogNameInput}
                    onChange={(e) => setCatalogNameInput(e.target.value)}
                    disabled={settings.isLoading || saving}
                  />
                  <label className="flex min-h-9 items-center gap-2 text-sm">
                    <input
                      type="checkbox"
                      checked={createIfMissing}
                      disabled={settings.isLoading || saving}
                      onChange={(event) => setCreateIfMissing(event.target.checked)}
                    />
                    <span className="text-muted-foreground whitespace-nowrap">
                      {t('settings.createIfMissing')}
                    </span>
                  </label>
                </div>
              </div>

              <div className="grid max-w-3xl gap-3 md:grid-cols-[160px_minmax(0,1fr)]">
                <FieldLabel className="md:pt-2">{t('settings.medallion.schemaLabel')}</FieldLabel>
                <div className="grid gap-3 md:grid-cols-3">
                  {MEDALLION_SCHEMA_FIELDS.map(({ key, defaultValue, labelKey }) => (
                    <Field key={key}>
                      <FieldLabel>{t(labelKey)}</FieldLabel>
                      <Input
                        value={medallionSchemas[key]}
                        onChange={(e) =>
                          setMedallionSchemas((cur) => ({ ...cur, [key]: e.target.value }))
                        }
                        placeholder={defaultValue}
                        disabled={settings.isLoading || saving}
                      />
                    </Field>
                  ))}
                </div>
              </div>

              <div className="grid max-w-3xl gap-3 md:grid-cols-[160px_minmax(0,1fr)] md:items-center">
                <FieldLabel className="inline-flex items-center gap-1.5">
                  {t('settings.catalogUserGroupLabel')}
                  <Tooltip>
                    <TooltipTrigger asChild>
                      <Info
                        className="text-muted-foreground size-3.5 cursor-help"
                        aria-label={t('settings.catalogUserGroupHelp')}
                      />
                    </TooltipTrigger>
                    <TooltipContent>{t('settings.catalogUserGroupHelp')}</TooltipContent>
                  </Tooltip>
                </FieldLabel>
                <Input
                  value={catalogUserGroup}
                  onChange={(e) => setCatalogUserGroup(e.target.value)}
                  placeholder={CATALOG_USER_GROUP_DEFAULT}
                  disabled={settings.isLoading || saving}
                />
              </div>

              {!medallionValid ? (
                <Alert variant="destructive">
                  <AlertCircle />
                  <AlertDescription>{t('settings.medallion.invalid')}</AlertDescription>
                </Alert>
              ) : null}

              {!catalogUserGroupValid ? (
                <Alert variant="destructive">
                  <AlertCircle />
                  <AlertDescription>{t('settings.catalogUserGroupInvalid')}</AlertDescription>
                </Alert>
              ) : null}

              <div className="flex items-center gap-3">
                <Button
                  type="submit"
                  disabled={submitDisabled}
                  className={hasConfiguredCatalog ? 'success-action-button' : undefined}
                >
                  {saving ? (
                    <>
                      <Spinner /> {t('common.saving')}
                    </>
                  ) : hasConfiguredCatalog ? (
                    dirty ? (
                      t('settings.save')
                    ) : (
                      t('settings.fixPermission')
                    )
                  ) : createIfMissing ? (
                    t('settings.saveAndCreate')
                  ) : (
                    t('settings.save')
                  )}
                </Button>
                {savedAt && !dirty && !saving ? (
                  <span className="text-muted-foreground inline-flex items-center gap-1.5 text-xs">
                    <CheckCircle2 className="size-3.5 text-(--success)" />
                    {t('settings.saved')}
                  </span>
                ) : null}
              </div>

              {errorMessage ? (
                <Alert variant="destructive">
                  <AlertCircle />
                  <AlertDescription>{errorMessage}</AlertDescription>
                </Alert>
              ) : null}

              {provisionMessages ? (
                <Alert variant={SEVERITY_VARIANT[provisionMessages.severity]}>
                  {SEVERITY_ICON[provisionMessages.severity]()}
                  <AlertTitle>{t(SEVERITY_TITLE_KEY[provisionMessages.severity])}</AlertTitle>
                  <AlertDescription>
                    <ul className="list-disc pl-4 text-xs">
                      {provisionMessages.lines.map((line) => (
                        <li key={line}>{line}</li>
                      ))}
                    </ul>
                    {provisionMessages.remediation ? (
                      <pre className="bg-muted text-muted-foreground mt-2 overflow-auto rounded p-2 text-xs">
                        {provisionMessages.remediation}
                      </pre>
                    ) : null}
                  </AlertDescription>
                </Alert>
              ) : null}
            </FieldGroup>
          </CardContent>
        </Card>
      </form>
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

function medallionSchemaValues(
  settings?: Record<string, string>,
): Record<MedallionSchemaSettingKey, string> {
  return {
    [MEDALLION_SCHEMA_SETTING_KEYS.gold]:
      settings?.[MEDALLION_SCHEMA_SETTING_KEYS.gold]?.trim() || MEDALLION_SCHEMA_DEFAULTS.gold,
    [MEDALLION_SCHEMA_SETTING_KEYS.silver]:
      settings?.[MEDALLION_SCHEMA_SETTING_KEYS.silver]?.trim() || MEDALLION_SCHEMA_DEFAULTS.silver,
    [MEDALLION_SCHEMA_SETTING_KEYS.bronze]:
      settings?.[MEDALLION_SCHEMA_SETTING_KEYS.bronze]?.trim() || MEDALLION_SCHEMA_DEFAULTS.bronze,
  };
}

function trimMedallionSchemaValues(
  values: Record<MedallionSchemaSettingKey, string>,
): Record<MedallionSchemaSettingKey, string> {
  return {
    [MEDALLION_SCHEMA_SETTING_KEYS.gold]: values[MEDALLION_SCHEMA_SETTING_KEYS.gold].trim(),
    [MEDALLION_SCHEMA_SETTING_KEYS.silver]: values[MEDALLION_SCHEMA_SETTING_KEYS.silver].trim(),
    [MEDALLION_SCHEMA_SETTING_KEYS.bronze]: values[MEDALLION_SCHEMA_SETTING_KEYS.bronze].trim(),
  };
}

interface ProvisionMessages {
  severity: Severity;
  lines: string[];
  remediation: string | null;
}

function buildProvisionMessages(
  p: ProvisionResult,
  t: (key: string, params?: Record<string, string | number>) => string,
  schemaNames: Record<MedallionSchemaSettingKey, string>,
  catalogUserGroup: string,
): ProvisionMessages {
  const lines: string[] = [];
  if (p.catalogCreated) {
    lines.push(t('settings.provisionCatalogCreated', { name: p.catalog }));
  }
  for (const s of MEDALLION_SCHEMAS) {
    if (p.schemasEnsured[s] === 'error') {
      lines.push(
        t('settings.provisionSchemaFailed', {
          schema: schemaNames[MEDALLION_SCHEMA_SETTING_KEYS[s]],
        }),
      );
    }
  }

  const grantEntries: Array<{ scope: string; status: string }> = [
    { scope: t('settings.provisionScopeCatalog'), status: p.grants.catalog },
    {
      scope: t('settings.provisionScopeUsersCatalog', { group: catalogUserGroup }),
      status: p.grants.usersCatalog,
    },
    ...MEDALLION_SCHEMAS.map((s) => ({
      scope: schemaNames[MEDALLION_SCHEMA_SETTING_KEYS[s]],
      status: p.grants[s],
    })),
  ];
  const grantFailures = grantEntries.filter((e) => e.status.startsWith('error:'));
  const grantSkips = grantEntries.filter((e) => e.status.startsWith('skipped:'));
  const grantsOk = grantEntries.every((e) => e.status === 'granted');

  for (const f of grantFailures) {
    lines.push(
      t('settings.provisionGrantFailed', {
        scope: f.scope,
        message: f.status.slice('error:'.length),
      }),
    );
  }

  for (const w of p.warnings) lines.push(w);

  const hasSchemaError = Object.values(p.schemasEnsured).includes('error');
  const severity: Severity =
    grantFailures.length > 0 || hasSchemaError
      ? 'error'
      : grantSkips.length > 0
        ? 'warning'
        : 'success';

  if (lines.length === 0 && grantsOk) {
    lines.push(t('settings.provisionAllOk', { name: p.catalog }));
  }
  if (grantsOk) {
    lines.push(t('settings.provisionAwsCredentialNote'));
  }

  const remediation =
    grantFailures.length > 0
      ? renderRemediationSql(p.catalog, p.servicePrincipalId, schemaNames, catalogUserGroup)
      : null;

  return { severity, lines, remediation };
}

function renderRemediationSql(
  catalog: string,
  sp: string | null,
  schemaNames: Record<MedallionSchemaSettingKey, string>,
  catalogUserGroup: string,
): string {
  const cat = quoteIdent(catalog);
  const lines: string[] = [];
  const accountUsers = quotePrincipal(catalogUserGroup);
  lines.push(`GRANT BROWSE, USE CATALOG, USE SCHEMA, SELECT ON CATALOG ${cat} TO ${accountUsers};`);
  if (sp) {
    const principal = quotePrincipal(sp);
    lines.push(`GRANT USE CATALOG ON CATALOG ${cat} TO ${principal};`);
    for (const s of MEDALLION_SCHEMAS) {
      lines.push(
        `GRANT ${schemaGrantPrivileges(s)} ON SCHEMA ${cat}.${quoteIdent(
          schemaNames[MEDALLION_SCHEMA_SETTING_KEYS[s]],
        )} TO ${principal};`,
      );
    }
  }
  return lines.join('\n');
}
