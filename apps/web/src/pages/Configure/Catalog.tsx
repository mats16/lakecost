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
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
  Spinner,
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from '@databricks/appkit-ui/react';
import { CheckCircle2, AlertCircle, Info, Play, X } from 'lucide-react';
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
  useAppSettings,
  useCatalogs,
  useRunSharedTransformationJob,
  useUpdateAppSettings,
} from '../../api/hooks';
import { CatalogCombobox } from '../../components/CatalogCombobox';
import { messageOf } from './utils';

type CatalogMode = 'existing' | 'create';
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

export function Catalog() {
  const { t } = useI18n();
  const settings = useAppSettings();
  const catalogs = useCatalogs();
  const updateSettings = useUpdateAppSettings();
  const runSharedJob = useRunSharedTransformationJob();

  const remoteCatalog = settings.data?.settings[CATALOG_SETTING_KEY] ?? '';
  const remoteCatalogUserGroup =
    settings.data?.settings[CATALOG_USER_GROUP_SETTING_KEY]?.trim() || CATALOG_USER_GROUP_DEFAULT;
  const remoteMedallionSchemas = useMemo(
    () => medallionSchemaValues(settings.data?.settings),
    [settings.data?.settings],
  );
  const [mode, setMode] = useState<CatalogMode>('create');
  const [selectedCatalog, setSelectedCatalog] = useState(remoteCatalog);
  const [newCatalogName, setNewCatalogName] = useState('');
  const [catalogUserGroup, setCatalogUserGroup] = useState(remoteCatalogUserGroup);
  const [medallionSchemas, setMedallionSchemas] = useState(remoteMedallionSchemas);
  const [savedAt, setSavedAt] = useState<number | null>(null);
  const [pipelineRunModalOpen, setPipelineRunModalOpen] = useState(false);

  useEffect(() => {
    setSelectedCatalog(remoteCatalog);
    setNewCatalogName('');
    if (remoteCatalog) setMode('existing');
  }, [remoteCatalog]);

  useEffect(() => {
    setMedallionSchemas(remoteMedallionSchemas);
  }, [remoteMedallionSchemas]);

  useEffect(() => {
    setCatalogUserGroup(remoteCatalogUserGroup);
  }, [remoteCatalogUserGroup]);

  const hasConfiguredCatalog = remoteCatalog.length > 0;
  const catalogName = mode === 'existing' ? selectedCatalog.trim() : newCatalogName.trim();
  const isCreate = mode === 'create';
  const catalogDirty = catalogName !== remoteCatalog;
  const validName = catalogName.length > 0 && (mode === 'existing' || IDENT_RE.test(catalogName));
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
        provision: { createIfMissing: isCreate },
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
  const catalogsError = messageOf(catalogs.error);

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
                <FieldLabel>{t('settings.catalogTypeLabel')}</FieldLabel>
                <Select
                  value={mode}
                  onValueChange={(v: string) => setMode(v as CatalogMode)}
                  disabled={settings.isLoading || saving}
                >
                  <SelectTrigger>
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="create">{t('settings.catalogModeCreate')}</SelectItem>
                    <SelectItem value="existing">{t('settings.catalogModeExisting')}</SelectItem>
                  </SelectContent>
                </Select>
              </div>

              <div className="grid max-w-3xl gap-3 md:grid-cols-[160px_minmax(0,1fr)] md:items-center">
                <FieldLabel>{t('settings.catalogNameLabel')}</FieldLabel>
                {mode === 'existing' ? (
                  <CatalogCombobox
                    value={selectedCatalog}
                    onChange={(sel) => setSelectedCatalog(sel.name)}
                    options={catalogs.data?.catalogs ?? []}
                    loading={catalogs.isLoading}
                    disabled={settings.isLoading || saving}
                    placeholder={t('settings.catalogSelectPlaceholder')}
                    searchPlaceholder={t('settings.catalogSearchPlaceholder')}
                    emptyText={t('settings.catalogEmpty')}
                    allowCreate={false}
                  />
                ) : (
                  <Input
                    id="new-catalog-name"
                    value={newCatalogName}
                    onChange={(e) => setNewCatalogName(e.target.value)}
                    placeholder={t('settings.catalogCreatePlaceholder')}
                    disabled={settings.isLoading || saving}
                  />
                )}
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

              {mode === 'existing' && catalogsError ? (
                <Alert variant="destructive">
                  <AlertCircle />
                  <AlertTitle>{t('settings.catalogLoadFailed')}</AlertTitle>
                  <AlertDescription>{catalogsError}</AlertDescription>
                </Alert>
              ) : null}

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
                  className="bg-(--success) text-(--background) hover:bg-(--success)/90 disabled:bg-muted disabled:text-muted-foreground"
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
                  ) : isCreate ? (
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
