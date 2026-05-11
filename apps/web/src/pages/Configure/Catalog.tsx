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
} from '@databricks/appkit-ui/react';
import { CheckCircle2, AlertCircle, Info } from 'lucide-react';
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
import { useAppSettings, useCatalogs, useUpdateAppSettings } from '../../api/hooks';
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

  useEffect(() => {
    setSelectedCatalog(remoteCatalog);
    if (remoteCatalog) setMode('existing');
  }, [remoteCatalog]);

  useEffect(() => {
    setMedallionSchemas(remoteMedallionSchemas);
  }, [remoteMedallionSchemas]);

  useEffect(() => {
    setCatalogUserGroup(remoteCatalogUserGroup);
  }, [remoteCatalogUserGroup]);

  const hasConfiguredCatalog = remoteCatalog.length > 0;
  const catalogName = hasConfiguredCatalog
    ? remoteCatalog
    : mode === 'existing'
      ? selectedCatalog.trim()
      : newCatalogName.trim();
  const isCreate = !hasConfiguredCatalog && mode === 'create';
  const catalogDirty = !hasConfiguredCatalog && catalogName !== remoteCatalog;
  const validName =
    catalogName.length > 0 &&
    (hasConfiguredCatalog || mode === 'existing' || IDENT_RE.test(catalogName));
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
        onSuccess: () => setSavedAt(Date.now()),
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
    <form onSubmit={onSubmit}>
      <Card>
        <CardHeader>
          <CardTitle>{t('settings.mainCatalogHeading')}</CardTitle>
          <CardDescription>{t('settings.mainCatalogDesc')}</CardDescription>
        </CardHeader>
        <CardContent>
          <FieldGroup>
            {!hasConfiguredCatalog ? (
              <div className="grid max-w-3xl gap-3 md:grid-cols-[160px_minmax(0,1fr)] md:items-center">
                <FieldLabel>{t('settings.catalogTypeLabel')}</FieldLabel>
                <Select value={mode} onValueChange={(v: string) => setMode(v as CatalogMode)}>
                  <SelectTrigger>
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="create">{t('settings.catalogModeCreate')}</SelectItem>
                    <SelectItem value="existing">{t('settings.catalogModeExisting')}</SelectItem>
                  </SelectContent>
                </Select>
              </div>
            ) : null}

            <div className="grid max-w-3xl gap-3 md:grid-cols-[160px_minmax(0,1fr)] md:items-center">
              <FieldLabel>{t('settings.catalogNameLabel')}</FieldLabel>
              <div>
                {hasConfiguredCatalog ? (
                  <Input value={remoteCatalog} disabled readOnly />
                ) : mode === 'existing' ? (
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
            </div>

            <div className="grid max-w-3xl gap-3 md:grid-cols-[160px_minmax(0,1fr)] md:items-center">
              <FieldLabel>{t('settings.catalogUserGroupLabel')}</FieldLabel>
              <Input
                value={catalogUserGroup}
                onChange={(e) => setCatalogUserGroup(e.target.value)}
                placeholder={CATALOG_USER_GROUP_DEFAULT}
                disabled={settings.isLoading || saving}
              />
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
                      disabled={settings.isLoading || saving || hasConfiguredCatalog}
                      readOnly={hasConfiguredCatalog}
                    />
                  </Field>
                ))}
              </div>
            </div>

            {!hasConfiguredCatalog && catalogsError ? (
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
                  medallionDirty || catalogUserGroupDirty ? (
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
      lines.push(t('settings.provisionSchemaFailed', { schema: s }));
    }
  }

  const grantEntries: Array<{ scope: string; status: string }> = [
    { scope: t('settings.provisionScopeCatalog'), status: p.grants.catalog },
    {
      scope: t('settings.provisionScopeUsersCatalog', { group: catalogUserGroup }),
      status: p.grants.usersCatalog,
    },
    ...MEDALLION_SCHEMAS.map((s) => ({ scope: s, status: p.grants[s] })),
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
