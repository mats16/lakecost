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
  IDENT_RE,
  MEDALLION_SCHEMAS,
  quoteIdent,
  quotePrincipal,
  schemaGrantPrivileges,
  type ProvisionResult,
} from '@lakecost/shared';
import { useI18n } from '../../i18n';
import { useAppSettings, useCatalogs, useUpdateAppSettings } from '../../api/hooks';
import { CatalogCombobox } from '../../components/CatalogCombobox';

type CatalogMode = 'existing' | 'create';
type Severity = 'success' | 'warning' | 'error';

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

function messageOf(err: unknown): string | null {
  return err && typeof err === 'object' ? ((err as { message?: string }).message ?? null) : null;
}

export function Admin() {
  const { t } = useI18n();
  const settings = useAppSettings();
  const catalogs = useCatalogs();
  const updateSettings = useUpdateAppSettings();

  const remoteCatalog = settings.data?.settings[CATALOG_SETTING_KEY] ?? '';
  const [mode, setMode] = useState<CatalogMode>('create');
  const [selectedCatalog, setSelectedCatalog] = useState(remoteCatalog);
  const [newCatalogName, setNewCatalogName] = useState('');
  const [savedAt, setSavedAt] = useState<number | null>(null);

  useEffect(() => {
    setSelectedCatalog(remoteCatalog);
    if (remoteCatalog) setMode('existing');
  }, [remoteCatalog]);

  const hasConfiguredCatalog = remoteCatalog.length > 0;
  const catalogName = hasConfiguredCatalog
    ? remoteCatalog
    : mode === 'existing'
      ? selectedCatalog.trim()
      : newCatalogName.trim();
  const isCreate = !hasConfiguredCatalog && mode === 'create';
  const dirty = !hasConfiguredCatalog && catalogName !== remoteCatalog;
  const validName =
    catalogName.length > 0 &&
    (hasConfiguredCatalog || mode === 'existing' || IDENT_RE.test(catalogName));
  const saving = updateSettings.isPending;
  const submitDisabled = hasConfiguredCatalog
    ? saving || !validName
    : !dirty || saving || !validName;

  const onSubmit = (e: FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    if (!validName) return;
    updateSettings.reset();
    updateSettings.mutate(
      {
        settings: { [CATALOG_SETTING_KEY]: catalogName },
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
    () => (provision ? buildProvisionMessages(provision, t) : null),
    [provision, t],
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
              <Field>
                <FieldLabel>{t('settings.catalogTypeLabel')}</FieldLabel>
                <Select value={mode} onValueChange={(v: string) => setMode(v as CatalogMode)}>
                  <SelectTrigger className="max-w-md">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="create">{t('settings.catalogModeCreate')}</SelectItem>
                    <SelectItem value="existing">{t('settings.catalogModeExisting')}</SelectItem>
                  </SelectContent>
                </Select>
              </Field>
            ) : null}

            <Field>
              <FieldLabel>{t('settings.catalogNameLabel')}</FieldLabel>
              {hasConfiguredCatalog ? (
                <Input className="max-w-md" value={remoteCatalog} disabled readOnly />
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
                  className="max-w-md"
                  value={newCatalogName}
                  onChange={(e) => setNewCatalogName(e.target.value)}
                  placeholder={t('settings.catalogCreatePlaceholder')}
                  disabled={settings.isLoading || saving}
                />
              )}
            </Field>

            {!hasConfiguredCatalog && catalogsError ? (
              <Alert variant="destructive">
                <AlertCircle />
                <AlertTitle>{t('settings.catalogLoadFailed')}</AlertTitle>
                <AlertDescription>{catalogsError}</AlertDescription>
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
                  t('settings.fixPermission')
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

interface ProvisionMessages {
  severity: Severity;
  lines: string[];
  remediation: string | null;
}

function buildProvisionMessages(
  p: ProvisionResult,
  t: (key: string, params?: Record<string, string | number>) => string,
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

  const remediation =
    grantFailures.length > 0 && p.servicePrincipalId
      ? renderRemediationSql(p.catalog, p.servicePrincipalId)
      : null;

  return { severity, lines, remediation };
}

function renderRemediationSql(catalog: string, sp: string): string {
  const cat = quoteIdent(catalog);
  const principal = quotePrincipal(sp);
  const lines: string[] = [];
  lines.push(`GRANT USE CATALOG ON CATALOG ${cat} TO ${principal};`);
  for (const s of MEDALLION_SCHEMAS) {
    lines.push(`GRANT ${schemaGrantPrivileges(s)} ON SCHEMA ${cat}.\`${s}\` TO ${principal};`);
  }
  return lines.join('\n');
}
