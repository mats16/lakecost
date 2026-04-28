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
  FieldDescription,
  FieldGroup,
  FieldLabel,
  Spinner,
} from '@databricks/appkit-ui/react';
import { CheckCircle2, AlertCircle, Info } from 'lucide-react';
import { CATALOG_SETTING_KEY, MEDALLION_SCHEMAS, type ProvisionResult } from '@lakecost/shared';
import { useI18n } from '../../i18n';
import { useAppSettings, useCatalogs, useUpdateAppSettings } from '../../api/hooks';
import { CatalogCombobox, type CatalogSelection } from '../../components/CatalogCombobox';

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
  return err && typeof err === 'object'
    ? ((err as { message?: string }).message ?? null)
    : null;
}

export function Admin() {
  const { t } = useI18n();
  const settings = useAppSettings();
  const catalogs = useCatalogs();
  const updateSettings = useUpdateAppSettings();

  const remoteCatalog = settings.data?.settings[CATALOG_SETTING_KEY] ?? '';
  const [selection, setSelection] = useState<CatalogSelection>({
    name: remoteCatalog,
    create: false,
  });
  const [savedAt, setSavedAt] = useState<number | null>(null);

  useEffect(() => {
    setSelection({ name: remoteCatalog, create: false });
  }, [remoteCatalog]);

  const onSubmit = (e: FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    const name = selection.name.trim();
    if (!name) return;
    updateSettings.mutate(
      {
        settings: { [CATALOG_SETTING_KEY]: name },
        provision: { createIfMissing: selection.create },
      },
      { onSuccess: () => setSavedAt(Date.now()) },
    );
  };

  const dirty = selection.name.trim() !== remoteCatalog;
  const saving = updateSettings.isPending;
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
            <Field>
              <FieldLabel htmlFor="catalog-name">{t('settings.mainCatalogHeading')}</FieldLabel>
              <CatalogCombobox
                value={selection.name}
                onChange={setSelection}
                options={catalogs.data?.catalogs ?? []}
                loading={catalogs.isLoading}
                disabled={settings.isLoading || saving}
                placeholder={t('settings.catalogSelectPlaceholder')}
                searchPlaceholder={t('settings.catalogSearchPlaceholder')}
                emptyText={t('settings.catalogEmpty')}
                createLabel={(name) => t('settings.catalogCreateOption', { name })}
              />
              <FieldDescription>{t('settings.mainCatalogDesc')}</FieldDescription>
            </Field>

            {catalogsError ? (
              <Alert variant="destructive">
                <AlertCircle />
                <AlertTitle>{t('settings.catalogLoadFailed')}</AlertTitle>
                <AlertDescription>{catalogsError}</AlertDescription>
              </Alert>
            ) : null}

            <div className="flex items-center gap-3">
              <Button type="submit" disabled={!dirty || saving || !selection.name.trim()}>
                {saving ? (
                  <>
                    <Spinner /> {t('common.saving')}
                  </>
                ) : selection.create ? (
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
  const lines: string[] = [];
  lines.push(`GRANT USE CATALOG ON CATALOG \`${catalog}\` TO \`${sp}\`;`);
  for (const s of MEDALLION_SCHEMAS) {
    lines.push(`GRANT USE SCHEMA, SELECT ON SCHEMA \`${catalog}\`.\`${s}\` TO \`${sp}\`;`);
  }
  return lines.join('\n');
}
