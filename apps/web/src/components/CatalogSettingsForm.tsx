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
import { AlertCircle, CheckCircle2, Info } from 'lucide-react';
import {
  CATALOG_SETTING_KEY,
  CATALOG_USER_GROUP_DEFAULT,
  CATALOG_USER_GROUP_SETTING_KEY,
  DOWNLOADS_VOLUME_DEFAULT,
  IDENT_RE,
  MEDALLION_SCHEMA_DEFAULTS,
  MEDALLION_SCHEMAS,
  MEDALLION_SCHEMA_SETTING_KEYS,
  PRICING_SCHEMA_DEFAULT,
  catalogUserGroupFromSettings,
  medallionSchemaNamesFromSettings,
  quoteIdent,
  quotePrincipal,
  schemaGrantPrivileges,
  type ProvisionResult,
} from '@finlake/shared';
import { useI18n, type TFunction } from '../i18n';
import { useAppSettings, useUpdateAppSettings, type AppSettingsUpdateResponse } from '../api/hooks';
import { messageOf } from '../pages/Configure/utils';

type Severity = 'success' | 'warning' | 'error';

type MedallionSchemaSettingKey =
  (typeof MEDALLION_SCHEMA_SETTING_KEYS)[keyof typeof MEDALLION_SCHEMA_SETTING_KEYS];

const MEDALLION_SCHEMA_FIELDS = [
  {
    key: MEDALLION_SCHEMA_SETTING_KEYS.bronze,
    defaultValue: MEDALLION_SCHEMA_DEFAULTS.bronze,
    labelKey: 'settings.medallion.bronzeLabel',
    helpKey: 'settings.medallion.bronzeHelp',
  },
  {
    key: MEDALLION_SCHEMA_SETTING_KEYS.silver,
    defaultValue: MEDALLION_SCHEMA_DEFAULTS.silver,
    labelKey: 'settings.medallion.silverLabel',
    helpKey: 'settings.medallion.silverHelp',
  },
  {
    key: MEDALLION_SCHEMA_SETTING_KEYS.gold,
    defaultValue: MEDALLION_SCHEMA_DEFAULTS.gold,
    labelKey: 'settings.medallion.goldLabel',
    helpKey: 'settings.medallion.goldHelp',
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

export interface CatalogSettingsFormProps {
  variant?: 'page' | 'modal';
  onSaved?: (data: AppSettingsUpdateResponse, hadPreviousCatalog: boolean) => void;
}

export function CatalogSettingsForm({ variant = 'page', onSaved }: CatalogSettingsFormProps) {
  const { t } = useI18n();
  const settings = useAppSettings();
  const updateSettings = useUpdateAppSettings();

  const remoteCatalog = settings.data?.settings[CATALOG_SETTING_KEY] ?? '';
  const remoteCatalogUserGroup = catalogUserGroupFromSettings(settings.data?.settings ?? {});
  const remoteMedallionSchemas = useMemo(
    () => medallionSchemaValues(settings.data?.settings),
    [settings.data?.settings],
  );
  const [catalogNameInput, setCatalogNameInput] = useState(remoteCatalog || DEFAULT_CATALOG_NAME);
  const [createIfMissing, setCreateIfMissing] = useState(false);
  const [catalogUserGroup, setCatalogUserGroup] = useState(remoteCatalogUserGroup);
  const [medallionSchemas, setMedallionSchemas] = useState(remoteMedallionSchemas);

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
          setCreateIfMissing(false);
          onSaved?.(data, hasConfiguredCatalog);
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

  const formBody = (
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
        <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
          {MEDALLION_SCHEMA_FIELDS.map(({ key, defaultValue, labelKey, helpKey }) => (
            <Field key={key}>
              <SchemaFieldLabel label={t(labelKey)} help={t(helpKey)} />
              <Input
                value={medallionSchemas[key]}
                onChange={(e) => setMedallionSchemas((cur) => ({ ...cur, [key]: e.target.value }))}
                placeholder={defaultValue}
                disabled={settings.isLoading || saving}
              />
            </Field>
          ))}
          <Field>
            <SchemaFieldLabel
              label={t('settings.medallion.pricingLabel')}
              help={t('settings.medallion.pricingHelp')}
            />
            <Input value={PRICING_SCHEMA_DEFAULT} readOnly disabled />
          </Field>
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
          ) : (
            t(submitLabelKey(hasConfiguredCatalog, dirty, createIfMissing))
          )}
        </Button>
        {updateSettings.isSuccess && !dirty && !saving ? (
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
  );

  if (variant === 'modal') {
    return <form onSubmit={onSubmit}>{formBody}</form>;
  }

  return (
    <form onSubmit={onSubmit}>
      <Card>
        <CardHeader>
          <CardTitle>{t('settings.mainCatalogHeading')}</CardTitle>
          <CardDescription>{t('settings.mainCatalogDesc')}</CardDescription>
        </CardHeader>
        <CardContent>{formBody}</CardContent>
      </Card>
    </form>
  );
}

function SchemaFieldLabel({ label, help }: { label: string; help: string }) {
  return (
    <FieldLabel className="inline-flex items-center gap-1.5">
      {label}
      <Tooltip>
        <TooltipTrigger asChild>
          <Info className="text-muted-foreground size-3.5 cursor-help" aria-label={help} />
        </TooltipTrigger>
        <TooltipContent>{help}</TooltipContent>
      </Tooltip>
    </FieldLabel>
  );
}

function submitLabelKey(
  hasConfiguredCatalog: boolean,
  dirty: boolean,
  createIfMissing: boolean,
): string {
  if (hasConfiguredCatalog) {
    return dirty ? 'settings.save' : 'settings.fixPermission';
  }
  return createIfMissing ? 'settings.saveAndCreate' : 'settings.save';
}

function medallionSchemaValues(
  settings?: Record<string, string>,
): Record<MedallionSchemaSettingKey, string> {
  const names = medallionSchemaNamesFromSettings(settings ?? {});
  return {
    [MEDALLION_SCHEMA_SETTING_KEYS.gold]: names.gold,
    [MEDALLION_SCHEMA_SETTING_KEYS.silver]: names.silver,
    [MEDALLION_SCHEMA_SETTING_KEYS.bronze]: names.bronze,
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
  t: TFunction,
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
  if (p.pricingSchemaEnsured === 'error') {
    lines.push(t('settings.provisionSchemaFailed', { schema: PRICING_SCHEMA_DEFAULT }));
  }
  if (p.downloadsVolumeEnsured === 'error') {
    lines.push(t('settings.provisionVolumeFailed', { volume: DOWNLOADS_VOLUME_DEFAULT }));
  }

  const grantEntries: Array<{ scope: string; status: string }> = [
    { scope: t('settings.provisionScopeCatalog'), status: p.grants.catalog },
    {
      scope: t('settings.provisionScopeUsersCatalog', { group: catalogUserGroup }),
      status: p.grants.usersCatalog,
    },
    { scope: PRICING_SCHEMA_DEFAULT, status: p.grants.pricingSchema },
    { scope: DOWNLOADS_VOLUME_DEFAULT, status: p.grants.downloadsVolume },
    {
      scope: t('settings.provisionScopeUsersDownloadsVolume', { group: catalogUserGroup }),
      status: p.grants.usersDownloadsVolume,
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

  const hasSchemaError =
    Object.values(p.schemasEnsured).includes('error') ||
    p.pricingSchemaEnsured === 'error' ||
    p.downloadsVolumeEnsured === 'error';
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
    grantFailures.length > 0 || hasSchemaError
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
  lines.push(
    `CREATE SCHEMA IF NOT EXISTS ${cat}.${quoteIdent(PRICING_SCHEMA_DEFAULT)};`,
    `CREATE VOLUME IF NOT EXISTS ${cat}.${quoteIdent(
      schemaNames[MEDALLION_SCHEMA_SETTING_KEYS.bronze],
    )}.${quoteIdent(DOWNLOADS_VOLUME_DEFAULT)};`,
    `GRANT READ VOLUME ON VOLUME ${cat}.${quoteIdent(
      schemaNames[MEDALLION_SCHEMA_SETTING_KEYS.bronze],
    )}.${quoteIdent(DOWNLOADS_VOLUME_DEFAULT)} TO ${accountUsers};`,
  );
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
    lines.push(
      `GRANT USE SCHEMA, SELECT, CREATE TABLE ON SCHEMA ${cat}.${quoteIdent(
        PRICING_SCHEMA_DEFAULT,
      )} TO ${principal};`,
      `GRANT READ VOLUME, WRITE VOLUME ON VOLUME ${cat}.${quoteIdent(
        schemaNames[MEDALLION_SCHEMA_SETTING_KEYS.bronze],
      )}.${quoteIdent(DOWNLOADS_VOLUME_DEFAULT)} TO ${principal};`,
    );
  }
  return lines.join('\n');
}
