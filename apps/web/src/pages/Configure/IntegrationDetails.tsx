import { useCallback, useEffect, useMemo, useState, type FormEvent } from 'react';
import { Link } from 'react-router-dom';
import {
  Alert,
  AlertDescription,
  Button,
  Card,
  CardContent,
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
  cn,
} from '@databricks/appkit-ui/react';
import {
  ExternalLink,
  HardDrive,
  Info,
  KeyRound,
  MoreHorizontal,
  Plug,
  Settings,
  ShieldCheck,
  X,
} from 'lucide-react';
import {
  dataSourceKeyString,
  isAwsProvider,
  isDatabricksProvider,
  toDataSourceKey,
  type DataSource,
  type ServiceCredentialSummary,
} from '@finlake/shared';
import { useCreateServiceCredential, useDataSources, useDeleteDataSource } from '../../api/hooks';
import { useI18n, type Locale } from '../../i18n';
import { AwsFocusSection, DataSourceConfigurator, FocusViewSection } from './DataSourceDrawer';
import { VendorLogo } from './VendorLogo';
import {
  AwsSetupModal,
  buildAwsSetupArtifacts,
  CreateCredentialModal,
  DEFAULT_CREDENTIAL_NAME,
  DEFAULT_ROLE_NAME,
} from '../ExternalData/Credentials';
import {
  findTemplateById,
  findTemplateForRow,
  getTemplateInputConfig,
  getTemplateRegistryEntry,
} from './dataSourceCatalog';
import type { DatabricksFocusDraft } from './DataSources';
import type { AwsFocusDraft } from './useAwsFocusForm';
import { configString, messageOf, nextTableName } from './utils';

type AwsConnectAction = 'service-role' | 'external-location';

function providerRows(rows: DataSource[], templateId: string): DataSource[] {
  return rows.filter((row) => {
    if (templateId === 'aws') {
      return isAwsProvider(row.providerName);
    }
    if (templateId === 'databricks_focus13') {
      return isDatabricksProvider(row.providerName);
    }
    return findTemplateForRow(row)?.id === templateId;
  });
}

function isRegisteredAwsSource(row: DataSource): boolean {
  return ['awsAccountId', 'externalLocationName', 'exportName', 's3Prefix'].every(
    (key) => configString(row.config, key).trim().length > 0,
  );
}

// `row.accountId` is the AWS account id under the new (provider_name, account_id)
// PK. We still read `config.awsAccountId` first to keep legacy rows (created
// before the migration) rendering correctly until they are re-saved.
function awsAccountIdFor(row: DataSource): string {
  return configString(row.config, 'awsAccountId') || row.accountId;
}

function formatUpdatedAt(value: string, locale: Locale): string {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return '-';
  return new Intl.DateTimeFormat(locale === 'ja' ? 'ja-JP' : 'en-US', {
    year: 'numeric',
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  }).format(date);
}

function IntegrationHeader({ templateId }: { templateId: 'aws' | 'databricks_focus13' }) {
  const { t } = useI18n();
  const template = findTemplateById(templateId);
  const registryEntry = template ? getTemplateRegistryEntry(template) : undefined;
  if (!template) return null;

  return (
    <div className="mb-5 flex flex-wrap items-start justify-between gap-4">
      <div className="flex items-center gap-3">
        <VendorLogo source={template} logo={registryEntry?.logo} size={44} />
        <div>
          <Link
            to="/integrations"
            aria-label={t('dataSources.detail.backToIntegrations')}
            className="text-muted-foreground hover:text-foreground text-sm transition-colors"
          >
            {t('dataSources.detail.eyebrow')}
          </Link>
          <h3 className="m-0 text-xl font-semibold">{template.name}</h3>
        </div>
      </div>
      {templateId === 'aws' ? (
        <Button type="button" variant="outline" className="gap-2" asChild>
          <a
            href="https://docs.aws.amazon.com/cur/latest/userguide/what-is-data-exports.html"
            target="_blank"
            rel="noreferrer"
          >
            {t('dataSources.detail.docs')}
            <ExternalLink className="size-4" aria-hidden="true" />
          </a>
        </Button>
      ) : null}
    </div>
  );
}

export function DatabricksIntegrationDetail() {
  const dataSources = useDataSources();
  const [createdRow, setCreatedRow] = useState<DataSource | null>(null);
  const rows = dataSources.data?.items ?? [];
  const dbRows = useMemo(() => providerRows(rows, 'databricks_focus13'), [rows]);
  const row = dbRows[0] ?? createdRow;
  const template = findTemplateById('databricks_focus13');
  const input = template ? getTemplateInputConfig(template) : undefined;
  const draft: DatabricksFocusDraft | undefined =
    template && input
      ? {
          templateId: template.id,
          name: template.name,
          providerName: input.providerName,
          tableName: input.defaultTableName,
        }
      : undefined;

  return (
    <>
      <IntegrationHeader templateId="databricks_focus13" />
      {row ? (
        <DataSourceConfigurator row={row} onClose={() => setCreatedRow(null)} />
      ) : draft ? (
        <FocusViewSection row={null} draft={draft} onCreated={setCreatedRow} />
      ) : null}
    </>
  );
}

export function AwsIntegrationDetail() {
  const { locale, t } = useI18n();
  const dataSources = useDataSources();
  const createCredential = useCreateServiceCredential();
  const rows = dataSources.data?.items ?? [];
  const awsRows = useMemo(() => providerRows(rows, 'aws'), [rows]);
  const [selectedKey, setSelectedKey] = useState<string | null>(null);
  const [connectChooserOpen, setConnectChooserOpen] = useState(false);
  const [connectAction, setConnectAction] = useState<AwsConnectAction | null>(null);
  const [createServiceRoleOpen, setCreateServiceRoleOpen] = useState(false);
  const [serviceAwsAccountId, setServiceAwsAccountId] = useState('');
  const [serviceRoleName, setServiceRoleName] = useState(DEFAULT_ROLE_NAME);
  const [serviceCredentialName, setServiceCredentialName] = useState(DEFAULT_CREDENTIAL_NAME);
  const [serviceCredentialNameEdited, setServiceCredentialNameEdited] = useState(false);
  const [setupModalCredential, setSetupModalCredential] = useState<ServiceCredentialSummary | null>(
    null,
  );
  const template = findTemplateById('aws');
  const input = template ? getTemplateInputConfig(template) : undefined;
  const hasExistingAwsSources = awsRows.length > 0;
  const draft =
    template && input
      ? {
          templateId: template.id,
          name: template.name,
          providerName: input.providerName,
          tableName: nextTableName(input.defaultTableName, rows),
        }
      : undefined;
  const registeredAccountIds = useMemo(
    () => Array.from(new Set(awsRows.map(awsAccountIdFor))),
    [awsRows],
  );
  const selectedRow = selectedKey
    ? (awsRows.find((row) => dataSourceKeyString(row) === selectedKey) ?? null)
    : null;
  const normalizedServiceAccountId = serviceAwsAccountId.trim();
  const normalizedServiceRoleName = serviceRoleName.trim();
  const normalizedServiceCredentialName = serviceCredentialName.trim();
  const validServiceAccountId = /^\d{12}$/.test(normalizedServiceAccountId);
  const validServiceRoleName = /^[A-Za-z0-9_+=,.@-]{1,64}$/.test(normalizedServiceRoleName);
  const validServiceCredentialName = /^[A-Za-z_][A-Za-z0-9_]*$/.test(
    normalizedServiceCredentialName,
  );
  const canCreateServiceCredential =
    validServiceAccountId &&
    validServiceRoleName &&
    validServiceCredentialName &&
    !createCredential.isPending;
  const createServiceCredentialError = messageOf(createCredential.error);
  const setupArtifacts = useMemo(
    () => (setupModalCredential ? buildAwsSetupArtifacts(setupModalCredential) : null),
    [setupModalCredential],
  );

  const closeConnectModal = useCallback(() => setConnectAction(null), []);
  const closeConnectChooser = useCallback(() => setConnectChooserOpen(false), []);
  const openConnectAction = (action: AwsConnectAction) => {
    setConnectChooserOpen(false);
    setConnectAction(action);
  };

  const onCreated = (row: DataSource) => {
    setConnectAction(null);
    setSelectedKey(dataSourceKeyString(row));
  };
  const onServiceAccountIdChange = (value: string) => {
    setServiceAwsAccountId(value);
    if (!serviceCredentialNameEdited) {
      const trimmed = value.trim();
      setServiceCredentialName(
        trimmed ? `finlake_service_credential_${trimmed}` : DEFAULT_CREDENTIAL_NAME,
      );
    }
  };
  const openCreateServiceRole = () => {
    setConnectChooserOpen(false);
    createCredential.reset();
    setServiceCredentialNameEdited(false);
    setServiceCredentialName(
      normalizedServiceAccountId
        ? `finlake_service_credential_${normalizedServiceAccountId}`
        : DEFAULT_CREDENTIAL_NAME,
    );
    setCreateServiceRoleOpen(true);
  };
  const onSubmitServiceCredential = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (!canCreateServiceCredential) return;
    createCredential.reset();
    createCredential.mutate(
      {
        name: normalizedServiceCredentialName,
        awsAccountId: normalizedServiceAccountId,
        roleName: normalizedServiceRoleName,
      },
      {
        onSuccess: (data) => {
          setCreateServiceRoleOpen(false);
          setSetupModalCredential(data.serviceCredential);
        },
      },
    );
  };

  return (
    <>
      <IntegrationHeader templateId="aws" />
      {hasExistingAwsSources ? (
        <div className="grid gap-5">
          <div className="flex flex-wrap items-center justify-between gap-3">
            <div>
              <h4 className="m-0 text-base font-semibold">
                {t('dataSources.detail.awsAccountsTitle')}
              </h4>
              <p className="text-muted-foreground mt-1 mb-0 text-sm">
                {t('dataSources.detail.connectIntro.description')}
              </p>
            </div>
            {draft ? (
              <Button type="button" className="gap-2" onClick={() => setConnectChooserOpen(true)}>
                <Plug className="size-4" aria-hidden="true" />
                {t('dataSources.detail.connectAccount')}
              </Button>
            ) : null}
          </div>
          <AwsAccountsTable
            rows={awsRows}
            locale={locale}
            onConfigure={(row) => setSelectedKey(dataSourceKeyString(row))}
            onRemoved={(row) => {
              if (selectedKey === dataSourceKeyString(row)) setSelectedKey(null);
            }}
          />
          {selectedRow ? (
            <section className="border-border grid gap-4 rounded-md border p-4">
              <h4 className="m-0 text-sm font-semibold">
                {t('dataSources.detail.selectedSettings', {
                  account: awsAccountIdFor(selectedRow),
                })}
              </h4>
              <DataSourceConfigurator
                row={selectedRow}
                onClose={() => setSelectedKey(null)}
                showDelete={false}
              />
            </section>
          ) : null}
        </div>
      ) : draft ? (
        <div className="grid gap-5">
          <AwsConnectIntro
            onConnectWithServiceRole={() => openConnectAction('service-role')}
            onConnectWithExternalLocation={() => openConnectAction('external-location')}
            onCreateServiceRole={openCreateServiceRole}
          />
        </div>
      ) : null}
      <AwsConnectChooserModal
        open={connectChooserOpen}
        onConnectWithServiceRole={() => openConnectAction('service-role')}
        onConnectWithExternalLocation={() => openConnectAction('external-location')}
        onCreateServiceRole={openCreateServiceRole}
        onClose={closeConnectChooser}
      />
      {draft ? (
        <AwsConnectSetupModal
          action={connectAction}
          draft={draft}
          excludedAccountIds={registeredAccountIds}
          onCreated={onCreated}
          onClose={closeConnectModal}
        />
      ) : null}
      <CreateCredentialModal
        open={createServiceRoleOpen}
        awsAccountId={serviceAwsAccountId}
        roleName={serviceRoleName}
        createPending={createCredential.isPending}
        canSubmit={canCreateServiceCredential}
        validAccountId={validServiceAccountId}
        validRoleName={validServiceRoleName}
        validServiceCredentialName={validServiceCredentialName}
        createError={createServiceCredentialError}
        setServiceAwsAccountId={onServiceAccountIdChange}
        setServiceRoleName={setServiceRoleName}
        onSubmitService={onSubmitServiceCredential}
        onClose={() => setCreateServiceRoleOpen(false)}
      />
      <AwsSetupModal
        credential={setupModalCredential}
        artifacts={setupArtifacts}
        onClose={() => setSetupModalCredential(null)}
      />
    </>
  );
}

function AwsConnectActions({
  onConnectWithServiceRole,
  onConnectWithExternalLocation,
  onCreateServiceRole,
}: {
  onConnectWithServiceRole: () => void;
  onConnectWithExternalLocation: () => void;
  onCreateServiceRole: () => void;
}) {
  const { t } = useI18n();
  return (
    <div className="flex flex-wrap gap-3">
      <Button type="button" className="gap-2" onClick={onConnectWithServiceRole}>
        <ShieldCheck className="size-4" aria-hidden="true" />
        {t('dataSources.detail.connectIntro.actions.serviceRole')}
      </Button>
      <Button
        type="button"
        variant="outline"
        className="gap-2"
        onClick={onConnectWithExternalLocation}
      >
        <HardDrive className="size-4" aria-hidden="true" />
        {t('dataSources.detail.connectIntro.actions.externalLocation')}
      </Button>
      <Button type="button" variant="outline" className="gap-2" onClick={onCreateServiceRole}>
        <KeyRound className="size-4" aria-hidden="true" />
        {t('dataSources.detail.connectIntro.actions.createServiceRole')}
      </Button>
    </div>
  );
}

function AwsConnectIntro({
  onConnectWithServiceRole,
  onConnectWithExternalLocation,
  onCreateServiceRole,
}: {
  onConnectWithServiceRole: () => void;
  onConnectWithExternalLocation: () => void;
  onCreateServiceRole: () => void;
}) {
  const { t } = useI18n();
  return (
    <section className="grid max-w-5xl gap-5">
      <h4 className="m-0 text-2xl font-semibold">{t('dataSources.detail.connectIntro.title')}</h4>
      <p className="text-muted-foreground m-0 text-base">
        {t('dataSources.detail.connectIntro.description')}
      </p>
      <div className="grid gap-4">
        <section className="grid gap-2">
          <h5 className="text-foreground m-0 text-sm font-semibold">
            {t('dataSources.detail.connectIntro.serviceCredentialTitle')}
          </h5>
          <p className="text-muted-foreground m-0 text-sm">
            {t('dataSources.detail.connectIntro.serviceCredentialDesc')}
          </p>
        </section>
        <section className="grid gap-2">
          <h5 className="text-foreground m-0 text-sm font-semibold">
            {t('dataSources.detail.connectIntro.setupPathTitle')}
          </h5>
          <ul className="text-muted-foreground m-0 grid gap-1 pl-5 text-sm">
            <li>{t('dataSources.detail.connectIntro.setupPathServiceRole')}</li>
            <li>{t('dataSources.detail.connectIntro.setupPathExternalLocation')}</li>
          </ul>
          <p className="text-muted-foreground m-0 text-sm">
            <strong className="text-foreground">
              {t('dataSources.detail.connectIntro.noteLabel')}
            </strong>{' '}
            {t('dataSources.detail.connectIntro.note')}
          </p>
        </section>
        <section className="grid gap-2">
          <h5 className="text-foreground m-0 text-sm font-semibold">
            {t('dataSources.detail.connectIntro.afterSetupTitle')}
          </h5>
          <p className="text-muted-foreground m-0 text-sm">
            <strong className="text-foreground">
              {t('dataSources.detail.connectIntro.returnLabel')}
            </strong>{' '}
            {t('dataSources.detail.connectIntro.afterSetupDesc')}
          </p>
        </section>
      </div>
      <div className="mt-2">
        <AwsConnectActions
          onConnectWithServiceRole={onConnectWithServiceRole}
          onConnectWithExternalLocation={onConnectWithExternalLocation}
          onCreateServiceRole={onCreateServiceRole}
        />
      </div>
    </section>
  );
}

function AwsConnectChooserModal({
  open,
  onConnectWithServiceRole,
  onConnectWithExternalLocation,
  onCreateServiceRole,
  onClose,
}: {
  open: boolean;
  onConnectWithServiceRole: () => void;
  onConnectWithExternalLocation: () => void;
  onCreateServiceRole: () => void;
  onClose: () => void;
}) {
  const { t } = useI18n();

  useEffect(() => {
    if (!open) return;
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') onClose();
    };
    window.addEventListener('keydown', onKeyDown);
    return () => window.removeEventListener('keydown', onKeyDown);
  }, [open, onClose]);

  if (!open) return null;

  return (
    <div
      className="fixed inset-0 z-[60] flex items-center justify-center bg-black/55 p-4"
      role="presentation"
      onMouseDown={onClose}
    >
      <div
        role="dialog"
        aria-modal="true"
        aria-labelledby="aws-connect-chooser-title"
        className="bg-background border-border grid w-full max-w-2xl gap-5 rounded-lg border p-5 shadow-xl"
        onMouseDown={(event) => event.stopPropagation()}
      >
        <div className="flex items-start justify-between gap-4">
          <div className="min-w-0">
            <h3 id="aws-connect-chooser-title" className="m-0 text-base font-semibold">
              {t('dataSources.detail.connectAccount')}
            </h3>
            <p className="text-muted-foreground mt-1 mb-0 text-sm">
              {t('dataSources.detail.connectIntro.description')}
            </p>
          </div>
          <button
            type="button"
            className="text-muted-foreground hover:text-foreground hover:bg-muted/40 grid size-8 place-items-center rounded-md transition-colors"
            aria-label={t('common.close')}
            onClick={onClose}
          >
            <X className="size-4" aria-hidden="true" />
          </button>
        </div>
        <AwsConnectActions
          onConnectWithServiceRole={onConnectWithServiceRole}
          onConnectWithExternalLocation={onConnectWithExternalLocation}
          onCreateServiceRole={onCreateServiceRole}
        />
      </div>
    </div>
  );
}

function AwsConnectSetupModal({
  action,
  draft,
  excludedAccountIds,
  onCreated,
  onClose,
}: {
  action: AwsConnectAction | null;
  draft: AwsFocusDraft;
  excludedAccountIds: string[];
  onCreated: (row: DataSource) => void;
  onClose: () => void;
}) {
  const { t } = useI18n();
  const [createProgressOpen, setCreateProgressOpen] = useState(false);
  const [createProgressComplete, setCreateProgressComplete] = useState(false);

  useEffect(() => {
    if (!action) return;
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape' && !createProgressOpen) onClose();
    };
    window.addEventListener('keydown', onKeyDown);
    return () => window.removeEventListener('keydown', onKeyDown);
  }, [action, createProgressOpen, onClose]);

  useEffect(() => {
    if (!action) return;
    setCreateProgressOpen(false);
    setCreateProgressComplete(false);
  }, [action]);

  if (!action) return null;

  const setupMode = action === 'service-role' ? 'create' : 'existing';
  const titleKey =
    action === 'service-role'
      ? 'dataSources.detail.connectIntro.actions.serviceRole'
      : 'dataSources.detail.connectIntro.actions.externalLocation';
  const descriptionKey =
    action === 'service-role'
      ? 'dataSources.detail.connectIntro.actionDescriptions.serviceRole'
      : 'dataSources.detail.connectIntro.actionDescriptions.externalLocation';

  return (
    <div
      className={cn(
        'fixed inset-0 z-[60] flex items-center justify-center p-4',
        createProgressComplete ? 'bg-transparent' : 'bg-black/55',
      )}
      role="presentation"
      onMouseDown={createProgressOpen ? undefined : onClose}
    >
      <div
        role="dialog"
        aria-modal="true"
        aria-labelledby="aws-connect-setup-modal-title"
        className={cn(
          'bg-background border-border grid max-h-[88vh] w-full max-w-3xl grid-rows-[auto_1fr] rounded-lg border shadow-xl',
          createProgressComplete ? 'contents' : null,
        )}
        onMouseDown={(event) => event.stopPropagation()}
      >
        <div
          className={cn(
            'flex items-start justify-between gap-4 p-5',
            createProgressComplete ? 'hidden' : null,
          )}
        >
          <div className="min-w-0">
            <h3 id="aws-connect-setup-modal-title" className="text-base font-semibold">
              {t(titleKey)}
            </h3>
            <p className="text-muted-foreground mt-1 mb-0 text-sm">{t(descriptionKey)}</p>
          </div>
          <button
            type="button"
            className="text-muted-foreground hover:text-foreground hover:bg-muted/40 grid size-8 place-items-center rounded-md transition-colors"
            aria-label={t('common.close')}
            onClick={onClose}
          >
            <X className="size-4" aria-hidden="true" />
          </button>
        </div>
        <div
          className={cn('min-h-0 overflow-y-auto p-5', createProgressComplete ? 'contents' : null)}
        >
          <AwsFocusSection
            key={action}
            row={null}
            draft={draft}
            excludedAccountIds={excludedAccountIds}
            initialSetupMode={setupMode}
            hideSetupMode
            onCreated={onCreated}
            onCreateProgressOpenChange={setCreateProgressOpen}
            onCreateProgressCompleteChange={setCreateProgressComplete}
          />
        </div>
      </div>
    </div>
  );
}

function AwsAccountsTable({
  rows,
  locale,
  onConfigure,
  onRemoved,
}: {
  rows: DataSource[];
  locale: Locale;
  onConfigure: (row: DataSource) => void;
  onRemoved: (row: DataSource) => void;
}) {
  const { t } = useI18n();
  const deleteDs = useDeleteDataSource();
  const deleteErrorMessage = messageOf(deleteDs.error);

  const onRemove = (row: DataSource) => {
    if (!window.confirm(t('dataSources.confirmDelete', { name: awsAccountIdFor(row) }))) return;
    deleteDs.mutate(toDataSourceKey(row), { onSuccess: () => onRemoved(row) });
  };

  if (rows.length === 0) {
    return (
      <Card>
        <CardContent className="text-muted-foreground py-8 text-sm">
          {t('dataSources.detail.awsEmpty')}
        </CardContent>
      </Card>
    );
  }

  return (
    <div className="grid gap-3">
      {deleteErrorMessage ? (
        <Alert variant="destructive">
          <Info />
          <AlertDescription>{deleteErrorMessage}</AlertDescription>
        </Alert>
      ) : null}
      <div className="overflow-x-auto rounded-md border">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>{t('dataSources.detail.columns.account')}</TableHead>
              <TableHead>{t('dataSources.columns.table')}</TableHead>
              <TableHead>{t('dataSources.detail.columns.costsAggregation')}</TableHead>
              <TableHead>{t('dataSources.detail.columns.perResourceCosts')}</TableHead>
              <TableHead>{t('dataSources.detail.columns.lastUpdated')}</TableHead>
              <TableHead>{t('dataSources.detail.columns.status')}</TableHead>
              <TableHead className="text-right" aria-label={t('dataSources.columns.actions')} />
            </TableRow>
          </TableHeader>
          <TableBody>
            {rows.map((row) => (
              <TableRow key={dataSourceKeyString(row)}>
                <TableCell>
                  <div className="min-w-40 font-medium">{awsAccountIdFor(row)}</div>
                </TableCell>
                <TableCell>
                  <span className="text-muted-foreground font-mono text-xs">{row.tableName}</span>
                </TableCell>
                <TableCell>
                  {row.enabled
                    ? t('dataSources.badges.enabled')
                    : t('dataSources.badges.setupRequired')}
                </TableCell>
                <TableCell>-</TableCell>
                <TableCell>{formatUpdatedAt(row.updatedAt, locale)}</TableCell>
                <TableCell>
                  {isRegisteredAwsSource(row)
                    ? t('dataSources.detail.connected')
                    : t('dataSources.badges.setupRequired')}
                </TableCell>
                <TableCell className="text-right">
                  <div className="flex justify-end gap-2">
                    <Button
                      type="button"
                      variant="outline"
                      size="icon"
                      className="size-8"
                      aria-label={t('dataSources.detail.configureAccount', {
                        account: awsAccountIdFor(row),
                      })}
                      onClick={() => onConfigure(row)}
                    >
                      <Settings className="size-4" aria-hidden="true" />
                    </Button>
                    <DropdownMenu>
                      <DropdownMenuTrigger asChild>
                        <Button
                          type="button"
                          variant="outline"
                          size="icon"
                          className="size-8"
                          aria-label={t('dataSources.detail.moreActions')}
                        >
                          <MoreHorizontal className="size-4" aria-hidden="true" />
                        </Button>
                      </DropdownMenuTrigger>
                      <DropdownMenuContent align="end">
                        <DropdownMenuItem asChild>
                          <a
                            href="https://console.aws.amazon.com/"
                            target="_blank"
                            rel="noreferrer"
                          >
                            {t('dataSources.detail.openInAws')}
                          </a>
                        </DropdownMenuItem>
                        <DropdownMenuItem
                          disabled={deleteDs.isPending}
                          onClick={() => onRemove(row)}
                        >
                          {t('dataSources.detail.remove')}
                        </DropdownMenuItem>
                      </DropdownMenuContent>
                    </DropdownMenu>
                  </div>
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </div>
    </div>
  );
}
