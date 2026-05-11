import { useEffect, useMemo, useState, type FormEvent } from 'react';
import {
  Alert,
  AlertDescription,
  AlertTitle,
  Badge,
  Button,
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
  Field,
  FieldGroup,
  FieldLabel,
  Input,
  Skeleton,
  Spinner,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@databricks/appkit-ui/react';
import { roleNameFromArn, type ServiceCredentialSummary } from '@finlake/shared';
import { AlertCircle, ExternalLink, MoreVertical, RefreshCcw, Trash2, X } from 'lucide-react';
import {
  useCreateServiceCredential,
  useDeleteCredential,
  useMe,
  useServiceCredentials,
} from '../../api/hooks';
import { CodeBlock } from '../../components/CodeBlock';
import { useI18n } from '../../i18n';
import { messageOf } from '../Configure/utils';

const DEFAULT_CREDENTIAL_NAME = 'finlake_service_credential_{undefined}';
const DEFAULT_ROLE_NAME = 'FinLakeServiceRole';
const SERVICE_POLICY_NAME = 'FinLakeDataExportManagement';
const STORAGE_ROLE_BOUNDARY_POLICY_NAME = 'FinLakeStorageRoleBoundary';
const STORAGE_ROLE_NAME_PREFIX = 'FinLakeStorageRole';
type ServicePermissionType = 'fullSetup' | 'readOnly';

export function Credentials() {
  const { t } = useI18n();
  const credentials = useServiceCredentials();
  const createCredential = useCreateServiceCredential();
  const deleteCredential = useDeleteCredential();
  const me = useMe();
  const workspaceUrl = me.data?.workspaceUrl ?? null;
  const [awsAccountId, setAwsAccountId] = useState('');
  const [roleName, setRoleName] = useState(DEFAULT_ROLE_NAME);
  const [serviceCredentialName, setServiceCredentialName] = useState(DEFAULT_CREDENTIAL_NAME);
  const [serviceCredentialNameEdited, setServiceCredentialNameEdited] = useState(false);
  const [createModalOpen, setCreateModalOpen] = useState(false);
  const [setupModalCredential, setSetupModalCredential] = useState<ServiceCredentialSummary | null>(
    null,
  );
  const [servicePermissionType, setServicePermissionType] =
    useState<ServicePermissionType>('fullSetup');

  const normalizedAccountId = awsAccountId.trim();
  const normalizedRoleName = roleName.trim();
  const normalizedName = serviceCredentialName.trim();
  const validAccountId = /^\d{12}$/.test(normalizedAccountId);
  const validRoleName = /^[A-Za-z0-9_+=,.@-]{1,64}$/.test(normalizedRoleName);
  const validServiceName = /^[A-Za-z_][A-Za-z0-9_]*$/.test(normalizedName);
  const canSubmit =
    validAccountId && validRoleName && validServiceName && !createCredential.isPending;
  const listError = messageOf(credentials.error);
  const createError = messageOf(createCredential.error);

  const setupArtifacts = useMemo(
    () =>
      setupModalCredential
        ? buildAwsSetupArtifacts(setupModalCredential, servicePermissionType)
        : null,
    [servicePermissionType, setupModalCredential],
  );

  const onSubmit = (e: FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    if (!canSubmit) return;
    createCredential.reset();
    createCredential.mutate(
      {
        name: normalizedName,
        awsAccountId: normalizedAccountId,
        roleName: normalizedRoleName,
      },
      {
        onSuccess: (data) => {
          setCreateModalOpen(false);
          setServicePermissionType('fullSetup');
          setSetupModalCredential(data.serviceCredential);
        },
      },
    );
  };

  const onServiceAccountIdChange = (value: string) => {
    setAwsAccountId(value);
    if (!serviceCredentialNameEdited) {
      const trimmed = value.trim();
      setServiceCredentialName(
        trimmed ? `finlake_service_credential_${trimmed}` : DEFAULT_CREDENTIAL_NAME,
      );
    }
  };

  const openSetupModal = (credential: ServiceCredentialSummary) => {
    setServicePermissionType('fullSetup');
    setSetupModalCredential(credential);
  };

  const onDeleteCredential = (name: string) => {
    if (!window.confirm(t('credentials.deleteConfirm', { name }))) return;
    deleteCredential.mutate(name);
  };

  return (
    <div className="space-y-6">
      <Card>
        <CardHeader>
          <div className="flex flex-wrap items-start justify-between gap-3">
            <div>
              <CardTitle>{t('credentials.serviceTitle')}</CardTitle>
              <CardDescription>{t('credentials.desc')}</CardDescription>
            </div>
            <div className="flex items-center gap-2">
              <Button
                type="button"
                variant="secondary"
                size="sm"
                onClick={() => credentials.refetch()}
                disabled={credentials.isFetching}
                aria-label={t('credentials.refresh')}
                title={t('credentials.refresh')}
              >
                <RefreshCcw className={credentials.isFetching ? 'animate-spin' : undefined} />
              </Button>
              <Button
                type="button"
                size="sm"
                className="bg-(--success) text-(--background) hover:bg-(--success)/90"
                onClick={() => {
                  createCredential.reset();
                  setServiceCredentialNameEdited(false);
                  setServiceCredentialName(
                    normalizedAccountId
                      ? `finlake_service_credential_${normalizedAccountId}`
                      : DEFAULT_CREDENTIAL_NAME,
                  );
                  setCreateModalOpen(true);
                }}
              >
                {t('credentials.createCredential')}
              </Button>
            </div>
          </div>
        </CardHeader>
        <CardContent>
          {listError ? (
            <Alert variant="destructive" className="mb-4">
              <AlertCircle />
              <AlertTitle>{t('credentials.loadFailed')}</AlertTitle>
              <AlertDescription>{listError}</AlertDescription>
            </Alert>
          ) : null}

          {credentials.isLoading ? (
            <LoadingRows />
          ) : (
            <ServiceCredentialTable
              rows={credentials.data?.serviceCredentials ?? []}
              workspaceUrl={workspaceUrl}
              onSelect={openSetupModal}
              onDelete={onDeleteCredential}
              deletingName={
                deleteCredential.isPending ? (deleteCredential.variables ?? null) : null
              }
            />
          )}
        </CardContent>
      </Card>

      <CreateCredentialModal
        open={createModalOpen}
        awsAccountId={awsAccountId}
        roleName={roleName}
        serviceCredentialName={serviceCredentialName}
        createPending={createCredential.isPending}
        canSubmit={canSubmit}
        validAccountId={validAccountId}
        validRoleName={validRoleName}
        validServiceCredentialName={validServiceName}
        createError={createError}
        setServiceAwsAccountId={onServiceAccountIdChange}
        setServiceRoleName={setRoleName}
        setServiceCredentialName={(value) => {
          setServiceCredentialNameEdited(true);
          setServiceCredentialName(value);
        }}
        onSubmitService={onSubmit}
        onClose={() => setCreateModalOpen(false)}
      />

      <AwsSetupModal
        credential={setupModalCredential}
        artifacts={setupArtifacts}
        permissionType={servicePermissionType}
        setPermissionType={setServicePermissionType}
        onClose={() => setSetupModalCredential(null)}
      />
    </div>
  );
}

function CreateCredentialModal({
  open,
  awsAccountId,
  roleName,
  serviceCredentialName,
  createPending,
  canSubmit,
  validAccountId,
  validRoleName,
  validServiceCredentialName,
  createError,
  setServiceAwsAccountId,
  setServiceRoleName,
  setServiceCredentialName,
  onSubmitService,
  onClose,
}: {
  open: boolean;
  awsAccountId: string;
  roleName: string;
  serviceCredentialName: string;
  createPending: boolean;
  canSubmit: boolean;
  validAccountId: boolean;
  validRoleName: boolean;
  validServiceCredentialName: boolean;
  createError: string | null;
  setServiceAwsAccountId: (value: string) => void;
  setServiceRoleName: (value: string) => void;
  setServiceCredentialName: (value: string) => void;
  onSubmitService: (e: FormEvent<HTMLFormElement>) => void;
  onClose: () => void;
}) {
  const { t } = useI18n();
  const [submitted, setSubmitted] = useState(false);
  const showValidationHint =
    submitted && (!validAccountId || !validRoleName || !validServiceCredentialName);

  useEffect(() => {
    if (!open) return;
    setSubmitted(false);
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape' && !createPending) onClose();
    };
    window.addEventListener('keydown', onKeyDown);
    return () => window.removeEventListener('keydown', onKeyDown);
  }, [createPending, onClose, open]);

  if (!open) return null;

  return (
    <div
      className="fixed inset-0 z-[80] flex items-center justify-center bg-black/55 p-4"
      role="presentation"
      onMouseDown={() => {
        if (!createPending) onClose();
      }}
    >
      <form
        role="dialog"
        aria-modal="true"
        aria-labelledby="create-credential-modal-title"
        className="bg-background border-border w-full max-w-2xl rounded-lg border shadow-xl"
        onMouseDown={(event) => event.stopPropagation()}
        onSubmit={(event) => {
          setSubmitted(true);
          if (!canSubmit) {
            event.preventDefault();
            return;
          }
          onSubmitService(event);
        }}
      >
        <div className="flex items-start justify-between gap-4 p-5">
          <div className="min-w-0">
            <h3 id="create-credential-modal-title" className="text-base font-semibold">
              {t('credentials.createModalTitle')}
            </h3>
            <p className="text-muted-foreground mt-1 mb-0 text-sm">{t('credentials.createDesc')}</p>
          </div>
          <button
            type="button"
            className="text-muted-foreground hover:text-foreground hover:bg-muted/40 grid size-8 place-items-center rounded-md transition-colors disabled:opacity-50"
            aria-label={t('common.close')}
            onClick={onClose}
            disabled={createPending}
          >
            <X className="size-4" aria-hidden="true" />
          </button>
        </div>

        <div className="p-5">
          <FieldGroup>
            <div className="grid max-w-2xl gap-3">
              <Field>
                <FieldLabel>{t('credentials.awsAccountId')}</FieldLabel>
                <Input
                  value={awsAccountId}
                  onChange={(e) => setServiceAwsAccountId(e.target.value)}
                  placeholder="123456789012"
                  inputMode="numeric"
                  disabled={createPending}
                />
              </Field>
              <Field>
                <FieldLabel>{t('credentials.roleName')}</FieldLabel>
                <Input
                  value={roleName}
                  onChange={(e) => setServiceRoleName(e.target.value)}
                  disabled={createPending}
                />
              </Field>
            </div>

            <Field>
              <FieldLabel>{t('credentials.credentialName')}</FieldLabel>
              <Input
                value={serviceCredentialName}
                onChange={(e) => setServiceCredentialName(e.target.value)}
                disabled={createPending}
              />
            </Field>

            {showValidationHint ? (
              <Alert>
                <AlertCircle />
                <AlertDescription>{t('credentials.validationHint')}</AlertDescription>
              </Alert>
            ) : null}

            {createError ? (
              <Alert variant="destructive">
                <AlertCircle />
                <AlertTitle>{t('credentials.createFailed')}</AlertTitle>
                <AlertDescription>{createError}</AlertDescription>
              </Alert>
            ) : null}
          </FieldGroup>
        </div>

        <div className="flex justify-end gap-2 p-5 pt-0">
          <Button type="button" variant="secondary" onClick={onClose} disabled={createPending}>
            {t('common.cancel')}
          </Button>
          <Button
            type="submit"
            className="bg-(--success) text-(--background) hover:bg-(--success)/90"
            disabled={createPending}
          >
            {createPending ? (
              <>
                <Spinner /> {t('credentials.creating')}
              </>
            ) : (
              t('credentials.createShort')
            )}
          </Button>
        </div>
      </form>
    </div>
  );
}

function LoadingRows() {
  return (
    <div className="space-y-2">
      <Skeleton className="h-10 w-full" />
      <Skeleton className="h-10 w-full" />
      <Skeleton className="h-10 w-full" />
    </div>
  );
}

function ServiceCredentialTable({
  rows,
  workspaceUrl,
  onSelect,
  deletingName,
  onDelete,
}: {
  rows: ServiceCredentialSummary[];
  workspaceUrl: string | null;
  onSelect: (row: ServiceCredentialSummary) => void;
  deletingName: string | null;
  onDelete: (name: string) => void;
}) {
  const { t } = useI18n();

  if (rows.length === 0) {
    return (
      <div className="border-border rounded-md border p-6 text-sm">
        <div className="font-medium">{t('credentials.emptyTitle')}</div>
        <p className="text-muted-foreground mt-1 mb-0">{t('credentials.emptyDesc')}</p>
      </div>
    );
  }

  return (
    <div className="overflow-x-auto">
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead>{t('credentials.columns.name')}</TableHead>
            <TableHead>{t('credentials.columns.roleArn')}</TableHead>
            <TableHead>{t('credentials.columns.comment')}</TableHead>
            <TableHead className="text-right" aria-label={t('credentials.columns.actions')} />
          </TableRow>
        </TableHeader>
        <TableBody>
          {rows.map((row) => (
            <TableRow key={row.name}>
              <TableCell>
                <CredentialNameLink name={row.name} workspaceUrl={workspaceUrl} />
              </TableCell>
              <TableCell>
                <code className="text-muted-foreground block max-w-96 truncate text-xs">
                  {row.roleArn ?? '-'}
                </code>
              </TableCell>
              <TableCell className="text-muted-foreground max-w-80 truncate">
                {row.comment ?? '-'}
              </TableCell>
              <TableCell className="text-right">
                <div className="flex justify-end gap-2">
                  <Button type="button" variant="secondary" size="sm" onClick={() => onSelect(row)}>
                    {t('credentials.setup')}
                  </Button>
                  <CredentialActions
                    name={row.name}
                    deleting={deletingName === row.name}
                    onDelete={onDelete}
                  />
                </div>
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </div>
  );
}

function CredentialNameLink({ name, workspaceUrl }: { name: string; workspaceUrl: string | null }) {
  const href = workspaceUrl
    ? `${workspaceUrl}/explore/credentials/${encodeURIComponent(name)}`
    : null;
  if (!href) {
    return <div className="min-w-48 font-medium">{name}</div>;
  }
  return (
    <a
      href={href}
      target="_blank"
      rel="noreferrer noopener"
      className="text-primary inline-flex min-w-48 items-center gap-1 font-medium hover:underline"
    >
      {name}
      <ExternalLink className="size-3.5" aria-hidden="true" />
    </a>
  );
}

function CredentialActions({
  name,
  deleting,
  onDelete,
}: {
  name: string;
  deleting: boolean;
  onDelete: (name: string) => void;
}) {
  const { t } = useI18n();
  return (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <Button
          type="button"
          variant="secondary"
          size="sm"
          aria-label={t('credentials.actions.open')}
          disabled={deleting}
        >
          <MoreVertical className="size-4" aria-hidden="true" />
        </Button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end">
        <DropdownMenuItem onClick={() => onDelete(name)} disabled={deleting}>
          <Trash2 className="size-4" aria-hidden="true" />
          <span>
            {deleting ? t('credentials.actions.deleting') : t('credentials.actions.delete')}
          </span>
        </DropdownMenuItem>
      </DropdownMenuContent>
    </DropdownMenu>
  );
}

function ArtifactBlock({ title, body }: { title: string; body: string }) {
  return (
    <section className="space-y-2">
      <h3 className="text-sm font-semibold">{title}</h3>
      <CodeBlock>{body}</CodeBlock>
    </section>
  );
}

function AwsSetupModal({
  credential,
  artifacts,
  permissionType,
  setPermissionType,
  onClose,
}: {
  credential: ServiceCredentialSummary | null;
  artifacts: ReturnType<typeof buildAwsSetupArtifacts>;
  permissionType: ServicePermissionType;
  setPermissionType: (permissionType: ServicePermissionType) => void;
  onClose: () => void;
}) {
  const { t } = useI18n();

  useEffect(() => {
    if (!credential) return;
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') onClose();
    };
    window.addEventListener('keydown', onKeyDown);
    return () => window.removeEventListener('keydown', onKeyDown);
  }, [credential, onClose]);

  if (!credential) return null;

  return (
    <div
      className="fixed inset-0 z-[80] flex items-center justify-center bg-black/55 p-4"
      role="presentation"
      onMouseDown={onClose}
    >
      <div
        role="dialog"
        aria-modal="true"
        aria-labelledby="aws-service-credential-modal-title"
        className="bg-background border-border grid max-h-[88vh] w-full max-w-5xl grid-rows-[auto_1fr] rounded-lg border shadow-xl"
        onMouseDown={(event) => event.stopPropagation()}
      >
        <div className="flex items-start justify-between gap-4 p-5">
          <div className="min-w-0">
            <div className="flex flex-wrap items-center gap-3">
              <h3 id="aws-service-credential-modal-title" className="text-base font-semibold">
                {t('credentials.awsSetupTitle')}
              </h3>
              <Badge variant="secondary">{credential.name}</Badge>
              <Badge variant="secondary" className="gap-1.5">
                <span className="text-muted-foreground">{t('credentials.externalIdLabel')}</span>
                <span className="max-w-64 truncate">{credential.externalId ?? '-'}</span>
              </Badge>
            </div>
            <p className="text-muted-foreground mt-1 mb-0 text-sm">
              {t('credentials.awsSetupDesc')}
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

        <div className="min-h-0 overflow-y-auto p-5">
          {artifacts ? (
            <div className="space-y-5">
              <div className="border-border inline-flex flex-wrap rounded-md border p-1">
                <TabButton
                  active={permissionType === 'fullSetup'}
                  onClick={() => setPermissionType('fullSetup')}
                >
                  {t('credentials.permissionTypes.fullSetup')}
                </TabButton>
                <TabButton
                  active={permissionType === 'readOnly'}
                  onClick={() => setPermissionType('readOnly')}
                >
                  {t('credentials.permissionTypes.readOnly')}
                </TabButton>
              </div>

              <div className="grid gap-4">
                {artifacts.cliBlocks.map((block) => (
                  <ArtifactBlock key={block.titleKey} title={t(block.titleKey)} body={block.body} />
                ))}
              </div>
            </div>
          ) : (
            <Alert variant="destructive">
              <AlertCircle />
              <AlertTitle>{t('credentials.awsSetupUnavailableTitle')}</AlertTitle>
              <AlertDescription>{t('credentials.awsSetupUnavailableDesc')}</AlertDescription>
            </Alert>
          )}
        </div>
      </div>
    </div>
  );
}

function TabButton({
  active,
  onClick,
  children,
}: {
  active: boolean;
  onClick: () => void;
  children: string;
}) {
  return (
    <button
      type="button"
      role="tab"
      aria-selected={active}
      className={`rounded px-3 py-1.5 text-sm transition-colors ${
        active
          ? 'bg-secondary text-secondary-foreground shadow-sm'
          : 'text-muted-foreground hover:text-foreground hover:bg-muted/40'
      }`}
      onClick={onClick}
    >
      {children}
    </button>
  );
}

function buildAwsSetupArtifacts(
  credential: ServiceCredentialSummary,
  servicePermissionType: ServicePermissionType,
) {
  const roleArn = credential.roleArn;
  const externalId = credential.externalId;
  const unityCatalogIamArn = credential.unityCatalogIamArn;
  const roleName = roleArn ? roleNameFromArn(roleArn) : null;
  const awsAccountId = roleArn ? awsAccountIdFromRoleArn(roleArn) : null;
  if (!roleArn || !externalId || !unityCatalogIamArn || !roleName || !awsAccountId) return null;

  const trustPolicy = stableJson({
    Version: '2012-10-17',
    Statement: [
      {
        Effect: 'Allow',
        Principal: {
          AWS: [unityCatalogIamArn, roleArn],
        },
        Action: 'sts:AssumeRole',
        Condition: {
          StringEquals: {
            'sts:ExternalId': externalId,
          },
        },
      },
    ],
  });
  const initialTrustPolicy = stableJson({
    Version: '2012-10-17',
    Statement: [
      {
        Effect: 'Allow',
        Principal: {
          AWS: unityCatalogIamArn,
        },
        Action: 'sts:AssumeRole',
        Condition: {
          StringEquals: {
            'sts:ExternalId': externalId,
          },
        },
      },
    ],
  });

  const permissionsBoundaryPolicy =
    servicePermissionType === 'fullSetup'
      ? stableJson(buildStorageRoleBoundaryPolicy(awsAccountId))
      : null;
  const permissionPolicy = stableJson(
    buildServicePermissionPolicy(roleArn, awsAccountId, servicePermissionType),
  );

  const createBoundaryPolicyCli = permissionsBoundaryPolicy
    ? [
        `aws iam create-policy \\`,
        `  --policy-name ${shellArg(STORAGE_ROLE_BOUNDARY_POLICY_NAME)} \\`,
        `  --policy-document ${shellArg(permissionsBoundaryPolicy)}`,
      ].join('\n')
    : null;

  const createRoleCli = [
    `aws iam create-role \\`,
    `  --role-name ${shellArg(roleName)} \\`,
    `  --assume-role-policy-document ${shellArg(initialTrustPolicy)}`,
  ].join('\n');

  const updateTrustPolicyCli = [
    `aws iam update-assume-role-policy \\`,
    `  --role-name ${shellArg(roleName)} \\`,
    `  --policy-document ${shellArg(trustPolicy)}`,
  ].join('\n');

  const putRolePolicyCli = [
    `aws iam put-role-policy \\`,
    `  --role-name ${shellArg(roleName)} \\`,
    `  --policy-name ${shellArg(SERVICE_POLICY_NAME)} \\`,
    `  --policy-document ${shellArg(permissionPolicy)}`,
  ].join('\n');

  const cliBlocks = [
    createBoundaryPolicyCli
      ? {
          titleKey: 'credentials.createBoundaryPolicyCli',
          body: createBoundaryPolicyCli,
        }
      : null,
    {
      titleKey: 'credentials.createRoleCli',
      body: createRoleCli,
    },
    {
      titleKey: 'credentials.updateTrustPolicyCli',
      body: updateTrustPolicyCli,
    },
    {
      titleKey: 'credentials.putRolePolicyCli',
      body: putRolePolicyCli,
    },
  ].filter((block): block is { titleKey: string; body: string } => block !== null);

  return { cliBlocks, trustPolicy, permissionPolicy, permissionsBoundaryPolicy };
}

function buildServicePermissionPolicy(
  roleArn: string,
  awsAccountId: string,
  permissionType: ServicePermissionType,
) {
  const statements: Array<Record<string, unknown>> = [];

  if (permissionType === 'readOnly') {
    statements.push(
      {
        Sid: 'ReadBillingDataExports',
        Effect: 'Allow',
        Action: ['bcm-data-exports:GetExport', 'bcm-data-exports:ListExports'],
        Resource: '*',
      },
      {
        Sid: 'ReadCostAllocationTags',
        Effect: 'Allow',
        Action: ['ce:ListCostAllocationTags'],
        Resource: '*',
      },
    );
  } else {
    statements.push({
      Sid: 'ManageBillingDataExports',
      Effect: 'Allow',
      Action: [
        'bcm-data-exports:CreateExport',
        'bcm-data-exports:GetExport',
        'bcm-data-exports:ListExports',
        'bcm-data-exports:UpdateExport',
        'bcm-data-exports:DeleteExport',
        'bcm-data-exports:TagResource',
        'cur:PutReportDefinition',
      ],
      Resource: '*',
    });
  }

  if (permissionType === 'fullSetup') {
    const storageRoleArn = `arn:aws:iam::${awsAccountId}:role/${STORAGE_ROLE_NAME_PREFIX}*`;
    const boundaryPolicyArn = `arn:aws:iam::${awsAccountId}:policy/${STORAGE_ROLE_BOUNDARY_POLICY_NAME}`;
    statements.push(
      {
        Sid: 'ManageCostAllocationTags',
        Effect: 'Allow',
        Action: ['ce:ListCostAllocationTags', 'ce:UpdateCostAllocationTagsStatus'],
        Resource: '*',
      },
      {
        Sid: 'ManageFinLakeBuckets',
        Effect: 'Allow',
        Action: [
          's3:CreateBucket',
          's3:GetBucketLocation',
          's3:GetBucketPolicy',
          's3:ListBucket',
          's3:PutBucketPolicy',
        ],
        Resource: 'arn:aws:s3:::finlake-*',
      },
      {
        Sid: 'ReadAnyBucketPolicy',
        Effect: 'Allow',
        Action: ['s3:GetBucketLocation', 's3:GetBucketPolicy', 's3:ListBucket'],
        Resource: 'arn:aws:s3:::*',
      },
      {
        Sid: 'CreateStorageRolesWithBoundary',
        Effect: 'Allow',
        Action: ['iam:CreateRole'],
        Resource: storageRoleArn,
        Condition: {
          StringEquals: {
            'iam:PermissionsBoundary': boundaryPolicyArn,
          },
        },
      },
      {
        Sid: 'ManageStorageRoles',
        Effect: 'Allow',
        Action: [
          'iam:GetRole',
          'iam:TagRole',
          'iam:UntagRole',
          'iam:PassRole',
          'iam:PutRolePolicy',
          'iam:DeleteRolePolicy',
          'iam:UpdateAssumeRolePolicy',
          'iam:DeleteRole',
        ],
        Resource: storageRoleArn,
      },
      {
        Sid: 'ReadStorageRoleBoundary',
        Effect: 'Allow',
        Action: ['iam:GetPolicy', 'iam:GetPolicyVersion'],
        Resource: boundaryPolicyArn,
      },
    );
  }

  return {
    Version: '2012-10-17',
    Statement: statements,
  };
}

function buildStorageRoleBoundaryPolicy(awsAccountId: string) {
  return {
    Version: '2012-10-17',
    Statement: [
      {
        Sid: 'AllowStorageRoleSelfAssume',
        Effect: 'Allow',
        Action: ['sts:AssumeRole'],
        Resource: `arn:aws:iam::${awsAccountId}:role/${STORAGE_ROLE_NAME_PREFIX}*`,
      },
      {
        Sid: 'AllowExternalLocationStorageAccess',
        Effect: 'Allow',
        Action: [
          's3:GetBucketLocation',
          's3:ListBucket',
          's3:GetObject',
          's3:PutObject',
          's3:DeleteObject',
          's3:AbortMultipartUpload',
          's3:ListMultipartUploadParts',
        ],
        Resource: ['arn:aws:s3:::*', 'arn:aws:s3:::*/*'],
      },
    ],
  };
}

function awsAccountIdFromRoleArn(roleArn: string): string | null {
  const match = /^arn:aws(?:-[a-z]+)*:iam::(\d{12}):role\/.+$/.exec(roleArn);
  return match?.[1] ?? null;
}

function stableJson(value: unknown): string {
  return JSON.stringify(value, null, 2);
}

function shellArg(value: string): string {
  return `'${value.replace(/'/g, "'\\''")}'`;
}
