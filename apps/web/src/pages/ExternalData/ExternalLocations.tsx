import { useEffect, useState, type FormEvent } from 'react';
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
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
  Field,
  FieldGroup,
  FieldLabel,
  Input,
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
  Skeleton,
  Spinner,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@databricks/appkit-ui/react';
import type { ExternalLocationSummary, StorageCredentialSummary } from '@lakecost/shared';
import { AlertCircle, ExternalLink, MoreVertical, RefreshCcw, Trash2, X } from 'lucide-react';
import {
  useCreateExternalLocation,
  useDeleteExternalLocation,
  useExternalLocations,
  useMe,
  useServiceCredentials,
} from '../../api/hooks';
import { useI18n } from '../../i18n';
import { messageOf } from '../Configure/utils';

const EMPTY_CREDENTIALS: StorageCredentialSummary[] = [];
const DEFAULT_LOCATION_NAME = 'finlake_s3_external_{undefined}';
const DEFAULT_LOCATION_URL = 's3://finlake-{undefined}/bcm';

export function ExternalLocations() {
  const { t } = useI18n();
  const locations = useExternalLocations();
  const credentials = useServiceCredentials();
  const createLocation = useCreateExternalLocation();
  const deleteLocation = useDeleteExternalLocation();
  const me = useMe();
  const workspaceUrl = me.data?.workspaceUrl ?? null;
  const error = messageOf(locations.error);
  const createError = messageOf(createLocation.error);
  const storageCredentials = credentials.data?.storageCredentials ?? EMPTY_CREDENTIALS;
  const [createModalOpen, setCreateModalOpen] = useState(false);
  const [name, setName] = useState(DEFAULT_LOCATION_NAME);
  const [nameEdited, setNameEdited] = useState(false);
  const [url, setUrl] = useState(DEFAULT_LOCATION_URL);
  const [urlEdited, setUrlEdited] = useState(false);
  const [credentialName, setCredentialName] = useState('');
  const [readOnly, setReadOnly] = useState(false);

  useEffect(() => {
    if (!createModalOpen || credentialName || storageCredentials.length === 0) return;
    const firstCredential = storageCredentials[0];
    setCredentialName(firstCredential?.name ?? '');
    setName(defaultLocationName(firstCredential?.awsAccountId ?? null));
    setUrl(defaultLocationUrl(firstCredential?.awsAccountId ?? null));
  }, [createModalOpen, credentialName, storageCredentials]);

  const normalizedName = name.trim();
  const normalizedUrl = url.trim();
  const normalizedCredentialName = credentialName.trim();
  const validName = /^[A-Za-z_][A-Za-z0-9_]*$/.test(normalizedName);
  const validUrl = /^s3:\/\/[^/]+(?:\/.*)?$/i.test(normalizedUrl);
  const canSubmit =
    validName && validUrl && normalizedCredentialName.length > 0 && !createLocation.isPending;

  const onCredentialNameChange = (value: string) => {
    setCredentialName(value);
    const accountId = storageCredentials.find(
      (credential) => credential.name === value,
    )?.awsAccountId;
    if (!nameEdited) {
      setName(defaultLocationName(accountId ?? null));
    }
    if (!urlEdited) {
      setUrl(defaultLocationUrl(accountId ?? null));
    }
  };

  const onUrlChange = (value: string) => {
    setUrlEdited(true);
    setUrl(value);
  };

  const openCreateModal = () => {
    createLocation.reset();
    const initialCredentialName = credentialName || storageCredentials[0]?.name || '';
    const accountId =
      storageCredentials.find((credential) => credential.name === initialCredentialName)
        ?.awsAccountId ?? null;
    setNameEdited(false);
    setUrlEdited(false);
    setCredentialName(initialCredentialName);
    setName(defaultLocationName(accountId));
    setUrl(defaultLocationUrl(accountId));
    setCreateModalOpen(true);
  };

  const onSubmit = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (!canSubmit) return;
    createLocation.mutate(
      {
        name: normalizedName,
        url: normalizedUrl,
        credentialName: normalizedCredentialName,
        readOnly,
      },
      {
        onSuccess: () => {
          setCreateModalOpen(false);
          setUrl(DEFAULT_LOCATION_URL);
          setName(DEFAULT_LOCATION_NAME);
          setNameEdited(false);
          setUrlEdited(false);
          setReadOnly(false);
        },
      },
    );
  };

  const onDeleteLocation = (locationName: string) => {
    if (
      !window.confirm(t('externalData.externalLocations.deleteConfirm', { name: locationName }))
    ) {
      return;
    }
    deleteLocation.mutate(locationName);
  };

  return (
    <>
      <Card>
        <CardHeader>
          <div className="flex flex-wrap items-start justify-between gap-3">
            <div>
              <CardTitle>{t('externalData.externalLocations.title')}</CardTitle>
              <CardDescription>{t('externalData.externalLocations.desc')}</CardDescription>
            </div>
            <div className="flex items-center gap-2">
              <Button
                type="button"
                variant="secondary"
                size="sm"
                onClick={() => locations.refetch()}
                disabled={locations.isFetching}
                aria-label={t('externalData.refresh')}
                title={t('externalData.refresh')}
              >
                <RefreshCcw className={locations.isFetching ? 'animate-spin' : undefined} />
              </Button>
              <Button
                type="button"
                size="sm"
                className="bg-emerald-400 text-slate-950 hover:bg-emerald-300"
                onClick={openCreateModal}
              >
                {t('externalData.externalLocations.create')}
              </Button>
            </div>
          </div>
        </CardHeader>
        <CardContent>
          {error ? (
            <Alert variant="destructive" className="mb-4">
              <AlertCircle />
              <AlertTitle>{t('externalData.externalLocations.loadFailed')}</AlertTitle>
              <AlertDescription>{error}</AlertDescription>
            </Alert>
          ) : null}

          {locations.isLoading ? (
            <LoadingRows />
          ) : (
            <ExternalLocationsTable
              rows={locations.data?.externalLocations ?? []}
              workspaceUrl={workspaceUrl}
              deletingName={deleteLocation.isPending ? (deleteLocation.variables ?? null) : null}
              onDelete={onDeleteLocation}
            />
          )}
        </CardContent>
      </Card>

      <CreateExternalLocationModal
        open={createModalOpen}
        name={name}
        url={url}
        credentialName={credentialName}
        readOnly={readOnly}
        storageCredentials={storageCredentials}
        storageCredentialsLoading={credentials.isLoading}
        createPending={createLocation.isPending}
        canSubmit={canSubmit}
        validName={validName}
        validUrl={validUrl}
        createError={createError}
        setName={(value) => {
          setNameEdited(true);
          setName(value);
        }}
        setUrl={onUrlChange}
        setCredentialName={onCredentialNameChange}
        setReadOnly={setReadOnly}
        onSubmit={onSubmit}
        onClose={() => {
          if (!createLocation.isPending) setCreateModalOpen(false);
        }}
      />
    </>
  );
}

function ExternalLocationsTable({
  rows,
  workspaceUrl,
  deletingName,
  onDelete,
}: {
  rows: ExternalLocationSummary[];
  workspaceUrl: string | null;
  deletingName: string | null;
  onDelete: (name: string) => void;
}) {
  const { t } = useI18n();

  if (rows.length === 0) {
    return (
      <div className="border-border rounded-md border p-6 text-sm">
        <div className="font-medium">{t('externalData.externalLocations.emptyTitle')}</div>
        <p className="text-muted-foreground mt-1 mb-0">
          {t('externalData.externalLocations.emptyDesc')}
        </p>
      </div>
    );
  }

  return (
    <div className="overflow-x-auto">
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead>{t('externalData.columns.name')}</TableHead>
            <TableHead>{t('externalData.columns.credential')}</TableHead>
            <TableHead>{t('externalData.columns.url')}</TableHead>
            <TableHead>{t('externalData.columns.comment')}</TableHead>
            <TableHead
              className="text-right"
              aria-label={t('externalData.externalLocations.actions.open')}
            />
          </TableRow>
        </TableHeader>
        <TableBody>
          {rows.map((row) => (
            <TableRow key={row.name}>
              <TableCell>
                <ExternalLocationNameLink name={row.name} workspaceUrl={workspaceUrl} />
              </TableCell>
              <TableCell>{row.credentialName ?? '-'}</TableCell>
              <TableCell>
                <code className="text-muted-foreground block max-w-96 truncate text-xs">
                  {row.url ?? '-'}
                </code>
              </TableCell>
              <TableCell className="text-muted-foreground max-w-80 truncate">
                {row.comment ?? '-'}
              </TableCell>
              <TableCell className="text-right">
                <ExternalLocationActions
                  name={row.name}
                  deleting={deletingName === row.name}
                  onDelete={onDelete}
                />
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </div>
  );
}

function ExternalLocationNameLink({
  name,
  workspaceUrl,
}: {
  name: string;
  workspaceUrl: string | null;
}) {
  const href = workspaceUrl
    ? `${workspaceUrl}/explore/locations/${encodeURIComponent(name)}`
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

function ExternalLocationActions({
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
          aria-label={t('externalData.externalLocations.actions.open')}
          disabled={deleting}
        >
          <MoreVertical className="size-4" aria-hidden="true" />
        </Button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end">
        <DropdownMenuItem onClick={() => onDelete(name)} disabled={deleting}>
          <Trash2 className="size-4" aria-hidden="true" />
          <span>
            {deleting
              ? t('externalData.externalLocations.actions.deleting')
              : t('externalData.externalLocations.actions.delete')}
          </span>
        </DropdownMenuItem>
      </DropdownMenuContent>
    </DropdownMenu>
  );
}

function CreateExternalLocationModal({
  open,
  name,
  url,
  credentialName,
  readOnly,
  storageCredentials,
  storageCredentialsLoading,
  createPending,
  canSubmit,
  validName,
  validUrl,
  createError,
  setName,
  setUrl,
  setCredentialName,
  setReadOnly,
  onSubmit,
  onClose,
}: {
  open: boolean;
  name: string;
  url: string;
  credentialName: string;
  readOnly: boolean;
  storageCredentials: StorageCredentialSummary[];
  storageCredentialsLoading: boolean;
  createPending: boolean;
  canSubmit: boolean;
  validName: boolean;
  validUrl: boolean;
  createError: string | null;
  setName: (value: string) => void;
  setUrl: (value: string) => void;
  setCredentialName: (value: string) => void;
  setReadOnly: (value: boolean) => void;
  onSubmit: (event: FormEvent<HTMLFormElement>) => void;
  onClose: () => void;
}) {
  const { t } = useI18n();
  const [submitted, setSubmitted] = useState(false);
  const showValidationHint =
    submitted && (!validName || !validUrl || credentialName.trim().length === 0);

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
        aria-labelledby="create-external-location-modal-title"
        className="bg-background border-border w-full max-w-2xl rounded-lg border shadow-xl"
        onMouseDown={(event) => event.stopPropagation()}
        onSubmit={(event) => {
          setSubmitted(true);
          if (!canSubmit) {
            event.preventDefault();
            return;
          }
          onSubmit(event);
        }}
      >
        <div className="flex items-start justify-between gap-4 p-5">
          <div className="min-w-0">
            <h3 id="create-external-location-modal-title" className="text-base font-semibold">
              {t('externalData.externalLocations.createModalTitle')}
            </h3>
            <p className="text-muted-foreground mt-1 mb-0 text-sm">
              {t('externalData.externalLocations.createDesc')}
            </p>
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
            <Field>
              <FieldLabel>{t('externalData.externalLocations.storageCredential')}</FieldLabel>
              <Select
                value={credentialName}
                onValueChange={setCredentialName}
                disabled={
                  createPending || storageCredentialsLoading || storageCredentials.length === 0
                }
              >
                <SelectTrigger className="w-full">
                  <SelectValue
                    placeholder={t('externalData.externalLocations.credentialPlaceholder')}
                  />
                </SelectTrigger>
                <SelectContent>
                  {storageCredentials.map((credential) => (
                    <SelectItem key={credential.name} value={credential.name}>
                      {credential.name}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </Field>

            <Field>
              <FieldLabel>{t('externalData.columns.url')}</FieldLabel>
              <Input
                value={url}
                onChange={(event) => setUrl(event.target.value)}
                placeholder="s3://bucket/path"
                disabled={createPending}
              />
            </Field>

            <Field>
              <FieldLabel>{t('externalData.externalLocations.name')}</FieldLabel>
              <Input
                value={name}
                onChange={(event) => setName(event.target.value)}
                disabled={createPending}
              />
            </Field>

            <label className="flex items-center gap-2 text-sm">
              <input
                type="checkbox"
                checked={readOnly}
                disabled={createPending}
                onChange={(event) => setReadOnly(event.target.checked)}
              />
              <span>{t('externalData.storageCredentials.readOnly')}</span>
            </label>

            {showValidationHint ? (
              <Alert>
                <AlertCircle />
                <AlertDescription>
                  {t('externalData.externalLocations.validationHint')}
                </AlertDescription>
              </Alert>
            ) : null}

            {createError ? (
              <Alert variant="destructive">
                <AlertCircle />
                <AlertTitle>{t('externalData.externalLocations.createFailed')}</AlertTitle>
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
            className="bg-emerald-400 text-slate-950 hover:bg-emerald-300"
            disabled={createPending}
          >
            {createPending ? (
              <>
                <Spinner /> {t('externalData.externalLocations.creating')}
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

function defaultLocationName(accountId: string | null): string {
  return accountId ? `finlake_s3_external_${accountId}` : DEFAULT_LOCATION_NAME;
}

function defaultLocationUrl(accountId: string | null): string {
  return accountId ? `s3://finlake-${accountId}/bcm` : DEFAULT_LOCATION_URL;
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
