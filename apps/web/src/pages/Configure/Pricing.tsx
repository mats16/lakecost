import { useState } from 'react';
import type { PricingNotebookState } from '@finlake/shared';
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
  Sheet,
  SheetContent,
  SheetHeader,
  SheetTitle,
  Skeleton,
  Spinner,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@databricks/appkit-ui/react';
import { AlertCircle, ExternalLink, Play, RefreshCcw, UploadCloud } from 'lucide-react';
import {
  useMe,
  useGetJobRunLink,
  usePricingNotebook,
  useRunNotebook,
  useSetupPricingNotebook,
} from '../../api/hooks';
import { useI18n } from '../../i18n';
import { catalogTableUrl, messageOf, notebookEditorUrl, volumeFileUrl } from './utils';

export function Pricing() {
  const { t } = useI18n();
  const me = useMe();
  const pricing = usePricingNotebook();
  const setup = useSetupPricingNotebook();
  const runNotebook = useRunNotebook();
  const state = pricing.data;
  const [selectedSlug, setSelectedSlug] = useState<string | null>(null);
  const workspaceUrl = me.data?.workspaceUrl ?? null;
  const rows = state?.catalog ? [state] : [];
  const selected = rows.find((row) => row.slug === selectedSlug) ?? null;
  const setupError = messageOf(setup.error);
  const runError = messageOf(runNotebook.error);

  const onSetupNotebook = () => {
    setup.reset();
    setup.mutate();
  };

  const onRunNotebook = (notebookId: string) => {
    runNotebook.reset();
    runNotebook.mutate(notebookId);
  };

  return (
    <>
      <Card>
        <CardHeader>
          <div className="flex flex-wrap items-start justify-between gap-3">
            <div>
              <CardTitle>{t('pricing.title')}</CardTitle>
              <CardDescription>{t('pricing.desc')}</CardDescription>
            </div>
            <Button
              type="button"
              variant="secondary"
              size="sm"
              onClick={() => pricing.refetch()}
              disabled={pricing.isFetching}
            >
              <RefreshCcw className={pricing.isFetching ? 'animate-spin' : undefined} />
              {t('pricing.refresh')}
            </Button>
          </div>
        </CardHeader>
        <CardContent>
          <PricingBody
            pricing={pricing}
            state={state}
            rows={rows}
            workspaceUrl={workspaceUrl}
            setup={setup}
            runNotebook={runNotebook}
            setupError={setupError}
            runError={runError}
            onSelectRow={setSelectedSlug}
            onSetupNotebook={onSetupNotebook}
            onRunNotebook={onRunNotebook}
          />
        </CardContent>
      </Card>

      <PricingDetailsSheet
        row={selected}
        workspaceUrl={workspaceUrl}
        setupPending={setup.isPending}
        runPending={runNotebook.isPending}
        onClose={() => setSelectedSlug(null)}
        onSetupNotebook={onSetupNotebook}
        onRunNotebook={onRunNotebook}
      />
    </>
  );
}

type PricingHook = ReturnType<typeof usePricingNotebook>;
type SetupHook = ReturnType<typeof useSetupPricingNotebook>;
type RunHook = ReturnType<typeof useRunNotebook>;

function PricingBody({
  pricing,
  state,
  rows,
  workspaceUrl,
  setup,
  runNotebook,
  setupError,
  runError,
  onSelectRow,
  onSetupNotebook,
  onRunNotebook,
}: {
  pricing: PricingHook;
  state: PricingNotebookState | undefined;
  rows: PricingNotebookState[];
  workspaceUrl: string | null;
  setup: SetupHook;
  runNotebook: RunHook;
  setupError: string | null;
  runError: string | null;
  onSelectRow: (slug: string) => void;
  onSetupNotebook: () => void;
  onRunNotebook: (notebookId: string) => void;
}) {
  const { t } = useI18n();

  if (pricing.isLoading) {
    return (
      <div className="space-y-2">
        <Skeleton className="h-10 w-full" />
        <Skeleton className="h-10 w-full" />
        <Skeleton className="h-10 w-full" />
      </div>
    );
  }
  if (pricing.error) {
    return (
      <Alert variant="destructive">
        <AlertCircle />
        <AlertTitle>{t('pricing.loadFailed')}</AlertTitle>
        <AlertDescription>{messageOf(pricing.error)}</AlertDescription>
      </Alert>
    );
  }
  if (!state?.catalog) {
    return (
      <Alert variant="destructive">
        <AlertCircle />
        <AlertTitle>{t('pricing.catalogMissingTitle')}</AlertTitle>
        <AlertDescription>{t('pricing.catalogMissingDesc')}</AlertDescription>
      </Alert>
    );
  }

  return (
    <div className="grid gap-4">
      {setupError ? (
        <Alert variant="destructive">
          <AlertCircle />
          <AlertTitle>{t('pricing.setupFailed')}</AlertTitle>
          <AlertDescription>{setupError}</AlertDescription>
        </Alert>
      ) : null}
      {runError ? (
        <Alert variant="destructive">
          <AlertCircle />
          <AlertTitle>{t('pricing.runFailed')}</AlertTitle>
          <AlertDescription>{runError}</AlertDescription>
        </Alert>
      ) : null}
      {runNotebook.data ? (
        <Alert>
          <Play />
          <AlertTitle>
            <span className="flex flex-wrap items-center gap-2">
              <span>{t('pricing.runStarted')}</span>
              <RunLink runId={runNotebook.data.runId} />
            </span>
          </AlertTitle>
        </Alert>
      ) : null}
      {setup.data ? (
        <Alert>
          <UploadCloud />
          <AlertTitle>{t('pricing.setupReady')}</AlertTitle>
          <AlertDescription>
            {t('pricing.notebookUploaded', {
              path: setup.data.notebookWorkspacePath,
            })}
          </AlertDescription>
        </Alert>
      ) : null}
      {setup.data?.warnings.map((warning) => (
        <Alert key={warning}>
          <AlertCircle />
          <AlertTitle>{t('pricing.setupWarning')}</AlertTitle>
          <AlertDescription>{warning}</AlertDescription>
        </Alert>
      ))}

      <div className="overflow-x-auto">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>{t('pricing.columns.name')}</TableHead>
              <TableHead>{t('pricing.columns.table')}</TableHead>
              <TableHead>{t('pricing.columns.notebook')}</TableHead>
              <TableHead className="text-right">{t('pricing.columns.actions')}</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {rows.map((row) => (
              <PricingRow
                key={row.slug}
                row={row}
                workspaceUrl={workspaceUrl}
                setupPending={setup.isPending}
                runPending={runNotebook.isPending}
                onSelect={() => onSelectRow(row.slug)}
                onSetupNotebook={onSetupNotebook}
                onRunNotebook={onRunNotebook}
              />
            ))}
          </TableBody>
        </Table>
      </div>
    </div>
  );
}

function PricingRow({
  row,
  workspaceUrl,
  setupPending,
  runPending,
  onSelect,
  onSetupNotebook,
  onRunNotebook,
}: {
  row: PricingNotebookState;
  workspaceUrl: string | null;
  setupPending: boolean;
  runPending: boolean;
  onSelect: () => void;
  onSetupNotebook: () => void;
  onRunNotebook: (notebookId: string) => void;
}) {
  const { t } = useI18n();
  const notebookUrl = notebookEditorUrl(workspaceUrl, row.notebookId);
  const tableUrl = row.table ? catalogTableUrl(workspaceUrl, row.table) : null;

  return (
    <TableRow className="cursor-pointer" onClick={onSelect}>
      <TableCell>
        <span className="font-mono text-sm">{row.slug}</span>
      </TableCell>
      <TableCell>
        <ResourceLink href={tableUrl} value={row.table ?? '-'} />
      </TableCell>
      <TableCell>
        {row.notebookWorkspacePath ? (
          <ResourceLink href={notebookUrl} value={row.notebookWorkspacePath} />
        ) : (
          <span className="text-muted-foreground">{t('pricing.notCreated')}</span>
        )}
      </TableCell>
      <TableCell className="text-right" onClick={(event) => event.stopPropagation()}>
        <PricingActions
          row={row}
          setupPending={setupPending}
          runPending={runPending}
          onSetupNotebook={onSetupNotebook}
          onRunNotebook={onRunNotebook}
        />
      </TableCell>
    </TableRow>
  );
}

function PricingDetailsSheet({
  row,
  workspaceUrl,
  setupPending,
  runPending,
  onClose,
  onSetupNotebook,
  onRunNotebook,
}: {
  row: PricingNotebookState | null;
  workspaceUrl: string | null;
  setupPending: boolean;
  runPending: boolean;
  onClose: () => void;
  onSetupNotebook: () => void;
  onRunNotebook: (notebookId: string) => void;
}) {
  const { t } = useI18n();
  const notebookUrl = row ? notebookEditorUrl(workspaceUrl, row.notebookId) : null;
  const tableUrl = row?.table ? catalogTableUrl(workspaceUrl, row.table) : null;
  const volumeUrl = row?.rawDataPath ? volumeFileUrl(workspaceUrl, row.rawDataPath) : null;

  return (
    <Sheet open={Boolean(row)} onOpenChange={(open) => (open ? null : onClose())}>
      <SheetContent
        side="right"
        className="w-full max-w-(--container-md) sm:max-w-xl"
        onOpenAutoFocus={(event) => event.preventDefault()}
      >
        <SheetHeader>
          <SheetTitle>
            {row ? `Pricing - ${row.provider} / ${row.service}` : t('pricing.detailsTitle')}
          </SheetTitle>
        </SheetHeader>
        {row ? (
          <div className="flex flex-col gap-6 overflow-auto px-4 pb-6">
            <div className="grid gap-4">
              <InfoRow label={t('pricing.provider')} value={row.provider} />
              <InfoRow label={t('pricing.service')} value={row.service} />
              <InfoRow label={t('pricing.outputTable')} value={row.table} href={tableUrl} />
              <InfoRow
                label={t('pricing.downloadsVolume')}
                value={row.rawDataPath}
                href={volumeUrl}
              />
              <InfoRow
                label={t('pricing.notebook')}
                value={row.notebookWorkspacePath ?? t('pricing.notCreated')}
                href={notebookUrl}
              />
            </div>
            <div className="flex flex-wrap justify-end gap-2">
              <PricingActions
                row={row}
                setupPending={setupPending}
                runPending={runPending}
                onSetupNotebook={onSetupNotebook}
                onRunNotebook={onRunNotebook}
              />
            </div>
          </div>
        ) : null}
      </SheetContent>
    </Sheet>
  );
}

function PricingActions({
  row,
  setupPending,
  runPending,
  onSetupNotebook,
  onRunNotebook,
}: {
  row: PricingNotebookState;
  setupPending: boolean;
  runPending: boolean;
  onSetupNotebook: () => void;
  onRunNotebook: (notebookId: string) => void;
}) {
  const { t } = useI18n();
  return (
    <div className="flex justify-end gap-2">
      <Button
        type="button"
        size="sm"
        onClick={(event) => {
          event.stopPropagation();
          onSetupNotebook();
        }}
        disabled={setupPending}
      >
        {setupPending ? <Spinner /> : <UploadCloud />}
        {row.notebookWorkspacePath ? t('pricing.updateNotebook') : t('pricing.createNotebook')}
      </Button>
      <Button
        type="button"
        size="sm"
        variant="secondary"
        onClick={(event) => {
          event.stopPropagation();
          if (row.notebookId) onRunNotebook(row.notebookId);
        }}
        disabled={!row.notebookId || runPending}
      >
        {runPending ? <Spinner /> : <Play />}
        {t('pricing.runNotebook')}
      </Button>
    </div>
  );
}

function ResourceLink({ href, value }: { href: string | null; value: string }) {
  if (!href) return <span className="font-mono text-sm">{value}</span>;
  return (
    <a
      href={href}
      target="_blank"
      rel="noreferrer noopener"
      className="inline-flex items-center gap-1 font-mono text-sm"
      onClick={(event) => event.stopPropagation()}
    >
      {value}
      <ExternalLink className="inline-block h-3.5 w-3.5 shrink-0 align-[-2px]" />
    </a>
  );
}

function RunLink({ runId }: { runId: number }) {
  const runLink = useGetJobRunLink();
  const label = `Run #${runId}`;
  const cachedUrl = runLink.data?.runUrl ?? null;

  if (cachedUrl) {
    return (
      <a
        href={cachedUrl}
        target="_blank"
        rel="noreferrer noopener"
        className="inline-flex items-center gap-1 font-mono text-sm font-normal"
      >
        {label}
        <ExternalLink className="inline-block h-3.5 w-3.5 shrink-0 align-[-2px]" />
      </a>
    );
  }

  return (
    <button
      type="button"
      className="inline-flex items-center gap-1 font-mono text-sm font-normal"
      disabled={runLink.isPending}
      onClick={async () => {
        const popup = window.open('about:blank', '_blank');
        const result = await runLink.mutateAsync(runId);
        if (result.runUrl) {
          if (popup) {
            popup.location.href = result.runUrl;
          } else {
            window.open(result.runUrl, '_blank', 'noreferrer');
          }
        } else {
          popup?.close();
        }
      }}
    >
      {label}
      {runLink.isPending ? (
        <Spinner className="h-3.5 w-3.5" />
      ) : (
        <ExternalLink className="inline-block h-3.5 w-3.5 shrink-0 align-[-2px]" />
      )}
    </button>
  );
}

function InfoRow({
  label,
  value,
  href,
}: {
  label: string;
  value: string | null;
  href?: string | null;
}) {
  return (
    <div>
      <div className="text-xs font-medium text-muted-foreground">{label}</div>
      <div className="mt-1 break-all text-sm">
        {href && value ? (
          <a href={href} target="_blank" rel="noreferrer noopener">
            {value} <ExternalLink className="inline-block h-3.5 w-3.5 align-[-2px]" />
          </a>
        ) : (
          value
        )}
      </div>
    </div>
  );
}
