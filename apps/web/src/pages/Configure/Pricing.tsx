import { useState } from 'react';
import {
  isActivePricingRunStatus,
  type PricingNotebookState,
  type PricingRunStatus,
} from '@finlake/shared';
import {
  Alert,
  AlertDescription,
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
  AlertTitle,
  Badge,
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
  cn,
} from '@databricks/appkit-ui/react';
import { AlertCircle, ExternalLink, Trash2, UploadCloud } from 'lucide-react';
import {
  useMe,
  useDeletePricingNotebook,
  useGetJobRunLink,
  usePricingNotebook,
  useRunNotebook,
} from '../../api/hooks';
import { useI18n } from '../../i18n';
import {
  catalogTableUrl,
  fileNameFromPath,
  messageOf,
  notebookEditorUrl,
  volumeFileUrl,
} from './utils';

export function Pricing() {
  const { t } = useI18n();
  const me = useMe();
  const pricing = usePricingNotebook();
  const runNotebook = useRunNotebook();
  const deletePricing = useDeletePricingNotebook();
  const [selectedSlug, setSelectedSlug] = useState<string | null>(null);
  const [pendingDeleteSlug, setPendingDeleteSlug] = useState<string | null>(null);
  const workspaceUrl = me.data?.workspaceUrl ?? null;
  const rows = pricing.data?.items ?? [];
  const selected = rows.find((row) => row.slug === selectedSlug) ?? null;
  const pendingDelete = rows.find((row) => row.slug === pendingDeleteSlug) ?? null;
  const runError = messageOf(runNotebook.error);
  const deleteError = messageOf(deletePricing.error);
  const runPendingSlug = runNotebook.isPending ? (runNotebook.variables ?? null) : null;
  const deletePendingSlug = deletePricing.isPending ? (deletePricing.variables ?? null) : null;

  const onRunNotebook = (slug: string) => {
    runNotebook.reset();
    runNotebook.mutate(slug);
  };

  const requestDelete = (slug: string) => {
    deletePricing.reset();
    setPendingDeleteSlug(slug);
  };

  const confirmDelete = () => {
    if (!pendingDeleteSlug) return;
    deletePricing.mutate(pendingDeleteSlug, {
      onSuccess: () => {
        if (selectedSlug === pendingDeleteSlug) setSelectedSlug(null);
      },
      onSettled: () => {
        setPendingDeleteSlug(null);
      },
    });
  };

  return (
    <>
      <Card>
        <CardHeader>
          <CardTitle>{t('pricing.title')}</CardTitle>
          <CardDescription>{t('pricing.desc')}</CardDescription>
        </CardHeader>
        <CardContent>
          <PricingBody
            pricing={pricing}
            rows={rows}
            workspaceUrl={workspaceUrl}
            runError={runError}
            deleteError={deleteError}
            runPendingSlug={runPendingSlug}
            deletePendingSlug={deletePendingSlug}
            onSelectRow={setSelectedSlug}
            onRunNotebook={onRunNotebook}
            onRequestDelete={requestDelete}
          />
        </CardContent>
      </Card>

      <PricingDetailsSheet
        row={selected}
        workspaceUrl={workspaceUrl}
        runPending={runPendingSlug !== null && runPendingSlug === selected?.slug}
        deletePending={deletePendingSlug !== null && deletePendingSlug === selected?.slug}
        onClose={() => setSelectedSlug(null)}
        onRunNotebook={onRunNotebook}
        onRequestDelete={requestDelete}
      />

      <AlertDialog
        open={pendingDelete !== null}
        onOpenChange={(open) => {
          if (!open && !deletePricing.isPending) setPendingDeleteSlug(null);
        }}
      >
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>{t('pricing.deleteConfirmTitle')}</AlertDialogTitle>
            <AlertDialogDescription>
              {pendingDelete
                ? t('pricing.deleteConfirmDescription', {
                    name: pendingDelete.slug,
                    table: pendingDelete.table ?? t('pricing.notCreated'),
                  })
                : null}
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel disabled={deletePricing.isPending}>
              {t('common.cancel')}
            </AlertDialogCancel>
            <AlertDialogAction
              onClick={(event) => {
                event.preventDefault();
                confirmDelete();
              }}
              disabled={deletePricing.isPending}
            >
              {deletePricing.isPending ? (
                <>
                  <Spinner /> {t('pricing.deleting')}
                </>
              ) : (
                t('pricing.confirmDeleteAction')
              )}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </>
  );
}

type PricingHook = ReturnType<typeof usePricingNotebook>;

function PricingBody({
  pricing,
  rows,
  workspaceUrl,
  runError,
  deleteError,
  runPendingSlug,
  deletePendingSlug,
  onSelectRow,
  onRunNotebook,
  onRequestDelete,
}: {
  pricing: PricingHook;
  rows: PricingNotebookState[];
  workspaceUrl: string | null;
  runError: string | null;
  deleteError: string | null;
  runPendingSlug: string | null;
  deletePendingSlug: string | null;
  onSelectRow: (slug: string) => void;
  onRunNotebook: (slug: string) => void;
  onRequestDelete: (slug: string) => void;
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
  if (rows.every((row) => !row.catalog)) {
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
      {runError ? (
        <Alert variant="destructive">
          <AlertCircle />
          <AlertTitle>{t('pricing.runFailed')}</AlertTitle>
          <AlertDescription>{runError}</AlertDescription>
        </Alert>
      ) : null}
      {deleteError ? (
        <Alert variant="destructive">
          <AlertCircle />
          <AlertTitle>{t('pricing.deleteFailed')}</AlertTitle>
          <AlertDescription>{deleteError}</AlertDescription>
        </Alert>
      ) : null}
      <div className="overflow-x-auto">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>{t('pricing.columns.name')}</TableHead>
              <TableHead>{t('pricing.columns.table')}</TableHead>
              <TableHead>{t('pricing.columns.notebook')}</TableHead>
              <TableHead>{t('pricing.columns.status')}</TableHead>
              <TableHead>{t('pricing.columns.latestRun')}</TableHead>
              <TableHead className="text-right">{t('pricing.columns.actions')}</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {rows.map((row) => (
              <PricingRow
                key={row.slug}
                row={row}
                workspaceUrl={workspaceUrl}
                runPending={runPendingSlug === row.slug}
                deletePending={deletePendingSlug === row.slug}
                onSelect={() => onSelectRow(row.slug)}
                onRunNotebook={onRunNotebook}
                onRequestDelete={onRequestDelete}
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
  runPending,
  deletePending,
  onSelect,
  onRunNotebook,
  onRequestDelete,
}: {
  row: PricingNotebookState;
  workspaceUrl: string | null;
  runPending: boolean;
  deletePending: boolean;
  onSelect: () => void;
  onRunNotebook: (slug: string) => void;
  onRequestDelete: (slug: string) => void;
}) {
  const { t } = useI18n();
  const notebookUrl = notebookEditorUrl(workspaceUrl, row.notebookId);
  const tableUrl = row.table ? catalogTableUrl(workspaceUrl, row.table) : null;
  const notebookName = row.notebookWorkspacePath
    ? fileNameFromPath(row.notebookWorkspacePath)
    : null;

  return (
    <TableRow className="cursor-pointer" onClick={onSelect}>
      <TableCell>
        <span className="font-mono text-sm">{row.slug}</span>
      </TableCell>
      <TableCell>
        {row.table ? (
          <ResourceLink href={tableUrl} value={row.table} />
        ) : (
          <span className="text-muted-foreground">{t('pricing.notCreated')}</span>
        )}
      </TableCell>
      <TableCell>
        {notebookName ? (
          <ResourceLink href={notebookUrl} value={notebookName} />
        ) : (
          <span className="text-muted-foreground">{t('pricing.notCreated')}</span>
        )}
      </TableCell>
      <TableCell>
        <RunStatusBadge row={row} />
      </TableCell>
      <TableCell>
        {row.runId ? <RunLink runId={row.runId} cachedUrl={row.runUrl} /> : '-'}
      </TableCell>
      <TableCell className="text-right" onClick={(event) => event.stopPropagation()}>
        <PricingActions
          row={row}
          runPending={runPending}
          deletePending={deletePending}
          onRunNotebook={onRunNotebook}
          onRequestDelete={onRequestDelete}
        />
      </TableCell>
    </TableRow>
  );
}

function PricingDetailsSheet({
  row,
  workspaceUrl,
  runPending,
  deletePending,
  onClose,
  onRunNotebook,
  onRequestDelete,
}: {
  row: PricingNotebookState | null;
  workspaceUrl: string | null;
  runPending: boolean;
  deletePending: boolean;
  onClose: () => void;
  onRunNotebook: (slug: string) => void;
  onRequestDelete: (slug: string) => void;
}) {
  const { t } = useI18n();
  const notebookUrl = row ? notebookEditorUrl(workspaceUrl, row.notebookId) : null;
  const tableUrl = row?.table ? catalogTableUrl(workspaceUrl, row.table) : null;
  const volumeUrl = row?.rawDataPath ? volumeFileUrl(workspaceUrl, row.rawDataPath) : null;
  const notebookName = row?.notebookWorkspacePath
    ? fileNameFromPath(row.notebookWorkspacePath)
    : null;

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
              <InfoRow
                label={t('pricing.runStatus')}
                value={t(`pricing.status.${row.runStatus}`)}
              />
              <InfoRow label={t('pricing.runId')} value={row.runId ? String(row.runId) : null} />
              <InfoRow label={t('pricing.outputTable')} value={row.table} href={tableUrl} />
              <InfoRow
                label={t('pricing.downloadsVolume')}
                value={row.rawDataPath}
                href={volumeUrl}
              />
              <InfoRow
                label={t('pricing.notebook')}
                value={notebookName ?? t('pricing.notCreated')}
                href={notebookUrl}
              />
            </div>
            <div className="flex flex-wrap justify-end gap-2">
              <PricingActions
                row={row}
                runPending={runPending}
                deletePending={deletePending}
                onRunNotebook={onRunNotebook}
                onRequestDelete={onRequestDelete}
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
  runPending,
  deletePending,
  onRunNotebook,
  onRequestDelete,
}: {
  row: PricingNotebookState;
  runPending: boolean;
  deletePending: boolean;
  onRunNotebook: (slug: string) => void;
  onRequestDelete: (slug: string) => void;
}) {
  const { t } = useI18n();
  const activeRun = isActivePricingRunStatus(row.runStatus);
  const hasPricingData = Boolean(row.table);
  const actionLabel = hasPricingData ? t('pricing.update') : t('pricing.register');
  const pendingLabel = hasPricingData ? t('pricing.updating') : t('pricing.registering');
  return (
    <div className="flex justify-end gap-2">
      <Button
        type="button"
        size="sm"
        onClick={(event) => {
          event.stopPropagation();
          onRunNotebook(row.slug);
        }}
        disabled={runPending || activeRun}
      >
        {runPending || activeRun ? <Spinner /> : <UploadCloud />}
        {runPending || activeRun ? pendingLabel : actionLabel}
      </Button>
      <Button
        type="button"
        size="sm"
        variant="secondary"
        onClick={(event) => {
          event.stopPropagation();
          onRequestDelete(row.slug);
        }}
        disabled={deletePending || !hasPricingData || activeRun}
      >
        {deletePending ? <Spinner /> : <Trash2 />}
        {deletePending ? t('pricing.deleting') : t('pricing.delete')}
      </Button>
    </div>
  );
}

function RunStatusBadge({ row }: { row: PricingNotebookState }) {
  const { t } = useI18n();
  const active = isActivePricingRunStatus(row.runStatus);
  return (
    <Badge variant="outline" className={cn('gap-1.5', pricingStatusBadgeClass(row.runStatus))}>
      {active ? <Spinner className="h-3.5 w-3.5" /> : null}
      {t(`pricing.status.${row.runStatus}`)}
    </Badge>
  );
}

function pricingStatusBadgeClass(status: PricingRunStatus): string {
  switch (status) {
    case 'succeeded':
      return 'border-emerald-200 bg-emerald-50 text-emerald-700 dark:border-emerald-900/60 dark:bg-emerald-950/40 dark:text-emerald-300';
    case 'pending':
      return 'border-amber-200 bg-amber-50 text-amber-700 dark:border-amber-900/60 dark:bg-amber-950/40 dark:text-amber-300';
    case 'running':
      return 'border-sky-200 bg-sky-50 text-sky-700 dark:border-sky-900/60 dark:bg-sky-950/40 dark:text-sky-300';
    case 'failed':
      return 'border-red-200 bg-red-50 text-red-700 dark:border-red-900/60 dark:bg-red-950/40 dark:text-red-300';
    case 'canceled':
      return 'border-slate-200 bg-slate-50 text-slate-700 dark:border-slate-800 dark:bg-slate-950/40 dark:text-slate-300';
    case 'unknown':
      return 'border-violet-200 bg-violet-50 text-violet-700 dark:border-violet-900/60 dark:bg-violet-950/40 dark:text-violet-300';
    case 'not_started':
      return 'border-zinc-200 bg-zinc-50 text-zinc-600 dark:border-zinc-800 dark:bg-zinc-950/40 dark:text-zinc-300';
  }
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

function RunLink({ runId, cachedUrl: initialUrl }: { runId: number; cachedUrl?: string | null }) {
  const runLink = useGetJobRunLink();
  const label = `#${runId}`;
  const cachedUrl = initialUrl ?? runLink.data?.runUrl ?? null;

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
