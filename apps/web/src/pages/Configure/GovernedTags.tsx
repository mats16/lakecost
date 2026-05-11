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
  Skeleton,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@databricks/appkit-ui/react';
import type {
  GovernedTagAwsStatus,
  GovernedTagDatabricksStatus,
  GovernedTagRow,
  GovernedTagSyncResult,
} from '@finlake/shared';
import { AlertCircle, CheckCircle2, ExternalLink, Plus, RefreshCcw } from 'lucide-react';
import { useGovernedTags, useMe, useSyncGovernedTags } from '../../api/hooks';
import { useI18n } from '../../i18n';
import { messageOf } from './utils';

export function GovernedTags() {
  const { t, locale } = useI18n();
  const governedTags = useGovernedTags();
  const syncTags = useSyncGovernedTags();
  const me = useMe();

  const rows = governedTags.data?.items ?? [];
  const awsAccounts = governedTags.data?.awsAccounts ?? [];
  const loadError = messageOf(governedTags.error);
  const syncError = messageOf(syncTags.error);
  const hasAwsAccounts = awsAccounts.length > 0;
  const workspaceUrl = me.data?.workspaceUrl ?? null;
  const result = syncTags.data ?? null;

  const syncDatabricks = (tagKey: string) => {
    syncTags.mutate({ platform: 'databricks', tagKey });
  };

  const syncAws = (tagKey: string, awsAccountId: string) => {
    syncTags.mutate({ platform: 'aws', tagKey, awsAccountId });
  };

  return (
    <Card>
      <CardHeader>
        <div className="flex flex-wrap items-start justify-between gap-4">
          <div>
            <div className="flex items-center gap-2">
              <CardTitle>{t('governedTags.title')}</CardTitle>
            </div>
            <CardDescription>{t('governedTags.desc')}</CardDescription>
          </div>
          <Button
            type="button"
            variant="secondary"
            size="sm"
            className="h-7 gap-1.5 px-3"
            aria-label={t('governedTags.refresh')}
            onClick={() => governedTags.refetch()}
            disabled={governedTags.isFetching || syncTags.isPending}
          >
            <RefreshCcw
              className={governedTags.isFetching ? 'animate-spin' : undefined}
              aria-hidden="true"
            />
            {t('governedTags.refresh')}
          </Button>
        </div>
      </CardHeader>
      <CardContent>
        {loadError ? (
          <Alert variant="destructive" className="mb-4">
            <AlertCircle />
            <AlertTitle>{t('governedTags.loadFailed')}</AlertTitle>
            <AlertDescription>{loadError}</AlertDescription>
          </Alert>
        ) : null}

        {syncError ? (
          <Alert variant="destructive" className="mb-4">
            <AlertCircle />
            <AlertTitle>{t('governedTags.syncFailed')}</AlertTitle>
            <AlertDescription>{syncError}</AlertDescription>
          </Alert>
        ) : null}

        {result ? <SyncResult result={result} /> : null}

        {governedTags.data?.warnings.length ? (
          <Alert className="mb-4">
            <AlertCircle />
            <AlertTitle>{t('governedTags.warningTitle')}</AlertTitle>
            <AlertDescription>
              <ul className="m-0 list-disc pl-4">
                {governedTags.data.warnings.map((warning) => (
                  <li key={warning}>{warning}</li>
                ))}
              </ul>
            </AlertDescription>
          </Alert>
        ) : null}

        {!hasAwsAccounts && !governedTags.isLoading ? (
          <Alert className="mb-4">
            <AlertCircle />
            <AlertTitle>{t('governedTags.noAwsAccountsTitle')}</AlertTitle>
            <AlertDescription>{t('governedTags.noAwsAccountsDesc')}</AlertDescription>
          </Alert>
        ) : null}

        {governedTags.isLoading ? (
          <div className="space-y-2">
            <Skeleton className="h-10 w-full" />
            <Skeleton className="h-10 w-full" />
            <Skeleton className="h-10 w-full" />
          </div>
        ) : (
          <div className="overflow-x-auto">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>{t('governedTags.columns.tag')}</TableHead>
                  <TableHead>{t('governedTags.columns.databricks')}</TableHead>
                  <TableHead>{t('governedTags.columns.aws')}</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {rows.map((row) => (
                  <GovernedTagTableRow
                    key={row.definition.key}
                    row={row}
                    locale={locale}
                    workspaceUrl={workspaceUrl}
                    syncPending={syncTags.isPending}
                    onSyncDatabricks={syncDatabricks}
                    onSyncAws={syncAws}
                  />
                ))}
              </TableBody>
            </Table>
          </div>
        )}
      </CardContent>
    </Card>
  );
}

function GovernedTagTableRow({
  row,
  locale,
  workspaceUrl,
  syncPending,
  onSyncDatabricks,
  onSyncAws,
}: {
  row: GovernedTagRow;
  locale: 'en' | 'ja';
  workspaceUrl: string | null;
  syncPending: boolean;
  onSyncDatabricks: (tagKey: string) => void;
  onSyncAws: (tagKey: string, awsAccountId: string) => void;
}) {
  const { t } = useI18n();
  return (
    <TableRow>
      <TableCell className="min-w-52">
        <span className="font-mono text-sm font-medium">{row.definition.key}</span>
      </TableCell>
      <TableCell className="min-w-44">
        {row.databricks.status === 'governed' ? (
          <DatabricksStatusBadge
            status={row.databricks}
            tagKey={row.definition.key}
            locale={locale}
            workspaceUrl={workspaceUrl}
          />
        ) : (
          <button
            type="button"
            className="inline-flex h-7 cursor-pointer items-center gap-1.5 rounded-full border border-dashed border-border px-3 text-xs font-medium text-muted-foreground transition hover:border-primary hover:text-foreground disabled:cursor-not-allowed disabled:opacity-60"
            aria-label={t('governedTags.syncDatabricksTag', { tag: row.definition.key })}
            disabled={syncPending}
            onClick={() => onSyncDatabricks(row.definition.key)}
          >
            <Plus className="h-3.5 w-3.5" aria-hidden="true" />
            {t('governedTags.createGovernedTag')}
          </button>
        )}
      </TableCell>
      <TableCell className="min-w-72">
        <div className="flex flex-wrap items-center gap-2">
          {row.aws.length > 0 ? (
            row.aws.map((status) => (
              <AwsAccountStatus
                key={status.accountId}
                status={status}
                tagKey={row.definition.key}
                syncPending={syncPending}
                onSyncAws={onSyncAws}
              />
            ))
          ) : (
            <Badge variant="outline">{t('governedTags.awsNotLinked')}</Badge>
          )}
        </div>
      </TableCell>
    </TableRow>
  );
}

function DatabricksStatusBadge({
  status,
  tagKey,
  locale,
  workspaceUrl,
}: {
  status: GovernedTagDatabricksStatus;
  tagKey: string;
  locale: 'en' | 'ja';
  workspaceUrl: string | null;
}) {
  const { t } = useI18n();
  const governedTagUrl = workspaceUrl
    ? `${workspaceUrl.replace(/\/$/, '')}/governance/governed-tags/${encodeURIComponent(tagKey)}`
    : null;
  return (
    <div className="flex flex-col gap-1">
      {governedTagUrl ? (
        <a href={governedTagUrl} target="_blank" rel="noreferrer" className="w-fit">
          <Badge variant="secondary" className="gap-1.5">
            <CheckCircle2 className="h-3.5 w-3.5" />
            {t('governedTags.status.governed')}
            <ExternalLink className="h-3 w-3" />
          </Badge>
        </a>
      ) : (
        <Badge variant="secondary" className="w-fit gap-1.5">
          <CheckCircle2 className="h-3.5 w-3.5" />
          {t('governedTags.status.governed')}
        </Badge>
      )}
      {status.updatedAt ? (
        <span className="text-muted-foreground text-xs">
          {formatDate(status.updatedAt, locale)}
        </span>
      ) : null}
    </div>
  );
}

function AwsAccountStatus({
  status,
  tagKey,
  syncPending,
  onSyncAws,
}: {
  status: GovernedTagAwsStatus;
  tagKey: string;
  syncPending: boolean;
  onSyncAws: (tagKey: string, awsAccountId: string) => void;
}) {
  const { t } = useI18n();
  if (status.status === 'Active') {
    return (
      <Badge variant="secondary" title={status.message ?? undefined} className="gap-1.5">
        {status.accountId}: {t(`governedTags.awsStatus.${status.status}`)}
      </Badge>
    );
  }

  return (
    <button
      type="button"
      className="inline-flex h-7 cursor-pointer items-center gap-2 rounded-full border border-dashed border-border px-3 text-xs font-medium text-muted-foreground transition hover:border-primary hover:text-foreground disabled:cursor-not-allowed disabled:opacity-60"
      title={status.message ?? undefined}
      aria-label={t('governedTags.syncAwsAccountTag', {
        tag: tagKey,
        account: status.accountId,
      })}
      disabled={syncPending}
      onClick={() => onSyncAws(tagKey, status.accountId)}
    >
      <span>
        {status.accountId}: {t(`governedTags.awsStatus.${status.status}`)}
      </span>
      <span className="inline-flex items-center gap-1 border-l border-border pl-2">
        <Plus className="h-3.5 w-3.5" aria-hidden="true" />
        {t('governedTags.syncTag')}
      </span>
    </button>
  );
}

function SyncResult({ result }: { result: GovernedTagSyncResult }) {
  const { t } = useI18n();
  const failedCount =
    result.tags.filter((tag) => tag.status === 'failed').length +
    result.awsAccounts.filter((account) => account.status === 'failed').length;
  return (
    <Alert variant={failedCount > 0 ? 'destructive' : 'default'} className="mb-4">
      {failedCount > 0 ? <AlertCircle /> : <CheckCircle2 />}
      <AlertTitle>
        {failedCount > 0
          ? t('governedTags.syncPartial')
          : t(
              result.platform === 'aws'
                ? 'governedTags.syncAwsComplete'
                : 'governedTags.syncDatabricksComplete',
            )}
      </AlertTitle>
      <AlertDescription>
        {result.platform === 'aws'
          ? t('governedTags.syncedAwsAccount', {
              account: result.awsAccounts.map((account) => account.awsAccountId).join(', '),
            })
          : t('governedTags.syncedDatabricks')}
      </AlertDescription>
    </Alert>
  );
}

function formatDate(value: string, locale: 'en' | 'ja'): string {
  const date = new Date(value);
  if (Number.isNaN(date.valueOf())) return value;
  return new Intl.DateTimeFormat(locale === 'ja' ? 'ja-JP' : 'en-US', {
    dateStyle: 'medium',
    timeStyle: 'short',
  }).format(date);
}
