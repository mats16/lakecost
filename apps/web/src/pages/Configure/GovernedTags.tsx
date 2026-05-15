import {
  Alert,
  AlertDescription,
  AlertTitle,
  Badge,
  Button,
  Skeleton,
  Switch,
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
  const { t } = useI18n();
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

  const syncDatabricks = (tagKey: string, enabled: boolean) => {
    syncTags.mutate({ platform: 'databricks', tagKey, enabled });
  };

  const syncAws = (tagKey: string, awsAccountId: string) => {
    syncTags.mutate({ platform: 'aws', tagKey, awsAccountId });
  };

  return (
    <>
      <div className="mb-4 flex flex-wrap items-start justify-between gap-4">
        <p className="text-muted-foreground m-0 text-sm">{t('governedTags.desc')}</p>
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
        <div className="overflow-x-auto rounded-md border">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>{t('governedTags.columns.tag')}</TableHead>
                <TableHead>{t('governedTags.columns.allowedValues')}</TableHead>
                <TableHead>
                  <ColumnHeader
                    title={t('governedTags.columns.databricks')}
                    subtitle={t('governedTags.columns.databricksDetail')}
                  />
                </TableHead>
                <TableHead>
                  <ColumnHeader
                    title={t('governedTags.columns.aws')}
                    subtitle={t('governedTags.columns.awsDetail')}
                  />
                </TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {rows.map((row) => (
                <GovernedTagTableRow
                  key={row.definition.key}
                  row={row}
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
    </>
  );
}

function ColumnHeader({ title, subtitle }: { title: string; subtitle: string }) {
  return (
    <span className="flex flex-col leading-tight">
      <span>{title}</span>
      <span className="text-muted-foreground text-xs font-normal">{subtitle}</span>
    </span>
  );
}

function GovernedTagTableRow({
  row,
  workspaceUrl,
  syncPending,
  onSyncDatabricks,
  onSyncAws,
}: {
  row: GovernedTagRow;
  workspaceUrl: string | null;
  syncPending: boolean;
  onSyncDatabricks: (tagKey: string, enabled: boolean) => void;
  onSyncAws: (tagKey: string, awsAccountId: string) => void;
}) {
  const { t } = useI18n();
  return (
    <TableRow>
      <TableCell className="min-w-52">
        <span className="font-mono text-sm font-medium">{row.definition.key}</span>
      </TableCell>
      <TableCell className="min-w-52">
        <AllowedValuesCell status={row.databricks} />
      </TableCell>
      <TableCell className="min-w-44">
        <DatabricksGovernedSwitch
          status={row.databricks}
          tagKey={row.definition.key}
          workspaceUrl={workspaceUrl}
          syncPending={syncPending}
          onSyncDatabricks={onSyncDatabricks}
        />
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

function AllowedValuesCell({ status }: { status: GovernedTagDatabricksStatus }) {
  const { t } = useI18n();
  if (status.status !== 'governed' || status.allowedValues.length === 0) {
    return (
      <span className="text-muted-foreground text-xs">{t('governedTags.allowedValuesAny')}</span>
    );
  }
  return (
    <div className="flex flex-wrap items-center gap-1.5">
      {status.allowedValues.map((value) => (
        <Badge key={value} variant="outline" className="font-mono">
          {value}
        </Badge>
      ))}
    </div>
  );
}

function DatabricksGovernedSwitch({
  status,
  tagKey,
  workspaceUrl,
  syncPending,
  onSyncDatabricks,
}: {
  status: GovernedTagDatabricksStatus;
  tagKey: string;
  workspaceUrl: string | null;
  syncPending: boolean;
  onSyncDatabricks: (tagKey: string, enabled: boolean) => void;
}) {
  const { t } = useI18n();
  const isGoverned = status.status === 'governed';
  const governedTagUrl = workspaceUrl
    ? `${workspaceUrl.replace(/\/$/, '')}/governance/governed-tags/${encodeURIComponent(tagKey)}`
    : null;
  return (
    <div className="flex flex-col gap-1.5">
      <div className="flex items-center gap-2">
        <Switch
          checked={isGoverned}
          disabled={syncPending}
          aria-label={t(
            isGoverned ? 'governedTags.disableDatabricksTag' : 'governedTags.enableDatabricksTag',
            { tag: tagKey },
          )}
          onCheckedChange={(checked) => onSyncDatabricks(tagKey, checked)}
        />
        {isGoverned && governedTagUrl ? (
          <a href={governedTagUrl} target="_blank" rel="noreferrer" className="w-fit">
            <Badge variant="secondary" className="gap-1.5">
              <CheckCircle2 className="h-3.5 w-3.5" />
              {t('governedTags.status.governed')}
              <ExternalLink className="h-3 w-3" />
            </Badge>
          </a>
        ) : isGoverned || status.status === 'error' ? (
          <Badge variant={isGoverned ? 'secondary' : 'outline'} className="w-fit gap-1.5">
            {isGoverned ? <CheckCircle2 className="h-3.5 w-3.5" /> : null}
            {t(`governedTags.status.${status.status}`)}
          </Badge>
        ) : null}
      </div>
      {status.message ? (
        <span className="text-muted-foreground text-xs">{status.message}</span>
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
      className="inline-flex h-7 cursor-pointer items-center gap-2 rounded-full border border-dashed border-(--success) px-3 text-xs font-medium text-(--success) transition hover:bg-(--success)/10 disabled:cursor-not-allowed disabled:opacity-60"
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
  const tagCounts = countStatuses(result.tags);
  const accountCounts = countStatuses(result.awsAccounts);
  const failedItems = [
    ...result.tags
      .filter((tag) => tag.status === 'failed')
      .map((tag) =>
        t('governedTags.syncFailedTagDetail', {
          tag: tag.key,
          message: tag.message ?? t('governedTags.syncUnknownError'),
        }),
      ),
    ...result.awsAccounts
      .filter((account) => account.status === 'failed')
      .map((account) =>
        t('governedTags.syncFailedAccountDetail', {
          account: account.awsAccountId,
          message: account.message ?? t('governedTags.syncUnknownError'),
        }),
      ),
  ];
  const failedCount = tagCounts.failed + accountCounts.failed;
  const successCount = tagCounts.synced + accountCounts.synced;
  const isFailure = failedCount > 0;
  if (result.platform === 'databricks' && !isFailure) return null;
  const isPartialFailure = isFailure && successCount > 0;
  const platformKey = result.platform === 'aws' ? 'Aws' : 'Databricks';
  const titleKey = isFailure
    ? `governedTags.sync${platformKey}${isPartialFailure ? 'Partial' : 'ResultFailed'}`
    : `governedTags.sync${platformKey}Complete`;

  return (
    <Alert variant={isFailure ? 'destructive' : 'default'} className="mb-4">
      {isFailure ? <AlertCircle /> : <CheckCircle2 />}
      <AlertTitle>{t(titleKey)}</AlertTitle>
      <AlertDescription>
        <div className="space-y-2">
          <p>
            {result.platform === 'aws'
              ? t('governedTags.syncAwsSummary', {
                  syncedTags: tagCounts.synced,
                  failedTags: tagCounts.failed,
                  syncedAccounts: accountCounts.synced,
                  failedAccounts: accountCounts.failed,
                })
              : t('governedTags.syncDatabricksSummary', {
                  syncedTags: tagCounts.synced,
                  failedTags: tagCounts.failed,
                })}
          </p>
          {failedItems.length > 0 ? (
            <div>
              <p className="font-medium">{t('governedTags.syncFailureDetails')}</p>
              <ul className="m-0 list-disc pl-4">
                {failedItems.map((item) => (
                  <li key={item}>{item}</li>
                ))}
              </ul>
            </div>
          ) : (
            <p>
              {result.platform === 'aws'
                ? t('governedTags.syncedAwsAccount', {
                    account: result.awsAccounts.map((account) => account.awsAccountId).join(', '),
                  })
                : t('governedTags.syncedDatabricks')}
            </p>
          )}
        </div>
      </AlertDescription>
    </Alert>
  );
}

function countStatuses<T extends { status: string }>(items: T[]) {
  return {
    synced: items.filter((item) => item.status === 'synced').length,
    failed: items.filter((item) => item.status === 'failed').length,
  };
}
