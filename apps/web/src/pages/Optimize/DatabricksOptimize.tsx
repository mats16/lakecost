import { useMemo, useState } from 'react';
import {
  Bar,
  CartesianGrid,
  ComposedChart,
  Legend,
  Line,
  ResponsiveContainer,
  Tooltip as RechartsTooltip,
  XAxis,
  YAxis,
} from 'recharts';
import {
  Alert,
  AlertDescription,
  Badge,
  Button,
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
  Empty,
  EmptyDescription,
  EmptyHeader,
  EmptyTitle,
  Progress,
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
  Skeleton,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@databricks/appkit-ui/react';
import { AlertCircle, DollarSign, Gauge, ListChecks, RefreshCcw, Server } from 'lucide-react';
import {
  buildDatabricksRecommendationsStatement,
  buildDatabricksServicesStatement,
  buildDatabricksSummaryStatement,
  buildDatabricksTrendStatement,
  buildDatabricksWorkspacesStatement,
  resolveDatabricksOptimizeSources,
  type DatabricksOptimizationRecommendation,
  type DatabricksOptimizationServiceRow,
  type DatabricksOptimizationSummary,
  type DatabricksOptimizationWorkspace,
  type DatabricksTrendGrain,
} from '@finlake/shared';
import { PageHeader } from '../../components/PageHeader';
import { useAppSettings, useDataSources, useSqlStatement } from '../../api/hooks';
import { useCurrencyUsd, useI18n } from '../../i18n';

const PERIODS = ['last30', 'last90', 'last180', 'last12m'] as const;
type Period = (typeof PERIODS)[number];

const SERVERLESS_COLOR = '#49A078';
const NON_SERVERLESS_COLOR = '#E4572E';
const UNKNOWN_COLOR = '#718096';
const RATIO_COLOR = '#3B82F6';

interface DatabricksOptimizationTrendRow {
  period: string;
  totalCostUsd: number;
  serverlessCostUsd: number;
  nonServerlessCostUsd: number;
  unknownCostUsd: number;
  serverlessRatio: number | null;
}

function rangeForPeriod(period: Period) {
  const end = new Date();
  const start = new Date(end);
  if (period === 'last12m') {
    start.setFullYear(end.getFullYear() - 1, end.getMonth(), 1);
    start.setHours(0, 0, 0, 0);
  } else {
    const days = period === 'last30' ? 30 : period === 'last90' ? 90 : 180;
    start.setDate(end.getDate() - days);
  }
  return { start: start.toISOString(), end: end.toISOString() };
}

function sqlError(tableName: string, error: unknown) {
  if (!error) return null;
  return { tableName, message: error instanceof Error ? error.message : String(error) };
}

export function DatabricksOptimize() {
  const { t, locale } = useI18n();
  const formatUsd = useCurrencyUsd();
  const [period, setPeriod] = useState<Period>('last12m');
  const [selectedWorkspaceId, setSelectedWorkspaceId] = useState('all');
  const baseRange = useMemo(() => rangeForPeriod(period), [period]);
  const trendGrain: DatabricksTrendGrain = period === 'last30' ? 'day' : 'month';
  const dataSources = useDataSources();
  const appSettings = useAppSettings();
  const sourceTables = useMemo(
    () =>
      resolveDatabricksOptimizeSources(
        dataSources.data?.items ?? [],
        appSettings.data?.settings ?? {},
      ),
    [appSettings.data?.settings, dataSources.data?.items],
  );
  const sqlEnabled = dataSources.isSuccess && appSettings.isSuccess;
  const workspaceStatement = useMemo(
    () => buildDatabricksWorkspacesStatement(sourceTables, baseRange),
    [baseRange, sourceTables],
  );
  const workspacesQuery = useSqlStatement<DatabricksOptimizationWorkspace>(workspaceStatement, {
    enabled: sqlEnabled,
    requestKey: ['optimize', 'databricks', 'workspaces', baseRange, sourceTables],
  });
  const workspaceOptions = workspacesQuery.rows;
  const workspaceId =
    selectedWorkspaceId === 'all' ||
    workspaceOptions.some((w) => w.workspaceId === selectedWorkspaceId)
      ? selectedWorkspaceId
      : 'all';
  const scopedRange = useMemo(
    () => (workspaceId === 'all' ? baseRange : { ...baseRange, workspaceId }),
    [baseRange, workspaceId],
  );
  const summaryStatement = useMemo(
    () => buildDatabricksSummaryStatement(sourceTables, scopedRange),
    [scopedRange, sourceTables],
  );
  const trendStatement = useMemo(
    () => buildDatabricksTrendStatement(sourceTables, scopedRange, trendGrain),
    [scopedRange, sourceTables, trendGrain],
  );
  const servicesStatement = useMemo(
    () => buildDatabricksServicesStatement(sourceTables, scopedRange),
    [scopedRange, sourceTables],
  );
  const recommendationsStatement = useMemo(
    () => buildDatabricksRecommendationsStatement(sourceTables, scopedRange),
    [scopedRange, sourceTables],
  );
  const summaryQuery = useSqlStatement<DatabricksOptimizationSummary>(summaryStatement, {
    enabled: sqlEnabled,
    requestKey: ['optimize', 'databricks', 'summary', scopedRange, sourceTables],
  });
  const trendQuery = useSqlStatement<DatabricksOptimizationTrendRow>(trendStatement, {
    enabled: sqlEnabled,
    requestKey: ['optimize', 'databricks', 'trend', trendGrain, scopedRange, sourceTables],
  });
  const servicesQuery = useSqlStatement<DatabricksOptimizationServiceRow>(servicesStatement, {
    enabled: sqlEnabled,
    requestKey: ['optimize', 'databricks', 'services', scopedRange, sourceTables],
  });
  const recommendationsQuery = useSqlStatement<DatabricksOptimizationRecommendation>(
    recommendationsStatement,
    {
      enabled: sqlEnabled,
      requestKey: ['optimize', 'databricks', 'recommendations', scopedRange, sourceTables],
    },
  );
  const summary = summaryQuery.rows[0]
    ? {
        ...summaryQuery.rows[0],
        serverlessRatio: normalizeRatio(summaryQuery.rows[0].serverlessRatio),
      }
    : undefined;
  const loading =
    dataSources.isLoading ||
    appSettings.isLoading ||
    summaryQuery.isLoading ||
    workspacesQuery.isLoading ||
    trendQuery.isLoading ||
    servicesQuery.isLoading ||
    recommendationsQuery.isLoading;
  const errors = [
    sqlError('summary', summaryQuery.error),
    sqlError('workspaces', workspacesQuery.error),
    sqlError('trend', trendQuery.error),
    sqlError('services', servicesQuery.error),
    sqlError('recommendations', recommendationsQuery.error),
  ].filter((error): error is { tableName: string; message: string } => Boolean(error));

  const monthly = useMemo(
    () =>
      trendQuery.rows.map((row) => ({
        ...row,
        serverlessRatio: normalizeRatio(row.serverlessRatio),
        label: trendLabel(row.period, trendGrain, locale),
      })),
    [locale, trendGrain, trendQuery.rows],
  );
  const serviceRows = servicesQuery.rows.map((row) => ({
    ...row,
    serverlessRatio: normalizeRatio(row.serverlessRatio),
  }));
  const hasData = Boolean(summary && summary.totalCostUsd > 0);

  const refresh = () => {
    summaryQuery.refetch();
    workspacesQuery.refetch();
    trendQuery.refetch();
    servicesQuery.refetch();
    recommendationsQuery.refetch();
  };

  return (
    <>
      <PageHeader
        title={t('optimize.databricks.title')}
        subtitle={t('optimize.databricks.desc')}
        actions={
          <div className="flex flex-wrap justify-end gap-2">
            <Select value={workspaceId} onValueChange={setSelectedWorkspaceId}>
              <SelectTrigger className="w-56">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">{t('optimize.databricks.workspaces.all')}</SelectItem>
                {workspaceOptions.map((workspace) => {
                  const value = workspace.workspaceId ?? '';
                  if (!value) return null;
                  return (
                    <SelectItem key={value} value={value}>
                      {workspace.workspaceName || value}
                    </SelectItem>
                  );
                })}
              </SelectContent>
            </Select>
            <Select value={period} onValueChange={(value) => setPeriod(value as Period)}>
              <SelectTrigger className="w-40">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {PERIODS.map((option) => (
                  <SelectItem key={option} value={option}>
                    {t(`optimize.databricks.period.${option}`)}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
            <Button variant="outline" onClick={refresh} disabled={loading}>
              <RefreshCcw /> {t('dashboard.refresh')}
            </Button>
          </div>
        }
      />

      {errors.length > 0 ? (
        <Alert variant="destructive" className="mb-4">
          <AlertCircle />
          <AlertDescription>
            {t('optimize.databricks.failedToLoad')}{' '}
            {errors.map((error) => `${error.tableName}: ${error.message}`).join('; ')}
          </AlertDescription>
        </Alert>
      ) : null}

      <div className="mb-4 grid grid-cols-1 gap-3 md:grid-cols-2 xl:grid-cols-4">
        <KpiCard
          icon={DollarSign}
          label={t('optimize.databricks.kpi.totalCost')}
          value={summary ? formatUsd(summary.totalCostUsd) : ''}
          detail={t('optimize.databricks.kpi.effectiveCost')}
          loading={loading}
        />
        <KpiCard
          icon={Server}
          label={t('optimize.databricks.kpi.nonServerlessSpend')}
          value={summary ? formatUsd(summary.nonServerlessCostUsd) : ''}
          detail={t('optimize.databricks.kpi.spendToReview')}
          loading={loading}
          tone={summary && summary.nonServerlessCostUsd > 0 ? 'bad' : 'good'}
        />
        <KpiCard
          icon={Gauge}
          label={t('optimize.databricks.kpi.serverlessRatio')}
          value={formatRatio(summary?.serverlessRatio)}
          detail={t('optimize.databricks.kpi.knownSpendOnly')}
          loading={loading}
          tone={ratioTone(summary?.serverlessRatio)}
        />
        <KpiCard
          icon={ListChecks}
          label={t('optimize.databricks.kpi.candidates')}
          value={summary ? String(summary.candidateResourceCount) : ''}
          detail={t('optimize.databricks.kpi.resourceLevel')}
          loading={loading}
        />
      </div>

      {!loading && !hasData && errors.length === 0 ? (
        <Card className="mb-4">
          <CardContent>
            <EmptyState
              title={t('optimize.databricks.empty.noData')}
              description={t('optimize.databricks.empty.enableFocus')}
            />
          </CardContent>
        </Card>
      ) : null}

      <div className="mb-4 grid grid-cols-1 gap-4 xl:grid-cols-[2fr_1fr]">
        <Card>
          <CardHeader>
            <CardTitle className="text-sm">{t('optimize.databricks.monthly.title')}</CardTitle>
            <CardDescription>{t('optimize.databricks.monthly.desc')}</CardDescription>
          </CardHeader>
          <CardContent>
            {loading ? (
              <Skeleton className="h-80 w-full" />
            ) : monthly.length === 0 ? (
              <EmptyState
                title={t('optimize.databricks.empty.noMonthly')}
                description={t('optimize.databricks.empty.adjustFilters')}
              />
            ) : (
              <div className="h-80">
                <ResponsiveContainer width="100%" height="100%">
                  <ComposedChart data={monthly} margin={{ top: 8, right: 12, left: 0, bottom: 0 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
                    <XAxis dataKey="label" tick={{ fontSize: 12 }} />
                    <YAxis
                      yAxisId="cost"
                      tick={{ fontSize: 12 }}
                      tickFormatter={(value) => compactUsd(Number(value))}
                    />
                    <YAxis
                      yAxisId="ratio"
                      orientation="right"
                      domain={[0, 100]}
                      allowDataOverflow
                      tick={{ fontSize: 12 }}
                      tickFormatter={(value) => `${Math.round(Number(value))}%`}
                    />
                    <RechartsTooltip
                      content={<MonthlyTooltip formatUsd={formatUsd} formatRatio={formatRatio} />}
                    />
                    <Legend />
                    <Bar
                      yAxisId="cost"
                      dataKey="serverlessCostUsd"
                      stackId="cost"
                      name={t('optimize.databricks.legend.serverless')}
                      fill={SERVERLESS_COLOR}
                    />
                    <Bar
                      yAxisId="cost"
                      dataKey="nonServerlessCostUsd"
                      stackId="cost"
                      name={t('optimize.databricks.legend.nonServerless')}
                      fill={NON_SERVERLESS_COLOR}
                    />
                    <Bar
                      yAxisId="cost"
                      dataKey="unknownCostUsd"
                      stackId="cost"
                      name={t('optimize.databricks.legend.unknown')}
                      fill={UNKNOWN_COLOR}
                    />
                    <Line
                      yAxisId="ratio"
                      type="monotone"
                      dataKey="serverlessRatio"
                      name={t('optimize.databricks.legend.ratio')}
                      stroke={RATIO_COLOR}
                      strokeWidth={2}
                      dot={false}
                      connectNulls
                    />
                  </ComposedChart>
                </ResponsiveContainer>
              </div>
            )}
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle className="text-sm">{t('optimize.databricks.services.title')}</CardTitle>
            <CardDescription>{t('optimize.databricks.services.desc')}</CardDescription>
          </CardHeader>
          <CardContent>
            {loading ? (
              <div className="grid gap-3">
                {Array.from({ length: 6 }).map((_, index) => (
                  <Skeleton key={index} className="h-12 w-full" />
                ))}
              </div>
            ) : serviceRows.length === 0 ? (
              <EmptyState
                title={t('optimize.databricks.empty.noServices')}
                description={t('optimize.databricks.empty.adjustFilters')}
              />
            ) : (
              <div className="grid gap-4">
                {serviceRows.map((row) => (
                  <ServiceRatioRow key={`${row.serviceCategory}-${row.serviceName}`} row={row} />
                ))}
              </div>
            )}
          </CardContent>
        </Card>
      </div>

      <Card>
        <CardHeader>
          <CardTitle className="text-sm">
            {t('optimize.databricks.recommendations.title')}
          </CardTitle>
          <CardDescription>{t('optimize.databricks.recommendations.desc')}</CardDescription>
        </CardHeader>
        <CardContent>
          {loading ? (
            <Skeleton className="h-72 w-full" />
          ) : recommendationsQuery.rows.length === 0 ? (
            <EmptyState
              title={t('optimize.databricks.empty.noRecommendations')}
              description={t('optimize.databricks.empty.noNonServerless')}
            />
          ) : (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>{t('optimize.databricks.table.priority')}</TableHead>
                  <TableHead>{t('optimize.databricks.table.resource')}</TableHead>
                  <TableHead>{t('optimize.databricks.table.workspace')}</TableHead>
                  <TableHead>{t('optimize.databricks.table.service')}</TableHead>
                  <TableHead className="text-right">
                    {t('optimize.databricks.table.nonServerlessSpend')}
                  </TableHead>
                  <TableHead>{t('optimize.databricks.table.action')}</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {recommendationsQuery.rows.map((row) => (
                  <RecommendationRow key={`${row.rank}-${row.resourceId}`} row={row} />
                ))}
              </TableBody>
            </Table>
          )}
        </CardContent>
      </Card>
    </>
  );
}

function KpiCard({
  icon: Icon,
  label,
  value,
  detail,
  tone = 'neutral',
  loading,
}: {
  icon: typeof DollarSign;
  label: string;
  value: string;
  detail: string;
  tone?: 'good' | 'bad' | 'neutral';
  loading: boolean;
}) {
  const toneClass =
    tone === 'good'
      ? 'text-(--success)'
      : tone === 'bad'
        ? 'text-(--danger)'
        : 'text-muted-foreground';
  return (
    <Card className="relative overflow-hidden">
      <div
        className={`absolute inset-x-0 top-0 h-1 ${
          tone === 'good' ? 'bg-(--success)' : tone === 'bad' ? 'bg-(--danger)' : 'bg-primary'
        }`}
      />
      <CardHeader className="space-y-3">
        <Icon className="text-muted-foreground h-4 w-4" />
        <CardDescription className="text-[11px] tracking-wider uppercase">{label}</CardDescription>
        <CardTitle className="text-2xl font-semibold">
          {loading ? <Skeleton className="h-8 w-24" /> : value}
        </CardTitle>
      </CardHeader>
      <CardContent>
        <p className={`m-0 text-xs ${toneClass}`}>{detail}</p>
      </CardContent>
    </Card>
  );
}

function ServiceRatioRow({ row }: { row: DatabricksOptimizationServiceRow }) {
  const { t } = useI18n();
  const formatUsd = useCurrencyUsd();
  const knownCost = row.serverlessCostUsd + row.nonServerlessCostUsd;
  const denominator = Math.max(knownCost, 1);
  const serverlessWidth = knownCost > 0 ? (row.serverlessCostUsd / denominator) * 100 : 0;
  const nonServerlessWidth = knownCost > 0 ? (row.nonServerlessCostUsd / denominator) * 100 : 0;
  return (
    <div className="grid gap-2">
      <div className="flex items-start justify-between gap-3 text-sm">
        <div className="min-w-0">
          <p className="m-0 truncate font-medium">{row.serviceName}</p>
        </div>
        <div className="text-right">
          <p className="m-0 font-medium">{formatRatio(row.serverlessRatio)}</p>
          <p className="text-muted-foreground m-0 text-xs">
            {formatUsd(row.serverlessCostUsd)} / {formatUsd(row.totalCostUsd)}
          </p>
        </div>
      </div>
      <div
        className="bg-muted flex h-3 overflow-hidden rounded-sm"
        aria-label={t('optimize.databricks.services.title')}
      >
        <span style={{ width: `${serverlessWidth}%`, background: SERVERLESS_COLOR }} />
        <span style={{ width: `${nonServerlessWidth}%`, background: NON_SERVERLESS_COLOR }} />
      </div>
      <Progress value={row.serverlessRatio ?? 0} className="sr-only" />
    </div>
  );
}

function RecommendationRow({ row }: { row: DatabricksOptimizationRecommendation }) {
  const { t } = useI18n();
  const formatUsd = useCurrencyUsd();
  const resourceName = row.resourceName || row.resourceId;
  const workspace = row.workspaceName || row.workspaceId || t('dashboard.notAvailable');
  return (
    <TableRow>
      <TableCell>
        <PriorityBadge priority={row.priority} />
      </TableCell>
      <TableCell className="min-w-56">
        <div className="grid gap-0.5">
          <span className="font-medium">{resourceName}</span>
          <span className="text-muted-foreground text-xs">
            {row.resourceType ?? t('dashboard.notAvailable')} · {row.resourceId}
          </span>
        </div>
      </TableCell>
      <TableCell className="min-w-40">{workspace}</TableCell>
      <TableCell>
        <div className="grid gap-0.5">
          <span>{row.serviceName}</span>
          <span className="text-muted-foreground text-xs">{row.serviceCategory}</span>
        </div>
      </TableCell>
      <TableCell className="text-right font-medium">
        {formatUsd(row.nonServerlessCostUsd)}
      </TableCell>
      <TableCell className="min-w-72">
        <div className="grid gap-1">
          <span>{row.action}</span>
          <span className="text-muted-foreground text-xs">{row.reason}</span>
        </div>
      </TableCell>
    </TableRow>
  );
}

function PriorityBadge({
  priority,
}: {
  priority: DatabricksOptimizationRecommendation['priority'];
}) {
  const { t } = useI18n();
  if (priority === 'high') {
    return <Badge variant="destructive">{t('optimize.databricks.priority.high')}</Badge>;
  }
  if (priority === 'medium') {
    return <Badge variant="outline">{t('optimize.databricks.priority.medium')}</Badge>;
  }
  return <Badge variant="secondary">{t('optimize.databricks.priority.low')}</Badge>;
}

function EmptyState({ title, description }: { title: string; description: string }) {
  return (
    <Empty>
      <EmptyHeader>
        <EmptyTitle>{title}</EmptyTitle>
        <EmptyDescription>{description}</EmptyDescription>
      </EmptyHeader>
    </Empty>
  );
}

function MonthlyTooltip({
  active,
  payload,
  label,
  formatUsd,
  formatRatio: formatTooltipRatio,
}: {
  active?: boolean;
  payload?: Array<{ name?: string; value?: number; color?: string; dataKey?: string }>;
  label?: string;
  formatUsd: (value: number) => string;
  formatRatio: (value: number | null | undefined) => string;
}) {
  if (!active || !payload?.length) return null;
  return (
    <div className="bg-popover text-popover-foreground border-border rounded-md border px-3 py-2 text-xs shadow-md">
      <p className="m-0 mb-1 font-medium">{label}</p>
      {payload
        .filter((item) => item.value !== null && item.value !== undefined)
        .map((item) => (
          <p
            key={`${item.dataKey}-${item.name}`}
            className="m-0 flex items-center justify-between gap-5"
          >
            <span className="inline-flex items-center gap-2">
              <span className="h-2 w-2 rounded-sm" style={{ background: item.color }} />
              {item.name}
            </span>
            <span>
              {item.dataKey === 'serverlessRatio'
                ? formatTooltipRatio(Number(item.value))
                : formatUsd(Number(item.value ?? 0))}
            </span>
          </p>
        ))}
    </div>
  );
}

function ratioTone(value: number | null | undefined): 'good' | 'bad' | 'neutral' {
  const ratio = normalizeRatio(value);
  if (ratio === null) return 'neutral';
  if (ratio >= 70) return 'good';
  if (ratio < 30) return 'bad';
  return 'neutral';
}

function formatRatio(value: number | null | undefined): string {
  const ratio = normalizeRatio(value);
  if (ratio === null) return 'N/A';
  return `${Math.round(ratio)}%`;
}

function normalizeRatio(value: number | null | undefined): number | null {
  if (value === null || value === undefined || !Number.isFinite(value)) return null;
  return Math.max(0, Math.min(100, value));
}

function compactUsd(value: number): string {
  if (Math.abs(value) >= 1_000_000) return `$${Math.round(value / 1_000_000)}M`;
  if (Math.abs(value) >= 1_000) return `$${Math.round(value / 1_000)}K`;
  return `$${Math.round(value)}`;
}

function trendLabel(key: string, grain: 'day' | 'month', locale: string): string {
  const parts = key.split('-');
  const year = Number(parts[0]);
  const month = Number(parts[1]);
  const day = Number(parts[2]);
  const safeYear = Number.isFinite(year) ? year : new Date().getFullYear();
  const safeMonth = Number.isFinite(month) ? month : 1;
  const safeDay = Number.isFinite(day) ? day : 1;
  return new Intl.DateTimeFormat(locale === 'ja' ? 'ja-JP' : 'en-US', {
    day: grain === 'day' ? 'numeric' : undefined,
    month: 'short',
    year: grain === 'month' ? '2-digit' : undefined,
  }).format(new Date(safeYear, safeMonth - 1, safeDay));
}
