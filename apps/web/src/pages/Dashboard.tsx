import { useEffect, useMemo, useState } from 'react';
import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  Pie,
  PieChart,
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
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from '@databricks/appkit-ui/react';
import {
  AlertCircle,
  CalendarDays,
  CheckCircle2,
  ChevronLeft,
  ChevronRight,
  Database,
  DollarSign,
  RefreshCcw,
  Sparkles,
  Tags,
  TrendingUp,
  type LucideIcon,
} from 'lucide-react';
import { PageHeader } from '../components/PageHeader';
import {
  type FocusOverviewDailyRow,
  type FocusOverviewResponse,
  type FocusOverviewServiceRow,
  type FocusOverviewSkuRow,
  useBudgets,
  useFocusOverview,
} from '../api/hooks';
import { useCurrencyUsd, useI18n, type TFunction } from '../i18n';

type ProviderKey = 'databricks' | 'aws' | 'azure' | 'gcp' | 'snowflake' | 'other';

interface ProviderMeta {
  key: ProviderKey;
  label: string;
  color: string;
  freshness: string;
  costBasis: string;
}

interface Recommendation {
  title: string;
  provider: ProviderMeta;
  savingsUsd: number | null;
  reason: string;
}

interface Anomaly {
  label: string;
  impactUsd: number;
  severity: 'high' | 'medium' | 'resolved';
  when: string;
}

const COST_BREAKDOWN_PALETTE = [
  '#3B82F6',
  '#49A078',
  '#F2A72B',
  '#9B59B6',
  '#20C7A8',
  '#EC4899',
  '#6366F1',
  '#EAB308',
  '#EF4444',
  '#0EA5E9',
  '#84CC16',
  '#F97316',
  '#718096',
];

interface CostBreakdownSeriesMeta {
  // Synthetic id used as Recharts dataKey — values like "Other", "AWS S3"
  // can't be used directly because Recharts treats dataKey as a lodash path.
  key: string;
  value: string;
  color: string;
}

const PROVIDERS: Record<ProviderKey, ProviderMeta> = {
  databricks: {
    key: 'databricks',
    label: 'Databricks',
    color: '#3B82F6',
    freshness: 'system tables, delayed by a few hours',
    costBasis: 'effective list price',
  },
  aws: {
    key: 'aws',
    label: 'AWS',
    color: '#49A078',
    freshness: 'CUR / Data Export, refreshed multiple times per day',
    costBasis: 'amortized cost',
  },
  azure: {
    key: 'azure',
    label: 'Azure',
    color: '#F2A72B',
    freshness: 'Cost Management export, daily',
    costBasis: 'effective cost',
  },
  gcp: {
    key: 'gcp',
    label: 'GCP',
    color: '#9B59B6',
    freshness: 'BigQuery billing export, daily',
    costBasis: 'effective cost',
  },
  snowflake: {
    key: 'snowflake',
    label: 'Snowflake',
    color: '#20C7A8',
    freshness: 'ACCOUNT_USAGE, daily',
    costBasis: 'usage cost',
  },
  other: {
    key: 'other',
    label: 'Other',
    color: '#718096',
    freshness: 'Configured source',
    costBasis: 'source native cost',
  },
};

const periodOptions = ['mtd', 'last30'] as const;
type Period = (typeof periodOptions)[number];
const costBreakdownDimensions = ['providerName', 'serviceCategory', 'serviceName'] as const;
type CostBreakdownDimension = (typeof costBreakdownDimensions)[number];
const COST_BREAKDOWN_SERIES_LIMIT = 12;
const COST_BREAKDOWN_OTHER_COLOR = '#718096';

function overviewRange() {
  const now = new Date();
  const start = new Date(now.getFullYear() - 1, now.getMonth(), 1);
  return { start: start.toISOString(), end: now.toISOString() };
}

function monthToDateRange() {
  const now = new Date();
  const start = new Date(now.getFullYear(), now.getMonth(), 1);
  return { start: start.toISOString(), end: now.toISOString() };
}

function last30Range() {
  const end = new Date();
  const start = new Date(end.getTime() - 30 * 24 * 60 * 60 * 1000);
  return { start: start.toISOString(), end: end.toISOString() };
}

export function Dashboard() {
  const { t, locale } = useI18n();
  const formatUsd = useCurrencyUsd();
  const [period, setPeriod] = useState<Period>('mtd');
  const [costBreakdownDimension, setCostBreakdownDimension] =
    useState<CostBreakdownDimension>('providerName');
  const [coveragePage, setCoveragePage] = useState(0);
  const wideRange = useMemo(overviewRange, []);
  const mtdRange = useMemo(monthToDateRange, []);
  const rollingRange = useMemo(last30Range, []);
  const activeRange = period === 'mtd' ? mtdRange : rollingRange;

  const history = useFocusOverview(wideRange);
  const current = useFocusOverview(activeRange);
  const budgets = useBudgets();

  const sources = history.data?.sources ?? current.data?.sources ?? [];
  const activeProviders = useMemo(() => uniqueProviders(sources), [sources]);
  const dailyRows = history.data?.daily ?? [];
  const skuRows = current.data?.skus ?? [];
  const serviceRows = current.data?.services ?? [];
  const coverageRows = current.data?.coverage ?? [];

  const overview = useMemo(() => {
    const now = new Date();
    const currentMonthKey = monthKey(now);
    const previousMonthKey = monthKey(new Date(now.getFullYear(), now.getMonth() - 1, 1));
    const lastYearMonthKey = monthKey(new Date(now.getFullYear() - 1, now.getMonth(), 1));
    const monthlyDatabricks = monthlyTotals(dailyRows);
    const mtdTotal = monthlyDatabricks.get(currentMonthKey) ?? 0;
    const previousMonth = monthlyDatabricks.get(previousMonthKey) ?? 0;
    const lastYearMonth = monthlyDatabricks.get(lastYearMonthKey) ?? 0;
    const elapsedDays = Math.max(1, now.getDate());
    const daysInCurrentMonth = daysInMonth(now);
    const forecast = (mtdTotal / elapsedDays) * daysInCurrentMonth;
    const avgDaily = period === 'mtd' ? mtdTotal / elapsedDays : sumRecentDays(dailyRows, 30) / 30;
    const anomalies = detectAnomalies(dailyRows, locale, t);
    const recommendations = buildRecommendations(skuRows, activeProviders, t);
    const budgetTotal = budgets.data?.items.reduce((sum, b) => sum + b.amountUsd, 0) ?? 0;
    const budgetUtilization =
      budgetTotal > 0 ? Math.min(100, (forecast / budgetTotal) * 100) : null;

    return {
      mtdTotal,
      previousMonth,
      lastYearMonth,
      forecast,
      avgDaily,
      anomalies,
      recommendations,
      recommendationPotential: recommendations.reduce((sum, r) => sum + (r.savingsUsd ?? 0), 0),
      budgetTotal,
      budgetUtilization,
    };
  }, [activeProviders, budgets.data?.items, dailyRows, locale, period, skuRows, t]);

  const costBreakdownKeyOf = useMemo(
    () => (row: FocusOverviewDailyRow) => costBreakdownValue(row, costBreakdownDimension),
    [costBreakdownDimension],
  );
  const otherLabel = t('dashboard.costBreakdownOther');
  const { series: costBreakdownSeries, bucketKeyOf: costBreakdownBucketKeyOf } = useMemo(
    () => buildCostBreakdownSeries(dailyRows, costBreakdownKeyOf, otherLabel),
    [costBreakdownKeyOf, dailyRows, otherLabel],
  );
  const trendData = useMemo(
    () =>
      buildTrendData(
        dailyRows,
        costBreakdownSeries,
        costBreakdownBucketKeyOf,
        overview.forecast,
        locale,
        t,
      ),
    [costBreakdownBucketKeyOf, costBreakdownSeries, dailyRows, locale, overview.forecast, t],
  );
  const costBreakdownMtd = useMemo(
    () => buildCostBreakdownMtd(costBreakdownSeries, dailyRows, costBreakdownBucketKeyOf),
    [costBreakdownBucketKeyOf, costBreakdownSeries, dailyRows],
  );
  const topServices = useMemo(
    () => buildTopServices(serviceRows, skuRows, activeProviders),
    [activeProviders, serviceRows, skuRows],
  );
  const lastUpdated = useMemo(
    () => formatLastUpdated(history.dataUpdatedAt, current.dataUpdatedAt, locale),
    [current.dataUpdatedAt, history.dataUpdatedAt, locale],
  );
  const hasAnyCostData = dailyRows.length > 0 || skuRows.length > 0;

  const loading = history.isLoading || current.isLoading;
  const costError = history.isError || current.isError;
  const sourceErrors = [...(history.data?.errors ?? []), ...(current.data?.errors ?? [])];
  const tagCoverage = useMemo(() => {
    const totalResources = coverageRows.reduce((sum, row) => sum + row.rowCount, 0);
    if (totalResources <= 0) return null;
    const taggedResources = coverageRows.reduce((sum, row) => sum + row.taggedRows, 0);
    return (taggedResources / totalResources) * 100;
  }, [coverageRows]);
  const sortedCoverage = useMemo(
    () =>
      [...coverageRows].sort(
        (a, b) => b.tagCoveragePct - a.tagCoveragePct || b.rowCount - a.rowCount,
      ),
    [coverageRows],
  );
  const coveragePageSize = 5;
  const coveragePageCount = Math.max(1, Math.ceil(sortedCoverage.length / coveragePageSize));
  useEffect(() => {
    setCoveragePage((p) => Math.min(p, coveragePageCount - 1));
  }, [coveragePageCount]);
  const visibleCoverage = sortedCoverage.slice(
    coveragePage * coveragePageSize,
    coveragePage * coveragePageSize + coveragePageSize,
  );

  return (
    <>
      <div className="flex flex-col gap-4 md:flex-row md:items-start md:justify-between">
        <PageHeader title={t('nav.overview')} subtitle={t('dashboard.subtitle')} />
        <div className="flex flex-wrap items-center gap-2">
          <Select value={period} onValueChange={(value) => setPeriod(value as typeof period)}>
            <SelectTrigger className="w-[150px]">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {periodOptions.map((option) => (
                <SelectItem key={option} value={option}>
                  {t(`dashboard.period.${option}`)}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
          <Button variant="outline" onClick={() => window.location.reload()}>
            <RefreshCcw /> {t('dashboard.refresh')}
          </Button>
        </div>
      </div>

      {history.isSuccess && sources.length === 0 ? (
        <Alert className="mb-4">
          <Database />
          <AlertDescription>{t('dashboard.noEnabledSources')}</AlertDescription>
        </Alert>
      ) : null}

      {costError ? (
        <Alert variant="destructive" className="mb-4">
          <AlertCircle />
          <AlertDescription>{t('dashboard.focusLoadFailed')}</AlertDescription>
        </Alert>
      ) : null}

      {sourceErrors.length > 0 ? (
        <Alert className="mb-4">
          <AlertCircle />
          <AlertDescription>
            {t('dashboard.someSourcesFailed')}{' '}
            {sourceErrors
              .slice(0, 3)
              .map((error) => `${error.name} (${error.tableName})`)
              .join(', ')}
          </AlertDescription>
        </Alert>
      ) : null}

      <SectionTitle title={t('dashboard.sections.costSummary')} />
      <div className="mb-4 grid grid-cols-1 gap-3 sm:grid-cols-2 xl:grid-cols-6">
        <KpiCard
          icon={DollarSign}
          label={t('dashboard.kpi.totalCostMtd')}
          value={formatUsd(overview.mtdTotal)}
          delta={comparisonText(
            overview.mtdTotal,
            overview.previousMonth,
            t('dashboard.vsLastMonth'),
            t,
          )}
          tone={deltaTone(overview.mtdTotal, overview.previousMonth)}
          loading={loading}
        />
        <KpiCard
          icon={TrendingUp}
          label={t('dashboard.kpi.forecastedMonthEnd')}
          value={formatUsd(overview.forecast)}
          delta={
            overview.budgetTotal > 0
              ? t('dashboard.budgetAmount', { amount: formatUsd(overview.budgetTotal) })
              : t('dashboard.noMonthlyBudget')
          }
          badge={
            overview.budgetTotal > 0 && overview.forecast > overview.budgetTotal
              ? t('dashboard.overBudget')
              : undefined
          }
          tone={
            overview.budgetTotal > 0 && overview.forecast > overview.budgetTotal ? 'bad' : 'neutral'
          }
          loading={loading}
        />
        <KpiCard
          icon={CalendarDays}
          label={t('dashboard.kpi.avgDailyCost')}
          value={formatUsd(overview.avgDaily)}
          delta={comparisonText(
            overview.mtdTotal,
            overview.lastYearMonth,
            t('dashboard.vsSameMonthLastYear'),
            t,
          )}
          tone={deltaTone(overview.mtdTotal, overview.lastYearMonth)}
          loading={loading}
        />
        <KpiCard
          icon={CheckCircle2}
          label={t('dashboard.kpi.savingsRealizedMtd')}
          value={formatUsd(0)}
          delta={t('dashboard.commitmentFeedNotConnected')}
          tone="good"
          loading={history.isLoading}
        />
        <KpiCard
          icon={AlertCircle}
          label={t('dashboard.kpi.anomaliesLast7d')}
          value={String(overview.anomalies.filter((a) => a.severity !== 'resolved').length)}
          delta={
            overview.anomalies.length > 0
              ? t('dashboard.anomaliesDetected', { count: overview.anomalies.length })
              : t('dashboard.noSpikeDetected')
          }
          badge={
            overview.anomalies.some((a) => a.severity === 'high') ? t('dashboard.alert') : undefined
          }
          tone={overview.anomalies.length > 0 ? 'bad' : 'neutral'}
          loading={loading}
        />
        <KpiCard
          icon={Sparkles}
          label={t('dashboard.kpi.openRecommendations')}
          value={String(overview.recommendations.length)}
          delta={t('dashboard.monthlyPotential', {
            amount: formatUsd(overview.recommendationPotential),
          })}
          tone="good"
          loading={current.isLoading}
        />
      </div>

      <div className="mt-5 mb-2 flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
        <h3 className="m-0 text-base font-semibold">
          {t('dashboard.sections.costTrendsBreakdown')}
        </h3>
        <Select
          value={costBreakdownDimension}
          onValueChange={(value) => setCostBreakdownDimension(value as CostBreakdownDimension)}
        >
          <SelectTrigger className="w-full sm:w-[220px]">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            {costBreakdownDimensions.map((dimension) => (
              <SelectItem key={dimension} value={dimension}>
                {t(`dashboard.costBreakdownDimensions.${dimension}`)}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>
      <div className="mb-4 grid grid-cols-1 gap-4 xl:grid-cols-[2fr_1fr]">
        <Card>
          <CardHeader>
            <CardTitle className="text-sm">
              {t('dashboard.monthlyCostByDimension', {
                dimension: t(`dashboard.costBreakdownDimensions.${costBreakdownDimension}`),
              })}
            </CardTitle>
          </CardHeader>
          <CardContent>
            {loading ? (
              <Skeleton className="h-80 w-full" />
            ) : !hasAnyCostData ? (
              <EmptyState
                title={t('dashboard.empty.noCostTrendData')}
                description={t('dashboard.empty.enableSourceAndRunRefresh')}
              />
            ) : (
              <div className="h-80">
                <ResponsiveContainer>
                  <BarChart data={trendData}>
                    <CartesianGrid stroke="var(--border)" vertical={false} />
                    <XAxis dataKey="label" stroke="var(--muted-foreground)" fontSize={11} />
                    <YAxis
                      stroke="var(--muted-foreground)"
                      fontSize={11}
                      tickFormatter={shortUsd}
                    />
                    <RechartsTooltip content={<ChartTooltip formatUsd={formatUsd} />} />
                    {costBreakdownSeries.map((series) => (
                      <Bar
                        key={series.key}
                        dataKey={series.key}
                        name={series.value}
                        stackId="cost"
                        fill={series.color}
                        radius={[3, 3, 0, 0]}
                      />
                    ))}
                  </BarChart>
                </ResponsiveContainer>
              </div>
            )}
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle className="text-sm">
              {t('dashboard.breakdownMtdByDimension', {
                dimension: t(`dashboard.costBreakdownDimensions.${costBreakdownDimension}`),
              })}
            </CardTitle>
            <CardDescription>{t('dashboard.breakdownMtdDesc')}</CardDescription>
          </CardHeader>
          <CardContent>
            {loading ? (
              <Skeleton className="h-80 w-full" />
            ) : costBreakdownMtd.length === 0 ? (
              <EmptyState
                title={t('dashboard.empty.noMeasuredBreakdownSpend')}
                description={t('dashboard.empty.configuredSourcesAfterFacts')}
              />
            ) : (
              <div className="grid gap-3 lg:grid-cols-[minmax(10rem,1fr)_max-content]">
                <div className="relative h-56 w-full">
                  <ResponsiveContainer>
                    <PieChart>
                      <Pie
                        data={costBreakdownMtd}
                        innerRadius="60%"
                        outerRadius="90%"
                        dataKey="cost"
                        nameKey="value"
                        stroke="var(--card)"
                        strokeWidth={2}
                      >
                        {costBreakdownMtd.map((entry) => (
                          <Cell key={entry.key} fill={entry.color} />
                        ))}
                      </Pie>
                      <RechartsTooltip
                        content={<ChartTooltip formatUsd={formatUsd} />}
                        wrapperStyle={{ zIndex: 20 }}
                      />
                    </PieChart>
                  </ResponsiveContainer>
                  <div className="pointer-events-none absolute inset-0 z-0 flex flex-col items-center justify-center text-center">
                    <span className="text-base font-semibold">
                      {formatWholeUsd(overview.mtdTotal, locale)}
                    </span>
                    <span className="text-muted-foreground text-xs">{t('dashboard.totalMtd')}</span>
                  </div>
                </div>
                <div className="grid max-w-[14rem] content-center gap-2">
                  {costBreakdownMtd.map((item) => (
                    <div
                      key={item.key}
                      className="grid grid-cols-[0.625rem_minmax(0,1fr)_2rem] items-center gap-2 text-sm"
                    >
                      <span className="h-2.5 w-2.5 rounded-sm" style={{ background: item.color }} />
                      <span className="block truncate whitespace-nowrap" title={item.value}>
                        {item.value}
                      </span>
                      <span className="text-right font-medium">{item.percent}%</span>
                    </div>
                  ))}
                </div>
              </div>
            )}
          </CardContent>
        </Card>
      </div>

      <SectionTitle title={t('dashboard.sections.costAllocationTopSpenders')} />
      <div className="mb-4 grid grid-cols-1 gap-4 xl:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle className="text-sm">{t('dashboard.topServicesBySpend')}</CardTitle>
            <CardDescription>{t('dashboard.topServicesDesc')}</CardDescription>
          </CardHeader>
          <CardContent>
            {current.isLoading ? (
              <Skeleton className="h-56 w-full" />
            ) : topServices.length === 0 ? (
              <EmptyState
                title={t('dashboard.empty.noServicesYet')}
                description={t('dashboard.empty.servicesAfterUsage')}
              />
            ) : (
              <div className="grid gap-3">
                {topServices.map((service) => (
                  <HorizontalSpendBar
                    key={service.name}
                    name={service.name}
                    value={service.costUsd}
                    max={topServices[0]?.costUsd ?? 1}
                    color={service.provider.color}
                    formatUsd={formatUsd}
                  />
                ))}
              </div>
            )}
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle className="text-sm">{t('dashboard.coverageUtilizationRates')}</CardTitle>
            <CardDescription>{t('dashboard.coverageUtilizationDesc')}</CardDescription>
          </CardHeader>
          <CardContent>
            <div className="grid grid-cols-1 gap-4 sm:grid-cols-3">
              <Gauge label={t('dashboard.gauge.riSpCoverage')} value={null} color="#49A078" />
              <Gauge label={t('dashboard.gauge.tagCoverage')} value={tagCoverage} color="#3B82F6" />
              <Gauge
                label={t('dashboard.gauge.budgetUtil')}
                value={overview.budgetUtilization}
                color="#F2A72B"
              />
            </div>
          </CardContent>
        </Card>
      </div>

      <SectionTitle title={t('dashboard.sections.optimizationGovernance')} />
      <div className="mb-4 grid grid-cols-1 gap-4 xl:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle className="text-sm">
              {t('dashboard.topRecommendations')}
              {overview.recommendationPotential > 0
                ? t('dashboard.potentialSavingsSuffix', {
                    amount: formatUsd(overview.recommendationPotential),
                  })
                : ''}
            </CardTitle>
          </CardHeader>
          <CardContent>
            {current.isLoading ? (
              <Skeleton className="h-56 w-full" />
            ) : overview.recommendations.length === 0 ? (
              <EmptyState
                title={t('dashboard.empty.noRecommendationsYet')}
                description={t('dashboard.empty.recommendationsFromSignals')}
              />
            ) : (
              <div className="divide-border divide-y">
                {overview.recommendations.slice(0, 5).map((rec) => (
                  <div
                    key={rec.title}
                    className="grid grid-cols-[1fr_auto_auto] items-center gap-3 py-3"
                  >
                    <div>
                      <p className="m-0 text-sm font-medium">{rec.title}</p>
                      <p className="text-muted-foreground m-0 text-xs">{rec.reason}</p>
                    </div>
                    <Badge
                      variant="outline"
                      style={{ borderColor: rec.provider.color, color: rec.provider.color }}
                    >
                      {providerDisplayLabel(rec.provider, t)}
                    </Badge>
                    <span className="text-sm font-semibold text-(--success)">
                      {rec.savingsUsd
                        ? t('dashboard.perMonth', { amount: formatUsd(rec.savingsUsd) })
                        : '--'}
                    </span>
                  </div>
                ))}
              </div>
            )}
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle className="text-sm">{t('dashboard.anomalyAlertsLast7Days')}</CardTitle>
          </CardHeader>
          <CardContent>
            {history.isLoading ? (
              <Skeleton className="h-56 w-full" />
            ) : overview.anomalies.length === 0 ? (
              <EmptyState
                title={t('dashboard.empty.noAnomaliesDetected')}
                description={t('dashboard.empty.noAnomalies')}
              />
            ) : (
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>{t('dashboard.table.severity')}</TableHead>
                    <TableHead>{t('dashboard.table.signal')}</TableHead>
                    <TableHead className="text-right">{t('dashboard.table.impact')}</TableHead>
                    <TableHead className="text-right">{t('dashboard.table.when')}</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {overview.anomalies.map((anomaly) => (
                    <TableRow key={`${anomaly.label}-${anomaly.when}`}>
                      <TableCell>
                        <SeverityBadge severity={anomaly.severity} />
                      </TableCell>
                      <TableCell>{anomaly.label}</TableCell>
                      <TableCell className="text-right text-(--danger)">
                        +{formatUsd(anomaly.impactUsd)}
                      </TableCell>
                      <TableCell className="text-muted-foreground text-right">
                        {anomaly.when}
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            )}
          </CardContent>
        </Card>
      </div>

      <SectionTitle title={t('dashboard.sections.budgetTrackingTaggingHealth')} />
      <div className="mb-4 grid grid-cols-1 gap-4 xl:grid-cols-[2fr_1fr]">
        <Card>
          <CardHeader>
            <CardTitle className="text-sm">{t('dashboard.teamBudgets')}</CardTitle>
            <CardDescription>{t('dashboard.teamBudgetsDesc')}</CardDescription>
          </CardHeader>
          <CardContent>
            {budgets.isLoading ? (
              <Skeleton className="h-56 w-full" />
            ) : !budgets.data || budgets.data.items.length === 0 ? (
              <EmptyState
                title={t('dashboard.empty.noBudgetsConfigured')}
                description={t('dashboard.empty.createBudgets')}
              />
            ) : (
              <div className="grid gap-3 lg:grid-cols-3">
                {budgets.data.items.slice(0, 6).map((budget) => {
                  const utilization =
                    budget.amountUsd > 0
                      ? Math.min(100, (overview.forecast / budget.amountUsd) * 100)
                      : 0;
                  return (
                    <div
                      key={budget.id}
                      className="bg-muted/25 rounded-md border border-border p-4"
                    >
                      <div className="mb-4">
                        <p className="m-0 text-sm font-semibold">{budget.name}</p>
                        <p className="text-muted-foreground m-0 text-sm">
                          {formatUsd(Math.min(overview.forecast, budget.amountUsd))} /{' '}
                          {formatUsd(budget.amountUsd)}
                        </p>
                      </div>
                      <Progress value={utilization} />
                      <div className="mt-4 flex h-20 items-end gap-1">
                        {miniTrend(trendData).map((value, index) => (
                          <span
                            key={`${budget.id}-${index}`}
                            className="bg-primary/70 block flex-1 rounded-t-sm"
                            style={{ height: `${Math.max(8, value)}%` }}
                          />
                        ))}
                      </div>
                    </div>
                  );
                })}
              </div>
            )}
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <div className="flex items-start justify-between gap-2">
              <div>
                <CardTitle className="text-sm">{t('dashboard.taggingCoverage')}</CardTitle>
                <CardDescription>{t('dashboard.taggingCoverageDesc')}</CardDescription>
              </div>
              {sortedCoverage.length > coveragePageSize ? (
                <div className="flex shrink-0 items-center gap-1 text-xs text-muted-foreground">
                  <Button
                    variant="outline"
                    size="icon"
                    className="h-6 w-6"
                    onClick={() => setCoveragePage((p) => Math.max(0, p - 1))}
                    disabled={coveragePage === 0}
                  >
                    <ChevronLeft className="h-3.5 w-3.5" />
                  </Button>
                  <span className="whitespace-nowrap tabular-nums">
                    {coveragePage + 1} / {coveragePageCount}
                  </span>
                  <Button
                    variant="outline"
                    size="icon"
                    className="h-6 w-6"
                    onClick={() => setCoveragePage((p) => Math.min(coveragePageCount - 1, p + 1))}
                    disabled={coveragePage >= coveragePageCount - 1}
                  >
                    <ChevronRight className="h-3.5 w-3.5" />
                  </Button>
                </div>
              ) : null}
            </div>
          </CardHeader>
          <CardContent>
            <div className="grid gap-3">
              {activeProviders.length === 0 ? (
                <EmptyState
                  title={t('dashboard.empty.noProviders')}
                  description={t('dashboard.empty.enableSourcesForCoverage')}
                />
              ) : visibleCoverage.length === 0 ? (
                activeProviders.map((provider) => (
                  <div key={provider.key}>
                    <div className="mb-1 flex items-center justify-between text-sm">
                      <span className="inline-flex items-center gap-2">
                        <Tags className="h-3.5 w-3.5" />
                        {providerDisplayLabel(provider, t)}
                      </span>
                      <span className="text-muted-foreground">{t('dashboard.notMeasured')}</span>
                    </div>
                    <Progress value={0} />
                  </div>
                ))
              ) : (
                visibleCoverage.map((row) => {
                  const providerKey = normalizeProvider(row.providerName);
                  const provider =
                    activeProviders.find((p) => p.key === providerKey) ?? PROVIDERS[providerKey];
                  const subId = row.subAccountId?.trim() || '';
                  const subName = row.subAccountName?.trim() || '';
                  const subLabel = subId || subName || t('dashboard.notAvailable');
                  const tooltipText = subName && subName !== subId ? subName : null;
                  const label = `${providerDisplayLabel(provider, t)} (${subLabel})`;
                  return (
                    <div
                      key={`${row.dataSourceId}-${row.providerName}-${row.subAccountId ?? 'na'}`}
                    >
                      <div className="mb-1 flex items-center justify-between text-sm">
                        <span className="inline-flex items-center gap-2">
                          <Tags className="h-3.5 w-3.5" />
                          {tooltipText ? (
                            <Tooltip>
                              <TooltipTrigger asChild>
                                <span className="cursor-help">{label}</span>
                              </TooltipTrigger>
                              <TooltipContent>{tooltipText}</TooltipContent>
                            </Tooltip>
                          ) : (
                            label
                          )}
                        </span>
                        <span className="text-muted-foreground">
                          {`${Math.round(row.tagCoveragePct)}%`}
                        </span>
                      </div>
                      <Progress value={row.tagCoveragePct} />
                    </div>
                  );
                })
              )}
            </div>
          </CardContent>
        </Card>
      </div>

      <footer className="text-muted-foreground border-border mt-6 flex flex-col gap-2 border-t pt-4 text-xs lg:flex-row lg:items-center lg:justify-between">
        <div>
          {t('dashboard.footer.dataSources')}{' '}
          {sources.length > 0
            ? sources
                .map(
                  (source) =>
                    `${source.name} (${providerDisplayLabel(providerForSource(source), t)})`,
                )
                .join(' | ')
            : t('dashboard.none')}
        </div>
        <div>{t('dashboard.footer.lastUpdated', { time: lastUpdated })}</div>
      </footer>
    </>
  );
}

function SectionTitle({ title }: { title: string }) {
  return <h3 className="mt-5 mb-2 text-base font-semibold">{title}</h3>;
}

function KpiCard({
  icon: Icon,
  label,
  value,
  delta,
  badge,
  tone = 'neutral',
  loading,
}: {
  icon: LucideIcon;
  label: string;
  value: string;
  delta: string;
  badge?: string;
  tone?: 'good' | 'bad' | 'neutral';
  loading?: boolean;
}) {
  const { t } = useI18n();
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
        <div className="flex items-start justify-between gap-2">
          <Icon className="text-muted-foreground h-4 w-4" />
          {badge ? (
            <Badge variant={tone === 'bad' ? 'destructive' : 'secondary'}>{badge}</Badge>
          ) : null}
        </div>
        <CardDescription className="text-[11px] tracking-wider uppercase">{label}</CardDescription>
        <CardTitle className="text-2xl font-semibold">
          {loading ? <Skeleton className="h-8 w-24" /> : value}
        </CardTitle>
      </CardHeader>
      <CardContent>
        <p className={`m-0 text-xs ${toneClass}`}>{loading ? t('common.loading') : delta}</p>
      </CardContent>
    </Card>
  );
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

function ChartTooltip({
  active,
  payload,
  label,
  formatUsd,
}: {
  active?: boolean;
  payload?: Array<{
    name?: string;
    value?: number;
    color?: string;
    payload?: { provider?: ProviderMeta };
  }>;
  label?: string;
  formatUsd: (value: number) => string;
}) {
  if (!active || !payload?.length) return null;
  return (
    <div className="bg-popover text-popover-foreground border-border rounded-md border px-3 py-2 text-xs shadow-md">
      <p className="m-0 mb-1 font-medium">{label}</p>
      {payload
        .filter((item) => Number(item.value) > 0)
        .map((item) => (
          <p key={item.name} className="m-0 flex items-center justify-between gap-5">
            <span className="inline-flex items-center gap-2">
              <span className="h-2 w-2 rounded-sm" style={{ background: item.color }} />
              {item.payload?.provider?.label ?? providerLabel(item.name ?? '')}
            </span>
            <span>{formatUsd(Number(item.value ?? 0))}</span>
          </p>
        ))}
    </div>
  );
}

function HorizontalSpendBar({
  name,
  value,
  max,
  color,
  formatUsd,
}: {
  name: string;
  value: number;
  max: number;
  color: string;
  formatUsd: (value: number) => string;
}) {
  return (
    <div className="grid grid-cols-[minmax(120px,220px)_1fr_auto] items-center gap-3 text-sm">
      <span className="text-muted-foreground truncate">{name}</span>
      <div className="bg-muted h-3 overflow-hidden rounded-sm">
        <div
          className="h-full rounded-sm"
          style={{ width: `${Math.max(2, (value / Math.max(max, 1)) * 100)}%`, background: color }}
        />
      </div>
      <span className="font-medium">{formatUsd(value)}</span>
    </div>
  );
}

function Gauge({ label, value, color }: { label: string; value: number | null; color: string }) {
  const { t } = useI18n();
  const normalized = value === null ? 0 : Math.max(0, Math.min(100, value));
  return (
    <div className="rounded-md border border-border p-4 text-center">
      <div
        className="mx-auto mb-3 h-24 w-24 rounded-full p-2"
        style={{
          background: `conic-gradient(${color} ${normalized * 3.6}deg, var(--muted) 0deg)`,
        }}
      >
        <div className="bg-card flex h-full w-full items-center justify-center rounded-full">
          <span className="text-lg font-semibold">
            {value === null ? t('dashboard.notAvailable') : `${Math.round(normalized)}%`}
          </span>
        </div>
      </div>
      <p className="text-muted-foreground m-0 text-xs">{label}</p>
    </div>
  );
}

function SeverityBadge({ severity }: { severity: Anomaly['severity'] }) {
  const { t } = useI18n();
  if (severity === 'resolved') {
    return <Badge variant="secondary">{t('dashboard.severity.resolved')}</Badge>;
  }
  if (severity === 'high') {
    return <Badge variant="destructive">{t('dashboard.severity.high')}</Badge>;
  }
  return <Badge variant="outline">{t('dashboard.severity.medium')}</Badge>;
}

function normalizeProvider(value: string): ProviderKey {
  const lower = value.toLowerCase();
  if (lower.includes('databricks')) return 'databricks';
  if (lower.includes('amazon') || lower === 'aws') return 'aws';
  if (lower.includes('azure') || lower.includes('microsoft')) return 'azure';
  if (lower.includes('google') || lower === 'gcp') return 'gcp';
  if (lower.includes('snowflake')) return 'snowflake';
  return 'other';
}

function providerForSource(source: FocusOverviewResponse['sources'][number]): ProviderMeta {
  return PROVIDERS[normalizeProvider(`${source.templateId} ${source.providerName}`)];
}

function uniqueProviders(sources: FocusOverviewResponse['sources']): ProviderMeta[] {
  const seen = new Set<ProviderKey>();
  const providers: ProviderMeta[] = [];
  for (const source of sources) {
    const provider = providerForSource(source);
    if (seen.has(provider.key)) continue;
    seen.add(provider.key);
    providers.push(provider);
  }
  return providers;
}

function providerLabel(key: string): string {
  return PROVIDERS[key as ProviderKey]?.label ?? key;
}

function providerDisplayLabel(provider: ProviderMeta, t: TFunction): string {
  return provider.key === 'other' ? t('dashboard.providers.other') : provider.label;
}

function providerForName(providerName: string, providers: ProviderMeta[]): ProviderMeta {
  const key = normalizeProvider(providerName);
  return providers.find((provider) => provider.key === key) ?? PROVIDERS[key];
}

function monthKey(date: Date): string {
  return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}`;
}

function monthLabel(key: string, locale: string): string {
  const [year, month] = key.split('-').map(Number);
  const safeYear = year ?? new Date().getFullYear();
  const safeMonth = month ?? 1;
  return new Intl.DateTimeFormat(locale === 'ja' ? 'ja-JP' : 'en-US', {
    month: 'short',
    year: '2-digit',
  }).format(new Date(safeYear, safeMonth - 1, 1));
}

function daysInMonth(date: Date): number {
  return new Date(date.getFullYear(), date.getMonth() + 1, 0).getDate();
}

function monthlyTotals(rows: FocusOverviewDailyRow[]): Map<string, number> {
  const totals = new Map<string, number>();
  for (const row of rows) {
    const key = row.usageDate.slice(0, 7);
    totals.set(key, (totals.get(key) ?? 0) + row.costUsd);
  }
  return totals;
}

interface CostBreakdownSeriesResult {
  series: CostBreakdownSeriesMeta[];
  bucketKeyOf: (row: FocusOverviewDailyRow) => string;
}

function buildCostBreakdownSeries(
  rows: FocusOverviewDailyRow[],
  keyOf: (row: FocusOverviewDailyRow) => string,
  otherLabel: string,
): CostBreakdownSeriesResult {
  const totals = new Map<string, number>();
  for (const row of rows) {
    const value = keyOf(row);
    totals.set(value, (totals.get(value) ?? 0) + row.costUsd);
  }
  const sorted = Array.from(totals.entries()).sort((a, b) => b[1] - a[1]);
  const topEntries = sorted.slice(0, COST_BREAKDOWN_SERIES_LIMIT);
  const restEntries = sorted.slice(COST_BREAKDOWN_SERIES_LIMIT);
  const topValues = new Set(topEntries.map(([v]) => v));

  const series: CostBreakdownSeriesMeta[] = topEntries.map(([value], index) => ({
    key: `series_${index}`,
    value,
    color:
      COST_BREAKDOWN_PALETTE[index % COST_BREAKDOWN_PALETTE.length] ?? COST_BREAKDOWN_OTHER_COLOR,
  }));

  const restTotal = restEntries.reduce((sum, [, cost]) => sum + cost, 0);
  if (restTotal > 0) {
    series.push({
      key: 'series_other',
      value: otherLabel,
      color: COST_BREAKDOWN_OTHER_COLOR,
    });
  }

  const bucketKeyOf = (row: FocusOverviewDailyRow): string => {
    const value = keyOf(row);
    return topValues.has(value) ? value : otherLabel;
  };

  return { series, bucketKeyOf };
}

function buildTrendData(
  rows: FocusOverviewDailyRow[],
  seriesItems: CostBreakdownSeriesMeta[],
  keyOf: (row: FocusOverviewDailyRow) => string,
  forecast: number,
  locale: string,
  t: TFunction,
) {
  const now = new Date();
  const totals = monthlyTotalsBy(rows, keyOf);
  const months = Array.from({ length: 12 }, (_, index) => {
    const date = new Date(now.getFullYear(), now.getMonth() - 11 + index, 1);
    return monthKey(date);
  });
  const data = months.map((key) => {
    const record: Record<string, string | number | boolean> = {
      label: monthLabel(key, locale),
      forecast: false,
    };
    for (const series of seriesItems) {
      record[series.key] = totals.get(`${key}:${series.value}`) ?? 0;
    }
    return record;
  });
  const forecastRecord: Record<string, string | number | boolean> = {
    label: t('dashboard.forecast'),
    forecast: true,
  };
  const currentMonthKey = monthKey(now);
  const totalMtd = seriesItems.reduce(
    (sum, item) => sum + (totals.get(`${currentMonthKey}:${item.value}`) ?? 0),
    0,
  );
  for (const series of seriesItems) {
    const currentMonthCost = totals.get(`${currentMonthKey}:${series.value}`) ?? 0;
    forecastRecord[series.key] = totalMtd > 0 ? forecast * (currentMonthCost / totalMtd) : 0;
  }
  return [...data, forecastRecord];
}

function buildCostBreakdownMtd(
  seriesItems: CostBreakdownSeriesMeta[],
  rows: FocusOverviewDailyRow[],
  keyOf: (row: FocusOverviewDailyRow) => string,
) {
  const currentMonth = monthKey(new Date());
  const totals = monthlyTotalsBy(rows, keyOf);
  const breakdownRows = seriesItems
    .map((series) => ({
      ...series,
      cost: totals.get(`${currentMonth}:${series.value}`) ?? 0,
    }))
    .filter((series) => series.cost > 0)
    .sort((a, b) => b.cost - a.cost);
  const total = breakdownRows.reduce((sum, row) => sum + row.cost, 0);
  return breakdownRows.map((row) => ({
    ...row,
    percent: total > 0 ? Math.round((row.cost / total) * 100) : 0,
  }));
}

function costBreakdownValue(row: FocusOverviewDailyRow, dimension: CostBreakdownDimension): string {
  const value = row[dimension]?.trim();
  return value || 'Unknown';
}

function monthlyTotalsBy(
  rows: FocusOverviewDailyRow[],
  keyOf: (row: FocusOverviewDailyRow) => string,
): Map<string, number> {
  const totals = new Map<string, number>();
  for (const row of rows) {
    const key = `${row.usageDate.slice(0, 7)}:${keyOf(row)}`;
    totals.set(key, (totals.get(key) ?? 0) + row.costUsd);
  }
  return totals;
}

function sumRecentDays(rows: FocusOverviewDailyRow[], days: number): number {
  const end = new Date();
  const start = new Date(end.getTime() - days * 24 * 60 * 60 * 1000);
  return rows
    .filter((row) => {
      const date = new Date(`${row.usageDate}T00:00:00`);
      return date >= start && date <= end;
    })
    .reduce((sum, row) => sum + row.costUsd, 0);
}

function comparisonText(current: number, previous: number, label: string, t: TFunction): string {
  if (previous <= 0) return t('dashboard.noBaseline', { label });
  const delta = ((current - previous) / previous) * 100;
  const direction =
    delta >= 0 ? t('dashboard.deltaDirection.up') : t('dashboard.deltaDirection.down');
  return t('dashboard.deltaComparison', {
    pct: Math.abs(delta).toFixed(1),
    direction,
    label,
  });
}

function deltaTone(current: number, previous: number): 'good' | 'bad' | 'neutral' {
  if (previous <= 0) return 'neutral';
  if (current > previous * 1.05) return 'bad';
  if (current < previous * 0.95) return 'good';
  return 'neutral';
}

function buildTopServices(
  serviceRows: FocusOverviewServiceRow[],
  skuRows: FocusOverviewSkuRow[],
  providers: ProviderMeta[],
) {
  const services = serviceRows.slice(0, 7).map((row) => ({
    name: cleanSkuName(row.serviceName),
    costUsd: row.costUsd,
    provider: providerForName(row.providerName, providers),
  }));
  if (services.length > 0) return services;
  return skuRows.slice(0, 7).map((row) => ({
    name: cleanSkuName(row.skuName),
    costUsd: row.costUsd,
    provider: providerForName(row.providerName, providers),
  }));
}

function cleanSkuName(value: string): string {
  return value
    .replace(/^ENTERPRISE_/, '')
    .replace(/_/g, ' ')
    .replace(/\b\w/g, (char) => char.toUpperCase());
}

function buildRecommendations(
  rows: FocusOverviewSkuRow[],
  providers: ProviderMeta[],
  t: TFunction,
): Recommendation[] {
  const recs: Recommendation[] = [];
  const total = rows.reduce((sum, row) => sum + row.costUsd, 0);
  const top = rows[0];
  if (top && top.costUsd > 0) {
    recs.push({
      title: t('dashboard.recommendations.reviewSpend', { name: cleanSkuName(top.skuName) }),
      provider: providerForName(top.providerName, providers),
      savingsUsd: top.costUsd * 0.12,
      reason: t('dashboard.recommendations.largestMeasuredSku'),
    });
  }
  const jobs = rows
    .filter((row) => row.skuName.toUpperCase().includes('JOB'))
    .reduce((sum, row) => sum + row.costUsd, 0);
  if (jobs > 0) {
    recs.push({
      title: t('dashboard.recommendations.rightSizeJobs'),
      provider: PROVIDERS.databricks,
      savingsUsd: jobs * 0.1,
      reason: t('dashboard.recommendations.jobsEligible'),
    });
  }
  const sql = rows
    .filter((row) => row.skuName.toUpperCase().includes('SQL'))
    .reduce((sum, row) => sum + row.costUsd, 0);
  if (sql > 0) {
    recs.push({
      title: t('dashboard.recommendations.tuneSqlWarehouse'),
      provider: PROVIDERS.databricks,
      savingsUsd: sql * 0.08,
      reason: t('dashboard.recommendations.warehousePolicy'),
    });
  }
  if (total > 0) {
    recs.push({
      title: t('dashboard.recommendations.tagUnallocated'),
      provider: PROVIDERS.databricks,
      savingsUsd: null,
      reason: t('dashboard.recommendations.improvesChargeback'),
    });
  }
  for (const provider of providers.filter((p) => p.key !== 'databricks')) {
    recs.push({
      title: t('dashboard.recommendations.completeIngestion', {
        provider: providerDisplayLabel(provider, t),
      }),
      provider,
      savingsUsd: null,
      reason: t('dashboard.recommendations.sourceEnabledNoCost'),
    });
  }
  return recs.slice(0, 5);
}

function detectAnomalies(rows: FocusOverviewDailyRow[], locale: string, t: TFunction): Anomaly[] {
  const byDay = new Map<string, number>();
  for (const row of rows) byDay.set(row.usageDate, (byDay.get(row.usageDate) ?? 0) + row.costUsd);
  const entries = Array.from(byDay.entries()).sort(([a], [b]) => a.localeCompare(b));
  if (entries.length < 8) return [];
  const baselineRows = entries.slice(
    Math.max(0, entries.length - 28),
    Math.max(0, entries.length - 7),
  );
  const recentRows = entries.slice(-7);
  const baseline =
    baselineRows.reduce((sum, [, cost]) => sum + cost, 0) / Math.max(1, baselineRows.length);
  if (baseline <= 0) return [];
  return recentRows
    .filter(([, cost]) => cost > baseline * 1.35 && cost - baseline > 10)
    .map(([date, cost]) => ({
      label: t('dashboard.dailySpendSpike'),
      impactUsd: cost - baseline,
      severity: cost > baseline * 1.75 ? 'high' : 'medium',
      when: new Intl.DateTimeFormat(locale === 'ja' ? 'ja-JP' : 'en-US', {
        month: 'short',
        day: 'numeric',
      }).format(new Date(`${date}T00:00:00`)),
    }));
}

function miniTrend(trendData: Array<Record<string, string | number | boolean>>): number[] {
  const values = trendData
    .filter((row) => row.forecast !== true)
    .map((row) => {
      let total = 0;
      for (const [key, val] of Object.entries(row)) {
        if (key !== 'label' && key !== 'forecast' && typeof val === 'number') total += val;
      }
      return total;
    })
    .slice(-12);
  const max = Math.max(...values, 1);
  return values.map((value) => (value / max) * 100);
}

function formatLastUpdated(historyUpdatedAt: number, currentUpdatedAt: number, locale: string) {
  const timestamp = Math.max(historyUpdatedAt || 0, currentUpdatedAt || 0);
  if (!timestamp) return locale === 'ja' ? '未更新' : 'never';
  return new Intl.DateTimeFormat(locale === 'ja' ? 'ja-JP' : 'en-US', {
    dateStyle: 'medium',
    timeStyle: 'short',
  }).format(new Date(timestamp));
}

const wholeUsdFormatters = new Map<string, Intl.NumberFormat>();
function formatWholeUsd(value: number, locale: string): string {
  let fmt = wholeUsdFormatters.get(locale);
  if (!fmt) {
    fmt = new Intl.NumberFormat(locale === 'ja' ? 'ja-JP' : 'en-US', {
      style: 'currency',
      currency: 'USD',
      maximumFractionDigits: 0,
      minimumFractionDigits: 0,
    });
    wholeUsdFormatters.set(locale, fmt);
  }
  return fmt.format(value);
}

function shortUsd(value: number): string {
  if (Math.abs(value) >= 1_000_000) return `$${Math.round(value / 1_000_000)}M`;
  if (Math.abs(value) >= 1_000) return `$${Math.round(value / 1_000)}K`;
  return `$${Math.round(value)}`;
}
