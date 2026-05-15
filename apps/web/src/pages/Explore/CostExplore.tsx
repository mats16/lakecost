import { useMemo, useState } from 'react';
import {
  Bar,
  BarChart,
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip as RechartsTooltip,
  XAxis,
  YAxis,
} from 'recharts';
import {
  Alert,
  AlertDescription,
  Button,
  Card,
  CardContent,
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
  Empty,
  EmptyDescription,
  EmptyHeader,
  EmptyTitle,
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
  cn,
} from '@databricks/appkit-ui/react';
import {
  AlertCircle,
  BarChart3,
  CalendarDays,
  ChartLine,
  DollarSign,
  Filter,
  Grid2X2,
  Percent,
  Plus,
  Settings2,
  X,
} from 'lucide-react';
import {
  COST_EXPLORE_COST_METRICS,
  COST_EXPLORE_FILTER_KEYS,
  COST_EXPLORE_GROUP_KEYS,
  buildCostExploreFilterValuesStatement,
  buildCostExploreStatement,
  enabledFocusSources,
  type CostExploreCostMetric,
  type CostExploreDateGrain,
  type CostExploreFilterKey,
  type CostExploreFilters,
  type CostExploreGroupKey,
} from '@finlake/shared';
import { PageHeader } from '../../components/PageHeader';
import { useAppSettings, useDataSources, useSqlStatement } from '../../api/hooks';
import { useCurrencyUsd, useI18n, type TFunction } from '../../i18n';
import { stableTomorrow } from '../../lib/dateRanges';

type Aggregation = 'cumulative' | CostExploreDateGrain;
type DatePreset = 'thisMonth' | 'lastMonth' | 'last30' | 'last90' | 'ytd';
type TableView = 'cumulative' | 'byDate';
type ChangeDisplay = 'percent' | 'currency';
type ChartType = 'line' | 'stackedBar';

interface CostExploreRow {
  periodStart: string;
  groupPath: string;
  costUsd: number;
  [key: `group_${number}`]: string | undefined;
  [key: `group_${number}Label`]: string | undefined;
}

interface FilterValueRow {
  provider: string;
  billingAccount: string;
  billingAccountLabel: string;
  subAccount: string;
  subAccountLabel: string;
  costUsd: number;
}

interface DateRange {
  start: string;
  end: string;
}

interface PresetRanges {
  current: DateRange;
  previous: DateRange;
}

interface FilterOption {
  value: string;
  label: string;
  costUsd: number;
}

interface SeriesMeta {
  key: string;
  name: string;
  color: string;
}

interface SummaryRow {
  id: string;
  groupPath: string;
  groupValues: string[];
  groupLabels: string[];
  currentCost: number;
  previousCost: number;
  changePct: number | null;
}

interface DailyTableRow extends SummaryRow {
  byPeriod: Record<string, number>;
}

const DATE_PRESETS: DatePreset[] = ['thisMonth', 'lastMonth', 'last30', 'last90', 'ytd'];
const AGGREGATIONS: Aggregation[] = ['cumulative', 'daily', 'weekly', 'monthly', 'quarterly'];
const SERIES_LIMIT = 12;
const MAX_TABLE_ROWS = 100;
const OTHER_SERIES = '__other__';
const OTHER_COLOR = '#6B7280';
const PALETTE = [
  '#7C3AED',
  '#2563EB',
  '#059669',
  '#D97706',
  '#DC2626',
  '#0891B2',
  '#4F46E5',
  '#DB2777',
  '#65A30D',
  '#9333EA',
  '#0D9488',
  '#EA580C',
];

export function CostExplore() {
  const { t, locale } = useI18n();
  const formatUsd = useCurrencyUsd();
  const [datePreset, setDatePreset] = useState<DatePreset>('thisMonth');
  const [aggregation, setAggregation] = useState<Aggregation>('cumulative');
  const [tableView, setTableView] = useState<TableView>('cumulative');
  const [changeDisplay, setChangeDisplay] = useState<ChangeDisplay>('percent');
  const [chartType, setChartType] = useState<ChartType>('stackedBar');
  const [groupBy, setGroupBy] = useState<CostExploreGroupKey[]>([]);
  const [filters, setFilters] = useState<CostExploreFilters>({});
  const [costMetric, setCostMetric] = useState<CostExploreCostMetric>('EffectiveCost');

  const dataSources = useDataSources();
  const appSettings = useAppSettings();
  const sources = useMemo(
    () => enabledFocusSources(dataSources.data?.items ?? []),
    [dataSources.data?.items],
  );
  const settings = useMemo(() => appSettings.data?.settings ?? {}, [appSettings.data?.settings]);
  const presetRanges = useMemo(() => rangeForPreset(datePreset), [datePreset]);
  const range = presetRanges.current;
  const previous = presetRanges.previous;
  const sqlGrain: CostExploreDateGrain = aggregation === 'cumulative' ? 'daily' : aggregation;
  const sqlEnabled = dataSources.isSuccess && appSettings.isSuccess;

  const currentStatement = useMemo(
    () =>
      buildCostExploreStatement({
        sources,
        settings,
        range,
        groupBy,
        filters,
        costMetric,
        dateGrain: sqlGrain,
      }),
    [costMetric, filters, groupBy, range, settings, sources, sqlGrain],
  );
  const previousStatement = useMemo(
    () =>
      buildCostExploreStatement({
        sources,
        settings,
        range: previous,
        groupBy,
        filters,
        costMetric,
        dateGrain: sqlGrain,
      }),
    [costMetric, filters, groupBy, previous, settings, sources, sqlGrain],
  );
  const currentDailyStatement = useMemo(
    () =>
      buildCostExploreStatement({
        sources,
        settings,
        range,
        groupBy,
        filters,
        costMetric,
        dateGrain: 'daily',
      }),
    [costMetric, filters, groupBy, range, settings, sources],
  );
  const filterValuesStatement = useMemo(
    () => buildCostExploreFilterValuesStatement(sources, settings, range),
    [range, settings, sources],
  );

  const current = useSqlStatement<CostExploreRow>(currentStatement, {
    enabled: sqlEnabled && currentStatement !== null,
    requestKey: ['costExplore', 'current', range, groupBy, filters, costMetric, sqlGrain, sources],
  });
  const previousData = useSqlStatement<CostExploreRow>(previousStatement, {
    enabled: sqlEnabled && previousStatement !== null,
    requestKey: [
      'costExplore',
      'previous',
      previous,
      groupBy,
      filters,
      costMetric,
      sqlGrain,
      sources,
    ],
  });
  const filterValues = useSqlStatement<FilterValueRow>(filterValuesStatement, {
    enabled: sqlEnabled && filterValuesStatement !== null,
    requestKey: ['costExplore', 'filterValues', range, sources],
  });
  const currentDaily = useSqlStatement<CostExploreRow>(currentDailyStatement, {
    enabled:
      sqlEnabled &&
      tableView === 'byDate' &&
      sqlGrain !== 'daily' &&
      currentDailyStatement !== null,
    requestKey: ['costExplore', 'currentDaily', range, groupBy, filters, costMetric, sources],
  });

  const filterOptions = useMemo(() => buildFilterOptions(filterValues.rows), [filterValues.rows]);
  const chart = useMemo(
    () => buildChartData(current.rows, aggregation, t),
    [aggregation, current.rows, t],
  );
  const summaryRows = useMemo(
    () => buildSummaryRows(current.rows, previousData.rows, groupBy),
    [current.rows, groupBy, previousData.rows],
  );
  const dailyRows = sqlGrain === 'daily' ? current.rows : currentDaily.rows;
  const dailyTable = useMemo(
    () => buildDailyTableRows(dailyRows, groupBy, range, locale),
    [dailyRows, groupBy, locale, range],
  );
  const totalCost = summaryRows.reduce((sum, row) => sum + row.currentCost, 0);
  const previousCost = summaryRows.reduce((sum, row) => sum + row.previousCost, 0);
  const changePct = previousCost > 0 ? ((totalCost - previousCost) / previousCost) * 100 : null;
  const loading =
    dataSources.isLoading ||
    appSettings.isLoading ||
    current.isLoading ||
    previousData.isLoading ||
    filterValues.isLoading ||
    (tableView === 'byDate' && sqlGrain !== 'daily' && currentDaily.isLoading);
  const error =
    current.error ?? previousData.error ?? filterValues.error ?? currentDaily.error ?? null;

  return (
    <>
      <div className="flex flex-col gap-4">
        <PageHeader title={t('explore.costExplore.title')} />
      </div>

      <div className="mb-4 flex flex-wrap items-center justify-between gap-2">
        <div className="flex flex-wrap items-center gap-2">
          <FiltersMenu
            filters={filters}
            options={filterOptions}
            onToggle={toggleFilterValue(setFilters)}
            onClear={() => setFilters({})}
          />
          <GroupByMenu groupBy={groupBy} onChange={setGroupBy} />
          <CostSettingsMenu value={costMetric} onChange={setCostMetric} />
        </div>
        <div className="ml-auto flex flex-wrap items-center justify-end gap-2">
          <Select value={datePreset} onValueChange={(value) => setDatePreset(value as DatePreset)}>
            <SelectTrigger className="!h-8 !min-h-8 w-[145px] py-0 text-sm">
              <CalendarDays className="size-3.5" />
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {DATE_PRESETS.map((preset) => (
                <SelectItem key={preset} value={preset}>
                  {t(`explore.costExplore.datePresets.${preset}`)}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
          <Select
            value={aggregation}
            onValueChange={(value) => setAggregation(value as Aggregation)}
          >
            <SelectTrigger className="!h-8 !min-h-8 w-[130px] py-0 text-sm">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {AGGREGATIONS.map((option) => (
                <SelectItem key={option} value={option}>
                  {t(`explore.costExplore.aggregation.${option}`)}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
      </div>

      {sources.length === 0 && dataSources.isSuccess ? (
        <Alert className="mb-4">
          <AlertCircle />
          <AlertDescription>{t('dashboard.noEnabledSources')}</AlertDescription>
        </Alert>
      ) : null}

      {error ? (
        <Alert variant="destructive" className="mb-4">
          <AlertCircle />
          <AlertDescription>
            {t('explore.costExplore.loadFailed')}{' '}
            {error instanceof Error ? error.message : String(error)}
          </AlertDescription>
        </Alert>
      ) : null}

      <div className="mb-6 flex flex-wrap items-start justify-between gap-x-10 gap-y-4">
        <div className="flex flex-wrap items-start gap-x-10 gap-y-4">
          <MetricTile
            label={t('explore.costExplore.metrics.accruedCosts')}
            value={formatUsd(totalCost)}
            badge={formatDelta(changePct)}
            loading={loading}
          />
        </div>
        <ChartTypeToggle value={chartType} onChange={setChartType} />
      </div>

      <Card className="mb-4">
        <CardContent>
          {loading ? (
            <Skeleton className="h-[360px] w-full" />
          ) : current.rows.length === 0 ? (
            <EmptyState
              title={t('explore.costExplore.empty.noData')}
              description={t('explore.costExplore.empty.adjustFilters')}
            />
          ) : (
            <div className="h-[360px]">
              <ResponsiveContainer>
                {chartType === 'line' ? (
                  <LineChart data={chart.data} margin={{ top: 16, right: 20, bottom: 8, left: 0 }}>
                    <CartesianGrid stroke="var(--border)" vertical={false} />
                    <XAxis dataKey="label" stroke="var(--muted-foreground)" fontSize={11} />
                    <YAxis
                      stroke="var(--muted-foreground)"
                      fontSize={11}
                      tickFormatter={shortUsd}
                    />
                    <RechartsTooltip content={<ChartTooltip formatUsd={formatUsd} />} />
                    {chart.series.map((series) => (
                      <Line
                        key={series.key}
                        type="monotone"
                        dataKey={series.key}
                        name={series.name}
                        stroke={series.color}
                        strokeWidth={2}
                        dot={false}
                        activeDot={{ r: 4 }}
                      />
                    ))}
                  </LineChart>
                ) : (
                  <BarChart data={chart.data} margin={{ top: 16, right: 20, bottom: 8, left: 0 }}>
                    <CartesianGrid stroke="var(--border)" vertical={false} />
                    <XAxis dataKey="label" stroke="var(--muted-foreground)" fontSize={11} />
                    <YAxis
                      stroke="var(--muted-foreground)"
                      fontSize={11}
                      tickFormatter={shortUsd}
                    />
                    <RechartsTooltip content={<ChartTooltip formatUsd={formatUsd} />} />
                    {chart.series.map((series) => (
                      <Bar
                        key={series.key}
                        dataKey={series.key}
                        name={series.name}
                        stackId="cost"
                        fill={series.color}
                        maxBarSize={48}
                      />
                    ))}
                  </BarChart>
                )}
              </ResponsiveContainer>
            </div>
          )}
        </CardContent>
      </Card>

      <div className="mb-3 flex flex-wrap justify-end gap-3">
        <ChangeDisplayToggle value={changeDisplay} onChange={setChangeDisplay} />
        <TableViewToggle value={tableView} onChange={setTableView} />
      </div>

      <Card>
        <CardContent className="p-0">
          {loading ? (
            <div className="p-6">
              <Skeleton className="h-72 w-full" />
            </div>
          ) : summaryRows.length === 0 ? (
            <div className="p-6">
              <EmptyState
                title={t('explore.costExplore.empty.noRows')}
                description={t('explore.costExplore.empty.adjustFilters')}
              />
            </div>
          ) : (
            <>
              {tableView === 'byDate' ? (
                <DailyCostTable
                  groupBy={groupBy}
                  periods={dailyTable.periods}
                  rows={dailyTable.rows}
                  totals={dailyTable.totals}
                  range={range}
                  locale={locale}
                  formatUsd={formatUsd}
                  onFilter={toggleFilterValue(setFilters)}
                />
              ) : (
                <SummaryCostTable
                  groupBy={groupBy}
                  rows={summaryRows}
                  range={range}
                  previousRange={previous}
                  locale={locale}
                  formatUsd={formatUsd}
                  changeDisplay={changeDisplay}
                  onFilter={toggleFilterValue(setFilters)}
                />
              )}
            </>
          )}
        </CardContent>
      </Card>
    </>
  );
}

function ChangeDisplayToggle({
  value,
  onChange,
}: {
  value: ChangeDisplay;
  onChange: (value: ChangeDisplay) => void;
}) {
  const { t } = useI18n();
  return (
    <div className="bg-muted inline-flex rounded-md p-1">
      <Button
        type="button"
        size="sm"
        variant={value === 'percent' ? 'secondary' : 'ghost'}
        className={cn(
          'h-8 w-9 px-0',
          value === 'percent' ? 'bg-background shadow-sm' : 'text-muted-foreground',
        )}
        aria-label={t('explore.costExplore.changeDisplay.percent')}
        onClick={() => onChange('percent')}
      >
        <Percent className="size-4" />
      </Button>
      <Button
        type="button"
        size="sm"
        variant={value === 'currency' ? 'secondary' : 'ghost'}
        className={cn(
          'h-8 w-9 px-0',
          value === 'currency' ? 'bg-background shadow-sm' : 'text-muted-foreground',
        )}
        aria-label={t('explore.costExplore.changeDisplay.currency')}
        onClick={() => onChange('currency')}
      >
        <DollarSign className="size-4" />
      </Button>
    </div>
  );
}

function ChartTypeToggle({
  value,
  onChange,
}: {
  value: ChartType;
  onChange: (value: ChartType) => void;
}) {
  const { t } = useI18n();
  return (
    <div className="bg-muted inline-flex rounded-full p-1">
      <Button
        type="button"
        size="sm"
        variant={value === 'stackedBar' ? 'secondary' : 'ghost'}
        className={cn(
          'h-8 w-8 rounded-full px-0',
          value === 'stackedBar' ? 'bg-background shadow-sm' : 'text-muted-foreground',
        )}
        aria-label={t('explore.costExplore.chartType.stackedBar')}
        onClick={() => onChange('stackedBar')}
      >
        <BarChart3 className="size-4" />
      </Button>
      <Button
        type="button"
        size="sm"
        variant={value === 'line' ? 'secondary' : 'ghost'}
        className={cn(
          'h-8 w-8 rounded-full px-0',
          value === 'line' ? 'bg-background shadow-sm' : 'text-muted-foreground',
        )}
        aria-label={t('explore.costExplore.chartType.line')}
        onClick={() => onChange('line')}
      >
        <ChartLine className="size-4" />
      </Button>
    </div>
  );
}

function TableViewToggle({
  value,
  onChange,
}: {
  value: TableView;
  onChange: (value: TableView) => void;
}) {
  const { t } = useI18n();
  return (
    <div className="bg-muted inline-flex rounded-md p-1">
      {(['cumulative', 'byDate'] as const).map((option) => (
        <Button
          key={option}
          type="button"
          size="sm"
          variant={value === option ? 'secondary' : 'ghost'}
          className={cn(
            'h-8 px-3 text-sm',
            value === option ? 'bg-background shadow-sm' : 'text-muted-foreground',
          )}
          onClick={() => onChange(option)}
        >
          {t(`explore.costExplore.tableView.${option}`)}
        </Button>
      ))}
    </div>
  );
}

function SummaryCostTable({
  groupBy,
  rows,
  range,
  previousRange,
  locale,
  formatUsd,
  changeDisplay,
  onFilter,
}: {
  groupBy: CostExploreGroupKey[];
  rows: SummaryRow[];
  range: DateRange;
  previousRange: DateRange;
  locale: string;
  formatUsd: (value: number) => string;
  changeDisplay: ChangeDisplay;
  onFilter: (key: CostExploreFilterKey, mode: 'include' | 'exclude', value: string) => void;
}) {
  const { t } = useI18n();
  const visibleRows = rows.slice(0, MAX_TABLE_ROWS);
  return (
    <>
      <div className="overflow-x-auto">
        <Table>
          <TableHeader>
            <TableRow>
              {groupBy.length === 0 ? (
                <TableHead>{t('explore.costExplore.groupColumns.ungrouped')}</TableHead>
              ) : (
                groupBy.map((key) => <TableHead key={key}>{groupLabel(key, t)}</TableHead>)
              )}
              <TableHead className="text-right">
                <ColumnWithPeriod
                  label={t('explore.costExplore.columns.currentCost')}
                  period={rangeLabel(range, locale)}
                />
              </TableHead>
              <TableHead className="text-right">
                <ColumnWithPeriod
                  label={t('explore.costExplore.columns.previousCost')}
                  period={rangeLabel(previousRange, locale)}
                />
              </TableHead>
              <TableHead className="text-right">
                {t('explore.costExplore.columns.change')}
              </TableHead>
              <TableHead className="text-right">
                {t('explore.costExplore.columns.actions')}
              </TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {visibleRows.map((row) => (
              <TableRow key={row.id}>
                {groupBy.length === 0 ? (
                  <TableCell className="font-medium">{row.groupPath}</TableCell>
                ) : (
                  groupBy.map((key, index) => (
                    <TableCell key={`${row.id}:${key}`} className="min-w-40">
                      <span className="block truncate" title={row.groupLabels[index]}>
                        {row.groupLabels[index] ?? row.groupValues[index] ?? ''}
                      </span>
                    </TableCell>
                  ))
                )}
                <TableCell className="text-right font-medium">
                  {formatUsd(row.currentCost)}
                </TableCell>
                <TableCell className="text-right">{formatUsd(row.previousCost)}</TableCell>
                <TableCell
                  className={cn(
                    'text-right',
                    row.changePct !== null && row.changePct > 0
                      ? 'text-destructive'
                      : 'text-muted-foreground',
                  )}
                >
                  {changeDisplay === 'percent'
                    ? formatDelta(row.changePct)
                    : formatCurrencyDelta(row.currentCost - row.previousCost, formatUsd)}
                </TableCell>
                <TableCell className="text-right">
                  <RowActions row={row} groupBy={groupBy} onFilter={onFilter} />
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </div>
      <TruncationNotice total={rows.length} />
    </>
  );
}

function DailyCostTable({
  groupBy,
  periods,
  rows,
  totals,
  range,
  locale,
  formatUsd,
  onFilter,
}: {
  groupBy: CostExploreGroupKey[];
  periods: Array<{ value: string; label: string }>;
  rows: DailyTableRow[];
  totals: Record<string, number>;
  range: DateRange;
  locale: string;
  formatUsd: (value: number) => string;
  onFilter: (key: CostExploreFilterKey, mode: 'include' | 'exclude', value: string) => void;
}) {
  const { t } = useI18n();
  const totalCost = rows.reduce((sum, row) => sum + row.currentCost, 0);
  const visibleRows = rows.slice(0, MAX_TABLE_ROWS);
  return (
    <>
      <div className="overflow-x-auto">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead className="sticky left-0 z-20 w-56 min-w-56 max-w-56 bg-background">
                {groupBy.length === 0
                  ? t('explore.costExplore.groupColumns.ungrouped')
                  : groupBy.map((key) => groupLabel(key, t)).join(' / ')}
              </TableHead>
              <TableHead className="sticky left-56 z-20 w-48 min-w-48 max-w-48 border-r bg-background text-right">
                <ColumnWithPeriod
                  label={t('explore.costExplore.columns.currentCost')}
                  period={rangeLabel(range, locale)}
                />
              </TableHead>
              {periods.map((period) => (
                <TableHead key={period.value} className="min-w-32 text-right">
                  {period.label}
                </TableHead>
              ))}
              <TableHead className="min-w-20 text-right">
                {t('explore.costExplore.columns.actions')}
              </TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            <TableRow className="bg-muted/40">
              <TableCell className="sticky left-0 z-10 w-56 min-w-56 max-w-56 bg-muted font-semibold">
                {t('explore.costExplore.rows.totalCosts')}
              </TableCell>
              <TableCell className="sticky left-56 z-10 w-48 min-w-48 max-w-48 border-r bg-muted text-right font-semibold">
                {formatUsd(totalCost)}
              </TableCell>
              {periods.map((period) => (
                <TableCell key={period.value} className="text-right font-semibold">
                  {formatUsd(totals[period.value] ?? 0)}
                </TableCell>
              ))}
              <TableCell />
            </TableRow>
            {visibleRows.map((row) => (
              <TableRow key={row.id}>
                <TableCell className="sticky left-0 z-10 w-56 min-w-56 max-w-56 bg-background font-medium">
                  {groupBy.length === 0 ? (
                    row.groupPath
                  ) : (
                    <span className="block truncate" title={row.groupLabels.join(' / ')}>
                      {row.groupLabels.join(' / ')}
                    </span>
                  )}
                </TableCell>
                <TableCell className="sticky left-56 z-10 w-48 min-w-48 max-w-48 border-r bg-background text-right font-medium">
                  {formatUsd(row.currentCost)}
                </TableCell>
                {periods.map((period) => (
                  <TableCell key={period.value} className="text-right">
                    {formatUsd(row.byPeriod[period.value] ?? 0)}
                  </TableCell>
                ))}
                <TableCell className="text-right">
                  <RowActions row={row} groupBy={groupBy} onFilter={onFilter} />
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </div>
      <TruncationNotice total={rows.length} />
    </>
  );
}

function TruncationNotice({ total }: { total: number }) {
  const { t } = useI18n();
  if (total <= MAX_TABLE_ROWS) return null;
  return (
    <div className="border-t bg-muted/30 px-4 py-2 text-xs text-muted-foreground">
      {t('explore.costExplore.rows.truncated', { limit: MAX_TABLE_ROWS, total })}
    </div>
  );
}

function ColumnWithPeriod({ label, period }: { label: string; period: string }) {
  return (
    <span className="inline-flex flex-col items-end gap-0.5">
      <span>{label}</span>
      <span className="text-xs font-normal text-muted-foreground">{period}</span>
    </span>
  );
}

function FiltersMenu({
  filters,
  options,
  onToggle,
  onClear,
}: {
  filters: CostExploreFilters;
  options: Record<CostExploreFilterKey, FilterOption[]>;
  onToggle: (key: CostExploreFilterKey, mode: 'include' | 'exclude', value: string) => void;
  onClear: () => void;
}) {
  const { t } = useI18n();
  const activeCount = countActiveFilters(filters);

  return (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <Button variant="outline" size="sm" className="h-8 gap-1.5 px-3 text-sm">
          <Filter className="size-3.5" />
          {t('explore.costExplore.filters.title')}
          {activeCount > 0 ? <span className="text-primary font-medium">{activeCount}</span> : null}
        </Button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="start" className="max-h-[520px] w-[420px] overflow-y-auto p-3">
        <div className="mb-2 flex items-center justify-between gap-2">
          <span className="text-sm font-semibold">{t('explore.costExplore.filters.title')}</span>
          <Button type="button" variant="ghost" size="sm" onClick={onClear}>
            <X className="size-4" /> {t('explore.costExplore.filters.clear')}
          </Button>
        </div>
        {COST_EXPLORE_FILTER_KEYS.map((key) => (
          <div key={key} className="border-border border-t py-3 first:border-t-0">
            <div className="mb-2 text-xs font-semibold text-muted-foreground">
              {filterLabel(key, t)}
            </div>
            <div className="grid gap-2">
              {options[key].slice(0, 12).map((option) => (
                <div
                  key={`${key}:${option.value}`}
                  className="grid grid-cols-[minmax(0,1fr)_auto_auto] items-center gap-2 text-sm"
                >
                  <span className="truncate" title={option.label}>
                    {option.label}
                  </span>
                  <label className="flex items-center gap-1 text-xs">
                    <input
                      type="checkbox"
                      checked={filters[key]?.include?.includes(option.value) ?? false}
                      onChange={() => onToggle(key, 'include', option.value)}
                    />
                    {t('explore.costExplore.filters.include')}
                  </label>
                  <label className="flex items-center gap-1 text-xs">
                    <input
                      type="checkbox"
                      checked={filters[key]?.exclude?.includes(option.value) ?? false}
                      onChange={() => onToggle(key, 'exclude', option.value)}
                    />
                    {t('explore.costExplore.filters.exclude')}
                  </label>
                </div>
              ))}
              {options[key].length === 0 ? (
                <span className="text-muted-foreground text-xs">
                  {t('explore.costExplore.filters.noValues')}
                </span>
              ) : null}
            </div>
          </div>
        ))}
      </DropdownMenuContent>
    </DropdownMenu>
  );
}

function GroupByMenu({
  groupBy,
  onChange,
}: {
  groupBy: CostExploreGroupKey[];
  onChange: (next: CostExploreGroupKey[]) => void;
}) {
  const { t } = useI18n();
  const label =
    groupBy.length === 0
      ? t('explore.costExplore.ungrouped')
      : groupBy.map((key) => groupLabel(key, t)).join(' / ');

  const toggle = (key: CostExploreGroupKey) => {
    if (groupBy.includes(key)) onChange(groupBy.filter((value) => value !== key));
    else onChange([...groupBy, key]);
  };

  return (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <Button variant="outline" size="sm" className="h-8 max-w-[320px] gap-1.5 px-3 text-sm">
          <Grid2X2 className="size-3.5" />
          {t('explore.costExplore.groupBy')}
          <span className="text-primary truncate font-medium">{label}</span>
        </Button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="start" className="w-[320px] p-2">
        <DropdownMenuItem onClick={() => onChange([])}>
          <input type="checkbox" readOnly checked={groupBy.length === 0} />
          {t('explore.costExplore.ungrouped')}
        </DropdownMenuItem>
        <DropdownMenuSeparator />
        {COST_EXPLORE_GROUP_KEYS.map((key) => (
          <DropdownMenuItem key={key} onClick={() => toggle(key)}>
            <input type="checkbox" readOnly checked={groupBy.includes(key)} />
            {groupLabel(key, t)}
          </DropdownMenuItem>
        ))}
      </DropdownMenuContent>
    </DropdownMenu>
  );
}

function CostSettingsMenu({
  value,
  onChange,
}: {
  value: CostExploreCostMetric;
  onChange: (value: CostExploreCostMetric) => void;
}) {
  const { t } = useI18n();
  return (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <Button variant="outline" size="sm" className="h-8 gap-1.5 px-3 text-sm">
          <Settings2 className="size-3.5" />
          {t('explore.costExplore.costSettings')}
        </Button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="start" className="w-[260px] p-2">
        {COST_EXPLORE_COST_METRICS.map((metric) => (
          <DropdownMenuItem key={metric} onClick={() => onChange(metric)}>
            <input type="checkbox" readOnly checked={value === metric} />
            {t(`explore.costExplore.costMetrics.${metric}`)}
          </DropdownMenuItem>
        ))}
      </DropdownMenuContent>
    </DropdownMenu>
  );
}

function RowActions({
  row,
  groupBy,
  onFilter,
}: {
  row: SummaryRow;
  groupBy: CostExploreGroupKey[];
  onFilter: (key: CostExploreFilterKey, mode: 'include' | 'exclude', value: string) => void;
}) {
  const { t } = useI18n();
  const filterable = groupBy
    .map((key, index) => ({ key, value: row.groupValues[index], label: row.groupLabels[index] }))
    .filter(
      (item): item is { key: CostExploreFilterKey; value: string; label: string } =>
        isFilterKey(item.key) && Boolean(item.value),
    );

  if (filterable.length === 0) {
    return <span className="text-muted-foreground text-xs">{t('dashboard.emDash')}</span>;
  }

  return (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <Button
          type="button"
          variant="ghost"
          size="sm"
          aria-label={t('explore.costExplore.columns.actions')}
        >
          <Plus className="size-4" />
        </Button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end" className="w-[260px]">
        {filterable.flatMap((item) => [
          <DropdownMenuItem
            key={`${item.key}:${item.value}:include`}
            onClick={() => onFilter(item.key, 'include', item.value)}
          >
            {t('explore.costExplore.rowActions.include', { value: item.label })}
          </DropdownMenuItem>,
          <DropdownMenuItem
            key={`${item.key}:${item.value}:exclude`}
            onClick={() => onFilter(item.key, 'exclude', item.value)}
          >
            {t('explore.costExplore.rowActions.exclude', { value: item.label })}
          </DropdownMenuItem>,
        ])}
      </DropdownMenuContent>
    </DropdownMenu>
  );
}

function MetricTile({
  label,
  value,
  badge,
  detail,
  loading,
}: {
  label: string;
  value: string;
  badge?: string;
  detail?: string;
  loading: boolean;
}) {
  return (
    <div className="min-w-[13rem] py-1">
      {loading ? (
        <Skeleton className="h-16 w-56" />
      ) : (
        <>
          <div className="flex items-center gap-2">
            <span className="text-2xl leading-none font-semibold">{value}</span>
            {badge ? (
              <span className="bg-muted text-foreground rounded-md px-2 py-1 text-sm leading-none font-semibold">
                {badge}
              </span>
            ) : null}
          </div>
          <div className="text-muted-foreground mt-2 text-sm leading-tight">{label}</div>
          {detail ? <div className="text-muted-foreground mt-1 text-xs">{detail}</div> : null}
        </>
      )}
    </div>
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

function buildChartData(rows: CostExploreRow[], aggregation: Aggregation, t: TFunction) {
  const totals = new Map<string, number>();
  for (const row of rows) {
    totals.set(row.groupPath, (totals.get(row.groupPath) ?? 0) + row.costUsd);
  }
  const topGroups = Array.from(totals.entries())
    .sort((a, b) => b[1] - a[1])
    .slice(0, SERIES_LIMIT)
    .map(([name]) => name);
  const topSet = new Set(topGroups);
  const hasOther = rows.some((row) => !topSet.has(row.groupPath));
  const series: SeriesMeta[] = topGroups.map((name, index) => ({
    key: `series_${index}`,
    name,
    color: PALETTE[index % PALETTE.length] ?? OTHER_COLOR,
  }));
  if (hasOther) {
    series.push({ key: OTHER_SERIES, name: t('dashboard.costBreakdownOther'), color: OTHER_COLOR });
  }
  const seriesKeyByName = new Map(series.map((item) => [item.name, item.key]));
  const periods = Array.from(new Set(rows.map((row) => row.periodStart))).sort();
  const bucket = new Map<string, Record<string, string | number>>();

  for (const period of periods) {
    bucket.set(period, { period, label: period });
  }
  for (const row of rows) {
    const name = topSet.has(row.groupPath) ? row.groupPath : t('dashboard.costBreakdownOther');
    const key = seriesKeyByName.get(name);
    const record = bucket.get(row.periodStart);
    if (!key || !record) continue;
    record[key] = Number(record[key] ?? 0) + row.costUsd;
  }

  const data = periods.map((period) => bucket.get(period) ?? { period, label: period });
  if (aggregation === 'cumulative') {
    const running = new Map<string, number>();
    for (const record of data) {
      for (const item of series) {
        const next = (running.get(item.key) ?? 0) + Number(record[item.key] ?? 0);
        running.set(item.key, next);
        record[item.key] = next;
      }
    }
  }
  return { data, series };
}

function buildSummaryRows(
  currentRows: CostExploreRow[],
  previousRows: CostExploreRow[],
  groupBy: CostExploreGroupKey[],
): SummaryRow[] {
  const current = summarizeRows(currentRows, groupBy);
  const previous = summarizeRows(previousRows, groupBy);
  return Array.from(current.values())
    .map((row) => {
      const previousCost = previous.get(row.id)?.currentCost ?? 0;
      return {
        ...row,
        previousCost,
        changePct:
          previousCost > 0 ? ((row.currentCost - previousCost) / previousCost) * 100 : null,
      };
    })
    .sort((a, b) => b.currentCost - a.currentCost);
}

function buildDailyTableRows(
  rows: CostExploreRow[],
  groupBy: CostExploreGroupKey[],
  range: DateRange,
  locale: string,
): {
  periods: Array<{ value: string; label: string }>;
  rows: DailyTableRow[];
  totals: Record<string, number>;
} {
  const periods = dailyPeriods(range, locale);
  const periodSet = new Set(periods.map((period) => period.value));
  const totals: Record<string, number> = {};
  const grouped = new Map<string, DailyTableRow>();

  for (const row of rows) {
    const period = normalizeDateKey(row.periodStart);
    if (!periodSet.has(period)) continue;

    const groupValues = groupBy.map((_, index) => row[`group_${index}`] ?? '');
    const groupLabels = groupBy.map(
      (_, index) => row[`group_${index}Label`] ?? groupValues[index] ?? '',
    );
    const id = groupBy.length === 0 ? '__ungrouped__' : groupValues.join('\u001f');
    const existing = grouped.get(id);
    totals[period] = (totals[period] ?? 0) + row.costUsd;

    if (existing) {
      existing.currentCost += row.costUsd;
      existing.byPeriod[period] = (existing.byPeriod[period] ?? 0) + row.costUsd;
    } else {
      grouped.set(id, {
        id,
        groupPath: row.groupPath,
        groupValues,
        groupLabels,
        currentCost: row.costUsd,
        previousCost: 0,
        changePct: null,
        byPeriod: { [period]: row.costUsd },
      });
    }
  }

  return {
    periods,
    rows: Array.from(grouped.values()).sort((a, b) => b.currentCost - a.currentCost),
    totals,
  };
}

function summarizeRows(
  rows: CostExploreRow[],
  groupBy: CostExploreGroupKey[],
): Map<string, SummaryRow> {
  const summary = new Map<string, SummaryRow>();
  for (const row of rows) {
    const groupValues = groupBy.map((_, index) => row[`group_${index}`] ?? '');
    const groupLabels = groupBy.map(
      (_, index) => row[`group_${index}Label`] ?? groupValues[index] ?? '',
    );
    const id = groupBy.length === 0 ? '__ungrouped__' : groupValues.join('\u001f');
    const existing = summary.get(id);
    if (existing) {
      existing.currentCost += row.costUsd;
    } else {
      summary.set(id, {
        id,
        groupPath: row.groupPath,
        groupValues,
        groupLabels,
        currentCost: row.costUsd,
        previousCost: 0,
        changePct: null,
      });
    }
  }
  return summary;
}

function buildFilterOptions(rows: FilterValueRow[]): Record<CostExploreFilterKey, FilterOption[]> {
  return {
    provider: uniqueOptions(
      rows.map((row) => ({ value: row.provider, label: row.provider, costUsd: row.costUsd })),
    ),
    billingAccount: uniqueOptions(
      rows.map((row) => ({
        value: row.billingAccount,
        label: row.billingAccountLabel,
        costUsd: row.costUsd,
      })),
    ),
    subAccount: uniqueOptions(
      rows.map((row) => ({
        value: row.subAccount,
        label: row.subAccountLabel,
        costUsd: row.costUsd,
      })),
    ),
  };
}

function uniqueOptions(items: FilterOption[]): FilterOption[] {
  const byValue = new Map<string, FilterOption>();
  for (const item of items) {
    const value = item.value?.trim();
    if (!value) continue;
    const existing = byValue.get(value);
    if (existing) {
      existing.costUsd += item.costUsd;
      if (existing.label === value && item.label !== value) existing.label = item.label;
    } else {
      byValue.set(value, { ...item, value });
    }
  }
  return Array.from(byValue.values()).sort(
    (a, b) => b.costUsd - a.costUsd || a.label.localeCompare(b.label),
  );
}

function toggleFilterValue(setFilters: React.Dispatch<React.SetStateAction<CostExploreFilters>>) {
  return (key: CostExploreFilterKey, mode: 'include' | 'exclude', value: string) => {
    setFilters((current) => {
      const existing = current[key] ?? {};
      const values = new Set(existing[mode] ?? []);
      if (values.has(value)) values.delete(value);
      else values.add(value);
      const nextSelection = { ...existing, [mode]: Array.from(values) };
      if (nextSelection.include?.length === 0) delete nextSelection.include;
      if (nextSelection.exclude?.length === 0) delete nextSelection.exclude;
      return { ...current, [key]: nextSelection };
    });
  };
}

function countActiveFilters(filters: CostExploreFilters): number {
  return COST_EXPLORE_FILTER_KEYS.reduce(
    (sum, key) => sum + (filters[key]?.include?.length ?? 0) + (filters[key]?.exclude?.length ?? 0),
    0,
  );
}

function rangeForPreset(preset: DatePreset): PresetRanges {
  const tomorrow = stableTomorrow();
  const today = addDays(tomorrow, -1);

  switch (preset) {
    case 'thisMonth': {
      const currentStart = new Date(today.getFullYear(), today.getMonth(), 1);
      const currentEnd = new Date(today.getFullYear(), today.getMonth() + 1, 1);
      const previousStart = new Date(today.getFullYear(), today.getMonth() - 1, 1);
      const previousEnd = currentStart;
      return {
        current: dateRange(currentStart, currentEnd),
        previous: dateRange(previousStart, previousEnd),
      };
    }
    case 'lastMonth': {
      const currentStart = new Date(today.getFullYear(), today.getMonth() - 1, 1);
      const currentEnd = new Date(today.getFullYear(), today.getMonth(), 1);
      const previousStart = new Date(today.getFullYear(), today.getMonth() - 2, 1);
      const previousEnd = currentStart;
      return {
        current: dateRange(currentStart, currentEnd),
        previous: dateRange(previousStart, previousEnd),
      };
    }
    case 'last30':
      return rollingRange(tomorrow, 30);
    case 'last90':
      return rollingRange(tomorrow, 90);
    case 'ytd': {
      const currentStart = new Date(today.getFullYear(), 0, 1);
      const currentEnd = tomorrow;
      const previousStart = new Date(today.getFullYear() - 1, 0, 1);
      const previousDisplayEnd = sameMonthDayInYear(today, today.getFullYear() - 1);
      const previousEnd = addDays(previousDisplayEnd, 1);
      return {
        current: dateRange(currentStart, currentEnd),
        previous: dateRange(previousStart, previousEnd),
      };
    }
  }
}

function rollingRange(end: Date, days: number): PresetRanges {
  const currentStart = addDays(end, -days);
  const currentEnd = end;
  const previousStart = addDays(currentStart, -days);
  const previousEnd = currentStart;
  return {
    current: dateRange(currentStart, currentEnd),
    previous: dateRange(previousStart, previousEnd),
  };
}

function dateRange(start: Date, end: Date): DateRange {
  return { start: start.toISOString(), end: end.toISOString() };
}

function addDays(date: Date, days: number): Date {
  const next = new Date(date);
  next.setDate(next.getDate() + days);
  return next;
}

function sameMonthDayInYear(date: Date, year: number): Date {
  const next = new Date(year, date.getMonth(), date.getDate());
  if (next.getMonth() !== date.getMonth()) {
    return new Date(year, date.getMonth() + 1, 0);
  }
  return next;
}

function dailyPeriods(range: DateRange, locale: string): Array<{ value: string; label: string }> {
  const periods: Array<{ value: string; label: string }> = [];
  const formatter = new Intl.DateTimeFormat(locale === 'ja' ? 'ja-JP' : 'en-US', {
    month: 'short',
    day: '2-digit',
    year: 'numeric',
  });
  const cursor = startOfLocalDay(new Date(range.start));
  const end = startOfLocalDay(new Date(range.end));

  while (cursor < end) {
    periods.push({
      value: localDateKey(cursor),
      label: formatter.format(cursor),
    });
    cursor.setDate(cursor.getDate() + 1);
  }

  return periods;
}

function startOfLocalDay(date: Date): Date {
  return new Date(date.getFullYear(), date.getMonth(), date.getDate());
}

function normalizeDateKey(value: string): string {
  return value.slice(0, 10);
}

function localDateKey(date: Date): string {
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, '0');
  const day = String(date.getDate()).padStart(2, '0');
  return `${year}-${month}-${day}`;
}

function rangeLabel(range: DateRange, locale: string): string {
  const fmt = new Intl.DateTimeFormat(locale === 'ja' ? 'ja-JP' : 'en-US', {
    month: 'short',
    day: 'numeric',
    year: 'numeric',
  });
  const displayEnd = new Date(range.end);
  displayEnd.setDate(displayEnd.getDate() - 1);
  return `${fmt.format(new Date(range.start))} - ${fmt.format(displayEnd)}`;
}

function groupLabel(key: CostExploreGroupKey, t: TFunction): string {
  return t(`explore.costExplore.groupColumns.${key}`);
}

function filterLabel(key: CostExploreFilterKey, t: TFunction): string {
  return t(`explore.costExplore.filters.${key}`);
}

function isFilterKey(key: CostExploreGroupKey): key is CostExploreFilterKey {
  return (COST_EXPLORE_FILTER_KEYS as readonly string[]).includes(key);
}

function formatDelta(value: number | null): string {
  if (value === null || !Number.isFinite(value)) return 'N/A';
  const sign = value > 0 ? '+' : '';
  return `${sign}${value.toFixed(2)}%`;
}

function formatCurrencyDelta(value: number, formatUsd: (value: number) => string): string {
  if (!Number.isFinite(value)) return 'N/A';
  return value > 0 ? `+${formatUsd(value)}` : formatUsd(value);
}

function shortUsd(value: number): string {
  if (Math.abs(value) >= 1_000_000) return `$${(value / 1_000_000).toFixed(1)}M`;
  if (Math.abs(value) >= 1_000) return `$${(value / 1_000).toFixed(1)}K`;
  return `$${value.toFixed(0)}`;
}

function ChartTooltip({
  active,
  payload,
  label,
  formatUsd,
}: {
  active?: boolean;
  payload?: Array<{ name?: string; value?: number; color?: string }>;
  label?: string;
  formatUsd: (value: number) => string;
}) {
  if (!active || !payload?.length) return null;
  return (
    <div className="border-border bg-popover text-popover-foreground rounded-md border p-3 shadow-md">
      <div className="mb-2 text-xs font-semibold">{label}</div>
      <div className="grid gap-1">
        {payload
          .filter((item) => typeof item.value === 'number')
          .sort((a, b) => Number(b.value ?? 0) - Number(a.value ?? 0))
          .slice(0, 8)
          .map((item) => (
            <div
              key={item.name}
              className="grid grid-cols-[0.625rem_minmax(0,1fr)_auto] items-center gap-2 text-xs"
            >
              <span className="size-2 rounded-sm" style={{ background: item.color }} />
              <span className="max-w-56 truncate">{item.name}</span>
              <span className="font-medium">{formatUsd(Number(item.value ?? 0))}</span>
            </div>
          ))}
      </div>
    </div>
  );
}
