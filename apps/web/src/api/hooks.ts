import { useCallback, useMemo, useState } from 'react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { isActivePricingRunStatus, isTerminalSqlStatus } from '@finlake/shared';
import type {
  Budget,
  AdminCleanupResponse,
  AwsFocusExportCreateBody,
  AwsFocusExportCreateResponse,
  CatalogListResponse,
  CreateBudgetInput,
  DataSource,
  DataSourceCreateBody,
  DataSourceRunResult,
  DataSourceSetupBody,
  DataSourceSetupResult,
  DataSourceTemplate,
  DataSourceUpdateBody,
  ExternalLocationCreateBody,
  ExternalLocationCreateResponse,
  ExternalLocationListResponse,
  GenieChatRequest,
  GenieChatResponse,
  GenieSetupResponse,
  GovernedTagsResponse,
  GovernedTagSyncBody,
  GovernedTagSyncResult,
  DatabricksRunLinkResult,
  PricingNotebookRunResult,
  PricingNotebookDeleteResult,
  PricingNotebookListResponse,
  ProvisionResult,
  SetupCheckResult,
  SetupStateResponse,
  SetupStepId,
  ServiceCredentialCreateBody,
  ServiceCredentialCreateResponse,
  ServiceCredentialListResponse,
  SqlStatementColumn,
  SqlStatementResultResponse,
  SqlStatementSubmitRequest,
  StorageCredentialCreateBody,
  StorageCredentialCreateResponse,
  StorageCredentialListResponse,
  TransformationPipelinesResponse,
  TransformationSharedRunResult,
  UsageBySkuRow,
  UsageDailyResponse,
  UsageTopWorkloadRow,
  UpdateBudgetInput,
} from '@finlake/shared';
import { apiFetch } from './client';
import { getSqlStatement, submitSqlStatement } from './sql';

interface RangeParams {
  start: string;
  end: string;
  workspaceId?: string;
}

function rangeQuery(range: RangeParams): string {
  const sp = new URLSearchParams({ start: range.start, end: range.end });
  if (range.workspaceId) sp.set('workspaceId', range.workspaceId);
  return sp.toString();
}

export function useUsageDaily(range: RangeParams, enabled = true) {
  return useQuery({
    queryKey: ['usage', 'daily', range],
    queryFn: () => apiFetch<UsageDailyResponse>(`/api/usage/daily?${rangeQuery(range)}`),
    enabled,
  });
}

export function useUsageBySku(range: RangeParams, enabled = true) {
  return useQuery({
    queryKey: ['usage', 'bySku', range],
    queryFn: () => apiFetch<{ rows: UsageBySkuRow[] }>(`/api/usage/by-sku?${rangeQuery(range)}`),
    enabled,
  });
}

export function useUsageTopWorkloads(range: RangeParams, enabled = true) {
  return useQuery({
    queryKey: ['usage', 'top', range],
    queryFn: () =>
      apiFetch<{ rows: UsageTopWorkloadRow[] }>(`/api/usage/top-workloads?${rangeQuery(range)}`),
    enabled,
  });
}

export interface UseSqlStatementOptions {
  enabled?: boolean;
  /** Minimum poll interval (ms). Backoff doubles up to 5s. */
  initialPollIntervalMs?: number;
  requestKey?: unknown;
  /** How long results stay fresh in cache before being eligible for refetch. */
  staleTimeMs?: number;
}

const POLL_INITIAL_DELAY_MS = 600;
const POLL_MAX_DELAY_MS = 5_000;
const EMPTY_ROWS: ReadonlyArray<Record<string, unknown>> = [];

export function useSqlStatement<T = Record<string, unknown>>(
  input: SqlStatementSubmitRequest | null | undefined,
  options: UseSqlStatementOptions = {},
) {
  const {
    enabled = true,
    initialPollIntervalMs = POLL_INITIAL_DELAY_MS,
    requestKey,
    staleTimeMs = 60_000,
  } = options;
  const [refreshIndex, setRefreshIndex] = useState(0);
  const statementKey = requestKey ?? input ?? null;
  const canSubmit = enabled && input !== null && input !== undefined;

  const submitQuery = useQuery({
    queryKey: ['sql', 'submit', statementKey, refreshIndex],
    queryFn: () => submitSqlStatement(input!),
    enabled: canSubmit,
    retry: false,
    staleTime: staleTimeMs,
  });

  const statement_id = submitQuery.data?.statement_id;
  const submitResult = submitQuery.data?.result;
  const resultQuery = useQuery({
    queryKey: ['sql', 'result', statement_id],
    queryFn: () => getSqlStatement(statement_id!),
    enabled: canSubmit && Boolean(statement_id) && !submitResult,
    retry: false,
    staleTime: staleTimeMs,
    refetchInterval: (query) => {
      const data = query.state.data as SqlStatementResultResponse | undefined;
      if (isTerminalSqlStatus(data?.status)) return false;
      const fetchCount = query.state.dataUpdateCount + query.state.errorUpdateCount;
      return Math.min(initialPollIntervalMs * 2 ** fetchCount, POLL_MAX_DELAY_MS);
    },
    refetchIntervalInBackground: false,
  });
  const resultData = submitResult ?? resultQuery.data?.result;

  const rows = useMemo(() => (resultData?.rows ?? EMPTY_ROWS) as T[], [resultData?.rows]);

  const refetch = useCallback(() => {
    setRefreshIndex((index) => index + 1);
  }, []);

  const resultError =
    resultQuery.data?.error !== undefined ? new Error(resultQuery.data.error) : resultQuery.error;
  const error = submitQuery.error ?? resultError ?? null;
  const status = resultQuery.data?.status ?? submitQuery.data?.status ?? null;
  const isPolling =
    Boolean(statement_id) &&
    Boolean(status) &&
    !isTerminalSqlStatus(status) &&
    !resultQuery.isError;

  return {
    statement_id,
    status,
    rows,
    columns: (resultData?.columns ?? []) as SqlStatementColumn[],
    error,
    isSubmitting: submitQuery.isLoading,
    isPolling,
    isLoading: submitQuery.isLoading || resultQuery.isLoading || isPolling,
    isError: submitQuery.isError || resultQuery.isError || Boolean(resultQuery.data?.error),
    isSuccess: submitQuery.data?.result !== undefined || resultQuery.data?.status === 'SUCCEEDED',
    dataUpdatedAt:
      submitQuery.data?.result !== undefined
        ? submitQuery.dataUpdatedAt
        : resultQuery.dataUpdatedAt,
    refetch,
  };
}

export function useBudgets(workspaceId?: string) {
  return useQuery({
    queryKey: ['budgets', workspaceId ?? null],
    queryFn: () =>
      apiFetch<{ items: Budget[] }>(
        workspaceId
          ? `/api/budgets?workspaceId=${encodeURIComponent(workspaceId)}`
          : '/api/budgets',
      ),
  });
}

export function useCreateBudget() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (input: CreateBudgetInput) =>
      apiFetch<Budget>('/api/budgets', { method: 'POST', body: JSON.stringify(input) }),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['budgets'] }),
  });
}

export function useUpdateBudget() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ id, input }: { id: string; input: UpdateBudgetInput }) =>
      apiFetch<Budget>(`/api/budgets/${encodeURIComponent(id)}`, {
        method: 'PUT',
        body: JSON.stringify(input),
      }),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['budgets'] }),
  });
}

export function useDeleteBudget() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (id: string) =>
      apiFetch<void>(`/api/budgets/${encodeURIComponent(id)}`, { method: 'DELETE' }),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['budgets'] }),
  });
}

export function useSetupState(workspaceId?: string) {
  return useQuery({
    queryKey: ['setup', 'state', workspaceId ?? null],
    queryFn: () =>
      apiFetch<SetupStateResponse>(
        workspaceId
          ? `/api/setup/state?workspaceId=${encodeURIComponent(workspaceId)}`
          : '/api/setup/state',
      ),
  });
}

export interface MeResponse {
  email: string | null;
  userId: string | null;
  userName: string | null;
  workspaceUrl: string | null;
  workspaceId: string | null;
  appName: string | null;
}

export function useMe() {
  return useQuery({
    queryKey: ['me'],
    queryFn: () => apiFetch<MeResponse>('/api/me'),
    staleTime: 5 * 60 * 1000,
  });
}

export interface AppSettingsResponse {
  settings: Record<string, string>;
}

export interface AppSettingsUpdateResponse extends AppSettingsResponse {
  provision?: ProvisionResult;
  pipelineSynced?: boolean;
}

export interface UpdateAppSettingsArgs {
  settings: Record<string, string>;
  provision?: { createIfMissing?: boolean };
}

export function useAppSettings() {
  return useQuery({
    queryKey: ['appSettings'],
    queryFn: () => apiFetch<AppSettingsResponse>('/api/app-settings'),
    staleTime: 60 * 1000,
  });
}

export function useUpdateAppSettings() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (args: UpdateAppSettingsArgs) =>
      apiFetch<AppSettingsUpdateResponse>('/api/app-settings', {
        method: 'PUT',
        body: JSON.stringify(args),
      }),
    onSuccess: (data) => {
      qc.setQueryData(['appSettings'], { settings: data.settings });
      if (data.provision?.catalogCreated) {
        qc.invalidateQueries({ queryKey: ['catalogs'] });
      }
      if (data.pipelineSynced) {
        qc.invalidateQueries({ queryKey: ['transformations'] });
        qc.invalidateQueries({ queryKey: ['overview'] });
      }
    },
  });
}

export function useAdminCleanup() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (body: { deleteCatalog?: boolean } = {}) =>
      apiFetch<AdminCleanupResponse>('/api/admin/cleanup', {
        method: 'POST',
        body: JSON.stringify(body),
      }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['appSettings'] });
      qc.invalidateQueries({ queryKey: ['dataSources'] });
      qc.invalidateQueries({ queryKey: ['setup'] });
      qc.invalidateQueries({ queryKey: ['overview'] });
      qc.invalidateQueries({ queryKey: ['transformations'] });
      qc.invalidateQueries({ queryKey: ['catalogs'] });
    },
  });
}

export function useSetupGenieSpace() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: () =>
      apiFetch<GenieSetupResponse>('/api/genie/setup', {
        method: 'POST',
        body: JSON.stringify({}),
      }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['appSettings'] });
    },
  });
}

export function useDeleteGenieSpace() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: () =>
      apiFetch<void>('/api/genie/space', {
        method: 'DELETE',
      }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['appSettings'] });
    },
  });
}

export function useAskGenie() {
  return useMutation({
    mutationFn: (body: GenieChatRequest) =>
      apiFetch<GenieChatResponse>('/api/genie/chat', {
        method: 'POST',
        body: JSON.stringify(body),
      }),
  });
}

export function useCatalogs() {
  return useQuery({
    queryKey: ['catalogs'],
    queryFn: () => apiFetch<CatalogListResponse>('/api/catalogs'),
    staleTime: 60 * 1000,
    retry: false,
  });
}

export function useExternalLocations() {
  return useQuery({
    queryKey: ['externalLocations'],
    queryFn: () => apiFetch<ExternalLocationListResponse>('/api/unity-catalog/external-locations'),
    staleTime: 60 * 1000,
    retry: false,
  });
}

export function useCreateExternalLocation() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (body: ExternalLocationCreateBody) =>
      apiFetch<ExternalLocationCreateResponse>('/api/unity-catalog/external-locations', {
        method: 'POST',
        body: JSON.stringify(body),
      }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['externalLocations'] });
    },
  });
}

export function useDeleteExternalLocation() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (name: string) =>
      apiFetch<void>(`/api/unity-catalog/external-locations/${encodeURIComponent(name)}`, {
        method: 'DELETE',
      }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['externalLocations'] });
    },
  });
}

export function useStorageCredentials() {
  return useQuery({
    queryKey: ['storageCredentials'],
    queryFn: () => apiFetch<StorageCredentialListResponse>('/api/storage-credentials'),
    staleTime: 60 * 1000,
    retry: false,
  });
}

export function useServiceCredentials() {
  return useQuery({
    queryKey: ['serviceCredentials'],
    queryFn: () => apiFetch<ServiceCredentialListResponse>('/api/unity-catalog/credentials'),
    staleTime: 60 * 1000,
    retry: false,
  });
}

export function useCreateServiceCredential() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (body: ServiceCredentialCreateBody) =>
      apiFetch<ServiceCredentialCreateResponse>('/api/unity-catalog/credentials', {
        method: 'POST',
        body: JSON.stringify(body),
      }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['serviceCredentials'] });
    },
  });
}

export function useCreateStorageCredential() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (body: StorageCredentialCreateBody) =>
      apiFetch<StorageCredentialCreateResponse>('/api/unity-catalog/credentials', {
        method: 'POST',
        body: JSON.stringify(body),
      }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['serviceCredentials'] });
      qc.invalidateQueries({ queryKey: ['storageCredentials'] });
    },
  });
}

export function useCreateAwsFocusExport() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (body: AwsFocusExportCreateBody) =>
      apiFetch<AwsFocusExportCreateResponse>('/api/unity-catalog/credentials/aws-focus-export', {
        method: 'POST',
        body: JSON.stringify(body),
      }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['serviceCredentials'] });
      qc.invalidateQueries({ queryKey: ['storageCredentials'] });
      qc.invalidateQueries({ queryKey: ['externalLocations'] });
    },
  });
}

export function useDeleteCredential() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (name: string) =>
      apiFetch<void>(`/api/unity-catalog/credentials/${encodeURIComponent(name)}`, {
        method: 'DELETE',
      }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['serviceCredentials'] });
      qc.invalidateQueries({ queryKey: ['storageCredentials'] });
    },
  });
}

export function useDataSources() {
  return useQuery({
    queryKey: ['dataSources'],
    queryFn: () => apiFetch<{ items: DataSource[] }>('/api/integration/configurations'),
    staleTime: 60 * 1000,
  });
}

export function useDataSourceTemplates() {
  return useQuery({
    queryKey: ['dataSourceTemplates'],
    queryFn: () => apiFetch<{ items: DataSourceTemplate[] }>('/api/integration/templates'),
    staleTime: 5 * 60 * 1000,
  });
}

export function useGovernedTags() {
  return useQuery({
    queryKey: ['governedTags'],
    queryFn: () => apiFetch<GovernedTagsResponse>('/api/tags'),
    staleTime: 60 * 1000,
    retry: false,
  });
}

export function useSyncGovernedTags() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (body: GovernedTagSyncBody) =>
      apiFetch<GovernedTagSyncResult>('/api/tags/sync', {
        method: 'POST',
        body: JSON.stringify(body),
      }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['governedTags'] });
    },
  });
}

function dsConfigPath(id: number, suffix = '') {
  return `/api/integration/configurations/${id}${suffix}`;
}

export function useDataSource(id: number | undefined) {
  return useQuery({
    queryKey: ['dataSources', id],
    enabled: typeof id === 'number',
    queryFn: () => apiFetch<DataSource>(dsConfigPath(id!)),
    staleTime: 60 * 1000,
  });
}

export function useCreateDataSource() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (body: DataSourceCreateBody) =>
      apiFetch<DataSource>('/api/integration/configurations', {
        method: 'POST',
        body: JSON.stringify(body),
      }),
    onSuccess: (data) => {
      qc.setQueryData(['dataSources', data.id], data);
      qc.invalidateQueries({ queryKey: ['dataSources'] });
    },
  });
}

export function useUpdateDataSource() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ id, body }: { id: number; body: DataSourceUpdateBody }) =>
      apiFetch<DataSource>(dsConfigPath(id), {
        method: 'PATCH',
        body: JSON.stringify(body),
      }),
    onSuccess: (data) => {
      qc.setQueryData(['dataSources', data.id], data);
      qc.invalidateQueries({ queryKey: ['dataSources'] });
    },
  });
}

export function useDeleteDataSource() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (id: number) =>
      apiFetch<void>(dsConfigPath(id), {
        method: 'DELETE',
      }),
    onSuccess: (_data, id) => {
      qc.removeQueries({ queryKey: ['dataSources', id] });
      qc.invalidateQueries({ queryKey: ['dataSources'] });
      qc.invalidateQueries({ queryKey: ['appSettings'] });
      qc.invalidateQueries({ queryKey: ['transformations'] });
    },
  });
}

export function useSetupDataSource() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ id, body }: { id: number; body: DataSourceSetupBody }) =>
      apiFetch<DataSourceSetupResult>(dsConfigPath(id, '/setup'), {
        method: 'POST',
        body: JSON.stringify(body),
      }),
    onSuccess: (data) => {
      qc.invalidateQueries({ queryKey: ['dataSources', data.dataSourceId] });
      qc.invalidateQueries({ queryKey: ['dataSources'] });
      qc.invalidateQueries({ queryKey: ['appSettings'] });
      qc.invalidateQueries({ queryKey: ['transformations'] });
    },
  });
}

export function useRunDataSourceJob() {
  return useMutation({
    mutationFn: (id: number) =>
      apiFetch<DataSourceRunResult>(dsConfigPath(id, '/run'), {
        method: 'POST',
        body: JSON.stringify({}),
      }),
  });
}

export function useTransformationPipelines() {
  return useQuery({
    queryKey: ['transformations', 'pipelines'],
    queryFn: () => apiFetch<TransformationPipelinesResponse>('/api/transformations/pipelines'),
    staleTime: 60 * 1000,
    retry: false,
  });
}

export function useRunSharedTransformationJob() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: () =>
      apiFetch<TransformationSharedRunResult>('/api/transformations/shared-run', {
        method: 'POST',
        body: JSON.stringify({}),
      }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['transformations'] });
    },
  });
}

export function usePricingNotebook() {
  return useQuery({
    queryKey: ['pricing'],
    queryFn: () => apiFetch<PricingNotebookListResponse>('/api/pricing'),
    staleTime: 60 * 1000,
    retry: false,
    refetchInterval: (query) => {
      const data = query.state.data as PricingNotebookListResponse | undefined;
      return data?.items.some((item) => isActivePricingRunStatus(item.runStatus)) ? 5_000 : false;
    },
    refetchIntervalInBackground: false,
  });
}

export function useRunNotebook() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (id: string) =>
      apiFetch<PricingNotebookRunResult>(`/api/pricing/${encodeURIComponent(id)}`, {
        method: 'PUT',
      }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['pricing'] });
    },
  });
}

export function useDeletePricingNotebook() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (id: string) =>
      apiFetch<PricingNotebookDeleteResult>(`/api/pricing/${encodeURIComponent(id)}`, {
        method: 'DELETE',
      }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['pricing'] });
    },
  });
}

export function useGetJobRunLink() {
  return useMutation({
    mutationFn: (runId: number) =>
      apiFetch<DatabricksRunLinkResult>(
        `/api/jobs/runs/get?run_id=${encodeURIComponent(String(runId))}`,
      ),
  });
}

export function useRunSetupCheck() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ step, body }: { step: SetupStepId; body?: Record<string, unknown> }) =>
      apiFetch<SetupCheckResult>(`/api/setup/check/${step}`, {
        method: 'POST',
        body: JSON.stringify(body ?? {}),
      }),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['setup'] }),
  });
}
