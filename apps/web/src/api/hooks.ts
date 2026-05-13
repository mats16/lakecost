import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
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
  ProvisionResult,
  SetupCheckResult,
  SetupStateResponse,
  SetupStepId,
  ServiceCredentialCreateBody,
  ServiceCredentialCreateResponse,
  ServiceCredentialListResponse,
  StorageCredentialCreateBody,
  StorageCredentialCreateResponse,
  StorageCredentialListResponse,
  TransformationPipelinesResponse,
  TransformationSharedRunResult,
  UsageBySkuRow,
  UsageDailyResponse,
  UsageTopWorkloadRow,
} from '@finlake/shared';
import { apiFetch } from './client';

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

export interface FocusOverviewSource {
  id: number;
  templateId: string;
  name: string;
  providerName: string;
  tableName: string;
  focusVersion: string | null;
  updatedAt: string;
}

export interface FocusOverviewDailyRow {
  dataSourceId: number;
  usageDate: string;
  providerName: string;
  serviceCategory: string;
  serviceName: string;
  costUsd: number;
}

export interface FocusOverviewServiceRow {
  dataSourceId: number;
  providerName: string;
  serviceName: string;
  costUsd: number;
}

export interface FocusOverviewSkuRow {
  dataSourceId: number;
  providerName: string;
  skuName: string;
  costUsd: number;
}

export interface FocusOverviewCoverageRow {
  dataSourceId: number;
  providerName: string;
  subAccountId: string | null;
  subAccountName: string | null;
  rowCount: number;
  taggedRows: number;
  tagCoveragePct: number;
  lastChargeAt: string | null;
}

export interface FocusOverviewResponse {
  sources: FocusOverviewSource[];
  daily: FocusOverviewDailyRow[];
  services: FocusOverviewServiceRow[];
  skus: FocusOverviewSkuRow[];
  coverage: FocusOverviewCoverageRow[];
  errors: Array<{ dataSourceId: number; name: string; tableName: string; message: string }>;
  generatedAt: string;
}

export function useFocusOverview(range: RangeParams) {
  return useQuery({
    queryKey: ['overview', 'focus', range],
    queryFn: () => apiFetch<FocusOverviewResponse>(`/api/overview/focus?${rangeQuery(range)}`),
  });
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
    queryFn: () => apiFetch<{ items: DataSource[] }>('/api/data-sources/configurations'),
    staleTime: 60 * 1000,
  });
}

export function useDataSourceTemplates() {
  return useQuery({
    queryKey: ['dataSourceTemplates'],
    queryFn: () => apiFetch<{ items: DataSourceTemplate[] }>('/api/data-sources/templates'),
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
  return `/api/data-sources/configurations/${id}${suffix}`;
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
      apiFetch<DataSource>('/api/data-sources/configurations', {
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
