import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import type {
  Budget,
  CreateBudgetInput,
  SetupCheckResult,
  SetupStateResponse,
  SetupStepId,
  UsageBySkuRow,
  UsageDailyResponse,
  UsageTopWorkloadRow,
} from '@lakecost/shared';
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

export function useUsageDaily(range: RangeParams) {
  return useQuery({
    queryKey: ['usage', 'daily', range],
    queryFn: () => apiFetch<UsageDailyResponse>(`/api/usage/daily?${rangeQuery(range)}`),
  });
}

export function useUsageBySku(range: RangeParams) {
  return useQuery({
    queryKey: ['usage', 'bySku', range],
    queryFn: () => apiFetch<{ rows: UsageBySkuRow[] }>(`/api/usage/by-sku?${rangeQuery(range)}`),
  });
}

export function useUsageTopWorkloads(range: RangeParams) {
  return useQuery({
    queryKey: ['usage', 'top', range],
    queryFn: () =>
      apiFetch<{ rows: UsageTopWorkloadRow[] }>(`/api/usage/top-workloads?${rangeQuery(range)}`),
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

export function useAppSettings() {
  return useQuery({
    queryKey: ['appSettings'],
    queryFn: () => apiFetch<AppSettingsResponse>('/api/settings/app'),
    staleTime: 60 * 1000,
  });
}

export function useUpdateAppSettings() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (settings: Record<string, string>) =>
      apiFetch<AppSettingsResponse>('/api/settings/app', {
        method: 'PUT',
        body: JSON.stringify({ settings }),
      }),
    onSuccess: (data) => {
      qc.setQueryData(['appSettings'], data);
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
