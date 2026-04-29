import type { Budget, CreateBudgetInput, SetupCheckResult } from '@lakecost/shared';

export interface BudgetsRepo {
  list(workspaceId: string | null): Promise<Budget[]>;
  create(input: CreateBudgetInput, createdBy: string): Promise<Budget>;
  delete(id: string): Promise<void>;
}

export interface UserPreferencesValue {
  userId: string;
  currency: string;
  defaultWorkspaceId: string | null;
  theme: string;
  prefs: Record<string, unknown>;
  updatedAt: string;
}

export interface UserPreferencesRepo {
  get(userId: string): Promise<UserPreferencesValue | null>;
  upsert(value: UserPreferencesValue): Promise<UserPreferencesValue>;
}

export interface CachedAggregationValue {
  cacheKey: string;
  queryHash: string;
  payload: unknown;
  computedAt: string;
  expiresAt: string;
}

export interface CachedAggregationsRepo {
  get(cacheKey: string): Promise<CachedAggregationValue | null>;
  set(value: CachedAggregationValue): Promise<void>;
  prune(now: string): Promise<number>;
}

export interface SetupStateValue {
  workspaceId: string;
  systemTablesOk: boolean;
  permissionsOk: boolean;
  curConfigured: boolean;
  azureExportConfigured: boolean;
  lastCheckedAt: string;
  details: Record<string, unknown>;
}

export interface SetupStateRepo {
  get(workspaceId: string): Promise<SetupStateValue | null>;
  upsert(value: SetupStateValue): Promise<SetupStateValue>;
  recordCheck(workspaceId: string, result: SetupCheckResult): Promise<void>;
}

export interface DataSourceValue {
  id: number;
  templateId: string;
  name: string;
  description: string | null;
  providerName: string;
  billingAccountId: string | null;
  tableName: string;
  jobId: number | null;
  pipelineId: string | null;
  focusVersion: string | null;
  enabled: boolean;
  config: Record<string, unknown>;
  updatedAt: string;
}

export type DataSourceCreateInput = Omit<
  DataSourceValue,
  'id' | 'updatedAt' | 'jobId' | 'pipelineId' | 'focusVersion'
> & {
  jobId?: number | null;
  pipelineId?: string | null;
  focusVersion?: string | null;
};

export type DataSourceUpdatePatch = Partial<
  Omit<DataSourceValue, 'id' | 'templateId' | 'updatedAt'>
>;

export interface DataSourcesRepo {
  list(): Promise<DataSourceValue[]>;
  get(id: number): Promise<DataSourceValue | null>;
  create(input: DataSourceCreateInput): Promise<DataSourceValue>;
  update(id: number, patch: DataSourceUpdatePatch): Promise<DataSourceValue>;
  delete(id: number): Promise<void>;
}

export interface AppSettingValue {
  key: string;
  value: string;
  updatedAt: string;
}

export interface AppSettingsRepo {
  get(key: string): Promise<AppSettingValue | null>;
  list(): Promise<AppSettingValue[]>;
  upsert(key: string, value: string): Promise<AppSettingValue>;
  delete(key: string): Promise<void>;
}

export interface Repositories {
  budgets: BudgetsRepo;
  userPreferences: UserPreferencesRepo;
  cachedAggregations: CachedAggregationsRepo;
  setupState: SetupStateRepo;
  appSettings: AppSettingsRepo;
  dataSources: DataSourcesRepo;
}
