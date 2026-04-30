import {
  DATA_SOURCE_TEMPLATES,
  tableLeafName,
  type DataSourceTemplate,
  type SetupStepId,
} from '@lakecost/shared';

export { DATA_SOURCE_TEMPLATES, type DataSourceTemplate };

export type TemplateLogo = { kind: 'databricks' } | { kind: 'abbr'; label: string };

export interface DataSourceTemplateMatchRule {
  providerName: string;
  defaultTableName: string;
}

export interface DataSourceTemplateInputConfig extends DataSourceTemplateMatchRule {
  setupSteps: SetupStepId[];
}

export interface DataSourceTemplateRegistryEntry {
  input?: DataSourceTemplateInputConfig;
  matches?: DataSourceTemplateMatchRule[];
  logo: TemplateLogo;
}

const DATABRICKS_FOCUS13_INPUT: DataSourceTemplateInputConfig = {
  providerName: 'Databricks',
  defaultTableName: 'databricks_billing',
  setupSteps: ['systemTables', 'permissions'],
};

export const DATA_SOURCE_TEMPLATE_REGISTRY: Record<string, DataSourceTemplateRegistryEntry> = {
  databricks_focus13: {
    input: DATABRICKS_FOCUS13_INPUT,
    matches: [DATABRICKS_FOCUS13_INPUT],
    logo: { kind: 'databricks' },
  },
  aws: {
    input: {
      providerName: 'AWS',
      defaultTableName: 'aws_billing',
      setupSteps: ['awsCur'],
    },
    // 'Amazon Web Services' is kept for legacy DB rows created before providerName was standardized to 'AWS'.
    matches: [
      { providerName: 'Amazon Web Services', defaultTableName: 'aws_billing' },
      { providerName: 'AWS', defaultTableName: 'aws_billing' },
    ],
    logo: { kind: 'abbr', label: 'AWS' },
  },
  gcp: {
    matches: [
      { providerName: 'Google Cloud', defaultTableName: 'google_cloud_billing' },
      { providerName: 'GCP', defaultTableName: 'gcp_billing' },
    ],
    logo: { kind: 'abbr', label: 'GCP' },
  },
  snowflake: {
    matches: [{ providerName: 'Snowflake', defaultTableName: 'snowflake_credits' }],
    logo: { kind: 'abbr', label: 'SF' },
  },
  custom: {
    logo: { kind: 'abbr', label: 'src' },
  },
};

export function getTemplateRegistryEntry(
  template: DataSourceTemplate,
): DataSourceTemplateRegistryEntry | undefined {
  return DATA_SOURCE_TEMPLATE_REGISTRY[template.id];
}

export function getTemplateInputConfig(
  template: DataSourceTemplate,
): DataSourceTemplateInputConfig | undefined {
  return getTemplateRegistryEntry(template)?.input;
}

export function canCreateTemplate(template: DataSourceTemplate): boolean {
  return template.available && Boolean(getTemplateInputConfig(template));
}

export function findTemplateById(id: string): DataSourceTemplate | undefined {
  return DATA_SOURCE_TEMPLATES.find((t) => t.id === id);
}

/** Matches a DB row to its template using frontend-only input metadata. */
export function findTemplateForRow(row: {
  templateId?: string | null;
  providerName: string;
  tableName: string;
}): DataSourceTemplate | undefined {
  if (row.templateId) {
    const byId = findTemplateById(row.templateId);
    if (byId) return byId;
  }
  const leaf = tableLeafName(row.tableName);
  return DATA_SOURCE_TEMPLATES.find((template) =>
    (DATA_SOURCE_TEMPLATE_REGISTRY[template.id]?.matches ?? []).some(
      (match) => match.providerName === row.providerName && match.defaultTableName === leaf,
    ),
  );
}

/** Legacy names that should be treated as the template's canonical name. */
export const LEGACY_TEMPLATE_NAMES: Record<string, string[]> = {
  databricks_focus13: ['Databricks System Tables'],
};

/** Legacy descriptions that should be treated as the template's canonical description. */
export const LEGACY_TEMPLATE_DESCRIPTIONS: Record<string, string[]> = {
  databricks_focus13: [
    'DBU consumption from system.billing.usage and system.billing.list_prices',
    'System tables transformed to FOCUS format',
    'Databricks usage and list prices normalized to FOCUS 1.3',
    'system.billing.usage および system.billing.list_prices からの DBU 消費量',
    'Databricks の利用量とリスト価格を FOCUS 1.3 形式に正規化',
  ],
  aws: [
    'A data export tool that enables you to create customized exports from multiple AWS cost management and billing datasets.',
    'Ingest AWS Cost & Usage Reports from a Unity Catalog external location.',
    'Unity Catalog の外部ロケーションから AWS Cost & Usage Report を取り込みます。',
  ],
};

export function displayNameForRow(row: { name: string }, template: DataSourceTemplate): string {
  const defaultNames = [template.name, ...(LEGACY_TEMPLATE_NAMES[template.id] ?? [])];
  return defaultNames.includes(row.name) ? template.name : row.name;
}

export function displayDescriptionForRow(
  row: { description: string | null },
  template: DataSourceTemplate,
): string | undefined {
  const description = row.description?.trim();
  if (!description) return undefined;
  const defaultDescriptions = [
    template.description,
    ...(LEGACY_TEMPLATE_DESCRIPTIONS[template.id] ?? []),
  ];
  return defaultDescriptions.includes(description) ? undefined : description;
}
