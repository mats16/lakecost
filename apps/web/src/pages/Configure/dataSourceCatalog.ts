import {
  DATA_SOURCE_TEMPLATES,
  tableLeafName,
  type DataSourceTemplate,
  type SetupStepId,
} from '@finlake/shared';

export { DATA_SOURCE_TEMPLATES, type DataSourceTemplate };

export type TemplateLogo =
  | { kind: 'databricks' }
  | { kind: 'aws' }
  | { kind: 'google-cloud' }
  | { kind: 'snowflake' }
  | { kind: 'abbr'; label: string };

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
  providerName: 'databricks',
  defaultTableName: 'databricks_usage',
  setupSteps: ['systemTables', 'permissions'],
};

export const DATA_SOURCE_TEMPLATE_REGISTRY: Record<string, DataSourceTemplateRegistryEntry> = {
  databricks_focus13: {
    input: DATABRICKS_FOCUS13_INPUT,
    matches: [
      DATABRICKS_FOCUS13_INPUT,
      { ...DATABRICKS_FOCUS13_INPUT, providerName: 'Databricks' },
    ],
    logo: { kind: 'databricks' },
  },
  aws: {
    input: {
      providerName: 'aws',
      defaultTableName: 'aws_usage',
      setupSteps: ['awsCur'],
    },
    // Uppercase/display names are kept for legacy DB rows created before providerName was standardized to lowercase URL slugs.
    matches: [
      { providerName: 'aws', defaultTableName: 'aws_usage' },
      { providerName: 'Amazon Web Services', defaultTableName: 'aws_usage' },
      { providerName: 'AWS', defaultTableName: 'aws_usage' },
    ],
    logo: { kind: 'aws' },
  },
  gcp: {
    matches: [
      { providerName: 'Google Cloud', defaultTableName: 'google_cloud_billing' },
      { providerName: 'GCP', defaultTableName: 'gcp_billing' },
    ],
    logo: { kind: 'google-cloud' },
  },
  snowflake: {
    matches: [{ providerName: 'Snowflake', defaultTableName: 'snowflake_credits' }],
    logo: { kind: 'snowflake' },
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
  providerName: string;
  tableName: string;
}): DataSourceTemplate | undefined {
  const leaf = tableLeafName(row.tableName);
  const exactMatch = DATA_SOURCE_TEMPLATES.find((template) =>
    (DATA_SOURCE_TEMPLATE_REGISTRY[template.id]?.matches ?? []).some(
      (match) => match.providerName === row.providerName && match.defaultTableName === leaf,
    ),
  );
  if (exactMatch) return exactMatch;

  return DATA_SOURCE_TEMPLATES.find((template) =>
    (DATA_SOURCE_TEMPLATE_REGISTRY[template.id]?.matches ?? []).some(
      (match) => match.providerName === row.providerName,
    ),
  );
}

/** Legacy names that should be treated as the template's canonical name. */
export const LEGACY_TEMPLATE_NAMES: Record<string, string[]> = {
  databricks_focus13: ['Databricks System Tables'],
};

export const PRICING_AWS_TEMPLATE: DataSourceTemplate = {
  id: 'pricing_aws',
  name: 'AWS Pricing',
  description: '',
  subtitle: '',
  focus_version: null,
  available: true,
  appearance: {
    brandColor: '#FF9900',
    brandTextColor: '#232F3E',
  },
};

export const PRICING_DATABRICKS_TEMPLATE: DataSourceTemplate = {
  id: 'pricing_databricks',
  name: 'Databricks Pricing',
  description: '',
  subtitle: '',
  focus_version: null,
  available: true,
  appearance: {
    brandColor: '#FF3621',
  },
};


export function displayNameForRow(row: { name: string }, template: DataSourceTemplate): string {
  const defaultNames = [template.name, ...(LEGACY_TEMPLATE_NAMES[template.id] ?? [])];
  return defaultNames.includes(row.name) ? template.name : row.name;
}
