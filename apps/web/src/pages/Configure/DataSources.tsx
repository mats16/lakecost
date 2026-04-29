import { useMemo, useState } from 'react';
import { CATALOG_SETTING_KEY, FOCUS_VIEW_SCHEMA_DEFAULT, type DataSource } from '@lakecost/shared';
import { Input, Separator } from '@databricks/appkit-ui/react';
import {
  useAppSettings,
  useCreateDataSource,
  useDataSources,
  useDataSourceTemplates,
} from '../../api/hooks';
import { DataSourceTile, type TileBadge } from './DataSourceTile';
import { DataSourceDrawer } from './DataSourceDrawer';
import {
  DATA_SOURCE_TEMPLATES,
  canCreateTemplate,
  displayDescriptionForRow,
  displayNameForRow,
  findTemplateForRow,
  getTemplateInputConfig,
  getTemplateRegistryEntry,
  type DataSourceTemplateInputConfig,
  type DataSourceTemplate,
} from './dataSourceCatalog';
import { useI18n } from '../../i18n';

const FALLBACK_TEMPLATE: DataSourceTemplate = {
  id: 'custom',
  name: 'Custom data source',
  description: '',
  subtitle: '',
  focus_version: null,
  available: true,
  appearance: {
    brandColor: '#475467',
  },
};

function templateForRow(row: DataSource): DataSourceTemplate {
  return findTemplateForRow(row) ?? FALLBACK_TEMPLATE;
}

function initialTableName(input: DataSourceTemplateInputConfig, catalog: string): string {
  if (input.providerName !== 'Databricks' || !catalog) return input.defaultTableName;
  return `${catalog}.${FOCUS_VIEW_SCHEMA_DEFAULT}.${input.defaultTableName}`;
}

function rowMatchesTemplate(row: DataSource, template: DataSourceTemplate): boolean {
  return findTemplateForRow(row)?.id === template.id;
}

export function DataSources() {
  const { t } = useI18n();
  const [filter, setFilter] = useState('');
  const [openId, setOpenId] = useState<number | null>(null);
  const dataSources = useDataSources();
  const templates = useDataSourceTemplates();
  const settings = useAppSettings();
  const createDs = useCreateDataSource();

  const rows = dataSources.data?.items ?? [];
  const availableTemplates = useMemo(
    () => templates.data?.items ?? DATA_SOURCE_TEMPLATES,
    [templates.data?.items],
  );

  const filteredRows = useMemo(() => {
    const q = filter.trim().toLowerCase();
    if (!q) return rows;
    return rows.filter((r) => r.name.toLowerCase().includes(q));
  }, [rows, filter]);

  const candidates = useMemo(() => {
    const q = filter.trim().toLowerCase();
    return availableTemplates.filter((tpl) => {
      if (
        tpl.id === 'databricks_focus13' &&
        rows.some((row) => row.providerName === 'Databricks')
      ) {
        return false;
      }
      return q ? tpl.name.toLowerCase().includes(q) : true;
    });
  }, [availableTemplates, filter, rows]);

  const badgesFor = (row: DataSource): TileBadge[] => {
    if (row.jobId !== null) {
      return [
        row.enabled
          ? { label: t('dataSources.badges.enabled'), variant: 'enabled' }
          : { label: t('dataSources.badges.disabled'), variant: 'disabled' },
      ];
    }
    return [
      { label: t('dataSources.badges.added'), variant: 'unknown' },
      { label: t('dataSources.badges.setupRequired'), variant: 'unknown' },
    ];
  };

  const onAddTemplate = async (tpl: DataSourceTemplate) => {
    const input = getTemplateInputConfig(tpl);
    if (!tpl.available || !input) {
      return;
    }
    const existing = rows.find((row) => rowMatchesTemplate(row, tpl));
    if (existing) {
      setOpenId(existing.id);
      return;
    }
    const created = await createDs.mutateAsync({
      templateId: tpl.id,
      name: tpl.name,
      providerName: input.providerName,
      tableName: initialTableName(input, settings.data?.settings[CATALOG_SETTING_KEY]?.trim() ?? ''),
      description: tpl.description,
      enabled: false,
    });
    setOpenId(created.id);
  };

  return (
    <>
      <div className="mb-4 flex flex-wrap items-end justify-between gap-3">
        <h3 className="m-0 text-base font-semibold">{t('dataSources.currentTitle')}</h3>
        <div className="flex items-center gap-3">
          <Input
            placeholder={t('dataSources.filterPlaceholder')}
            value={filter}
            onChange={(e) => setFilter(e.target.value)}
            className="h-9 w-52"
          />
        </div>
      </div>

      {filteredRows.length === 0 ? (
        <p className="text-muted-foreground text-sm italic">{t('dataSources.empty')}</p>
      ) : (
        <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 xl:grid-cols-3">
          {filteredRows.map((row) => {
            const tpl = templateForRow(row);
            const registryEntry = getTemplateRegistryEntry(tpl);
            return (
              <DataSourceTile
                key={row.id}
                source={tpl}
                logo={registryEntry?.logo}
                displayName={displayNameForRow(row, tpl)}
                displayDescription={displayDescriptionForRow(row, tpl)}
                badges={badgesFor(row)}
                onClick={() => setOpenId(row.id)}
              />
            );
          })}
        </div>
      )}

      <Separator className="my-8" />

      <h3 className="mb-4 text-base font-semibold">{t('dataSources.addTitle')}</h3>

      <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 xl:grid-cols-3">
        {candidates.map((tpl) => {
          const existing = rows.find((row) => rowMatchesTemplate(row, tpl));
          const registryEntry = getTemplateRegistryEntry(tpl);
          const canCreate = canCreateTemplate(tpl);
          return (
            <DataSourceTile
              key={tpl.id}
              source={tpl}
              logo={registryEntry?.logo}
              badges={
                existing
                  ? badgesFor(existing)
                  : !canCreate
                  ? [{ label: t('dataSources.badges.comingSoon'), variant: 'unknown' }]
                  : []
              }
              onClick={canCreate ? () => onAddTemplate(tpl) : undefined}
              muted={!canCreate}
            />
          );
        })}
      </div>

      <DataSourceDrawer dataSourceId={openId} onClose={() => setOpenId(null)} />
    </>
  );
}
