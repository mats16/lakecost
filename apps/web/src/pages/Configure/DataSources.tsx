import { useMemo, useState } from 'react';
import { tableLeafName, type DataSource } from '@lakecost/shared';
import { Input, Separator } from '@databricks/appkit-ui/react';
import { useCreateDataSource, useDataSources, useDataSourceTemplates } from '../../api/hooks';
import { DataSourceTile, type TileBadge } from './DataSourceTile';
import { DataSourceDrawer } from './DataSourceDrawer';
import {
  DATA_SOURCE_TEMPLATES,
  canCreateTemplate,
  displayNameForRow,
  findTemplateForRow,
  getTemplateInputConfig,
  getTemplateRegistryEntry,
  type DataSourceTemplate,
} from './dataSourceCatalog';
import { useI18n } from '../../i18n';
import type { AwsFocusDraft } from './useAwsFocusForm';

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

function rowMatchesTemplate(row: DataSource, template: DataSourceTemplate): boolean {
  return findTemplateForRow(row)?.id === template.id;
}

function canAddMultiple(template: DataSourceTemplate): boolean {
  return template.id === 'aws';
}

function nextTableName(base: string, rows: DataSource[]): string {
  const used = new Set(rows.map((row) => tableLeafName(row.tableName)));
  if (!used.has(base)) return base;

  for (let i = 2; i < 1000; i += 1) {
    const candidate = `${base}_${i}`;
    if (!used.has(candidate)) return candidate;
  }
  return `${base}_${Date.now()}`;
}

export function DataSources() {
  const { t } = useI18n();
  const [filter, setFilter] = useState('');
  const [openId, setOpenId] = useState<number | null>(null);
  const [draftAwsSource, setDraftAwsSource] = useState<AwsFocusDraft | null>(null);
  const dataSources = useDataSources();
  const templates = useDataSourceTemplates();
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
    if (existing && !canAddMultiple(tpl)) {
      setOpenId(existing.id);
      return;
    }
    const tableName = canAddMultiple(tpl)
      ? nextTableName(input.defaultTableName, rows)
      : input.defaultTableName;
    if (tpl.id === 'aws') {
      setOpenId(null);
      setDraftAwsSource({
        templateId: tpl.id,
        name: tpl.name,
        providerName: input.providerName,
        tableName,
      });
      return;
    }
    const created = await createDs.mutateAsync({
      templateId: tpl.id,
      name: tpl.name,
      providerName: input.providerName,
      tableName,
      enabled: false,
    });
    setDraftAwsSource(null);
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
          const existing = canAddMultiple(tpl)
            ? undefined
            : rows.find((row) => rowMatchesTemplate(row, tpl));
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

      <DataSourceDrawer
        dataSourceId={openId}
        draftAwsSource={draftAwsSource}
        onClose={() => {
          setOpenId(null);
          setDraftAwsSource(null);
        }}
        onCreated={(row) => {
          setDraftAwsSource(null);
          setOpenId(row.id);
        }}
      />
    </>
  );
}
