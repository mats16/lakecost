import { useMemo, useState } from 'react';
import { tableLeafName, type DataSource } from '@finlake/shared';
import {
  Button,
  cn,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@databricks/appkit-ui/react';
import { Pencil } from 'lucide-react';
import { useCreateDataSource, useDataSources, useDataSourceTemplates } from '../../api/hooks';
import { DataSourceTile, type TileBadge } from './DataSourceTile';
import { DataSourceDrawer } from './DataSourceDrawer';
import { VendorLogo } from './VendorLogo';
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

export interface DatabricksFocusDraft {
  templateId: string;
  name: string;
  providerName: string;
  tableName: string;
}

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
  const [openId, setOpenId] = useState<number | null>(null);
  const [draftAwsSource, setDraftAwsSource] = useState<AwsFocusDraft | null>(null);
  const [draftDatabricksSource, setDraftDatabricksSource] = useState<DatabricksFocusDraft | null>(
    null,
  );
  const dataSources = useDataSources();
  const templates = useDataSourceTemplates();
  const createDs = useCreateDataSource();

  const rows = dataSources.data?.items ?? [];
  const availableTemplates = useMemo(
    () => templates.data?.items ?? DATA_SOURCE_TEMPLATES,
    [templates.data?.items],
  );

  const candidates = availableTemplates.filter((tpl) => {
    if (tpl.id === 'databricks_focus13' && rows.some((row) => row.providerName === 'Databricks')) {
      return false;
    }
    return true;
  });

  const badgesFor = (row: DataSource): TileBadge[] => {
    return [
      row.enabled
        ? { label: t('dataSources.badges.enabled'), variant: 'enabled' }
        : { label: t('dataSources.badges.setupRequired'), variant: 'unknown' },
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
      setDraftDatabricksSource(null);
      setDraftAwsSource({
        templateId: tpl.id,
        name: tpl.name,
        providerName: input.providerName,
        tableName,
      });
      return;
    }
    if (tpl.id === 'databricks_focus13') {
      setOpenId(null);
      setDraftAwsSource(null);
      setDraftDatabricksSource({
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
    setDraftDatabricksSource(null);
    setOpenId(created.id);
  };

  return (
    <>
      <div className="mb-4 flex flex-wrap items-end justify-between gap-3">
        <div>
          <h3 className="m-0 text-base font-semibold">{t('dataSources.currentTitle')}</h3>
          <p className="text-muted-foreground mt-1 text-sm">{t('dataSources.currentDesc')}</p>
        </div>
      </div>

      {rows.length === 0 ? (
        <p className="text-muted-foreground text-sm italic">{t('dataSources.empty')}</p>
      ) : (
        <div className="overflow-x-auto rounded-md border">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>{t('dataSources.columns.provider')}</TableHead>
                <TableHead>{t('dataSources.columns.status')}</TableHead>
                <TableHead>{t('dataSources.columns.table')}</TableHead>
                <TableHead>{t('dataSources.columns.type')}</TableHead>
                <TableHead className="text-right" aria-label={t('dataSources.columns.actions')} />
              </TableRow>
            </TableHeader>
            <TableBody>
              {rows.map((row) => {
                const tpl = templateForRow(row);
                const registryEntry = getTemplateRegistryEntry(tpl);
                return (
                  <TableRow
                    key={row.id}
                    className="cursor-pointer"
                    onClick={() => setOpenId(row.id)}
                  >
                    <TableCell>
                      <div className="flex min-w-56 items-center gap-3">
                        <VendorLogo source={tpl} logo={registryEntry?.logo} size={32} />
                        <div>
                          <div className="font-medium">{displayNameForRow(row, tpl)}</div>
                          <div className="text-muted-foreground text-xs">{row.providerName}</div>
                        </div>
                      </div>
                    </TableCell>
                    <TableCell>
                      <ConnectionStatus
                        enabled={row.enabled}
                        label={
                          row.enabled
                            ? t('dataSources.badges.enabled')
                            : t('dataSources.badges.setupRequired')
                        }
                      />
                    </TableCell>
                    <TableCell>
                      <span className="text-muted-foreground font-mono text-xs">
                        {row.tableName}
                      </span>
                    </TableCell>
                    <TableCell>{t('dataSources.type.dataSource')}</TableCell>
                    <TableCell className="text-right">
                      <Button
                        type="button"
                        variant="outline"
                        size="icon"
                        className="h-8 w-8"
                        aria-label={t('dataSources.edit')}
                      >
                        <Pencil className="h-4 w-4" />
                      </Button>
                    </TableCell>
                  </TableRow>
                );
              })}
            </TableBody>
          </Table>
        </div>
      )}

      <div className="mt-8 mb-4">
        <h3 className="m-0 text-base font-semibold">{t('dataSources.addTitle')}</h3>
        <p className="text-muted-foreground mt-1 text-sm">{t('dataSources.addDesc')}</p>
      </div>

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
        draftDatabricksSource={draftDatabricksSource}
        onClose={() => {
          setOpenId(null);
          setDraftAwsSource(null);
          setDraftDatabricksSource(null);
        }}
        onCreated={(row) => {
          setDraftAwsSource(null);
          setDraftDatabricksSource(null);
          setOpenId(row.id);
        }}
      />
    </>
  );
}

function ConnectionStatus({ enabled, label }: { enabled: boolean; label: string }) {
  return (
    <span
      className={cn(
        'inline-flex items-center gap-2 whitespace-nowrap text-sm font-medium',
        enabled ? 'text-(--success)' : 'text-(--warning)',
      )}
    >
      <span
        className={cn('h-2 w-2 rounded-full', enabled ? 'bg-(--success)' : 'bg-(--warning)')}
      />
      {label}
    </span>
  );
}
