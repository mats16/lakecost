import { useMemo } from 'react';
import { useNavigate } from 'react-router-dom';
import {
  dataSourceKeyString,
  isAwsProvider,
  isDatabricksProvider,
  type PricingNotebookState,
  type DataSource,
} from '@finlake/shared';
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
import {
  useCreateDataSource,
  useDataSources,
  useDataSourceTemplates,
  usePricingNotebook,
} from '../../api/hooks';
import { DataSourceTile, type TileBadge } from './DataSourceTile';
import { VendorLogo } from './VendorLogo';
import {
  DATA_SOURCE_TEMPLATES,
  PRICING_AWS_TEMPLATE,
  canCreateTemplate,
  displayNameForRow,
  findTemplateForRow,
  getTemplateInputConfig,
  getTemplateRegistryEntry,
  type DataSourceTemplate,
} from './dataSourceCatalog';
import { useI18n } from '../../i18n';
import { nextTableName } from './utils';

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

function isRegisteredPricing(row: PricingNotebookState): boolean {
  return Boolean(
    row.table ||
    row.rawDataTable ||
    row.rawDataPath ||
    row.notebookWorkspacePath ||
    row.runId ||
    row.runStatus !== 'not_started',
  );
}

function rowMatchesTemplate(row: DataSource, template: DataSourceTemplate): boolean {
  return findTemplateForRow(row)?.id === template.id;
}

function canAddMultiple(template: DataSourceTemplate): boolean {
  return template.id === 'aws';
}

function detailPathForTemplate(template: DataSourceTemplate): string | null {
  if (template.id === 'databricks_focus13') return '/integrations/databricks';
  if (template.id === 'aws') return '/integrations/aws';
  return null;
}

function detailPathForRow(row: DataSource): string | null {
  const template = findTemplateForRow(row);
  return template ? detailPathForTemplate(template) : null;
}

export function DataSources() {
  const { t } = useI18n();
  const navigate = useNavigate();
  const dataSources = useDataSources();
  const templates = useDataSourceTemplates();
  const pricing = usePricingNotebook();
  const createDs = useCreateDataSource();

  const rows = dataSources.data?.items ?? [];
  const { hasRegisteredPricingData, pricingDataEnabled } = useMemo(() => {
    const pricingRows = pricing.data?.items ?? [];
    const registered = pricingRows.filter(isRegisteredPricing);
    return {
      hasRegisteredPricingData: registered.length > 0,
      pricingDataEnabled: registered.some((row) => Boolean(row.table)),
    };
  }, [pricing.data?.items]);
  const availableTemplates = useMemo(
    () => templates.data?.items ?? DATA_SOURCE_TEMPLATES,
    [templates.data?.items],
  );

  const candidates = availableTemplates.filter((tpl) => {
    if (
      tpl.id === 'databricks_focus13' &&
      rows.some((row) => isDatabricksProvider(row.providerName))
    ) {
      return false;
    }
    if (tpl.id === 'aws' && rows.some((row) => isAwsProvider(row.providerName))) {
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
      navigate(detailPathForRow(existing) ?? '/integrations');
      return;
    }
    const detailPath = detailPathForTemplate(tpl);
    if (detailPath) {
      navigate(detailPath);
      return;
    }
    const tableName = canAddMultiple(tpl)
      ? nextTableName(input.defaultTableName, rows)
      : input.defaultTableName;
    await createDs.mutateAsync({
      templateId: tpl.id,
      name: tpl.name,
      providerName: input.providerName,
      tableName,
      enabled: false,
    });
    navigate('/integrations');
  };

  return (
    <>
      <div className="mb-4 flex flex-wrap items-end justify-between gap-3">
        <div>
          <h3 className="m-0 text-base font-semibold">{t('dataSources.currentTitle')}</h3>
          <p className="text-muted-foreground mt-1 text-sm">{t('dataSources.currentDesc')}</p>
        </div>
      </div>

      {rows.length === 0 && !hasRegisteredPricingData ? (
        <p className="text-muted-foreground text-sm italic">{t('dataSources.empty')}</p>
      ) : (
        <div className="overflow-x-auto rounded-md border">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>{t('dataSources.columns.provider')}</TableHead>
                <TableHead>{t('dataSources.columns.status')}</TableHead>
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
                    key={dataSourceKeyString(row)}
                    className="cursor-pointer"
                    onClick={() => navigate(detailPathForRow(row) ?? '/integrations')}
                  >
                    <TableCell>
                      <div className="flex min-w-56 items-center gap-3">
                        <VendorLogo source={tpl} logo={registryEntry?.logo} size={32} />
                        <div className="font-medium">{displayNameForRow(row, tpl)}</div>
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
              {hasRegisteredPricingData ? (
                <TableRow className="cursor-pointer" onClick={() => navigate('/pricing/aws')}>
                  <TableCell>
                    <div className="flex min-w-56 items-center gap-3">
                      <VendorLogo source={PRICING_AWS_TEMPLATE} logo={{ kind: 'aws' }} size={32} />
                      <div className="font-medium">{PRICING_AWS_TEMPLATE.name}</div>
                    </div>
                  </TableCell>
                  <TableCell>
                    <ConnectionStatus
                      enabled={pricingDataEnabled}
                      label={
                        pricingDataEnabled
                          ? t('dataSources.badges.enabled')
                          : t('dataSources.badges.setupRequired')
                      }
                    />
                  </TableCell>
                  <TableCell>{t('dataSources.type.pricingData')}</TableCell>
                  <TableCell className="text-right">
                    <Button
                      type="button"
                      variant="outline"
                      size="icon"
                      className="h-8 w-8"
                      aria-label={t('dataSources.editPricingData')}
                    >
                      <Pencil className="h-4 w-4" />
                    </Button>
                  </TableCell>
                </TableRow>
              ) : null}
            </TableBody>
          </Table>
        </div>
      )}

      <div className="mt-8 mb-4">
        <h3 className="m-0 text-base font-semibold">{t('dataSources.addTitle')}</h3>
        <p className="text-muted-foreground mt-1 text-sm">{t('dataSources.addDesc')}</p>
      </div>

      <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 md:grid-cols-3 lg:grid-cols-4 xl:grid-cols-5">
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

      {!hasRegisteredPricingData ? (
        <>
          <div className="mt-8 mb-4">
            <h3 className="m-0 text-base font-semibold">{t('dataSources.pricingTitle')}</h3>
            <p className="text-muted-foreground mt-1 text-sm">{t('dataSources.pricingDesc')}</p>
          </div>

          <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 md:grid-cols-3 lg:grid-cols-4 xl:grid-cols-5">
            <DataSourceTile
              source={PRICING_AWS_TEMPLATE}
              logo={{ kind: 'aws' }}
              onClick={() => navigate('/pricing/aws')}
            />
          </div>
        </>
      ) : null}
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
      <span className={cn('h-2 w-2 rounded-full', enabled ? 'bg-(--success)' : 'bg-(--warning)')} />
      {label}
    </span>
  );
}
