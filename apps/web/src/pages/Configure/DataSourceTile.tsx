import { Badge, Card } from '@databricks/appkit-ui/react';
import type { DataSourceTemplate, TemplateLogo } from './dataSourceCatalog';
import { VendorLogo } from './VendorLogo';
import { useI18n } from '../../i18n';

export interface TileBadge {
  label: string;
  variant: 'enabled' | 'disabled' | 'healthy' | 'error' | 'unknown' | 'neutral';
}

interface Props {
  /** Static template metadata from the API. */
  source: DataSourceTemplate;
  logo?: TemplateLogo;
  /** Override the rendered name (for DB rows the user has renamed). */
  displayName?: string;
  badges?: TileBadge[];
  onClick?: () => void;
  muted?: boolean;
}

const BADGE_VARIANT: Record<TileBadge['variant'], React.ComponentProps<typeof Badge>['variant']> = {
  enabled: 'default',
  disabled: 'secondary',
  healthy: 'default',
  error: 'destructive',
  unknown: 'outline',
  neutral: 'outline',
};

const BADGE_CLASSES: Record<TileBadge['variant'], string> = {
  enabled: 'bg-(--success)/15 text-(--success) border-(--success)/30',
  disabled: '',
  healthy: 'bg-(--success)/15 text-(--success) border-(--success)/30',
  error: '',
  unknown: 'bg-(--warning)/15 text-(--warning) border-(--warning)/30',
  neutral: 'border-border bg-muted/70 text-muted-foreground',
};

function renderBadges(items: TileBadge[]) {
  return items.map((b) => (
    <Badge key={b.label} variant={BADGE_VARIANT[b.variant]} className={BADGE_CLASSES[b.variant]}>
      {b.label}
    </Badge>
  ));
}

export function DataSourceTile({ source, logo, displayName, badges = [], onClick, muted }: Props) {
  const { t } = useI18n();
  const name = displayName ?? source.name;
  const interactive = Boolean(onClick);

  return (
    <Card
      data-slot="card"
      onClick={onClick}
      onKeyDown={(e) => {
        if (!interactive) return;
        if (e.key === 'Enter' || e.key === ' ') {
          e.preventDefault();
          onClick?.();
        }
      }}
      role={interactive ? 'button' : undefined}
      tabIndex={interactive ? 0 : -1}
      aria-disabled={!interactive}
      aria-label={t(`dataSources.catalog.${source.id}.description`)}
      className={[
        'relative flex h-30 min-h-30 flex-col items-center justify-center gap-2 p-4 text-center transition-colors',
        interactive
          ? 'hover:border-primary focus-visible:border-primary cursor-pointer'
          : 'cursor-default',
        muted ? 'opacity-75' : '',
      ].join(' ')}
    >
      {badges.length > 0 ? (
        <div className="absolute top-3 right-3 flex flex-col items-end gap-1">
          {renderBadges(badges)}
        </div>
      ) : null}
      <VendorLogo source={source} logo={logo} size={36} />
      <h4 className="m-0 max-w-full truncate text-sm font-semibold">{name}</h4>
    </Card>
  );
}
