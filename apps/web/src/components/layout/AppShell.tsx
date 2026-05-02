import { useEffect, useState, type ReactNode } from 'react';
import { NavLink, useLocation } from 'react-router-dom';
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuRadioGroup,
  DropdownMenuRadioItem,
  DropdownMenuSeparator,
  DropdownMenuSub,
  DropdownMenuSubContent,
  DropdownMenuSubTrigger,
  DropdownMenuTrigger,
} from '@databricks/appkit-ui/react';
import {
  Bolt,
  ChartLine,
  ChevronDown,
  ExternalLink,
  Globe,
  LayoutDashboard,
  LineChart,
  Notebook,
  Shapes,
  type LucideIcon,
} from 'lucide-react';
import { useI18n, type Locale } from '../../i18n';
import { useAppSettings, useMe } from '../../api/hooks';

interface NavItem {
  to: string;
  labelKey: string;
  icon?: LucideIcon;
  end?: boolean;
}

interface NavGroup {
  labelKey: string;
  icon: LucideIcon;
  items: NavItem[];
  matchPrefix: string;
}

const INFORM: NavGroup = {
  labelKey: 'nav.inform',
  icon: ChartLine,
  matchPrefix: '/overview',
  items: [
    { to: '/overview', labelKey: 'nav.overview', end: true },
    { to: '/overview/budgets', labelKey: 'nav.budgets' },
  ],
};

const CONFIGURE: NavGroup = {
  labelKey: 'nav.configure',
  icon: Bolt,
  matchPrefix: '/configure',
  items: [
    { to: '/configure/data-sources', labelKey: 'nav.dataSources' },
    { to: '/configure/transformations', labelKey: 'nav.transformations' },
    { to: '/configure/catalog', labelKey: 'nav.configureCatalog' },
  ],
};

const SECONDARY: NavItem[] = [{ to: '/explorer', labelKey: 'nav.costExplorer', icon: LineChart }];

interface ExternalNavItem {
  path: string;
  labelKey: string;
  icon: LucideIcon;
}

function buildDatabricksItems(catalogName: string | null): ExternalNavItem[] {
  return [
    {
      path: catalogName ? `/explore/data/${encodeURIComponent(catalogName)}` : '/explore/data',
      labelKey: 'nav.catalog',
      icon: Shapes,
    },
    { path: '/sql/dashboards', labelKey: 'nav.dashboards', icon: LayoutDashboard },
    { path: '/browse', labelKey: 'nav.workspace', icon: Notebook },
  ];
}

export function AppShell({ children }: { children: ReactNode }) {
  const location = useLocation();
  const { t } = useI18n();
  const me = useMe();
  const workspaceUrl = me.data?.workspaceUrl ?? null;
  const appSettings = useAppSettings();
  const catalogName = appSettings.data?.settings.catalog_name?.trim() || null;
  const databricksItems = buildDatabricksItems(catalogName);
  const onInformRoute = location.pathname.startsWith(INFORM.matchPrefix);
  const onConfigureRoute = location.pathname.startsWith(CONFIGURE.matchPrefix);
  const [informOpen, setInformOpen] = useState(onInformRoute);
  const [configureOpen, setConfigureOpen] = useState(onConfigureRoute);

  useEffect(() => {
    if (onInformRoute) {
      setInformOpen(true);
    }
    if (onConfigureRoute) {
      setConfigureOpen(true);
    }
  }, [onConfigureRoute, onInformRoute]);

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <h1>{t('appName')}</h1>
        <div className="nav-section-label">{t('nav.finops')}</div>
        <nav>
          <div className={`nav-group ${informOpen ? 'open' : ''} ${onInformRoute ? 'active' : ''}`}>
            <div className="nav-group-row">
              <NavLink
                to={INFORM.items[0]?.to ?? '/overview'}
                className={() => (onInformRoute ? 'group-head active' : 'group-head')}
              >
                <INFORM.icon className="nav-icon" aria-hidden="true" />
                <span>{t(INFORM.labelKey)}</span>
              </NavLink>
              <button
                type="button"
                className="nav-chevron"
                aria-expanded={informOpen}
                aria-label={t(INFORM.labelKey)}
                onClick={() => setInformOpen((v) => !v)}
              >
                <ChevronDown className="nav-icon" aria-hidden="true" />
              </button>
            </div>
            {informOpen ? (
              <div className="nav-children">
                {INFORM.items.map((item) => (
                  <NavLink
                    key={item.to}
                    to={item.to}
                    end={item.end}
                    className={({ isActive }) => (isActive ? 'active' : '')}
                  >
                    <span>{t(item.labelKey)}</span>
                  </NavLink>
                ))}
              </div>
            ) : null}
          </div>

          {SECONDARY.map((item) => {
            const Icon = item.icon;
            return (
              <NavLink
                key={item.to}
                to={item.to}
                className={({ isActive }) => (isActive ? 'active' : '')}
              >
                {Icon ? <Icon className="nav-icon" aria-hidden="true" /> : null}
                <span>{t(item.labelKey)}</span>
              </NavLink>
            );
          })}

          <div
            className={`nav-group ${configureOpen ? 'open' : ''} ${onConfigureRoute ? 'active' : ''}`}
          >
            <div className="nav-group-row">
              <NavLink
                to={CONFIGURE.items[0]?.to ?? '/configure/data-sources'}
                className={() => (onConfigureRoute ? 'group-head active' : 'group-head')}
              >
                <CONFIGURE.icon className="nav-icon" aria-hidden="true" />
                <span>{t(CONFIGURE.labelKey)}</span>
              </NavLink>
              <button
                type="button"
                className="nav-chevron"
                aria-expanded={configureOpen}
                aria-label={t(CONFIGURE.labelKey)}
                onClick={() => setConfigureOpen((v) => !v)}
              >
                <ChevronDown className="nav-icon" aria-hidden="true" />
              </button>
            </div>
            {configureOpen ? (
              <div className="nav-children">
                {CONFIGURE.items.map((item) => (
                  <NavLink
                    key={item.to}
                    to={item.to}
                    className={({ isActive }) => (isActive ? 'active' : '')}
                  >
                    <span>{t(item.labelKey)}</span>
                  </NavLink>
                ))}
              </div>
            ) : null}
          </div>

          {workspaceUrl ? (
            <>
              <div className="nav-section-label">{t('nav.databricks')}</div>
              {databricksItems.map((item) => {
                const Icon = item.icon;
                return (
                  <a
                    key={item.path}
                    href={`${workspaceUrl}${item.path}`}
                    target="_blank"
                    rel="noreferrer noopener"
                    className="nav-external"
                  >
                    <Icon className="nav-icon" aria-hidden="true" />
                    <span>{t(item.labelKey)}</span>
                    <ExternalLink className="nav-icon nav-icon-trailing" aria-hidden="true" />
                  </a>
                );
              })}
            </>
          ) : null}
        </nav>

        <div className="sidebar-footer">
          <AccountMenu />
        </div>
      </aside>
      <main className="main">{children}</main>
    </div>
  );
}

function AccountMenu() {
  const { t, locale, setLocale } = useI18n();
  const me = useMe();

  const email = me.data?.email ?? null;
  const userName = me.data?.userName ?? null;
  const displayName = email ?? userName ?? t('account.localUser');
  const initial = (email ?? userName ?? 'U').trim().charAt(0).toUpperCase() || 'U';
  const workspaceUrl = me.data?.workspaceUrl ?? null;

  return (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <button type="button" className="account-trigger" aria-label={t('account.openMenu')}>
          <span className="avatar" aria-hidden="true">
            {initial}
          </span>
        </button>
      </DropdownMenuTrigger>
      <DropdownMenuContent side="top" align="start" className="account-menu-content w-64">
        <DropdownMenuLabel className="flex flex-col gap-0.5 px-3 py-2">
          <span className="truncate text-sm font-semibold" title={displayName}>
            {displayName}
          </span>
          {userName && email && userName !== email ? (
            <span className="text-muted-foreground text-xs">{userName}</span>
          ) : null}
        </DropdownMenuLabel>
        <DropdownMenuSeparator />

        <DropdownMenuSub>
          <DropdownMenuSubTrigger>
            <Globe />
            <span className="flex-1">{t('common.language')}</span>
            <span className="text-muted-foreground text-xs">
              {locale === 'ja' ? t('common.japanese') : t('common.english')}
            </span>
          </DropdownMenuSubTrigger>
          <DropdownMenuSubContent>
            <DropdownMenuRadioGroup value={locale} onValueChange={(v) => setLocale(v as Locale)}>
              <DropdownMenuRadioItem value="en">{t('common.english')}</DropdownMenuRadioItem>
              <DropdownMenuRadioItem value="ja">{t('common.japanese')}</DropdownMenuRadioItem>
            </DropdownMenuRadioGroup>
          </DropdownMenuSubContent>
        </DropdownMenuSub>

        {workspaceUrl ? (
          <>
            <DropdownMenuSeparator />
            <DropdownMenuItem asChild>
              <a href={workspaceUrl} target="_blank" rel="noreferrer noopener">
                <ExternalLink />
                <span>{t('account.databricksConsole')}</span>
              </a>
            </DropdownMenuItem>
          </>
        ) : null}
      </DropdownMenuContent>
    </DropdownMenu>
  );
}
