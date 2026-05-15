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
  Layers,
  Gauge,
  Moon,
  Notebook,
  Shapes,
  Sparkles,
  Sun,
  type LucideIcon,
  Wallet,
} from 'lucide-react';
import { useI18n, type Locale } from '../../i18n';
import { useAppSettings, useMe } from '../../api/hooks';
import { CatalogSetupModal } from '../CatalogSetupModal';

interface NavItem {
  to: string;
  labelKey: string;
  icon?: LucideIcon;
  end?: boolean;
  activePrefixes?: string[];
}

interface NavGroup {
  labelKey: string;
  icon: LucideIcon;
  items: NavItem[];
  matchPrefix: string;
}

const TOP_LEVEL_ITEMS: NavItem[] = [
  { to: '/overview', labelKey: 'nav.overview', icon: LayoutDashboard, end: true },
  { to: '/cost-explore', labelKey: 'nav.costExplore', icon: ChartLine },
  { to: '/budgets', labelKey: 'nav.budgets', icon: Wallet },
  { to: '/genie', labelKey: 'nav.genie', icon: Sparkles, end: true },
];

export const CONFIGURE: NavGroup = {
  labelKey: 'nav.configure',
  icon: Bolt,
  matchPrefix: '/integrations',
  items: [
    { to: '/integrations', labelKey: 'nav.dataSources', activePrefixes: ['/pricing'] },
    { to: '/transformations', labelKey: 'nav.transformations' },
    { to: '/tags', labelKey: 'nav.tags' },
    { to: '/credentials', labelKey: 'nav.credentials' },
    { to: '/admin', labelKey: 'nav.configureCatalog' },
  ],
};

const OPTIMIZE: NavGroup = {
  labelKey: 'nav.optimize',
  icon: Gauge,
  matchPrefix: '/optimize',
  items: [
    { to: '/optimize/databricks', labelKey: 'nav.optimizeDatabricks' },
    { to: '/optimize/aws', labelKey: 'nav.optimizeAws' },
  ],
};

interface ExternalNavItem {
  path: string;
  labelKey: string;
  icon: LucideIcon;
}

type ThemeMode = 'light' | 'dark';

const THEME_STORAGE_KEY = 'finlake.theme';

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

function detectInitialTheme(): ThemeMode {
  if (typeof window === 'undefined') return 'dark';
  try {
    const stored = window.localStorage.getItem(THEME_STORAGE_KEY);
    if (stored === 'light' || stored === 'dark') return stored;
  } catch {
    // ignore
  }
  return document.documentElement.classList.contains('light') ? 'light' : 'dark';
}

function applyTheme(theme: ThemeMode) {
  const root = document.documentElement;
  root.classList.toggle('dark', theme === 'dark');
  root.classList.toggle('light', theme === 'light');
  root.style.colorScheme = theme;
}

export function AppShell({ children }: { children: ReactNode }) {
  const location = useLocation();
  const { t } = useI18n();
  const me = useMe();
  const workspaceUrl = me.data?.workspaceUrl ?? null;
  const appSettings = useAppSettings();
  const catalogName = appSettings.data?.settings.catalog_name?.trim() || null;
  const databricksItems = buildDatabricksItems(catalogName);
  const onConfigureRoute = CONFIGURE.items.some((item) => isNavItemActive(location.pathname, item));
  const onOptimizeRoute = OPTIMIZE.items.some((item) => isNavItemActive(location.pathname, item));
  const [configureOpen, setConfigureOpen] = useState(onConfigureRoute);
  const [optimizeOpen, setOptimizeOpen] = useState(onOptimizeRoute);
  const [theme, setTheme] = useState<ThemeMode>(detectInitialTheme);

  useEffect(() => {
    if (onConfigureRoute) {
      setConfigureOpen(true);
    }
    if (onOptimizeRoute) {
      setOptimizeOpen(true);
    }
  }, [onConfigureRoute, onOptimizeRoute]);

  useEffect(() => {
    applyTheme(theme);
    try {
      window.localStorage.setItem(THEME_STORAGE_KEY, theme);
    } catch {
      // ignore
    }
  }, [theme]);

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <div className="sidebar-brand">
          <Layers className="sidebar-brand-icon" aria-hidden="true" />
          <h1>{t('appName')}</h1>
        </div>
        <div className="nav-section-label">{t('nav.finops')}</div>
        <nav>
          {TOP_LEVEL_ITEMS.map((item) => {
            const Icon = item.icon;
            return (
              <NavLink
                key={item.to}
                to={item.to}
                end={item.end}
                className={({ isActive }) => (isActive ? 'active' : '')}
              >
                {Icon ? <Icon className="nav-icon" aria-hidden="true" /> : null}
                <span>{t(item.labelKey)}</span>
              </NavLink>
            );
          })}

          <div
            className={`nav-group ${optimizeOpen ? 'open' : ''} ${onOptimizeRoute ? 'active' : ''}`}
          >
            <div className="nav-group-row">
              <NavLink
                to={OPTIMIZE.items[0]?.to ?? '/optimize/databricks'}
                className={() => (onOptimizeRoute ? 'group-head active' : 'group-head')}
              >
                <OPTIMIZE.icon className="nav-icon" aria-hidden="true" />
                <span>{t(OPTIMIZE.labelKey)}</span>
              </NavLink>
              <button
                type="button"
                className="nav-chevron"
                aria-expanded={optimizeOpen}
                aria-label={t(OPTIMIZE.labelKey)}
                onClick={() => setOptimizeOpen((v) => !v)}
              >
                <ChevronDown className="nav-icon" aria-hidden="true" />
              </button>
            </div>
            {optimizeOpen ? (
              <div className="nav-children">
                {OPTIMIZE.items.map((item) => (
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

          <div
            className={`nav-group ${configureOpen ? 'open' : ''} ${onConfigureRoute ? 'active' : ''}`}
          >
            <div className="nav-group-row">
              <NavLink
                to={CONFIGURE.items[0]!.to}
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
                    end={item.end}
                    className={({ isActive }) =>
                      isActive ||
                      item.activePrefixes?.some((p) => matchesPathPrefix(location.pathname, p))
                        ? 'active'
                        : ''
                    }
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
          <ThemeToggle
            theme={theme}
            onToggle={() => setTheme(theme === 'dark' ? 'light' : 'dark')}
          />
        </div>
      </aside>
      <main className="main">{children}</main>
      {appSettings.isSuccess && !catalogName ? <CatalogSetupModal /> : null}
    </div>
  );
}

export function isNavItemActive(pathname: string, item: NavItem): boolean {
  return (
    matchesPathPrefix(pathname, item.to) ||
    (item.activePrefixes?.some((p) => matchesPathPrefix(pathname, p)) ?? false)
  );
}

export function matchesPathPrefix(pathname: string, prefix: string): boolean {
  return pathname === prefix || pathname.startsWith(`${prefix}/`);
}

function ThemeToggle({ theme, onToggle }: { theme: ThemeMode; onToggle: () => void }) {
  const { t } = useI18n();
  const isDark = theme === 'dark';
  return (
    <button
      type="button"
      className="theme-toggle"
      aria-pressed={isDark}
      aria-label={isDark ? t('theme.switchToLight') : t('theme.switchToDark')}
      title={isDark ? t('theme.switchToLight') : t('theme.switchToDark')}
      onClick={onToggle}
    >
      <span className="theme-toggle-thumb" aria-hidden="true">
        {isDark ? <Moon className="size-4" /> : <Sun className="size-4" />}
      </span>
    </button>
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
