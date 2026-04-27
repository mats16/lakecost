import { useEffect, useRef, useState, type ReactNode } from 'react';
import { NavLink, useLocation, useNavigate } from 'react-router-dom';
import { useI18n, type Locale } from '../../i18n';
import { useMe } from '../../api/hooks';

interface NavItem {
  to: string;
  labelKey: string;
  end?: boolean;
}

interface NavGroup {
  labelKey: string;
  items: NavItem[];
  matchPrefix: string;
}

const PRIMARY: NavItem[] = [{ to: '/dashboard', labelKey: 'nav.overview' }];

const CONFIGURE: NavGroup = {
  labelKey: 'nav.configure',
  matchPrefix: '/configure',
  items: [
    { to: '/configure/data-sources', labelKey: 'nav.dataSources' },
    { to: '/configure/detection-rules', labelKey: 'nav.detectionRules' },
    { to: '/configure/transformations', labelKey: 'nav.transformations' },
    { to: '/configure/admin', labelKey: 'nav.admin' },
  ],
};

const SECONDARY: NavItem[] = [
  { to: '/explorer', labelKey: 'nav.costExplorer' },
  { to: '/budgets', labelKey: 'nav.budgets' },
  { to: '/settings', labelKey: 'nav.settings' },
];

export function AppShell({ children }: { children: ReactNode }) {
  const location = useLocation();
  const { t } = useI18n();
  const configureOpen = location.pathname.startsWith(CONFIGURE.matchPrefix);

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <h1>{t('appName')}</h1>
        <div className="nav-section-label">{t('nav.finops')}</div>
        <nav>
          {PRIMARY.map((item) => (
            <NavLink
              key={item.to}
              to={item.to}
              end={item.end}
              className={({ isActive }) => (isActive ? 'active' : '')}
            >
              {t(item.labelKey)}
            </NavLink>
          ))}

          <div className={`nav-group ${configureOpen ? 'open' : ''}`}>
            <NavLink
              to={CONFIGURE.items[0]?.to ?? '/configure/data-sources'}
              className={() => (configureOpen ? 'group-head active' : 'group-head')}
            >
              {t(CONFIGURE.labelKey)}
            </NavLink>
            {configureOpen ? (
              <div className="nav-children">
                {CONFIGURE.items.map((item) => (
                  <NavLink
                    key={item.to}
                    to={item.to}
                    className={({ isActive }) => (isActive ? 'active' : '')}
                  >
                    {t(item.labelKey)}
                  </NavLink>
                ))}
              </div>
            ) : null}
          </div>

          {SECONDARY.map((item) => (
            <NavLink
              key={item.to}
              to={item.to}
              className={({ isActive }) => (isActive ? 'active' : '')}
            >
              {t(item.labelKey)}
            </NavLink>
          ))}
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
  const navigate = useNavigate();
  const me = useMe();
  const [open, setOpen] = useState(false);
  const [langOpen, setLangOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!open) return;
    function onDocMouseDown(e: MouseEvent) {
      if (ref.current && !ref.current.contains(e.target as Node)) {
        setOpen(false);
        setLangOpen(false);
      }
    }
    function onKey(e: KeyboardEvent) {
      if (e.key === 'Escape') {
        setOpen(false);
        setLangOpen(false);
      }
    }
    document.addEventListener('mousedown', onDocMouseDown);
    document.addEventListener('keydown', onKey);
    return () => {
      document.removeEventListener('mousedown', onDocMouseDown);
      document.removeEventListener('keydown', onKey);
    };
  }, [open]);

  useEffect(() => {
    if (!open) setLangOpen(false);
  }, [open]);

  const email = me.data?.email ?? null;
  const userName = me.data?.userName ?? null;
  const displayName = email ?? userName ?? t('account.localUser');
  const initial = (email ?? userName ?? 'U').trim().charAt(0).toUpperCase() || 'U';
  const workspaceUrl = me.data?.workspaceUrl ?? null;

  const selectLocale = (l: Locale) => {
    setLocale(l);
    setLangOpen(false);
    setOpen(false);
  };

  const goToSettings = () => {
    setOpen(false);
    setLangOpen(false);
    navigate('/settings');
  };

  return (
    <div className="account-menu" ref={ref}>
      <button
        type="button"
        className="account-trigger"
        aria-haspopup="menu"
        aria-expanded={open}
        aria-label={t('account.openMenu')}
        onClick={() => setOpen((v) => !v)}
      >
        <span className="avatar" aria-hidden="true">
          {initial}
        </span>
      </button>
      {open ? (
        <div className="account-popover" role="menu">
          <div className="account-header">
            <div className="account-name" title={displayName}>
              {displayName}
            </div>
            {userName && email && userName !== email ? (
              <div className="account-sub">{userName}</div>
            ) : null}
          </div>

          <div className="account-section">
            <div className="account-section-label">{t('account.sectionApp')}</div>
            <button type="button" role="menuitem" className="account-item" onClick={goToSettings}>
              <span className="account-item-icon" aria-hidden="true">
                ⚙
              </span>
              <span className="account-item-label">{t('account.settings')}</span>
            </button>
          </div>

          <div className="account-divider" />

          <div className="account-section">
            <button
              type="button"
              role="menuitem"
              aria-haspopup="menu"
              aria-expanded={langOpen}
              className={`account-item account-item-expandable ${langOpen ? 'active' : ''}`}
              onClick={() => setLangOpen((v) => !v)}
            >
              <span className="account-item-icon" aria-hidden="true">
                🌐
              </span>
              <span className="account-item-label">{t('common.language')}</span>
              <span className="account-item-current">
                {locale === 'ja' ? t('common.japanese') : t('common.english')}
              </span>
              <span className="account-item-chevron" aria-hidden="true">
                ›
              </span>
            </button>
            {langOpen ? (
              <div className="account-submenu" role="menu">
                {(['en', 'ja'] as Locale[]).map((l) => {
                  const active = locale === l;
                  return (
                    <button
                      key={l}
                      type="button"
                      role="menuitemradio"
                      aria-checked={active}
                      className="account-submenu-item"
                      onClick={() => selectLocale(l)}
                    >
                      <span className="check" aria-hidden="true">
                        {active ? '✓' : ''}
                      </span>
                      <span>{l === 'en' ? t('common.english') : t('common.japanese')}</span>
                    </button>
                  );
                })}
              </div>
            ) : null}
          </div>

          {workspaceUrl ? (
            <>
              <div className="account-divider" />
              <div className="account-section">
                <a
                  href={workspaceUrl}
                  target="_blank"
                  rel="noreferrer noopener"
                  role="menuitem"
                  className="account-item"
                  onClick={() => {
                    setOpen(false);
                    setLangOpen(false);
                  }}
                >
                  <span className="account-item-icon" aria-hidden="true">
                    ↗
                  </span>
                  <span className="account-item-label">{t('account.databricksConsole')}</span>
                </a>
              </div>
            </>
          ) : null}
        </div>
      ) : null}
    </div>
  );
}
