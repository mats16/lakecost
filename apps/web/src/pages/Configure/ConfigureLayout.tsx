import type { ReactNode } from 'react';
import { NavLink, Outlet } from 'react-router-dom';
import { useI18n } from '../../i18n';

const TABS = [
  { to: '/data-sources', labelKey: 'nav.dataSources', end: true },
  { to: '/tags', labelKey: 'nav.tags' },
  { to: '/transformations', labelKey: 'nav.transformations' },
  { to: '/pricing', labelKey: 'nav.pricing' },
  { to: '/credentials', labelKey: 'nav.credentials' },
  { to: '/admin', labelKey: 'nav.configureCatalog' },
];

export function ConfigureLayout({ children }: { children?: ReactNode }) {
  const { t } = useI18n();
  return (
    <>
      <header className="page-header configure-header">
        <h2>{t('configure.title')}</h2>
        <nav className="upper-tabs">
          {TABS.map((tab) => (
            <NavLink
              key={tab.to}
              to={tab.to}
              end={tab.end}
              className={({ isActive }) => (isActive ? 'active' : '')}
            >
              {t(tab.labelKey)}
            </NavLink>
          ))}
        </nav>
      </header>
      {children ?? <Outlet />}
    </>
  );
}
