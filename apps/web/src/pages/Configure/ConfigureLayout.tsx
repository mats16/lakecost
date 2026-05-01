import type { ReactNode } from 'react';
import { NavLink, Outlet } from 'react-router-dom';
import { useI18n } from '../../i18n';

const TABS = [
  { to: '/configure/data-sources', labelKey: 'nav.dataSources' },
  { to: '/configure/transformations', labelKey: 'nav.transformations' },
  { to: '/configure/catalog', labelKey: 'nav.configureCatalog' },
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
