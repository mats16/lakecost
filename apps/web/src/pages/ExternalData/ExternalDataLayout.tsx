import type { ReactNode } from 'react';
import { NavLink, Outlet } from 'react-router-dom';
import { useI18n } from '../../i18n';

const TABS = [
  { to: '/locations', labelKey: 'nav.externalLocations' },
  { to: '/credentials', labelKey: 'nav.credentials' },
];

export function ExternalDataLayout({ children }: { children?: ReactNode }) {
  const { t } = useI18n();
  return (
    <>
      <header className="page-header configure-header">
        <h2>{t('nav.externalData')}</h2>
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
