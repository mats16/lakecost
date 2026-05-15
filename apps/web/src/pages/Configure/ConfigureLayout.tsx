import { NavLink, Outlet } from 'react-router-dom';
import { useI18n } from '../../i18n';
import { CONFIGURE } from '../../components/layout/AppShell';

export function ConfigureLayout() {
  const { t } = useI18n();
  return (
    <>
      <header className="page-header configure-header">
        <h2>{t('configure.title')}</h2>
        <nav className="upper-tabs">
          {CONFIGURE.items.map((tab) => (
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
      <Outlet />
    </>
  );
}
