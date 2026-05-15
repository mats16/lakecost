import { NavLink, Outlet, useLocation } from 'react-router-dom';
import { useI18n } from '../../i18n';
import { CONFIGURE, matchesPathPrefix } from '../../components/layout/AppShell';

export function ConfigureLayout() {
  const { t } = useI18n();
  const location = useLocation();
  return (
    <>
      <header className="page-header configure-header">
        <h2>{t('configure.title')}</h2>
        <nav className="upper-tabs">
          {CONFIGURE.items.map((tab) => (
            <NavLink
              key={tab.to}
              to={tab.to}
              className={({ isActive }) =>
                isActive || tab.activePrefixes?.some((p) => matchesPathPrefix(location.pathname, p))
                  ? 'active'
                  : ''
              }
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
