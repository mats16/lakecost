import type { ReactNode } from 'react';
import { Outlet } from 'react-router-dom';
import { useI18n } from '../../i18n';

export function ConfigureLayout({ children }: { children?: ReactNode }) {
  const { t } = useI18n();
  return (
    <>
      <header className="page-header configure-header">
        <h2>{t('configure.title')}</h2>
      </header>
      {children ?? <Outlet />}
    </>
  );
}
