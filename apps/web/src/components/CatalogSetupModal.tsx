import { useI18n } from '../i18n';
import { CatalogSettingsForm } from './CatalogSettingsForm';

export function CatalogSetupModal() {
  const { t } = useI18n();

  return (
    <div
      className="fixed inset-0 z-[80] flex items-center justify-center bg-black/55 p-4"
      role="presentation"
    >
      <div
        role="dialog"
        aria-modal="true"
        aria-labelledby="catalog-setup-modal-title"
        className="bg-background border-border grid w-full max-w-2xl gap-4 rounded-lg border p-5 shadow-xl"
      >
        <div className="min-w-0">
          <h3 id="catalog-setup-modal-title" className="text-base font-semibold">
            {t('settings.setupModalTitle')}
          </h3>
          <p className="text-muted-foreground mt-1 mb-0 text-sm">{t('settings.setupModalDesc')}</p>
        </div>
        <CatalogSettingsForm variant="modal" />
      </div>
    </div>
  );
}
