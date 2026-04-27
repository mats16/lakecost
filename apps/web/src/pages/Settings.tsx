import { useEffect, useState, type FormEvent } from 'react';
import { PageHeader } from '../components/PageHeader';
import { useI18n } from '../i18n';
import { useAppSettings, useUpdateAppSettings } from '../api/hooks';

const MAIN_CATALOG_KEY = 'catalog_name';

export function Settings() {
  const { t } = useI18n();
  const settings = useAppSettings();
  const updateSettings = useUpdateAppSettings();

  const remoteCatalog = settings.data?.settings[MAIN_CATALOG_KEY] ?? '';
  const [catalog, setCatalog] = useState(remoteCatalog);
  const [savedAt, setSavedAt] = useState<number | null>(null);

  useEffect(() => {
    setCatalog(remoteCatalog);
  }, [remoteCatalog]);

  const onSubmit = (e: FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    updateSettings.mutate(
      { [MAIN_CATALOG_KEY]: catalog.trim() },
      { onSuccess: () => setSavedAt(Date.now()) },
    );
  };

  const dirty = catalog.trim() !== remoteCatalog;
  const saving = updateSettings.isPending;
  const errorMessage =
    updateSettings.error && typeof updateSettings.error === 'object'
      ? ((updateSettings.error as { message?: string }).message ?? null)
      : null;

  return (
    <>
      <PageHeader title={t('settings.title')} subtitle={t('settings.subtitle')} />
      <form className="card" style={{ marginBottom: 16 }} onSubmit={onSubmit}>
        <h3 style={{ marginTop: 0, fontSize: 14, color: 'var(--muted)' }}>
          {t('settings.mainCatalogHeading')}
        </h3>
        <p style={{ color: 'var(--muted)', fontSize: 13, marginTop: 0 }}>
          {t('settings.mainCatalogDesc')}
        </p>
        <input
          type="text"
          value={catalog}
          placeholder={t('settings.mainCatalogPlaceholder')}
          onChange={(e) => setCatalog(e.target.value)}
          disabled={settings.isLoading || saving}
          style={{
            width: '100%',
            maxWidth: 420,
            padding: '8px 10px',
            border: '1px solid var(--border)',
            borderRadius: 6,
            background: 'var(--bg)',
            color: 'var(--fg)',
          }}
        />
        <div style={{ marginTop: 12, display: 'flex', alignItems: 'center', gap: 12 }}>
          <button type="submit" disabled={!dirty || saving}>
            {saving ? t('common.saving') : t('settings.save')}
          </button>
          {savedAt && !dirty && !saving ? (
            <span style={{ color: 'var(--muted)', fontSize: 12 }}>{t('settings.saved')}</span>
          ) : null}
          {errorMessage ? (
            <span style={{ color: 'var(--danger, #c33)', fontSize: 12 }}>{errorMessage}</span>
          ) : null}
        </div>
      </form>
      <div className="card">
        <p style={{ color: 'var(--muted)', fontSize: 13 }}>{t('settings.body')}</p>
      </div>
    </>
  );
}
