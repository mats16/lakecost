import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from '@databricks/appkit-ui/react';
import { useI18n } from '../i18n';
import { CatalogSettingsForm } from './CatalogSettingsForm';

export function CatalogSetupModal() {
  const { t } = useI18n();

  return (
    <Dialog open modal>
      <DialogContent
        showCloseButton={false}
        className="sm:max-w-2xl"
        onEscapeKeyDown={(e) => e.preventDefault()}
        onPointerDownOutside={(e) => e.preventDefault()}
        onInteractOutside={(e) => e.preventDefault()}
      >
        <DialogHeader>
          <DialogTitle>{t('settings.setupModalTitle')}</DialogTitle>
          <DialogDescription>{t('settings.setupModalDesc')}</DialogDescription>
        </DialogHeader>
        <CatalogSettingsForm variant="modal" />
      </DialogContent>
    </Dialog>
  );
}
