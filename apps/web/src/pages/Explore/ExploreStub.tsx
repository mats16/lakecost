import {
  Alert,
  AlertDescription,
  AlertTitle,
  Card,
  CardContent,
} from '@databricks/appkit-ui/react';
import { Construction } from 'lucide-react';
import { PageHeader } from '../../components/PageHeader';
import { useI18n } from '../../i18n';

export function ExploreStub({ titleKey, descKey }: { titleKey: string; descKey: string }) {
  const { t } = useI18n();

  return (
    <>
      <PageHeader title={t(titleKey)} subtitle={t(descKey)} />
      <Card>
        <CardContent>
          <Alert>
            <Construction />
            <AlertTitle>{t('explore.underConstructionTitle')}</AlertTitle>
            <AlertDescription>{t('explore.underConstructionDesc')}</AlertDescription>
          </Alert>
        </CardContent>
      </Card>
    </>
  );
}
