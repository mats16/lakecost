import {
  Alert,
  AlertDescription,
  AlertTitle,
  Button,
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
  Skeleton,
  Spinner,
} from '@databricks/appkit-ui/react';
import { CATALOG_SETTING_KEY, GENIE_SPACE_SETTING_KEY } from '@finlake/shared';
import { AlertCircle, ExternalLink, MoreVertical, Sparkles, Trash2 } from 'lucide-react';
import { PageHeader } from '../../components/PageHeader';
import { useAppSettings, useDeleteGenieSpace, useMe, useSetupGenieSpace } from '../../api/hooks';
import { useI18n } from '../../i18n';

export function Genie() {
  const { t, locale } = useI18n();
  const settings = useAppSettings();
  const setup = useSetupGenieSpace();
  const deleteSpace = useDeleteGenieSpace();
  const me = useMe();
  const appSettings = settings.data?.settings ?? {};
  const genieSpaceId = appSettings[GENIE_SPACE_SETTING_KEY]?.trim() || '';
  const catalog = appSettings[CATALOG_SETTING_KEY]?.trim() || '';
  const workspaceUrl = me.data?.workspaceUrl ?? null;
  const workspaceId = me.data?.workspaceId ?? null;
  const genieSpaceUrl = genieSpaceId ? databricksGenieSpaceUrl(workspaceUrl, genieSpaceId) : null;
  const genieEmbedUrl = genieSpaceId
    ? databricksGenieEmbedUrl(workspaceUrl, workspaceId, genieSpaceId, locale)
    : null;

  return (
    <>
      <PageHeader
        title={t('explore.genie.title')}
        subtitle={t('explore.genie.desc')}
        actions={
          genieSpaceId ? (
            <GenieActions
              genieSpaceUrl={genieSpaceUrl}
              deletePending={deleteSpace.isPending}
              onDelete={() => {
                if (!window.confirm(t('genie.confirmDelete'))) return;
                deleteSpace.mutate();
              }}
            />
          ) : null
        }
      />

      {settings.isLoading || (genieSpaceId && me.isLoading) ? (
        <Card>
          <CardContent>
            <div className="space-y-2">
              <Skeleton className="h-8 w-64" />
              <Skeleton className="h-20 w-full" />
            </div>
          </CardContent>
        </Card>
      ) : genieSpaceId ? (
        <GenieChatSurface
          genieEmbedUrl={genieEmbedUrl}
          deleteError={deleteSpace.error instanceof Error ? deleteSpace.error.message : null}
        />
      ) : (
        <GenieSetupCard
          catalog={catalog}
          setupError={setup.error instanceof Error ? setup.error.message : null}
          setupPending={setup.isPending}
          onSetup={() => setup.mutate()}
        />
      )}
    </>
  );
}

function databricksGenieSpaceUrl(workspaceUrl: string | null, spaceId: string): string | null {
  if (!workspaceUrl) return null;
  return `${workspaceUrl.replace(/\/$/, '')}/genie/rooms/${encodeURIComponent(spaceId)}`;
}

function databricksGenieEmbedUrl(
  workspaceUrl: string | null,
  workspaceId: string | null,
  spaceId: string,
  locale: string,
): string | null {
  if (!workspaceUrl || !workspaceId) return null;
  const url = new URL(
    `/embed/genie/rooms/${encodeURIComponent(spaceId)}`,
    workspaceUrl.replace(/\/$/, ''),
  );
  url.searchParams.set('o', workspaceId);
  url.searchParams.set('l', databricksEmbedLocale(locale));
  return url.toString();
}

function databricksEmbedLocale(locale: string): 'en' | 'ja-JP' {
  return locale === 'ja' ? 'ja-JP' : 'en';
}

function GenieActions({
  genieSpaceUrl,
  deletePending,
  onDelete,
}: {
  genieSpaceUrl: string | null;
  deletePending: boolean;
  onDelete: () => void;
}) {
  const { t } = useI18n();

  return (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <Button type="button" variant="secondary" size="sm" aria-label={t('genie.actions.open')}>
          <MoreVertical className="size-4" aria-hidden="true" />
        </Button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end">
        {genieSpaceUrl ? (
          <DropdownMenuItem asChild>
            <a href={genieSpaceUrl} target="_blank" rel="noreferrer noopener">
              <ExternalLink className="size-4" aria-hidden="true" />
              <span>{t('genie.openInDatabricks')}</span>
            </a>
          </DropdownMenuItem>
        ) : null}
        {genieSpaceUrl ? <DropdownMenuSeparator /> : null}
        <DropdownMenuItem
          className="text-(--warning) focus:text-(--warning)"
          onClick={onDelete}
          disabled={deletePending}
        >
          {deletePending ? <Spinner className="size-4" /> : <Trash2 className="size-4" />}
          <span>{deletePending ? t('genie.deleting') : t('genie.deleteAction')}</span>
        </DropdownMenuItem>
      </DropdownMenuContent>
    </DropdownMenu>
  );
}

function GenieChatSurface({
  genieEmbedUrl,
  deleteError,
}: {
  genieEmbedUrl: string | null;
  deleteError: string | null;
}) {
  const { t } = useI18n();

  return (
    <>
      {deleteError ? (
        <Alert variant="destructive" className="mb-3">
          <AlertCircle />
          <AlertTitle>{t('genie.deleteFailed')}</AlertTitle>
          <AlertDescription>{deleteError}</AlertDescription>
        </Alert>
      ) : null}

      {genieEmbedUrl ? (
        <div className="border-border bg-background h-[calc(100vh-176px)] min-h-[560px] overflow-hidden rounded-md border">
          <iframe
            src={genieEmbedUrl}
            title={t('genie.iframeTitle')}
            allow="clipboard-write"
            className="size-full border-0"
          />
        </div>
      ) : (
        <Alert>
          <AlertCircle />
          <AlertTitle>{t('genie.workspaceUrlMissingTitle')}</AlertTitle>
          <AlertDescription>{t('genie.workspaceUrlMissingDesc')}</AlertDescription>
        </Alert>
      )}
    </>
  );
}

function GenieSetupCard({
  catalog,
  setupError,
  setupPending,
  onSetup,
}: {
  catalog: string;
  setupError: string | null;
  setupPending: boolean;
  onSetup: () => void;
}) {
  const { t } = useI18n();

  return (
    <Card>
      <CardHeader>
        <div className="text-center">
          <CardTitle>{t('genie.setupTitle')}</CardTitle>
          <CardDescription>{t('genie.setupDesc')}</CardDescription>
        </div>
      </CardHeader>
      <CardContent>
        <div className="grid justify-items-center gap-4">
          {!catalog ? (
            <Alert>
              <AlertCircle />
              <AlertTitle>{t('genie.catalogMissingTitle')}</AlertTitle>
              <AlertDescription>{t('genie.catalogMissingDesc')}</AlertDescription>
            </Alert>
          ) : null}

          {setupError ? (
            <Alert variant="destructive">
              <AlertCircle />
              <AlertTitle>{t('genie.setupFailed')}</AlertTitle>
              <AlertDescription>{setupError}</AlertDescription>
            </Alert>
          ) : null}

          <Button type="button" onClick={onSetup} disabled={!catalog || setupPending}>
            {setupPending ? <Spinner /> : <Sparkles />}
            {setupPending ? t('genie.settingUp') : t('genie.setupAction')}
          </Button>
        </div>
      </CardContent>
    </Card>
  );
}
