import { useRef, useState, type KeyboardEvent } from 'react';
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
  DropdownMenuTrigger,
  GenieChatMessageList,
  Skeleton,
  Spinner,
  Textarea,
  useGenieChat,
} from '@databricks/appkit-ui/react';
import { CATALOG_SETTING_KEY, GENIE_SPACE_SETTING_KEY } from '@finlake/shared';
import { AlertCircle, ExternalLink, MoreVertical, Send, Sparkles, Trash2 } from 'lucide-react';
import { PageHeader } from '../../components/PageHeader';
import { useAppSettings, useDeleteGenieSpace, useMe, useSetupGenieSpace } from '../../api/hooks';
import { useI18n } from '../../i18n';

export function Genie() {
  const { t } = useI18n();
  const settings = useAppSettings();
  const setup = useSetupGenieSpace();
  const deleteSpace = useDeleteGenieSpace();
  const me = useMe();
  const appSettings = settings.data?.settings ?? {};
  const genieSpaceId = appSettings[GENIE_SPACE_SETTING_KEY]?.trim() || '';
  const catalog = appSettings[CATALOG_SETTING_KEY]?.trim() || '';
  const workspaceUrl = me.data?.workspaceUrl ?? null;
  const genieSpaceUrl = genieSpaceId ? databricksGenieSpaceUrl(workspaceUrl, genieSpaceId) : null;

  return (
    <>
      <PageHeader
        title={t('explore.genie.title')}
        subtitle={t('explore.genie.desc')}
        actions={
          genieSpaceId ? (
            <div className="flex items-center gap-2">
              {genieSpaceUrl ? (
                <Button type="button" variant="secondary" size="sm" asChild>
                  <a href={genieSpaceUrl} target="_blank" rel="noreferrer noopener">
                    <ExternalLink className="size-4" aria-hidden="true" />
                    {t('genie.openInDatabricks')}
                  </a>
                </Button>
              ) : null}
              <GenieActions
                deletePending={deleteSpace.isPending}
                onDelete={() => {
                  if (!window.confirm(t('genie.confirmDelete'))) return;
                  deleteSpace.mutate();
                }}
              />
            </div>
          ) : null
        }
      />

      {settings.isLoading ? (
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

function GenieActions({
  deletePending,
  onDelete,
}: {
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
        <DropdownMenuItem onClick={onDelete} disabled={deletePending}>
          {deletePending ? <Spinner className="size-4" /> : <Trash2 className="size-4" />}
          <span>{deletePending ? t('genie.deleting') : t('genie.deleteAction')}</span>
        </DropdownMenuItem>
      </DropdownMenuContent>
    </DropdownMenu>
  );
}

function GenieChatSurface({ deleteError }: { deleteError: string | null }) {
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

      <FinLakeGenieChat
        alias="finlake"
        placeholder={t('genie.inputPlaceholder')}
        className="border-border bg-background h-[calc(100vh-176px)] min-h-[560px] overflow-hidden rounded-md border"
      />
    </>
  );
}

function FinLakeGenieChat({
  alias,
  placeholder,
  className,
}: {
  alias: string;
  placeholder: string;
  className?: string;
}) {
  const chat = useGenieChat({ alias });
  const { t } = useI18n();
  const [value, setValue] = useState('');
  const composingRef = useRef(false);
  const disabled = chat.status === 'streaming' || chat.status === 'loading-history';

  function send() {
    const trimmed = value.trim();
    if (!trimmed || disabled) return;
    chat.sendMessage(trimmed);
    setValue('');
  }

  function onKeyDown(event: KeyboardEvent<HTMLTextAreaElement>) {
    if (event.key !== 'Enter' || event.shiftKey) return;
    if (event.nativeEvent.isComposing || composingRef.current) return;
    event.preventDefault();
    send();
  }

  return (
    <div className={`flex flex-col ${className ?? ''}`}>
      {chat.messages.length > 0 ? (
        <div className="flex shrink-0 justify-end px-4 pt-3 pb-1">
          <Button
            type="button"
            variant="ghost"
            size="sm"
            onClick={chat.reset}
            className="text-muted-foreground text-xs"
          >
            {t('genie.newConversation')}
          </Button>
        </div>
      ) : null}

      <GenieChatMessageList
        messages={chat.messages}
        status={chat.status}
        hasPreviousPage={chat.hasPreviousPage}
        onFetchPreviousPage={chat.fetchPreviousPage}
      />

      {chat.error ? (
        <div className="bg-destructive/10 text-destructive shrink-0 border-t px-4 py-2 text-sm">
          {chat.error}
        </div>
      ) : null}

      <div className="flex shrink-0 gap-2 border-t p-4">
        <Textarea
          value={value}
          onChange={(event) => setValue(event.target.value)}
          onKeyDown={onKeyDown}
          onCompositionStart={() => {
            composingRef.current = true;
          }}
          onCompositionEnd={() => {
            composingRef.current = false;
          }}
          placeholder={placeholder}
          disabled={disabled}
          rows={1}
          className="max-h-48 min-h-10 flex-1 resize-none"
        />
        <Button
          type="button"
          onClick={send}
          disabled={disabled || !value.trim()}
          className="self-end"
        >
          <Send className="size-4" />
          {t('genie.send')}
        </Button>
      </div>
    </div>
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
