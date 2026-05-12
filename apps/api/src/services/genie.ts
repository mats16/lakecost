import { settingsToRecord, type DatabaseClient } from '@finlake/db';
import {
  CATALOG_SETTING_KEY,
  DataSourceIdentifierSchema,
  GENIE_SPACE_SETTING_KEY,
  medallionSchemaNamesFromSettings,
  type Env,
  type GenieChatResponse,
  type GenieSetupResponse,
} from '@finlake/shared';
import { fetchServicePrincipalToken } from '../auth/appServicePrincipal.js';
import { sleep } from '../utils/sleep.js';
import {
  genieMessageError,
  toGenieStreamMessage,
  type GenieMessageResponse,
  type GenieStreamEvent,
  type GenieStreamMessage,
} from './genieUtils.js';
import {
  getGenieAttachmentStatementResponse,
  normalizeGenieAttachments,
} from './genieAttachments.js';
import {
  createGenieMessage,
  createGenieSpace,
  getGenieMessage,
  listGenieConversationMessages,
  trashGenieSpace,
} from './genieClient.js';
import { normalizeHost } from './normalizeHost.js';
import { WorkspaceServiceError } from './workspaceClientErrors.js';

export type { GenieStreamEvent } from './genieUtils.js';
export class GenieServiceError extends WorkspaceServiceError {}

const GENIE_SPACE_TITLE = 'FinOps Agent';
const GENIE_SPACE_PARENT_PATH = '/Workspace/Shared';

const GENIE_POLL_TIMEOUT_MS = 2 * 60 * 1000;
const GENIE_POLL_INITIAL_DELAY_MS = 1_000;
const GENIE_POLL_MAX_DELAY_MS = 5_000;
const GENIE_TERMINAL_STATUSES = new Set(['COMPLETED', 'FAILED', 'CANCELLED', 'CANCELED']);

export async function setupFinLakeGenieSpace(
  env: Env,
  db: DatabaseClient,
): Promise<GenieSetupResponse> {
  const settings = settingsToRecord(await db.repos.appSettings.list());
  const existingSpaceId = settings[GENIE_SPACE_SETTING_KEY]?.trim();
  const medallionSchemas = medallionSchemaNamesFromSettings(settings);
  const catalog = settings[CATALOG_SETTING_KEY]?.trim() ?? '';
  if (!catalog) {
    throw new GenieServiceError(
      'Main catalog not configured. Set catalog_name in Catalog first.',
      400,
    );
  }

  const tables = [
    `${catalog}.${medallionSchemas.silver}.usage`,
    `${catalog}.${medallionSchemas.gold}.daily_usage`,
  ];
  if (existingSpaceId) {
    return { spaceId: existingSpaceId, title: GENIE_SPACE_TITLE, tableIdentifiers: tables };
  }

  for (const part of [catalog, medallionSchemas.silver, medallionSchemas.gold]) {
    const parsed = DataSourceIdentifierSchema.safeParse(part);
    if (!parsed.success) {
      throw new GenieServiceError(
        `Catalog and schema names must be simple Unity Catalog identifiers for Genie setup: ${part}`,
        400,
      );
    }
  }

  const host = normalizeHost(env.DATABRICKS_HOST);
  if (!host || !env.SQL_WAREHOUSE_ID) {
    throw new GenieServiceError('DATABRICKS_HOST and SQL_WAREHOUSE_ID are required.', 500);
  }

  const token = await fetchServicePrincipalToken(host, env, GenieServiceError);
  const space = await createGenieSpace(
    host,
    token,
    {
      title: GENIE_SPACE_TITLE,
      description: 'FinLake Genie Space for exploring usage and daily cost facts.',
      warehouseId: env.SQL_WAREHOUSE_ID,
      parentPath: GENIE_SPACE_PARENT_PATH,
      serializedSpace: buildSerializedSpace(tables),
    },
    GenieServiceError,
  );

  const spaceId = space.space_id?.trim();
  if (!spaceId) {
    throw new GenieServiceError('Create Genie Space returned no space_id.', 502);
  }

  await db.repos.appSettings.upsert(GENIE_SPACE_SETTING_KEY, spaceId);
  return { spaceId, title: space.title?.trim() || GENIE_SPACE_TITLE, tableIdentifiers: tables };
}

export async function deleteFinLakeGenieSpace(env: Env, db: DatabaseClient): Promise<void> {
  const settings = settingsToRecord(await db.repos.appSettings.list());
  const spaceId = settings[GENIE_SPACE_SETTING_KEY]?.trim();
  if (!spaceId) return;

  const host = normalizeHost(env.DATABRICKS_HOST);
  if (!host) {
    throw new GenieServiceError('DATABRICKS_HOST is required.', 500);
  }

  const token = await fetchServicePrincipalToken(host, env, GenieServiceError);
  await trashGenieSpace(host, token, spaceId, GenieServiceError);
  await db.repos.appSettings.delete(GENIE_SPACE_SETTING_KEY);
}

export async function askFinLakeGenie(
  env: Env,
  db: DatabaseClient,
  opts: {
    content: string;
    conversationId?: string;
    userAccessToken?: string;
  },
): Promise<GenieChatResponse> {
  const settings = settingsToRecord(await db.repos.appSettings.list());
  const spaceId = settings[GENIE_SPACE_SETTING_KEY]?.trim();
  if (!spaceId) {
    throw new GenieServiceError('Genie Space has not been configured yet.', 400);
  }

  const host = normalizeHost(env.DATABRICKS_HOST);
  if (!host) {
    throw new GenieServiceError('DATABRICKS_HOST is required.', 500);
  }

  const userToken = opts.userAccessToken?.trim();
  const token = userToken || (await fetchServicePrincipalToken(host, env, GenieServiceError));
  const authMode = userToken ? 'obo' : 'service_principal';
  const started = await createGenieMessage(
    host,
    token,
    spaceId,
    {
      content: opts.content,
      conversationId: opts.conversationId,
    },
    GenieServiceError,
  );

  const startedMessage = started.message ?? started;
  const conversationId =
    startedMessage.conversation_id?.trim() ||
    started.conversation?.id?.trim() ||
    opts.conversationId?.trim();
  const messageId = startedMessage.message_id?.trim() || startedMessage.id?.trim();
  if (!conversationId || !messageId) {
    throw new GenieServiceError(
      'Genie response did not include a conversation or message id.',
      502,
    );
  }

  const message = await pollGenieMessage(host, token, spaceId, conversationId, messageId);
  const status = message.status?.trim() || 'UNKNOWN';
  const normalizedStatus = status.toUpperCase();
  if (normalizedStatus !== 'COMPLETED') {
    const detail = genieMessageError(message) ?? status;
    throw new GenieServiceError(`Genie message did not complete: ${detail}`, 502);
  }

  const attachments = await normalizeGenieAttachments(
    host,
    token,
    spaceId,
    conversationId,
    messageId,
    message.attachments ?? [],
  );
  const answer = attachments.map((attachment) => attachment.text).find(Boolean) ?? null;
  return {
    conversationId,
    messageId,
    status,
    answer,
    attachments,
    authMode,
  };
}

export async function streamFinLakeGenieMessage(
  env: Env,
  db: DatabaseClient,
  opts: {
    content: string;
    conversationId?: string;
    userAccessToken?: string;
    emit: (event: GenieStreamEvent) => void;
  },
): Promise<void> {
  const settings = settingsToRecord(await db.repos.appSettings.list());
  const spaceId = settings[GENIE_SPACE_SETTING_KEY]?.trim();
  if (!spaceId) {
    throw new GenieServiceError('Genie Space has not been configured yet.', 400);
  }

  const host = normalizeHost(env.DATABRICKS_HOST);
  if (!host) {
    throw new GenieServiceError('DATABRICKS_HOST is required.', 500);
  }

  const userToken = opts.userAccessToken?.trim();
  const token = userToken || (await fetchServicePrincipalToken(host, env, GenieServiceError));
  const started = await createGenieMessage(
    host,
    token,
    spaceId,
    {
      content: opts.content,
      conversationId: opts.conversationId,
    },
    GenieServiceError,
  );
  const startedMessage = started.message ?? started;
  const conversationId =
    startedMessage.conversation_id?.trim() ||
    started.conversation?.id?.trim() ||
    opts.conversationId?.trim();
  const messageId = startedMessage.message_id?.trim() || startedMessage.id?.trim();
  if (!conversationId || !messageId) {
    throw new GenieServiceError(
      'Genie response did not include a conversation or message id.',
      502,
    );
  }

  opts.emit({ type: 'message_start', conversationId, messageId, spaceId });

  let message = startedMessage;
  let delay = GENIE_POLL_INITIAL_DELAY_MS;
  const deadline = Date.now() + GENIE_POLL_TIMEOUT_MS;
  while (!GENIE_TERMINAL_STATUSES.has((message.status ?? '').toUpperCase())) {
    if (message.status) {
      opts.emit({ type: 'status', status: message.status });
    }
    if (Date.now() > deadline) {
      throw new GenieServiceError('Genie response timed out. Try a narrower question.', 504);
    }
    await sleep(delay);
    delay = Math.min(Math.round(delay * 1.5), GENIE_POLL_MAX_DELAY_MS);
    message = await getGenieMessage(
      host,
      token,
      spaceId,
      conversationId,
      messageId,
      GenieServiceError,
    );
  }

  const status = message.status?.trim() || 'UNKNOWN';
  opts.emit({ type: 'status', status });
  const streamMessage = toGenieStreamMessage(message, {
    conversationId,
    messageId,
    spaceId,
  });
  opts.emit({
    type: 'message_result',
    message: {
      ...streamMessage,
      status,
      content: streamMessage.content || opts.content,
    },
  });
  await emitQueryResultsForMessage(
    { host, token, spaceId },
    conversationId,
    streamMessage,
    opts.emit,
  );
}

export async function streamFinLakeGenieConversation(
  env: Env,
  db: DatabaseClient,
  opts: {
    conversationId: string;
    pageToken?: string;
    includeQueryResults?: boolean;
    userAccessToken?: string;
    emit: (event: GenieStreamEvent) => void;
  },
): Promise<void> {
  const context = await resolveGenieContext(env, db, opts.userAccessToken);
  const page = await listGenieConversationMessages(
    context.host,
    context.token,
    context.spaceId,
    opts.conversationId,
    opts.pageToken,
    GenieServiceError,
  );
  const messages = page.messages.reverse().map((message) => toGenieStreamMessage(message));

  for (const message of messages) {
    opts.emit({ type: 'message_result', message });
  }
  opts.emit({
    type: 'history_info',
    conversationId: opts.conversationId,
    spaceId: context.spaceId,
    nextPageToken: page.nextPageToken,
    loadedCount: messages.length,
  });

  if (opts.includeQueryResults === false) return;
  for (const message of messages) {
    await emitQueryResultsForMessage(context, opts.conversationId, message, opts.emit);
  }
}

export async function streamFinLakeGenieExistingMessage(
  env: Env,
  db: DatabaseClient,
  opts: {
    conversationId: string;
    messageId: string;
    userAccessToken?: string;
    emit: (event: GenieStreamEvent) => void;
  },
): Promise<void> {
  const context = await resolveGenieContext(env, db, opts.userAccessToken);
  const message = await pollGenieMessage(
    context.host,
    context.token,
    context.spaceId,
    opts.conversationId,
    opts.messageId,
  );
  const streamMessage = toGenieStreamMessage(message, {
    conversationId: opts.conversationId,
    messageId: opts.messageId,
    spaceId: context.spaceId,
  });
  opts.emit({ type: 'message_result', message: streamMessage });
  await emitQueryResultsForMessage(context, opts.conversationId, streamMessage, opts.emit);
}

async function resolveGenieContext(
  env: Env,
  db: DatabaseClient,
  userAccessToken: string | undefined,
): Promise<{ host: string; token: string; spaceId: string }> {
  const settings = settingsToRecord(await db.repos.appSettings.list());
  const spaceId = settings[GENIE_SPACE_SETTING_KEY]?.trim();
  if (!spaceId) {
    throw new GenieServiceError('Genie Space has not been configured yet.', 400);
  }
  const host = normalizeHost(env.DATABRICKS_HOST);
  if (!host) {
    throw new GenieServiceError('DATABRICKS_HOST is required.', 500);
  }
  const userToken = userAccessToken?.trim();
  return {
    host,
    spaceId,
    token: userToken || (await fetchServicePrincipalToken(host, env, GenieServiceError)),
  };
}

async function pollGenieMessage(
  host: string,
  token: string,
  spaceId: string,
  conversationId: string,
  messageId: string,
): Promise<GenieMessageResponse> {
  let message = await getGenieMessage(
    host,
    token,
    spaceId,
    conversationId,
    messageId,
    GenieServiceError,
  );
  let delay = GENIE_POLL_INITIAL_DELAY_MS;
  const deadline = Date.now() + GENIE_POLL_TIMEOUT_MS;

  while (!GENIE_TERMINAL_STATUSES.has((message.status ?? '').toUpperCase())) {
    if (Date.now() > deadline) {
      throw new GenieServiceError('Genie response timed out. Try a narrower question.', 504);
    }
    await sleep(delay);
    delay = Math.min(Math.round(delay * 1.5), GENIE_POLL_MAX_DELAY_MS);
    message = await getGenieMessage(
      host,
      token,
      spaceId,
      conversationId,
      messageId,
      GenieServiceError,
    );
  }

  return message;
}

function buildSerializedSpace(tableIdentifiers: string[]) {
  const sortedTableIdentifiers = [...tableIdentifiers].sort((a, b) => a.localeCompare(b));

  return {
    version: 2,
    config: {
      sample_questions: [
        {
          id: '01f1a100000000000000000000000001',
          question: ['What was total cost last month?'],
        },
        {
          id: '01f1a100000000000000000000000002',
          question: ['Show daily usage cost by provider for the last 30 days.'],
        },
      ],
    },
    data_sources: {
      tables: sortedTableIdentifiers.map((identifier) => ({
        identifier,
        description: [descriptionForTable(identifier)],
      })),
    },
    instructions: {
      text_instructions: [
        {
          id: '01f1a100000000000000000000000003',
          content: [
            'Use the gold daily_usage table for trend and cost summary questions. ',
            'Use the silver usage table when the user asks for record-level usage details. ',
            'Treat cost values as USD unless the user explicitly asks otherwise.',
          ],
        },
      ],
    },
  };
}

function descriptionForTable(identifier: string): string {
  if (identifier.endsWith('.daily_usage')) {
    return 'Gold daily usage cost facts aggregated for FinOps analysis.';
  }
  return 'Silver usage facts normalized for FinLake exploration.';
}

async function emitQueryResultsForMessage(
  context: { host: string; token: string; spaceId: string },
  conversationId: string,
  message: GenieStreamMessage,
  emit: (event: GenieStreamEvent) => void,
): Promise<void> {
  for (const attachment of message.attachments ?? []) {
    const attachmentId = attachment.attachmentId;
    const statementId = attachment.query?.statementId;
    if (!attachmentId || !statementId) continue;
    const statement = await getGenieAttachmentStatementResponse(
      context.host,
      context.token,
      context.spaceId,
      conversationId,
      message.messageId,
      attachmentId,
    );
    if (!statement) continue;
    emit({
      type: 'query_result',
      attachmentId,
      statementId,
      data: statement,
    });
  }
}
