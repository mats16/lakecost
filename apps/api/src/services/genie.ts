import { Buffer } from 'node:buffer';
import { settingsToRecord, type DatabaseClient } from '@finlake/db';
import {
  CATALOG_SETTING_KEY,
  DataSourceIdentifierSchema,
  GENIE_SPACE_SETTING_KEY,
  medallionSchemaNamesFromSettings,
  type Env,
  type GenieAttachment,
  type GenieChatResponse,
  type GenieSetupResponse,
} from '@finlake/shared';
import { logger } from '../config/logger.js';
import { sleep } from '../utils/sleep.js';
import { normalizeHost } from './normalizeHost.js';
import { WorkspaceServiceError, isPermissionDenied } from './workspaceClientErrors.js';

export class GenieServiceError extends WorkspaceServiceError {}

const GENIE_SPACE_TITLE = 'FinOps Agent';
const GENIE_SPACE_PARENT_PATH = '/Workspace/Shared';

interface CreateGenieSpaceResponse {
  space_id?: string;
  title?: string;
}

interface OAuthTokenResponse {
  access_token?: string;
  token_type?: string;
  expires_in?: number;
}

interface GenieConversationResponse {
  id?: string;
  conversation_id?: string;
  message_id?: string;
  space_id?: string;
  status?: string;
  content?: string;
  attachments?: unknown[] | null;
  error?: string | { message?: string; error_code?: string } | null;
  conversation?: {
    id?: string;
  };
  message?: GenieMessageResponse;
}

interface GenieMessageResponse {
  id?: string;
  message_id?: string;
  conversation_id?: string;
  space_id?: string;
  status?: string;
  content?: string;
  attachments?: unknown[] | null;
  error?: string | { message?: string; error_code?: string } | null;
}

interface GenieStreamMessage {
  messageId: string;
  conversationId: string;
  spaceId: string;
  status: string;
  content: string;
  attachments?: GenieStreamAttachment[];
  error?: string;
}

interface GenieStreamAttachment {
  attachmentId?: string;
  query?: {
    title?: string;
    description?: string;
    query?: string;
    statementId?: string;
  };
  text?: {
    content?: string;
  };
}

interface GenieStatementResponse {
  manifest: {
    schema: {
      columns: Array<{
        name: string;
        type_name: string;
      }>;
    };
  };
  result: {
    data_array: (string | null)[][];
  };
}

export type GenieStreamEvent =
  | { type: 'message_start'; conversationId: string; messageId: string; spaceId: string }
  | { type: 'status'; status: string }
  | { type: 'message_result'; message: GenieStreamMessage }
  | {
      type: 'query_result';
      attachmentId: string;
      statementId: string;
      data: GenieStatementResponse;
    }
  | { type: 'error'; error: string }
  | {
      type: 'history_info';
      conversationId: string;
      spaceId: string;
      nextPageToken: string | null;
      loadedCount: number;
    };

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

  const token = await fetchServicePrincipalToken(host, env);
  const space = await createGenieSpace(host, token, {
    title: GENIE_SPACE_TITLE,
    description: 'FinLake Genie Space for exploring usage and daily cost facts.',
    warehouseId: env.SQL_WAREHOUSE_ID,
    parentPath: GENIE_SPACE_PARENT_PATH,
    tableIdentifiers: tables,
  });

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

  const token = await fetchServicePrincipalToken(host, env);
  await trashGenieSpace(host, token, spaceId);
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
  const token = userToken || (await fetchServicePrincipalToken(host, env));
  const authMode = userToken ? 'obo' : 'service_principal';
  const started = await createGenieMessage(host, token, spaceId, {
    content: opts.content,
    conversationId: opts.conversationId,
  });

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
  const token = userToken || (await fetchServicePrincipalToken(host, env));
  const started = await createGenieMessage(host, token, spaceId, {
    content: opts.content,
    conversationId: opts.conversationId,
  });
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
    message = await getGenieMessage(host, token, spaceId, conversationId, messageId);
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

async function createGenieSpace(
  host: string,
  token: string,
  opts: {
    title: string;
    description: string;
    warehouseId: string;
    parentPath: string;
    tableIdentifiers: string[];
  },
): Promise<CreateGenieSpaceResponse> {
  const body = {
    title: opts.title,
    description: opts.description,
    warehouse_id: opts.warehouseId,
    parent_path: opts.parentPath,
    serialized_space: JSON.stringify(buildSerializedSpace(opts.tableIdentifiers)),
  };

  let response: Response;
  try {
    response = await fetch(`${host}/api/2.0/genie/spaces`, {
      method: 'POST',
      headers: {
        authorization: `Bearer ${token}`,
        'content-type': 'application/json',
      },
      body: JSON.stringify(body),
    });
  } catch (err) {
    logger.error({ err }, 'Create Genie Space request failed');
    throw new GenieServiceError(`Failed to create Genie Space: ${(err as Error).message}`, 502);
  }

  if (!response.ok) {
    const message = await databricksErrorMessage(response);
    logger.warn({ status: response.status, message }, 'Create Genie Space failed');
    throw new GenieServiceError(
      `Failed to create Genie Space: ${message}`,
      mapDatabricksStatusCode(response, message),
    );
  }

  return (await response.json()) as CreateGenieSpaceResponse;
}

async function createGenieMessage(
  host: string,
  token: string,
  spaceId: string,
  opts: {
    content: string;
    conversationId?: string;
  },
): Promise<GenieConversationResponse> {
  const encodedSpaceId = encodeURIComponent(spaceId);
  const conversationId = opts.conversationId?.trim();
  const path = conversationId
    ? `/api/2.0/genie/spaces/${encodedSpaceId}/conversations/${encodeURIComponent(conversationId)}/messages`
    : `/api/2.0/genie/spaces/${encodedSpaceId}/start-conversation`;

  let response: Response;
  try {
    response = await fetch(`${host}${path}`, {
      method: 'POST',
      headers: {
        authorization: `Bearer ${token}`,
        'content-type': 'application/json',
      },
      body: JSON.stringify({ content: opts.content }),
    });
  } catch (err) {
    logger.error({ err, spaceId, conversationId }, 'Create Genie message request failed');
    throw new GenieServiceError(`Failed to ask Genie: ${(err as Error).message}`, 502);
  }

  if (!response.ok) {
    const message = await databricksErrorMessage(response);
    logger.warn({ status: response.status, message, spaceId, conversationId }, 'Ask Genie failed');
    throw new GenieServiceError(
      `Failed to ask Genie: ${message}`,
      mapDatabricksStatusCode(response, message),
    );
  }

  return (await response.json()) as GenieConversationResponse;
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
    token: userToken || (await fetchServicePrincipalToken(host, env)),
  };
}

async function listGenieConversationMessages(
  host: string,
  token: string,
  spaceId: string,
  conversationId: string,
  pageToken: string | undefined,
): Promise<{ messages: GenieMessageResponse[]; nextPageToken: string | null }> {
  const params = new URLSearchParams({ page_size: '50' });
  if (pageToken) params.set('page_token', pageToken);
  let response: Response;
  try {
    response = await fetch(
      `${host}/api/2.0/genie/spaces/${encodeURIComponent(spaceId)}/conversations/${encodeURIComponent(conversationId)}/messages?${params}`,
      {
        headers: {
          authorization: `Bearer ${token}`,
        },
      },
    );
  } catch (err) {
    logger.error({ err, spaceId, conversationId }, 'List Genie messages request failed');
    throw new GenieServiceError(
      `Failed to load Genie conversation: ${(err as Error).message}`,
      502,
    );
  }

  if (!response.ok) {
    const message = await databricksErrorMessage(response);
    logger.warn(
      { status: response.status, message, spaceId, conversationId },
      'List Genie messages failed',
    );
    throw new GenieServiceError(
      `Failed to load Genie conversation: ${message}`,
      mapDatabricksStatusCode(response, message),
    );
  }

  const body = (await response.json()) as {
    messages?: GenieMessageResponse[];
    next_page_token?: string;
  };
  return {
    messages: body.messages ?? [],
    nextPageToken: body.next_page_token ?? null,
  };
}

async function pollGenieMessage(
  host: string,
  token: string,
  spaceId: string,
  conversationId: string,
  messageId: string,
): Promise<GenieMessageResponse> {
  let message = await getGenieMessage(host, token, spaceId, conversationId, messageId);
  let delay = GENIE_POLL_INITIAL_DELAY_MS;
  const deadline = Date.now() + GENIE_POLL_TIMEOUT_MS;

  while (!GENIE_TERMINAL_STATUSES.has((message.status ?? '').toUpperCase())) {
    if (Date.now() > deadline) {
      throw new GenieServiceError('Genie response timed out. Try a narrower question.', 504);
    }
    await sleep(delay);
    delay = Math.min(Math.round(delay * 1.5), GENIE_POLL_MAX_DELAY_MS);
    message = await getGenieMessage(host, token, spaceId, conversationId, messageId);
  }

  return message;
}

async function getGenieMessage(
  host: string,
  token: string,
  spaceId: string,
  conversationId: string,
  messageId: string,
): Promise<GenieMessageResponse> {
  let response: Response;
  try {
    response = await fetch(
      `${host}/api/2.0/genie/spaces/${encodeURIComponent(spaceId)}/conversations/${encodeURIComponent(conversationId)}/messages/${encodeURIComponent(messageId)}`,
      {
        headers: {
          authorization: `Bearer ${token}`,
        },
      },
    );
  } catch (err) {
    logger.error({ err, spaceId, conversationId, messageId }, 'Get Genie message request failed');
    throw new GenieServiceError(`Failed to poll Genie: ${(err as Error).message}`, 502);
  }

  if (!response.ok) {
    const message = await databricksErrorMessage(response);
    logger.warn(
      { status: response.status, message, spaceId, conversationId, messageId },
      'Poll Genie failed',
    );
    throw new GenieServiceError(
      `Failed to poll Genie: ${message}`,
      mapDatabricksStatusCode(response, message),
    );
  }

  return (await response.json()) as GenieMessageResponse;
}

async function normalizeGenieAttachments(
  host: string,
  token: string,
  spaceId: string,
  conversationId: string,
  messageId: string,
  attachments: unknown[],
): Promise<GenieAttachment[]> {
  const items = attachments.map((attachment) => {
    const record = asRecord(attachment);
    const id = textValue(record.attachment_id) ?? textValue(record.id);
    return {
      id,
      text: textValue(record.text),
      sql: sqlValue(record.query),
    };
  });

  const queryResults = await Promise.all(
    items.map((item) =>
      item.id && item.sql
        ? getGenieAttachmentQueryResult(host, token, spaceId, conversationId, messageId, item.id)
        : Promise.resolve(null),
    ),
  );

  return items.map(
    (item, i): GenieAttachment => ({
      id: item.id,
      text: item.text,
      sql: item.sql,
      queryResult: queryResults[i] ?? null,
    }),
  );
}

async function fetchGenieAttachmentRaw(
  host: string,
  token: string,
  spaceId: string,
  conversationId: string,
  messageId: string,
  attachmentId: string,
): Promise<unknown | null> {
  let response: Response;
  try {
    response = await fetch(
      `${host}/api/2.0/genie/spaces/${encodeURIComponent(spaceId)}/conversations/${encodeURIComponent(conversationId)}/messages/${encodeURIComponent(messageId)}/attachments/${encodeURIComponent(attachmentId)}/query-result`,
      {
        headers: {
          authorization: `Bearer ${token}`,
        },
      },
    );
  } catch (err) {
    logger.warn({ err, attachmentId }, 'Get Genie query result request failed');
    return null;
  }

  if (!response.ok) {
    const message = await databricksErrorMessage(response);
    logger.warn(
      { status: response.status, message, attachmentId },
      'Get Genie query result failed',
    );
    return null;
  }

  return response.json();
}

async function getGenieAttachmentQueryResult(
  host: string,
  token: string,
  spaceId: string,
  conversationId: string,
  messageId: string,
  attachmentId: string,
): Promise<GenieAttachment['queryResult']> {
  const body = await fetchGenieAttachmentRaw(
    host,
    token,
    spaceId,
    conversationId,
    messageId,
    attachmentId,
  );
  return body ? normalizeQueryResult(body) : null;
}

async function getGenieAttachmentStatementResponse(
  host: string,
  token: string,
  spaceId: string,
  conversationId: string,
  messageId: string,
  attachmentId: string,
): Promise<GenieStatementResponse | null> {
  const body = await fetchGenieAttachmentRaw(
    host,
    token,
    spaceId,
    conversationId,
    messageId,
    attachmentId,
  );
  return body ? normalizeStatementResponse(body) : null;
}

async function trashGenieSpace(host: string, token: string, spaceId: string): Promise<void> {
  let response: Response;
  try {
    response = await fetch(`${host}/api/2.0/genie/spaces/${encodeURIComponent(spaceId)}`, {
      method: 'DELETE',
      headers: {
        authorization: `Bearer ${token}`,
      },
    });
  } catch (err) {
    logger.error({ err, spaceId }, 'Delete Genie Space request failed');
    throw new GenieServiceError(`Failed to delete Genie Space: ${(err as Error).message}`, 502);
  }

  if (response.status === 404) {
    logger.warn({ spaceId }, 'Genie Space was not found during delete; clearing app setting');
    return;
  }
  if (!response.ok) {
    const message = await databricksErrorMessage(response);
    logger.warn({ status: response.status, message, spaceId }, 'Delete Genie Space failed');
    throw new GenieServiceError(
      `Failed to delete Genie Space: ${message}`,
      mapDatabricksStatusCode(response, message),
    );
  }
}

let cachedSpToken: { token: string; expiresAt: number } | null = null;

async function fetchServicePrincipalToken(host: string, env: Env): Promise<string> {
  if (cachedSpToken && Date.now() < cachedSpToken.expiresAt) {
    return cachedSpToken.token;
  }
  const clientId = env.DATABRICKS_CLIENT_ID;
  const clientSecret = env.DATABRICKS_CLIENT_SECRET;
  if (!clientId || !clientSecret) {
    throw new GenieServiceError(
      'Databricks service principal credentials are not configured.',
      500,
    );
  }

  let response: Response;
  try {
    response = await fetch(`${host}/oidc/v1/token`, {
      method: 'POST',
      headers: {
        authorization: `Basic ${Buffer.from(`${clientId}:${clientSecret}`).toString('base64')}`,
        'content-type': 'application/x-www-form-urlencoded',
      },
      body: new URLSearchParams({
        grant_type: 'client_credentials',
        scope: 'all-apis',
      }).toString(),
    });
  } catch (err) {
    logger.error({ err }, 'Databricks service principal token request failed');
    throw new GenieServiceError(
      `Failed to get service principal OAuth token: ${(err as Error).message}`,
      502,
    );
  }

  if (!response.ok) {
    const message = await databricksErrorMessage(response);
    logger.warn({ status: response.status, message }, 'Databricks service principal token failed');
    throw new GenieServiceError(
      `Failed to get service principal OAuth token: ${message}`,
      response.status === 401 ? 401 : response.status === 403 ? 403 : 502,
    );
  }

  const body = (await response.json()) as OAuthTokenResponse;
  const token = body.access_token?.trim();
  if (!token) {
    throw new GenieServiceError(
      'Databricks service principal token response had no access_token.',
      502,
    );
  }
  const expiresIn = body.expires_in ?? 3600;
  cachedSpToken = { token, expiresAt: Date.now() + (expiresIn - 60) * 1000 };
  return token;
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

function genieMessageError(message: GenieMessageResponse): string | undefined {
  if (!message.error) return undefined;
  if (typeof message.error === 'string') return message.error;
  return message.error.message ?? message.error.error_code;
}

function normalizeQueryResult(body: unknown): GenieAttachment['queryResult'] {
  const root = asRecord(body);
  const statement = asRecord(root.statement_response) ?? root;
  const manifest = asRecord(statement.manifest);
  const schema = asRecord(manifest.schema);
  const columnsRaw = Array.isArray(schema.columns) ? schema.columns : [];
  const columns = columnsRaw
    .map((column, index) => textValue(asRecord(column)?.name) ?? `column_${index + 1}`)
    .slice(0, 50);

  const result = asRecord(statement.result) ?? asRecord(root.result);
  const dataArray = Array.isArray(result?.data_array) ? result.data_array : [];
  const rows = dataArray
    .filter((row): row is unknown[] => Array.isArray(row))
    .map((row) => row.slice(0, columns.length || 50))
    .slice(0, 25);

  if (columns.length === 0 && rows.length === 0) return null;
  return { columns, rows };
}

function normalizeStatementResponse(body: unknown): GenieStatementResponse | null {
  const root = asRecord(body);
  const statementCandidate = root.statement_response ?? root;
  const statement = asRecord(statementCandidate);
  const manifest = asRecord(statement.manifest);
  const schema = asRecord(manifest.schema);
  const columnsRaw = Array.isArray(schema.columns) ? schema.columns : [];
  const columns = columnsRaw.map((column, index) => {
    const record = asRecord(column);
    return {
      name: textValue(record.name) ?? `column_${index + 1}`,
      type_name: textValue(record.type_name) ?? 'STRING',
    };
  });
  const result = asRecord(statement.result);
  const dataArray = Array.isArray(result.data_array) ? result.data_array : [];
  const rows = dataArray
    .filter((row): row is unknown[] => Array.isArray(row))
    .map((row) => row.map((cell) => (cell == null ? null : String(cell))));

  if (columns.length === 0 && rows.length === 0) return null;
  return {
    manifest: { schema: { columns } },
    result: { data_array: rows },
  };
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

function toGenieStreamMessage(
  message: GenieMessageResponse,
  fallback?: { conversationId?: string; messageId?: string; spaceId?: string },
): GenieStreamMessage {
  return {
    messageId: message.message_id ?? message.id ?? fallback?.messageId ?? '',
    conversationId: message.conversation_id ?? fallback?.conversationId ?? '',
    spaceId: message.space_id ?? fallback?.spaceId ?? '',
    status: message.status ?? 'COMPLETED',
    content: message.content ?? '',
    attachments: normalizeGenieStreamAttachments(message.attachments ?? []),
    error: genieMessageError(message),
  };
}

function normalizeGenieStreamAttachments(attachments: unknown[]): GenieStreamAttachment[] {
  return attachments.map((attachment) => {
    const record = asRecord(attachment);
    const attachmentId =
      textValue(record.attachment_id) ?? textValue(record.attachmentId) ?? textValue(record.id);
    const query = asRecord(record.query);
    const sql = sqlValue(query);
    const text = textValue(record.text);
    const normalized: GenieStreamAttachment = {};
    if (attachmentId) normalized.attachmentId = attachmentId;
    if (text) normalized.text = { content: text };
    if (sql) {
      normalized.query = {
        query: sql,
        title: textValue(query.title) ?? undefined,
        description: textValue(query.description) ?? undefined,
        statementId: textValue(query.statement_id) ?? textValue(query.statementId) ?? undefined,
      };
    }
    return normalized;
  });
}

function asRecord(value: unknown): Record<string, unknown> {
  return value && typeof value === 'object' && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
}

function textValue(value: unknown): string | null {
  if (value === null || value === undefined) return null;
  if (typeof value === 'string') {
    const trimmed = value.trim();
    return trimmed || null;
  }
  if (Array.isArray(value)) {
    const joined = value.map((item) => textValue(item) ?? '').join('');
    const trimmed = joined.trim();
    return trimmed || null;
  }
  const record = asRecord(value);
  if (Object.keys(record).length === 0) return null;
  return (
    textValue(record.content) ??
    textValue(record.text) ??
    textValue(record.value) ??
    textValue(record.markdown)
  );
}

function sqlValue(value: unknown): string | null {
  const record = asRecord(value);
  return (
    textValue(record.query) ??
    textValue(record.sql) ??
    textValue(record.statement) ??
    textValue(record.content)
  );
}

function mapDatabricksStatusCode(response: Response, errorMessage: string): number {
  if (response.status === 401) return 401;
  if (response.status === 403 || isPermissionDenied(errorMessage)) return 403;
  return 502;
}

async function databricksErrorMessage(response: Response): Promise<string> {
  try {
    const body = (await response.json()) as {
      message?: string;
      error_code?: string;
      error?: { message?: string };
    };
    return body.message ?? body.error?.message ?? body.error_code ?? response.statusText;
  } catch {
    return response.statusText;
  }
}
