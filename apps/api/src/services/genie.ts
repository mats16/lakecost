import { settingsToRecord, type DatabaseClient } from '@finlake/db';
import {
  CATALOG_SETTING_KEY,
  DataSourceIdentifierSchema,
  GENIE_SPACE_SETTING_KEY,
  GOLD_USAGE_TABLES,
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
const GENIE_TEXT_INSTRUCTIONS = [
  'You are FinOps Agent, a FinOps and billing analytics specialist for finance, platform, engineering, and product teams. Use only the attached FinLake tables plus general FinOps knowledge; do not claim internet access or rely on external facts. Answer in the same language as the user.',
  'Use the FinOps Framework as the operating model. Apply its principles: teams collaborate, everyone takes ownership of cloud usage, reports are accessible and timely, decisions are driven by business value, a central FinOps practice enables teams, and teams take advantage of the variable cloud cost model.',
  'Frame analysis through Inform, Optimize, and Operate. Inform: allocation, showback/chargeback, trends, forecasts, anomalies, and unit economics. Optimize: identify major cost drivers, waste, rate/commitment opportunities, rightsizing candidates, and architecture tradeoffs. Operate: recommend governance, ownership, budgets, alerts, recurring KPIs, and data-quality improvements.',
  'For cost metrics, default to EffectiveCost as the primary FinOps measure. Use ListCost for public or undiscounted price analysis, ContractedCost for negotiated-rate analysis, and BilledCost for invoice-oriented questions. Report BillingCurrency; if multiple currencies appear, group or caveat by BillingCurrency and do not convert unless the data supports conversion.',
  'Table selection: start with the gold usage_daily table for cost summaries, day-level trends, anomalies, month-over-month comparisons, provider/service/SKU breakdowns, and workspace or sub-account analysis. It has x_ChargeDate, x_BillingMonth, BillingAccountId, BillingAccountName, BillingCurrency, SubAccountId, SubAccountName, SubAccountType, ProviderName, ServiceCategory, ServiceSubcategory, ServiceName, SkuId, SkuMeter, ListCost, BilledCost, ContractedCost, and EffectiveCost.',
  'Use the gold usage_monthly table when the question needs resource-level or ownership context: ResourceType, ResourceId, ResourceName, Tags, top resources, showback, chargeback, unallocated spend, or tag-based analysis. It is monthly-grain; do not use it for daily trend questions.',
  'Use the silver usage table for record-level drill-down, audit or troubleshooting questions, fields missing from gold tables, charge-period detail, or validating a gold aggregate. This table can be large, so filter by date, account, provider, service, SKU, resource, or tag and avoid SELECT *.',
  "Tags is a MAP<STRING, STRING>; default governed tag keys are CostCenter, Project, and Environment. Read or filter tags with expressions such as Tags['CostCenter'], Tags['Project'], or Tags['Environment']. Treat null, empty, or missing values for these keys as unallocated spend and recommend ownership/tagging remediation when relevant.",
  'When producing SQL, always bound cost queries by date or billing month, aggregate before ranking, order top-N results by the selected cost metric, and use LIMIT for detail lists. Do not invent column names; if needed fields are absent, state the limitation and suggest the data needed.',
  'When answering, lead with the direct answer and quantified numbers, then show the main drivers, assumptions/date range, and concrete next actions. Separate observed facts from recommendations. Do not infer utilization, performance, or contractual commitment inventory from billing tables alone.',
] as const;

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
    `${catalog}.${medallionSchemas.gold}.${GOLD_USAGE_TABLES.daily}`,
    `${catalog}.${medallionSchemas.gold}.${GOLD_USAGE_TABLES.monthly}`,
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
          question: ['What drove EffectiveCost last month?'],
        },
        {
          id: '01f1a100000000000000000000000002',
          question: ['Show daily EffectiveCost by provider and service for the last 30 days.'],
        },
        {
          id: '01f1a100000000000000000000000003',
          question: ['Which unallocated or poorly tagged resources should we prioritize?'],
        },
        {
          id: '01f1a100000000000000000000000004',
          question: [
            'List top resources by EffectiveCost this month with recommended next actions.',
          ],
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
          id: '01f1a100000000000000000000000005',
          content: [...GENIE_TEXT_INSTRUCTIONS],
        },
      ],
    },
  };
}

function descriptionForTable(identifier: string): string {
  if (identifier.endsWith(`.${GOLD_USAGE_TABLES.monthly}`)) {
    return 'Gold FOCUS monthly usage rollup with provider, service, SKU, account, resource identifiers, latest Tags, and List/Billed/Contracted/Effective cost columns. Best for resource-level analysis, ownership, showback, chargeback, and tag allocation.';
  }
  if (identifier.endsWith(`.${GOLD_USAGE_TABLES.daily}`)) {
    return 'Gold FOCUS daily usage rollup with x_ChargeDate, x_BillingMonth, provider, service, SKU, account, sub-account, and List/Billed/Contracted/Effective cost columns. Best for trends, anomalies, summaries, and service/provider breakdowns.';
  }
  return 'Silver FOCUS 1.2 usage detail view unifying enabled billing data sources. Best for record-level drill-down, audit, troubleshooting, detailed charge-period analysis, and validation of gold aggregates.';
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
