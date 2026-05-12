import type { GenieAttachment } from '@finlake/shared';
import { fetchGenieAttachmentRaw, type GenieStatementResponse } from './genieClient.js';

export async function normalizeGenieAttachments(
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

export async function getGenieAttachmentStatementResponse(
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

export function normalizeGenieStreamAttachments(attachments: unknown[]): GenieStreamAttachment[] {
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

export function normalizeQueryResult(body: unknown): GenieAttachment['queryResult'] {
  const root = asRecord(body);
  const statement = root.statement_response ? asRecord(root.statement_response) : root;
  const manifest = asRecord(statement.manifest);
  const schema = asRecord(manifest.schema);
  const columnsRaw = Array.isArray(schema.columns) ? schema.columns : [];
  const columns = columnsRaw
    .map((column, index) => textValue(asRecord(column)?.name) ?? `column_${index + 1}`)
    .slice(0, 50);

  const result = asRecord(statement.result || root.result);
  const dataArray = Array.isArray(result?.data_array) ? result.data_array : [];
  const rows = dataArray
    .filter((row): row is unknown[] => Array.isArray(row))
    .map((row) => row.slice(0, columns.length || 50))
    .slice(0, 25);

  if (columns.length === 0 && rows.length === 0) return null;
  return { columns, rows };
}

export function normalizeStatementResponse(body: unknown): GenieStatementResponse | null {
  const root = asRecord(body);
  const statement = root.statement_response ? asRecord(root.statement_response) : root;
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

export interface GenieStreamAttachment {
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
