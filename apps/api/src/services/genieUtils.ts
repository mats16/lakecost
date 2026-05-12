import type { GenieAttachment } from '@finlake/shared';

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

export interface GenieMessageResponse {
  id?: string;
  message_id?: string;
  conversation_id?: string;
  space_id?: string;
  status?: string;
  content?: string;
  attachments?: unknown[] | null;
  error?: string | { message?: string; error_code?: string } | null;
}

export interface GenieStreamMessage {
  messageId: string;
  conversationId: string;
  spaceId: string;
  status: string;
  content: string;
  attachments?: GenieStreamAttachment[];
  error?: string;
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

export interface GenieStatementResponse {
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

export function asRecord(value: unknown): Record<string, unknown> {
  return value && typeof value === 'object' && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
}

export function textValue(value: unknown): string | null {
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

export function sqlValue(value: unknown): string | null {
  const record = asRecord(value);
  return (
    textValue(record.query) ??
    textValue(record.sql) ??
    textValue(record.statement) ??
    textValue(record.content)
  );
}

export function genieMessageError(message: GenieMessageResponse): string | undefined {
  if (!message.error) return undefined;
  if (typeof message.error === 'string') return message.error;
  return message.error.message ?? message.error.error_code;
}

export function normalizeQueryResult(body: unknown): GenieAttachment['queryResult'] {
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

export function normalizeStatementResponse(body: unknown): GenieStatementResponse | null {
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

export function toGenieStreamMessage(
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
