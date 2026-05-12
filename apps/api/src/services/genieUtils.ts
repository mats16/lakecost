import { normalizeGenieStreamAttachments, type GenieStreamAttachment } from './genieAttachments.js';
import type { GenieMessageResponse, GenieStatementResponse } from './genieClient.js';

export type { GenieMessageResponse, GenieStatementResponse } from './genieClient.js';
export type { GenieStreamAttachment } from './genieAttachments.js';

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

export interface GenieStreamMessage {
  messageId: string;
  conversationId: string;
  spaceId: string;
  status: string;
  content: string;
  attachments?: GenieStreamAttachment[];
  error?: string;
}

export function genieMessageError(message: GenieMessageResponse): string | undefined {
  if (!message.error) return undefined;
  if (typeof message.error === 'string') return message.error;
  return message.error.message ?? message.error.error_code;
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
