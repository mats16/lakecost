import { logger } from '../config/logger.js';
import { isPermissionDenied, type ServiceErrorCtor } from './workspaceClientErrors.js';

export interface CreateGenieSpaceResponse {
  space_id?: string;
  title?: string;
}

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

export interface GenieConversationResponse {
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

export async function createGenieSpace(
  host: string,
  token: string,
  opts: {
    title: string;
    description: string;
    warehouseId: string;
    parentPath: string;
    serializedSpace: unknown;
  },
  ErrorClass: ServiceErrorCtor,
): Promise<CreateGenieSpaceResponse> {
  const body = {
    title: opts.title,
    description: opts.description,
    warehouse_id: opts.warehouseId,
    parent_path: opts.parentPath,
    serialized_space: JSON.stringify(opts.serializedSpace),
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
    throw new ErrorClass(`Failed to create Genie Space: ${(err as Error).message}`, 502);
  }

  if (!response.ok) {
    const message = await databricksErrorMessage(response);
    logger.warn({ status: response.status, message }, 'Create Genie Space failed');
    throw new ErrorClass(
      `Failed to create Genie Space: ${message}`,
      mapDatabricksStatusCode(response, message),
    );
  }

  return (await response.json()) as CreateGenieSpaceResponse;
}

export async function createGenieMessage(
  host: string,
  token: string,
  spaceId: string,
  opts: {
    content: string;
    conversationId?: string;
  },
  ErrorClass: ServiceErrorCtor,
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
    throw new ErrorClass(`Failed to ask Genie: ${(err as Error).message}`, 502);
  }

  if (!response.ok) {
    const message = await databricksErrorMessage(response);
    logger.warn({ status: response.status, message, spaceId, conversationId }, 'Ask Genie failed');
    throw new ErrorClass(
      `Failed to ask Genie: ${message}`,
      mapDatabricksStatusCode(response, message),
    );
  }

  return (await response.json()) as GenieConversationResponse;
}

export async function listGenieConversationMessages(
  host: string,
  token: string,
  spaceId: string,
  conversationId: string,
  pageToken: string | undefined,
  ErrorClass: ServiceErrorCtor,
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
    throw new ErrorClass(`Failed to load Genie conversation: ${(err as Error).message}`, 502);
  }

  if (!response.ok) {
    const message = await databricksErrorMessage(response);
    logger.warn(
      { status: response.status, message, spaceId, conversationId },
      'List Genie messages failed',
    );
    throw new ErrorClass(
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

export async function getGenieMessage(
  host: string,
  token: string,
  spaceId: string,
  conversationId: string,
  messageId: string,
  ErrorClass: ServiceErrorCtor,
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
    throw new ErrorClass(`Failed to poll Genie: ${(err as Error).message}`, 502);
  }

  if (!response.ok) {
    const message = await databricksErrorMessage(response);
    logger.warn(
      { status: response.status, message, spaceId, conversationId, messageId },
      'Poll Genie failed',
    );
    throw new ErrorClass(
      `Failed to poll Genie: ${message}`,
      mapDatabricksStatusCode(response, message),
    );
  }

  return (await response.json()) as GenieMessageResponse;
}

export async function fetchGenieAttachmentRaw(
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

export async function trashGenieSpace(
  host: string,
  token: string,
  spaceId: string,
  ErrorClass: ServiceErrorCtor,
): Promise<void> {
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
    throw new ErrorClass(`Failed to delete Genie Space: ${(err as Error).message}`, 502);
  }

  if (response.status === 404) {
    logger.warn({ spaceId }, 'Genie Space was not found during delete; clearing app setting');
    return;
  }
  if (!response.ok) {
    const message = await databricksErrorMessage(response);
    logger.warn({ status: response.status, message, spaceId }, 'Delete Genie Space failed');
    throw new ErrorClass(
      `Failed to delete Genie Space: ${message}`,
      mapDatabricksStatusCode(response, message),
    );
  }
}

export async function databricksErrorMessage(response: Response): Promise<string> {
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

function mapDatabricksStatusCode(response: Response, errorMessage: string): number {
  if (response.status === 401) return 401;
  if (response.status === 403 || isPermissionDenied(errorMessage)) return 403;
  return 502;
}
