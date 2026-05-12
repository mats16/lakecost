import { z } from 'zod';

export const GENIE_SPACE_SETTING_KEY = 'genie_space_id';

export const GenieSetupResponseSchema = z.object({
  spaceId: z.string().min(1),
  title: z.string().min(1),
  tableIdentifiers: z.array(z.string().min(1)),
});
export type GenieSetupResponse = z.infer<typeof GenieSetupResponseSchema>;

export const GenieChatRequestSchema = z.object({
  content: z.string().trim().min(1).max(10_000),
  conversationId: z.string().trim().min(1).optional(),
});
export type GenieChatRequest = z.infer<typeof GenieChatRequestSchema>;

export const GenieQueryResultSchema = z.object({
  columns: z.array(z.string()),
  rows: z.array(z.array(z.unknown())),
});
export type GenieQueryResult = z.infer<typeof GenieQueryResultSchema>;

export const GenieAttachmentSchema = z.object({
  id: z.string().nullable(),
  text: z.string().nullable(),
  sql: z.string().nullable(),
  queryResult: GenieQueryResultSchema.nullable(),
});
export type GenieAttachment = z.infer<typeof GenieAttachmentSchema>;

export const GenieChatResponseSchema = z.object({
  conversationId: z.string().min(1),
  messageId: z.string().min(1),
  status: z.string().min(1),
  answer: z.string().nullable(),
  attachments: z.array(GenieAttachmentSchema),
  authMode: z.enum(['obo', 'service_principal']),
});
export type GenieChatResponse = z.infer<typeof GenieChatResponseSchema>;
