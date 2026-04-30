import { z } from 'zod';

export const StorageCredentialSummarySchema = z.object({
  name: z.string().min(1),
  awsAccountId: z
    .string()
    .regex(/^\d{12}$/)
    .nullable(),
  roleArn: z.string().min(1).nullable(),
  readOnly: z.boolean().nullable(),
  comment: z.string().nullable(),
});
export type StorageCredentialSummary = z.infer<typeof StorageCredentialSummarySchema>;

export const StorageCredentialListResponseSchema = z.object({
  storageCredentials: z.array(StorageCredentialSummarySchema),
});
export type StorageCredentialListResponse = z.infer<typeof StorageCredentialListResponseSchema>;
