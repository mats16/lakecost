import { z } from 'zod';
import { UnityCatalogCredentialNameSchema } from './externalLocation.js';

export const AwsAccountIdSchema = z.string().regex(/^\d{12}$/, 'must be a 12-digit AWS account ID');

export const AwsIamRoleNameSchema = z
  .string()
  .min(1)
  .max(64)
  .regex(/^[A-Za-z0-9_+=,.@-]+$/, 'must be a valid AWS IAM role name');

export const StorageCredentialSummarySchema = z.object({
  name: z.string().min(1),
  awsAccountId: AwsAccountIdSchema.nullable(),
  roleArn: z.string().min(1).nullable(),
  externalId: z.string().min(1).nullable(),
  unityCatalogIamArn: z.string().min(1).nullable(),
  readOnly: z.boolean().nullable(),
  comment: z.string().nullable(),
});
export type StorageCredentialSummary = z.infer<typeof StorageCredentialSummarySchema>;

export const StorageCredentialListResponseSchema = z.object({
  storageCredentials: z.array(StorageCredentialSummarySchema),
});
export type StorageCredentialListResponse = z.infer<typeof StorageCredentialListResponseSchema>;

export const StorageCredentialCreateBodySchema = z.object({
  purpose: z.literal('STORAGE'),
  name: UnityCatalogCredentialNameSchema,
  awsAccountId: AwsAccountIdSchema,
  roleName: AwsIamRoleNameSchema,
  readOnly: z.boolean().optional(),
  comment: z.string().max(1024).optional(),
});
export type StorageCredentialCreateBody = z.infer<typeof StorageCredentialCreateBodySchema>;

export const StorageCredentialCreateResponseSchema = z.object({
  storageCredential: StorageCredentialSummarySchema,
});
export type StorageCredentialCreateResponse = z.infer<typeof StorageCredentialCreateResponseSchema>;
