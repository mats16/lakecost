import { z } from 'zod';

export const GOVERNED_TAG_DEFINITIONS = [
  {
    key: 'CostCenter',
    displayName: 'Cost Center',
    description: 'Finance cost center code used to align cloud spend to accounting.',
    required: true,
    allowedValues: [],
  },
  {
    key: 'Environment',
    displayName: 'Environment',
    description: 'Deployment environment for separating production and non-production spend.',
    required: true,
    allowedValues: ['production', 'staging', 'development', 'sandbox'],
  },
  {
    key: 'Project',
    displayName: 'Project',
    description: 'Product, initiative, or project that owns the workload spend.',
    required: false,
    allowedValues: [],
  },
] as const;

export const GovernedTagDefinitionSchema = z.object({
  key: z.string().min(1).max(128),
  displayName: z.string().min(1).max(128),
  description: z.string().min(1).max(512),
  required: z.boolean(),
  allowedValues: z.array(z.string().min(1).max(256)),
});
export type GovernedTagDefinition = z.infer<typeof GovernedTagDefinitionSchema>;

export const GovernedTagDatabricksStatusSchema = z.object({
  status: z.enum(['governed', 'missing', 'error']),
  policyId: z.string().min(1).nullable(),
  updatedAt: z.string().nullable(),
  message: z.string().nullable(),
});
export type GovernedTagDatabricksStatus = z.infer<typeof GovernedTagDatabricksStatusSchema>;

export const GovernedTagAwsStatusSchema = z.object({
  accountId: z.string().regex(/^\d{12}$/),
  status: z.enum(['Active', 'Inactive', 'NotFound', 'Error']),
  lastUpdatedDate: z.string().nullable(),
  lastUsedDate: z.string().nullable(),
  message: z.string().nullable(),
});
export type GovernedTagAwsStatus = z.infer<typeof GovernedTagAwsStatusSchema>;

export const GovernedTagRowSchema = z.object({
  definition: GovernedTagDefinitionSchema,
  databricks: GovernedTagDatabricksStatusSchema,
  aws: z.array(GovernedTagAwsStatusSchema),
});
export type GovernedTagRow = z.infer<typeof GovernedTagRowSchema>;

export const GovernedTagAwsAccountSchema = z.object({
  awsAccountId: z.string().regex(/^\d{12}$/),
  credentialName: z.string().min(1),
});
export type GovernedTagAwsAccount = z.infer<typeof GovernedTagAwsAccountSchema>;

export const GovernedTagsResponseSchema = z.object({
  items: z.array(GovernedTagRowSchema),
  awsAccounts: z.array(GovernedTagAwsAccountSchema),
  warnings: z.array(z.string()),
  generatedAt: z.string().datetime(),
});
export type GovernedTagsResponse = z.infer<typeof GovernedTagsResponseSchema>;

export const GovernedTagSyncBodySchema = z.object({
  platform: z.enum(['databricks', 'aws']),
  awsAccountId: z
    .string()
    .regex(/^\d{12}$/)
    .optional(),
  tagKey: z.string().min(1).max(128).optional(),
});
export type GovernedTagSyncBody = z.infer<typeof GovernedTagSyncBodySchema>;

export const GovernedTagSyncTagResultSchema = z.object({
  key: z.string().min(1).max(128),
  status: z.enum(['synced', 'skipped', 'failed']),
  message: z.string().nullable(),
});
export type GovernedTagSyncTagResult = z.infer<typeof GovernedTagSyncTagResultSchema>;

export const GovernedTagSyncAccountResultSchema = z.object({
  awsAccountId: z.string().regex(/^\d{12}$/),
  credentialName: z.string().min(1),
  status: z.enum(['synced', 'failed']),
  message: z.string().nullable(),
});
export type GovernedTagSyncAccountResult = z.infer<typeof GovernedTagSyncAccountResultSchema>;

export const GovernedTagSyncResultSchema = z.object({
  platform: z.enum(['databricks', 'aws']),
  syncedAt: z.string().datetime(),
  tags: z.array(GovernedTagSyncTagResultSchema),
  awsAccounts: z.array(GovernedTagSyncAccountResultSchema),
});
export type GovernedTagSyncResult = z.infer<typeof GovernedTagSyncResultSchema>;
