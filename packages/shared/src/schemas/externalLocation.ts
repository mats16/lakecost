import { z } from 'zod';

export const UnityCatalogCredentialNameSchema = z
  .string()
  .min(1)
  .max(128)
  .regex(/^[A-Za-z_][A-Za-z0-9_-]*$/, 'must match /^[A-Za-z_][A-Za-z0-9_-]*$/');

export const ExternalLocationSummarySchema = z.object({
  name: z.string().min(1),
  url: z.string().min(1).nullable(),
  credentialName: z.string().min(1).nullable(),
  readOnly: z.boolean().nullable(),
  comment: z.string().nullable(),
});
export type ExternalLocationSummary = z.infer<typeof ExternalLocationSummarySchema>;

export const ExternalLocationListResponseSchema = z.object({
  externalLocations: z.array(ExternalLocationSummarySchema),
});
export type ExternalLocationListResponse = z.infer<typeof ExternalLocationListResponseSchema>;

export const ExternalLocationCreateBodySchema = z.object({
  name: UnityCatalogCredentialNameSchema,
  url: z.string().regex(/^s3:\/\/[^/]+(?:\/.*)?$/i, 'must be an s3:// URL'),
  credentialName: z.string().min(1),
  readOnly: z.boolean().optional(),
  comment: z.string().max(1024).optional(),
});
export type ExternalLocationCreateBody = z.infer<typeof ExternalLocationCreateBodySchema>;

export const ExternalLocationCreateResponseSchema = z.object({
  externalLocation: ExternalLocationSummarySchema,
});
export type ExternalLocationCreateResponse = z.infer<typeof ExternalLocationCreateResponseSchema>;
