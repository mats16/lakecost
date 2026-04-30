import { z } from 'zod';

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
