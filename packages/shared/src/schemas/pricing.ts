import { z } from 'zod';

export const PricingDataSchema = z.object({
  provider: z.string().min(1),
  service: z.string().min(1),
  slug: z.string().min(1),
  table: z.string().min(1),
  rawDataTable: z.string().nullable(),
  rawDataPath: z.string().nullable(),
  notebookPath: z.string().nullable(),
  notebookId: z.string().nullable(),
  metadata: z.record(z.string(), z.unknown()),
  updatedAt: z.string().datetime(),
});
export type PricingData = z.infer<typeof PricingDataSchema>;

export const PricingNotebookStateSchema = z.object({
  provider: z.string(),
  service: z.string(),
  slug: z.string(),
  catalog: z.string().nullable(),
  table: z.string().nullable(),
  rawDataTable: z.string().nullable(),
  rawDataPath: z.string().nullable(),
  notebookWorkspacePath: z.string().nullable(),
  notebookId: z.string().nullable(),
  metadata: z.record(z.string(), z.unknown()),
});
export type PricingNotebookState = z.infer<typeof PricingNotebookStateSchema>;

export const PricingNotebookListResponseSchema = z.object({
  items: z.array(PricingNotebookStateSchema),
});
export type PricingNotebookListResponse = z.infer<typeof PricingNotebookListResponseSchema>;

export const AWS_PRICING_SLUGS = ['aws_ec2', 'aws_rds'] as const;
export type AwsPricingSlug = (typeof AWS_PRICING_SLUGS)[number];

export const PricingNotebookSetupInputSchema = z.object({
  slug: z.enum(AWS_PRICING_SLUGS),
});
export type PricingNotebookSetupInput = z.infer<typeof PricingNotebookSetupInputSchema>;

export const PricingNotebookSetupResultSchema = PricingNotebookStateSchema.extend({
  notebookWorkspacePath: z.string(),
  warnings: z.array(z.string()),
});
export type PricingNotebookSetupResult = z.infer<typeof PricingNotebookSetupResultSchema>;

export const PricingNotebookRunResultSchema = z.object({
  provider: z.string(),
  service: z.string(),
  slug: z.string(),
  runId: z.number(),
});
export type PricingNotebookRunResult = z.infer<typeof PricingNotebookRunResultSchema>;

export const JobRunSubmitInputSchema = z.object({
  slug: z.enum(AWS_PRICING_SLUGS),
});
export type JobRunSubmitInput = z.infer<typeof JobRunSubmitInputSchema>;

export const JobRunLinkQuerySchema = z.object({
  run_id: z.coerce.number().int().positive(),
});
export type JobRunLinkQuery = z.infer<typeof JobRunLinkQuerySchema>;

export const DatabricksRunLinkResultSchema = z.object({
  jobId: z.number().nullable(),
  runId: z.number(),
  runUrl: z.string().nullable(),
});
export type DatabricksRunLinkResult = z.infer<typeof DatabricksRunLinkResultSchema>;
