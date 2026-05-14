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
  runId: z.number().int().positive().nullable(),
  runStatus: z.enum([
    'not_started',
    'pending',
    'running',
    'succeeded',
    'failed',
    'canceled',
    'unknown',
  ]),
  runUrl: z.string().nullable(),
  runStartedAt: z.string().datetime().nullable(),
  runFinishedAt: z.string().datetime().nullable(),
  runCheckedAt: z.string().datetime().nullable(),
  updatedAt: z.string().datetime(),
});
export type PricingData = z.infer<typeof PricingDataSchema>;

export const PricingRunStatusSchema = PricingDataSchema.shape.runStatus;
export type PricingRunStatus = z.infer<typeof PricingRunStatusSchema>;

export function isActivePricingRunStatus(status: PricingRunStatus): boolean {
  return status === 'pending' || status === 'running';
}

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
  runId: z.number().int().positive().nullable(),
  runStatus: PricingRunStatusSchema,
  runUrl: z.string().nullable(),
  runStartedAt: z.string().datetime().nullable(),
  runFinishedAt: z.string().datetime().nullable(),
  runCheckedAt: z.string().datetime().nullable(),
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

export const PricingNotebookDeleteResultSchema = z.object({
  slug: z.enum(AWS_PRICING_SLUGS),
  table: z.string().nullable(),
  droppedTable: z.boolean(),
  deletedPricingData: z.boolean(),
});
export type PricingNotebookDeleteResult = z.infer<typeof PricingNotebookDeleteResultSchema>;

export const PricingNotebookRunResultSchema = z.object({
  provider: z.string(),
  service: z.string(),
  slug: z.string(),
  runId: z.number(),
  runStatus: PricingRunStatusSchema,
  runUrl: z.string().nullable(),
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
