import { z } from 'zod';

export const PricingDataSchema = z.object({
  id: z.string().min(1),
  provider: z.string().min(1),
  service: z.string().min(1),
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
  id: z.string(),
  provider: z.string(),
  service: z.string(),
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

export const AWS_PRICING_IDS = ['aws_ec2', 'aws_rds'] as const;
export type AwsPricingId = (typeof AWS_PRICING_IDS)[number];
export const DATABRICKS_PRICING_IDS = [
  'databricks_list_prices',
  'databricks_account_prices',
] as const;
export type DatabricksPricingId = (typeof DATABRICKS_PRICING_IDS)[number];
export const PRICING_IDS = [...AWS_PRICING_IDS, ...DATABRICKS_PRICING_IDS] as const;
export type PricingId = (typeof PRICING_IDS)[number];

export const PricingNotebookSetupInputSchema = z.object({
  id: z.enum(PRICING_IDS),
});
export type PricingNotebookSetupInput = z.infer<typeof PricingNotebookSetupInputSchema>;

export const PricingNotebookDeleteResultSchema = z.object({
  id: z.enum(PRICING_IDS),
  table: z.string().nullable(),
  droppedTable: z.boolean(),
  deletedPricingData: z.boolean(),
});
export type PricingNotebookDeleteResult = z.infer<typeof PricingNotebookDeleteResultSchema>;

export const PricingNotebookRunResultSchema = z.object({
  id: z.string(),
  provider: z.string(),
  service: z.string(),
  runId: z.number(),
  runStatus: PricingRunStatusSchema,
  runUrl: z.string().nullable(),
});
export type PricingNotebookRunResult = z.infer<typeof PricingNotebookRunResultSchema>;

export const JobRunSubmitInputSchema = z.object({
  id: z.enum(PRICING_IDS),
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
