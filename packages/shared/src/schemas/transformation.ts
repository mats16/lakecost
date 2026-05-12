import { z } from 'zod';

export const TransformationPipelineStatusDaySchema = z.object({
  date: z.string(),
  resultState: z.string().nullable(),
  updateCount: z.number().int().nonnegative(),
});

export type TransformationPipelineStatusDay = z.infer<typeof TransformationPipelineStatusDaySchema>;

export const TransformationSharedRunResultSchema = z.object({
  jobId: z.number().int().positive(),
  runId: z.number().int().positive(),
});
export type TransformationSharedRunResult = z.infer<typeof TransformationSharedRunResultSchema>;

export const TransformationResourceSchema = z.object({
  resourceType: z.enum(['job', 'pipeline']),
  resourceId: z.string(),
  name: z.string(),
  url: z.string().nullable(),
  owner: z.string().nullable(),
  cronExpression: z.string().nullable(),
  timezoneId: z.string().nullable(),
  createTime: z.string().nullable(),
  changeTime: z.string().nullable(),
  updateId: z.string().nullable(),
  resultState: z.string().nullable(),
  periodStartTime: z.string().nullable(),
  periodEndTime: z.string().nullable(),
  durationSeconds: z.number().nullable(),
  statusDays: z.array(TransformationPipelineStatusDaySchema),
});

export type TransformationResource = z.infer<typeof TransformationResourceSchema>;

export const TransformationPipelinesResponseSchema = z.object({
  resources: z.array(TransformationResourceSchema),
  generatedAt: z.string().datetime(),
});

export type TransformationPipelinesResponse = z.infer<typeof TransformationPipelinesResponseSchema>;
