import { z } from 'zod';

export const TransformationPipelineStatusDaySchema = z.object({
  date: z.string(),
  resultState: z.string().nullable(),
  updateCount: z.number().int().nonnegative(),
});

export type TransformationPipelineStatusDay = z.infer<typeof TransformationPipelineStatusDaySchema>;

export const TransformationPipelineRowSchema = z.object({
  dataSourceId: z.number().int().positive(),
  dataSourceName: z.string(),
  providerName: z.string(),
  tableName: z.string(),
  jobId: z.number().int().positive().nullable(),
  pipelineId: z.string().nullable(),
  cronExpression: z.string().nullable(),
  timezoneId: z.string().nullable(),
  accountId: z.string().nullable(),
  workspaceId: z.string().nullable(),
  pipelineUrl: z.string().nullable(),
  pipelineName: z.string().nullable(),
  pipelineType: z.string().nullable(),
  createdBy: z.string().nullable(),
  runAs: z.string().nullable(),
  createTime: z.string().nullable(),
  changeTime: z.string().nullable(),
  deleteTime: z.string().nullable(),
  updateId: z.string().nullable(),
  updateType: z.string().nullable(),
  triggerType: z.string().nullable(),
  resultState: z.string().nullable(),
  runAsUserName: z.string().nullable(),
  periodStartTime: z.string().nullable(),
  periodEndTime: z.string().nullable(),
  durationSeconds: z.number().nullable(),
  statusDays: z.array(TransformationPipelineStatusDaySchema),
});

export type TransformationPipelineRow = z.infer<typeof TransformationPipelineRowSchema>;

export const TransformationPipelineSharedSchema = z.object({
  jobId: z.number().int().positive().nullable(),
  jobUrl: z.string().nullable(),
  pipelineId: z.string().nullable(),
  pipelineUrl: z.string().nullable(),
  cronExpression: z.string().nullable(),
  timezoneId: z.string().nullable(),
});
export type TransformationPipelineShared = z.infer<typeof TransformationPipelineSharedSchema>;

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
  shared: TransformationPipelineSharedSchema,
  resources: z.array(TransformationResourceSchema),
  rows: z.array(TransformationPipelineRowSchema),
  generatedAt: z.string().datetime(),
});

export type TransformationPipelinesResponse = z.infer<typeof TransformationPipelinesResponseSchema>;
