import { z } from 'zod';

export const DatabricksOptimizationRangeSchema = z.object({
  start: z.string().datetime(),
  end: z.string().datetime(),
  workspaceId: z.string().optional(),
});

export type DatabricksOptimizationRange = z.infer<typeof DatabricksOptimizationRangeSchema>;

export const DatabricksOptimizationSummarySchema = z.object({
  totalCostUsd: z.number(),
  serverlessCostUsd: z.number(),
  nonServerlessCostUsd: z.number(),
  unknownCostUsd: z.number(),
  serverlessRatio: z.number().nullable(),
  candidateResourceCount: z.number(),
});

export type DatabricksOptimizationSummary = z.infer<typeof DatabricksOptimizationSummarySchema>;

export const DatabricksOptimizationWorkspaceSchema = z.object({
  workspaceId: z.string().nullable(),
  workspaceName: z.string().nullable(),
  totalCostUsd: z.number(),
  serverlessCostUsd: z.number(),
  nonServerlessCostUsd: z.number(),
  serverlessRatio: z.number().nullable(),
});

export type DatabricksOptimizationWorkspace = z.infer<typeof DatabricksOptimizationWorkspaceSchema>;

export const DatabricksOptimizationMonthlyRowSchema = z.object({
  month: z.string(),
  totalCostUsd: z.number(),
  serverlessCostUsd: z.number(),
  nonServerlessCostUsd: z.number(),
  unknownCostUsd: z.number(),
  serverlessRatio: z.number().nullable(),
});

export type DatabricksOptimizationMonthlyRow = z.infer<
  typeof DatabricksOptimizationMonthlyRowSchema
>;

export const DatabricksOptimizationServiceRowSchema = z.object({
  serviceCategory: z.string(),
  serviceName: z.string(),
  totalCostUsd: z.number(),
  serverlessCostUsd: z.number(),
  nonServerlessCostUsd: z.number(),
  serverlessRatio: z.number().nullable(),
});

export type DatabricksOptimizationServiceRow = z.infer<
  typeof DatabricksOptimizationServiceRowSchema
>;

export const DatabricksOptimizationRecommendationSchema = z.object({
  rank: z.number(),
  priority: z.enum(['high', 'medium', 'low']),
  workspaceId: z.string().nullable(),
  workspaceName: z.string().nullable(),
  serviceCategory: z.string(),
  serviceName: z.string(),
  resourceType: z.string().nullable(),
  resourceId: z.string(),
  resourceName: z.string().nullable(),
  totalCostUsd: z.number(),
  nonServerlessCostUsd: z.number(),
  serverlessRatio: z.number().nullable(),
  reason: z.string(),
  action: z.string(),
});

export type DatabricksOptimizationRecommendation = z.infer<
  typeof DatabricksOptimizationRecommendationSchema
>;

export const DatabricksOptimizationErrorSchema = z.object({
  tableName: z.string(),
  message: z.string(),
});

export type DatabricksOptimizationError = z.infer<typeof DatabricksOptimizationErrorSchema>;

export const DatabricksOptimizationResponseSchema = z.object({
  summary: DatabricksOptimizationSummarySchema,
  workspaces: z.array(DatabricksOptimizationWorkspaceSchema),
  monthly: z.array(DatabricksOptimizationMonthlyRowSchema),
  services: z.array(DatabricksOptimizationServiceRowSchema),
  recommendations: z.array(DatabricksOptimizationRecommendationSchema),
  errors: z.array(DatabricksOptimizationErrorSchema),
  generatedAt: z.string().datetime(),
});

export type DatabricksOptimizationResponse = z.infer<typeof DatabricksOptimizationResponseSchema>;
