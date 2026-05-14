import { z } from 'zod';

export const AdminCleanupRequestSchema = z.object({
  deleteCatalog: z.boolean().optional().default(false),
});
export type AdminCleanupRequest = z.infer<typeof AdminCleanupRequestSchema>;

export const AdminCleanupResourceResultSchema = z.object({
  resourceType: z.enum(['job', 'pipeline', 'workspace', 'genie_space', 'catalog', 'database']),
  resourceId: z.string().nullable(),
  status: z.enum(['deleted', 'skipped', 'failed']),
  message: z.string().nullable(),
});
export type AdminCleanupResourceResult = z.infer<typeof AdminCleanupResourceResultSchema>;

export const AdminCleanupDatabaseResultSchema = z.object({
  status: z.enum(['deleted', 'failed']),
  message: z.string().nullable(),
  deletedSettings: z.number().int().nonnegative(),
  deletedDataSources: z.number().int().nonnegative(),
  deletedPricingData: z.number().int().nonnegative(),
  deletedCachedAggregations: z.number().int().nonnegative(),
  deletedSetupState: z.number().int().nonnegative(),
});
export type AdminCleanupDatabaseResult = z.infer<typeof AdminCleanupDatabaseResultSchema>;

export const AdminCleanupResponseSchema = z.object({
  resources: z.array(AdminCleanupResourceResultSchema),
  database: AdminCleanupDatabaseResultSchema,
});
export type AdminCleanupResponse = z.infer<typeof AdminCleanupResponseSchema>;
