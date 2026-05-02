import { z } from 'zod';

export const BudgetScopeTypeSchema = z.enum([
  'provider',
  'billingAccount',
  'subAccount',
  'sku',
  'tag',
]);
export type BudgetScopeType = z.infer<typeof BudgetScopeTypeSchema>;

export const BudgetPeriodSchema = z.enum(['monthly', 'quarterly', 'yearly']);
export type BudgetPeriod = z.infer<typeof BudgetPeriodSchema>;

export const BudgetSchema = z.object({
  id: z.string(),
  workspaceId: z.string().nullable(),
  name: z.string().min(1).max(120),
  scopeType: BudgetScopeTypeSchema,
  scopeValue: z.string(),
  amountUsd: z.number().positive(),
  period: BudgetPeriodSchema,
  thresholdsPct: z.array(z.number().int().min(1).max(200)).default([80, 100]),
  notifyEmails: z.array(z.string().email()).default([]),
  createdBy: z.string(),
  createdAt: z.string().datetime(),
});

export type Budget = z.infer<typeof BudgetSchema>;

export const CreateBudgetInputSchema = BudgetSchema.omit({
  id: true,
  createdAt: true,
  createdBy: true,
});

export type CreateBudgetInput = z.infer<typeof CreateBudgetInputSchema>;
