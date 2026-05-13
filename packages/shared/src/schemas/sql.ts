import { z } from 'zod';

export const SQL_STATEMENT_TERMINAL_STATUSES = [
  'SUCCEEDED',
  'FAILED',
  'CANCELED',
  'CLOSED',
] as const;
export type SqlStatementTerminalStatus = (typeof SQL_STATEMENT_TERMINAL_STATUSES)[number];

const TERMINAL_STATUS_SET: ReadonlySet<string> = new Set(SQL_STATEMENT_TERMINAL_STATUSES);

export function isTerminalSqlStatus(status: string | null | undefined): boolean {
  return status !== null && status !== undefined && TERMINAL_STATUS_SET.has(status);
}

export const SqlParamSchema = z.object({
  name: z
    .string()
    .min(1)
    .max(128)
    .regex(/^[A-Za-z_][A-Za-z0-9_]*$/, 'invalid parameter name'),
  value: z.union([z.string(), z.number(), z.null()]),
  type: z.enum(['STRING', 'INT', 'BIGINT', 'TIMESTAMP', 'DATE']).optional(),
});

export type SqlParam = z.infer<typeof SqlParamSchema>;

export const SqlStatementSubmitRequestSchema = z.object({
  query: z.string().min(1).max(50_000),
  warehouse_id: z.string().min(1).max(256).optional(),
  params: z.array(SqlParamSchema).max(100).optional().default([]),
});

export type SqlStatementSubmitRequest = z.infer<typeof SqlStatementSubmitRequestSchema>;

export const SqlStatementColumnSchema = z.object({
  name: z.string(),
  typeName: z.string().nullable(),
});

export type SqlStatementColumn = z.infer<typeof SqlStatementColumnSchema>;

export const SqlStatementDataSchema = z.object({
  columns: z.array(SqlStatementColumnSchema).optional(),
  rows: z.array(z.record(z.unknown())).optional(),
});

export type SqlStatementData = z.infer<typeof SqlStatementDataSchema>;

export const SqlStatementSubmitResponseSchema = z.object({
  statement_id: z.string().min(1).optional(),
  status: z.string().min(1),
  result: SqlStatementDataSchema.optional(),
  generatedAt: z.string().datetime(),
});

export type SqlStatementSubmitResponse = z.infer<typeof SqlStatementSubmitResponseSchema>;

export const SqlStatementResultResponseSchema = z.object({
  statement_id: z.string().min(1),
  status: z.string().min(1),
  result: SqlStatementDataSchema.optional(),
  error: z.string().optional(),
  generatedAt: z.string().datetime(),
});

export type SqlStatementResultResponse = z.infer<typeof SqlStatementResultResponseSchema>;
