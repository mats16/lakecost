import { z } from 'zod';
import { DataSourceIdentifierSchema } from './dataSource.js';
import {
  ExternalLocationSummarySchema,
  UnityCatalogCredentialNameSchema,
} from './externalLocation.js';
import {
  AwsAccountIdSchema,
  AwsIamRoleNameSchema,
  StorageCredentialSummarySchema,
} from './storageCredential.js';

export { AwsAccountIdSchema, AwsIamRoleNameSchema };

export const ServiceCredentialSummarySchema = z.object({
  name: z.string().min(1),
  awsAccountId: AwsAccountIdSchema.nullable(),
  roleArn: z.string().min(1).nullable(),
  externalId: z.string().min(1).nullable(),
  unityCatalogIamArn: z.string().min(1).nullable(),
  owner: z.string().min(1).nullable(),
  createdAt: z.number().int().nullable(),
  comment: z.string().nullable(),
});
export type ServiceCredentialSummary = z.infer<typeof ServiceCredentialSummarySchema>;

export const ServiceCredentialListResponseSchema = z.object({
  storageCredentials: z.array(StorageCredentialSummarySchema),
  serviceCredentials: z.array(ServiceCredentialSummarySchema),
});
export type ServiceCredentialListResponse = z.infer<typeof ServiceCredentialListResponseSchema>;

export const ServiceCredentialCreateBodySchema = z.object({
  name: DataSourceIdentifierSchema,
  awsAccountId: AwsAccountIdSchema,
  roleName: AwsIamRoleNameSchema,
  comment: z.string().max(1024).optional(),
});
export type ServiceCredentialCreateBody = z.infer<typeof ServiceCredentialCreateBodySchema>;

export const ServiceCredentialCreateResponseSchema = z.object({
  serviceCredential: ServiceCredentialSummarySchema,
});
export type ServiceCredentialCreateResponse = z.infer<typeof ServiceCredentialCreateResponseSchema>;

export const AwsFocusExportCreateBodySchema = z.object({
  awsAccountId: AwsAccountIdSchema,
  s3Bucket: z
    .string()
    .min(3)
    .max(63)
    .regex(/^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$/, 'must be a valid S3 bucket name')
    .refine((bucket) => !bucket.includes('..'), 'must be a valid S3 bucket name')
    .refine((bucket) => !bucket.includes('.-'), 'must be a valid S3 bucket name')
    .refine((bucket) => !bucket.includes('-.'), 'must be a valid S3 bucket name')
    .refine(
      (bucket) => !/^\d{1,3}(?:\.\d{1,3}){3}$/.test(bucket),
      'must be a valid S3 bucket name',
    ),
  s3Prefix: z.string().min(1).max(512),
  exportName: z
    .string()
    .min(1)
    .max(128)
    .regex(/^[A-Za-z0-9_.-]+$/, 'must be a valid AWS Data Export name'),
  externalLocationName: UnityCatalogCredentialNameSchema,
  createBucketIfMissing: z.boolean().optional(),
});
export type AwsFocusExportCreateBody = z.infer<typeof AwsFocusExportCreateBodySchema>;

export const AwsFocusExportResourceStatusSchema = z.enum(['created', 'skipped']);
export type AwsFocusExportResourceStatus = z.infer<typeof AwsFocusExportResourceStatusSchema>;

export const AwsFocusExportCreateResponseSchema = z.object({
  exportArn: z.string(),
  storageRoleArn: z.string(),
  storageCredential: StorageCredentialSummarySchema,
  externalLocation: ExternalLocationSummarySchema,
  resourceStatuses: z.object({
    bucket: AwsFocusExportResourceStatusSchema,
    storageCredential: AwsFocusExportResourceStatusSchema,
    storageRole: AwsFocusExportResourceStatusSchema,
    externalLocation: AwsFocusExportResourceStatusSchema,
    dataExport: AwsFocusExportResourceStatusSchema,
  }),
});
export type AwsFocusExportCreateResponse = z.infer<typeof AwsFocusExportCreateResponseSchema>;
