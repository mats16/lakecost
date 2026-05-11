export interface AwsResourceTag {
  Key: string;
  Value: string;
}

export const AWS_FOCUS_12_STANDARD_COLUMNS = [
  'AvailabilityZone',
  'BilledCost',
  'BillingAccountId',
  'BillingAccountName',
  'BillingAccountType',
  'BillingCurrency',
  'BillingPeriodEnd',
  'BillingPeriodStart',
  'CapacityReservationId',
  'CapacityReservationStatus',
  'ChargeCategory',
  'ChargeClass',
  'ChargeDescription',
  'ChargeFrequency',
  'ChargePeriodEnd',
  'ChargePeriodStart',
  'CommitmentDiscountCategory',
  'CommitmentDiscountId',
  'CommitmentDiscountName',
  'CommitmentDiscountQuantity',
  'CommitmentDiscountStatus',
  'CommitmentDiscountType',
  'CommitmentDiscountUnit',
  'ConsumedQuantity',
  'ConsumedUnit',
  'ContractedCost',
  'ContractedUnitPrice',
  'EffectiveCost',
  'InvoiceId',
  'InvoiceIssuerName',
  'ListCost',
  'ListUnitPrice',
  'PricingCategory',
  'PricingCurrency',
  'PricingCurrencyContractedUnitPrice',
  'PricingCurrencyEffectiveCost',
  'PricingCurrencyListUnitPrice',
  'PricingQuantity',
  'PricingUnit',
  'ProviderName',
  'PublisherName',
  'RegionId',
  'RegionName',
  'ResourceId',
  'ResourceName',
  'ResourceType',
  'ServiceCategory',
  'ServiceName',
  'ServiceSubcategory',
  'SkuId',
  'SkuMeter',
  'SkuPriceDetails',
  'SkuPriceId',
  'SubAccountId',
  'SubAccountName',
  'SubAccountType',
  'Tags',
] as const;

export const AWS_FOCUS_12_AWS_EXTENSION_COLUMNS = [
  'x_Discounts',
  'x_Operation',
  'x_ServiceCode',
] as const;

export const AWS_FOCUS_12_WITH_AWS_COLUMNS = [
  ...AWS_FOCUS_12_STANDARD_COLUMNS,
  ...AWS_FOCUS_12_AWS_EXTENSION_COLUMNS,
] as const;

export const AWS_FOCUS_12_WITH_AWS_COLUMNS_QUERY_STATEMENT = `SELECT ${AWS_FOCUS_12_WITH_AWS_COLUMNS.join(', ')} FROM FOCUS_1_2_AWS`;

export const FINLAKE_AWS_RESOURCE_TAGS: readonly AwsResourceTag[] = Object.freeze([
  { Key: 'CostCenter', Value: 'finlake' },
  { Key: 'Project', Value: 'finops' },
  { Key: 'Environment', Value: 'production' },
]);

export function finlakeAwsResourceTags(): AwsResourceTag[] {
  return [...FINLAKE_AWS_RESOURCE_TAGS];
}

export function roleNameFromArn(roleArn: string | null): string | null {
  if (!roleArn) return null;
  const match = /^arn:aws(?:-[a-z]+)*:iam::\d{12}:role\/(.+)$/.exec(roleArn);
  return match?.[1] ?? null;
}

export function ucNameSuffixFromBucket(bucket: string): string {
  return bucket
    .toLowerCase()
    .replace(/[^a-z0-9-]+/g, '-')
    .replace(/-+/g, '-')
    .replace(/^-+|-+$/g, '');
}

export function storageCredentialNameForBucket(bucket: string): string {
  return `db_s3_credential_${ucNameSuffixFromBucket(bucket) || 'bucket'}`.slice(0, 128);
}

export function externalLocationNameForBucket(bucket: string): string {
  return `db_s3_external_${ucNameSuffixFromBucket(bucket) || 'bucket'}`.slice(0, 128);
}

export function isValidS3BucketName(bucket: string): boolean {
  return (
    /^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$/.test(bucket) &&
    !bucket.includes('..') &&
    !bucket.includes('.-') &&
    !bucket.includes('-.') &&
    !/^\d{1,3}(?:\.\d{1,3}){3}$/.test(bucket)
  );
}
