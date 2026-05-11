export function messageOf(err: unknown): string | null {
  return err && typeof err === 'object' ? ((err as { message?: string }).message ?? null) : null;
}

export function numberSetting(value: string | undefined): number | null {
  if (!value) return null;
  const n = Number(value);
  return Number.isSafeInteger(n) && n > 0 ? n : null;
}
