export function messageOf(err: unknown): string | null {
  return err && typeof err === 'object' ? ((err as { message?: string }).message ?? null) : null;
}
