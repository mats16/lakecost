export class WorkspaceServiceError extends Error {
  constructor(
    message: string,
    readonly statusCode: number,
  ) {
    super(message);
    this.name = new.target.name;
  }
}

export function isPermissionDenied(err: unknown): boolean {
  if (err != null && typeof err === 'object' && 'errorCode' in err) {
    return (err as { errorCode: unknown }).errorCode === 'PERMISSION_DENIED';
  }
  const message = err instanceof Error ? err.message : String(err);
  return /PERMISSION_DENIED|not authorized/i.test(message);
}
