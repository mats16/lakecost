export function messageOf(err: unknown): string | null {
  return err && typeof err === 'object' ? ((err as { message?: string }).message ?? null) : null;
}

export function numberSetting(value: string | undefined): number | null {
  if (!value) return null;
  const n = Number(value);
  return Number.isSafeInteger(n) && n > 0 ? n : null;
}

export function catalogTableUrl(workspaceUrl: string | null, fqn: string): string | null {
  if (!workspaceUrl) return null;
  return `${workspaceUrl.replace(/\/$/, '')}/explore/data/${fqn.split('.').map(encodeURIComponent).join('/')}`;
}

export function notebookEditorUrl(
  workspaceUrl: string | null,
  notebookId: string | null,
): string | null {
  if (!workspaceUrl || !notebookId) return null;
  return `${workspaceUrl.replace(/\/$/, '')}/editor/notebooks/${notebookId}`;
}

export function volumeFileUrl(
  workspaceUrl: string | null,
  volumePath: string,
): string | null {
  if (!workspaceUrl) return null;
  const parts = volumePath.split('/').filter(Boolean);
  if (parts.length < 5 || parts[0] !== 'Volumes') return null;
  const [catalog, schema, volume] = parts.slice(1, 4);
  const filePreviewPath = parts.slice(4).join('/');
  if (!catalog || !schema || !volume || !filePreviewPath) return null;
  const volumeRoute = [catalog, schema, volume].map(encodeURIComponent).join('/');
  return `${workspaceUrl.replace(/\/$/, '')}/explore/data/volumes/${volumeRoute}?filePreviewPath=${encodeURIComponent(filePreviewPath)}`;
}
