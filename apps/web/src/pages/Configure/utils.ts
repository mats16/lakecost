import { tableLeafName, type DataSource } from '@finlake/shared';

export function nextTableName(base: string, rows: DataSource[]): string {
  const used = new Set(rows.map((row) => tableLeafName(row.tableName)));
  if (!used.has(base)) return base;

  for (let i = 2; i < 1000; i += 1) {
    const candidate = `${base}_${i}`;
    if (!used.has(candidate)) return candidate;
  }
  return `${base}_${Date.now()}`;
}

export function configString(config: Record<string, unknown>, key: string): string {
  const value = config[key];
  return typeof value === 'string' ? value : '';
}

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

export function fileNameFromPath(path: string): string {
  const parts = path.split('/').filter(Boolean);
  return parts.at(-1) ?? path;
}

export function volumeFileUrl(workspaceUrl: string | null, volumePath: string): string | null {
  if (!workspaceUrl) return null;
  const parts = volumePath.split('/').filter(Boolean);
  if (parts.length < 5 || parts[0] !== 'Volumes') return null;
  const [catalog, schema, volume] = parts.slice(1, 4);
  const filePreviewPath = parts.slice(4).join('/');
  if (!catalog || !schema || !volume || !filePreviewPath) return null;
  const volumeRoute = [catalog, schema, volume].map(encodeURIComponent).join('/');
  return `${workspaceUrl.replace(/\/$/, '')}/explore/data/volumes/${volumeRoute}?filePreviewPath=${encodeURIComponent(filePreviewPath)}`;
}
