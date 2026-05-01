export function normalizeHost(host: string | undefined): string | null {
  if (!host) return null;
  if (host.startsWith('http://') || host.startsWith('https://')) return host.replace(/\/+$/, '');
  return `https://${host.replace(/\/+$/, '')}`;
}
