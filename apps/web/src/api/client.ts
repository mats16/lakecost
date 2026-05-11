export interface ApiError {
  status: number;
  message: string;
}

export async function apiFetch<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(path, {
    credentials: 'include',
    ...init,
    headers: {
      'content-type': 'application/json',
      ...(init?.headers ?? {}),
    },
  });
  if (!res.ok) {
    let message = res.statusText;
    let step: string | undefined;
    try {
      const body = (await res.json()) as { error?: { message?: string; step?: string } };
      if (body.error?.message) message = body.error.message;
      if (body.error?.step) step = body.error.step;
    } catch {
      // ignore
    }
    const err: ApiError & { step?: string } = { status: res.status, message };
    if (step) err.step = step;
    throw err;
  }
  if (res.status === 204) return undefined as T;
  return (await res.json()) as T;
}
