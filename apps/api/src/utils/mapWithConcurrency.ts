/**
 * Run `fn` over each item in `items` with at most `limit` operations in flight.
 * Preserves the original order of results, mirrors Promise.all error semantics
 * (rejects on the first thrown error).
 */
export async function mapWithConcurrency<T, R>(
  items: readonly T[],
  limit: number,
  fn: (item: T, index: number) => Promise<R>,
): Promise<R[]> {
  if (!Number.isInteger(limit) || limit <= 0) {
    throw new Error(`mapWithConcurrency: limit must be a positive integer, got ${limit}`);
  }
  const results: R[] = new Array(items.length);
  let cursor = 0;
  const worker = async (): Promise<void> => {
    while (true) {
      const i = cursor++;
      if (i >= items.length) return;
      results[i] = await fn(items[i] as T, i);
    }
  };
  const workerCount = Math.min(limit, items.length);
  await Promise.all(Array.from({ length: workerCount }, () => worker()));
  return results;
}
