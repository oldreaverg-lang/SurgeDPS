// ─────────────────────────────────────────────────────────────────────────────
// Fetch utilities — thin wrappers around the Fetch API.
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Fetch JSON from `url`. Throws on non-2xx so callers can distinguish
 * network errors from application errors with a single try/catch.
 */
export async function fetchJson<T>(url: string, options?: RequestInit): Promise<T> {
  const r = await fetch(url, options);
  if (!r.ok) throw new Error(`HTTP ${r.status}`);
  return r.json() as Promise<T>;
}

/**
 * Fetch a JSON array from `url`. Returns `[]` (and calls `onError`)
 * on any failure or when the response is not an array — callers never
 * need to guard `Array.isArray`.
 */
export async function fetchJsonArray<T>(
  url: string,
  options?: RequestInit,
  onError?: () => void,
): Promise<T[]> {
  try {
    const data = await fetchJson<unknown>(url, options);
    if (Array.isArray(data)) return data as T[];
    onError?.();
    return [];
  } catch {
    onError?.();
    return [];
  }
}
