// ─────────────────────────────────────────────────────────────────────────────
// useStormSearch — full-text search across all storms.
// Debounced at call site; here we just cache per query string.
// ─────────────────────────────────────────────────────────────────────────────

import { useQuery } from '@tanstack/react-query';
import type { StormInfo } from '../types';

export const stormSearchKey = (q: string) => ['storms', 'search', q] as const;

async function fetchStormSearch(q: string): Promise<StormInfo[]> {
  const res = await fetch(`/surgedps/api/storm_search?q=${encodeURIComponent(q)}`);
  if (!res.ok) throw new Error(`storm_search ${res.status}`);
  const data = await res.json();
  return Array.isArray(data) ? data : [];
}

export function useStormSearch(q: string) {
  return useQuery({
    queryKey: stormSearchKey(q),
    queryFn: () => fetchStormSearch(q),
    enabled: q.trim().length >= 2,
    staleTime: 2 * 60 * 1000,
    gcTime:   10 * 60 * 1000,
    retry: 1,
  });
}
