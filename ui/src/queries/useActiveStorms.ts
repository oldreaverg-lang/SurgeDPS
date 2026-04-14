// ─────────────────────────────────────────────────────────────────────────────
// useActiveStorms — fetch storm list for a given season year.
// Cached 5 minutes; year rarely changes during a session.
// ─────────────────────────────────────────────────────────────────────────────

import { useQuery } from '@tanstack/react-query';
import type { StormInfo } from '../types';

export const activeStormsKey = (year: number | null) =>
  ['storms', 'season', year] as const;

async function fetchActiveStorms(year: number): Promise<StormInfo[]> {
  const res = await fetch(`/surgedps/api/active_storms?year=${year}`);
  if (!res.ok) throw new Error(`active_storms ${res.status}`);
  const data = await res.json();
  return Array.isArray(data) ? data : [];
}

export function useActiveStorms(year: number | null) {
  return useQuery({
    queryKey: activeStormsKey(year),
    queryFn: () => fetchActiveStorms(year!),
    enabled: year != null,
    staleTime: 5 * 60 * 1000,
    gcTime:   15 * 60 * 1000,
    retry: 2,
  });
}
