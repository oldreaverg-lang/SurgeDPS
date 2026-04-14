// ─────────────────────────────────────────────────────────────────────────────
// useSeasons — fetch the list of available HURDAT2 seasons.
// Cached for 10 minutes; virtually never changes mid-session.
// ─────────────────────────────────────────────────────────────────────────────

import { useQuery } from '@tanstack/react-query';
import type { Season } from '../types';

export const seasonsKey = () => ['seasons'] as const;

async function fetchSeasons(): Promise<Season[]> {
  const res = await fetch('/surgedps/api/seasons');
  if (!res.ok) throw new Error(`seasons ${res.status}`);
  const data = await res.json();
  return Array.isArray(data) ? data : [];
}

export function useSeasons() {
  return useQuery({
    queryKey: seasonsKey(),
    queryFn: fetchSeasons,
    staleTime: 10 * 60 * 1000,   // 10 min
    gcTime:    30 * 60 * 1000,   // 30 min
    retry: 2,
  });
}
