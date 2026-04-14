// ─────────────────────────────────────────────────────────────────────────────
// useShelters — active emergency shelters within the storm footprint.
// Polled every 5 min when enabled (occupancy updates frequently).
// ─────────────────────────────────────────────────────────────────────────────

import { useQuery } from '@tanstack/react-query';
import type { SheltersData } from '../types';

export const sheltersKey = (stormId: string | null) =>
  ['shelters', stormId] as const;

async function fetchShelters(stormId: string): Promise<SheltersData> {
  const res = await fetch(
    `/surgedps/api/shelters?storm_id=${encodeURIComponent(stormId)}`,
  );
  if (!res.ok) throw new Error(`shelters ${res.status}`);
  return res.json();
}

export function useShelters(stormId: string | null, enabled = false) {
  return useQuery({
    queryKey: sheltersKey(stormId),
    queryFn: () => fetchShelters(stormId!),
    enabled: enabled && stormId != null,
    staleTime: 5 * 60 * 1000,
    gcTime:   30 * 60 * 1000,
    refetchInterval: enabled ? 5 * 60 * 1000 : false,
    retry: 2,
  });
}
