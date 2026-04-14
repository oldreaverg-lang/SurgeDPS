// ─────────────────────────────────────────────────────────────────────────────
// useGauges — USGS/NOAA stream gauge status near the storm footprint.
// Polled every 10 min when enabled (gauge readings update hourly at best).
// ─────────────────────────────────────────────────────────────────────────────

import { useQuery } from '@tanstack/react-query';
import type { GaugesSummary } from '../types';

export const gaugesKey = (stormId: string | null) =>
  ['gauges', stormId] as const;

interface GaugesResult {
  geojson: any;
  summary: GaugesSummary;
}

async function fetchGauges(stormId: string): Promise<GaugesResult> {
  const res = await fetch(
    `/surgedps/api/gauge_overlay?storm_id=${encodeURIComponent(stormId)}`,
  );
  if (!res.ok) throw new Error(`gauge_overlay ${res.status}`);
  return res.json();
}

export function useGauges(stormId: string | null, enabled = false) {
  return useQuery({
    queryKey: gaugesKey(stormId),
    queryFn: () => fetchGauges(stormId!),
    enabled: enabled && stormId != null,
    staleTime: 10 * 60 * 1000,
    gcTime:   30 * 60 * 1000,
    refetchInterval: enabled ? 10 * 60 * 1000 : false,
    retry: 2,
  });
}
