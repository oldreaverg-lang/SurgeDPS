// ─────────────────────────────────────────────────────────────────────────────
// useCell — fetch a single grid cell's building GeoJSON.
// Results are cached indefinitely for a given storm; cells don't change once
// loaded. We set gcTime to 20 min to avoid evicting tiles the user may
// navigate back to within a session.
// ─────────────────────────────────────────────────────────────────────────────

import { useQuery } from '@tanstack/react-query';

export const cellKey = (stormId: string | null, col: number, row: number) =>
  ['cell', stormId, col, row] as const;

async function fetchCell(stormId: string, col: number, row: number): Promise<any> {
  const res = await fetch(
    `/surgedps/api/cell?col=${col}&row=${row}&storm_id=${encodeURIComponent(stormId)}`,
  );
  if (!res.ok) throw new Error(`cell ${col},${row} → ${res.status}`);
  return res.json();
}

/**
 * Fetch one grid cell. Only enabled when a storm is active and explicit
 * `enabled` flag is set — callers control which cells they want loaded.
 */
export function useCell(
  stormId: string | null,
  col: number,
  row: number,
  enabled = false,
) {
  return useQuery({
    queryKey: cellKey(stormId, col, row),
    queryFn: () => fetchCell(stormId!, col, row),
    enabled: enabled && stormId != null,
    staleTime: Infinity,
    gcTime: 20 * 60 * 1000,
    retry: 2,
  });
}
