// ─────────────────────────────────────────────────────────────────────────────
// useCellTicks — time-series tick bundle for one grid cell.
// Heavy payload (~50–200 KB); cached for the session (staleTime = Infinity).
// ─────────────────────────────────────────────────────────────────────────────

import { useQuery } from '@tanstack/react-query';
import type { TicksBundle } from '../types';

export const cellTicksKey = (stormId: string | null, col: number, row: number) =>
  ['cell_ticks', stormId, col, row] as const;

async function fetchCellTicks(
  stormId: string,
  col: number,
  row: number,
): Promise<TicksBundle> {
  const url =
    `/surgedps/api/cell_ticks?col=${col}&row=${row}&storm_id=${encodeURIComponent(stormId)}`;
  const res = await fetch(url);
  if (!res.ok) throw new Error(`cell_ticks ${col},${row} → ${res.status}`);
  return res.json();
}

/**
 * Fetch tick-level time-series for one cell.
 * Enabled only when `enabled` is explicitly true — the chart component sets this
 * when the user opens the timeline panel.
 */
export function useCellTicks(
  stormId: string | null,
  col: number,
  row: number,
  enabled = false,
) {
  return useQuery({
    queryKey: cellTicksKey(stormId, col, row),
    queryFn: () => fetchCellTicks(stormId!, col, row),
    enabled: enabled && stormId != null,
    staleTime: Infinity,
    gcTime: 30 * 60 * 1000,
    retry: 1,
  });
}
