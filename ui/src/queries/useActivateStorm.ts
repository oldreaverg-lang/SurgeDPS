// ─────────────────────────────────────────────────────────────────────────────
// useActivateStorm — mutation that POSTs a storm ID to the activate endpoint.
// On success the caller should update the Zustand storm slice with the returned
// StormInfo and clear any stale building/cell data.
//
// The activate endpoint is idempotent for the same storm_id, so we use GET
// (matching the existing server contract) wrapped in a mutation so the caller
// controls when it fires (not automatic on mount).
// ─────────────────────────────────────────────────────────────────────────────

import { useMutation, useQueryClient } from '@tanstack/react-query';
import type { StormInfo } from '../types';
import { activeStormsKey } from './useActiveStorms';

interface ActivateResult {
  storm: StormInfo;
  /** Grid cells that were pre-loaded by the server during activation */
  preloaded_cells?: Array<{ col: number; row: number }>;
}

async function activateStorm(stormId: string): Promise<ActivateResult> {
  const res = await fetch(`/surgedps/api/storm/${encodeURIComponent(stormId)}/activate`);
  if (!res.ok) {
    const body = await res.text().catch(() => '');
    throw new Error(body || `activate ${res.status}`);
  }
  return res.json();
}

export function useActivateStorm() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: activateStorm,
    onSuccess: (data) => {
      // Invalidate the season list so DPS scores refresh
      queryClient.invalidateQueries({ queryKey: activeStormsKey(data.storm.year) });
    },
  });
}
