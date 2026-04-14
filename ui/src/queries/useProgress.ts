// ─────────────────────────────────────────────────────────────────────────────
// useProgress — polls the activation progress endpoint while a storm is
// loading. Stops polling when progress.step_num === progress.total_steps.
// ─────────────────────────────────────────────────────────────────────────────

import { useQuery } from '@tanstack/react-query';
import type { LoadProgress } from '../types';

export const progressKey = (stormId: string | null) =>
  ['progress', stormId] as const;

async function fetchProgress(stormId: string): Promise<LoadProgress> {
  const res = await fetch(
    `/surgedps/api/progress?storm_id=${encodeURIComponent(stormId)}`,
  );
  if (!res.ok) throw new Error(`progress ${res.status}`);
  return res.json();
}

/**
 * Poll activation progress every 800 ms.
 * `enabled` should be true only while `activating === true` in the storm slice.
 */
export function useProgress(stormId: string | null, enabled = false) {
  return useQuery({
    queryKey: progressKey(stormId),
    queryFn: () => fetchProgress(stormId!),
    enabled: enabled && stormId != null,
    staleTime: 0,
    gcTime: 60_000,
    refetchInterval: (query) => {
      const data = query.state.data;
      if (!data) return 800;
      // Stop polling once done
      if (data.step_num >= data.total_steps) return false;
      return 800;
    },
    retry: false,
  });
}
