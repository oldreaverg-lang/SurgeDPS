// ─────────────────────────────────────────────────────────────────────────────
// useSimulation — forecast simulator state (active storms, track, cone).
// ─────────────────────────────────────────────────────────────────────────────

import { useState, useCallback } from 'react';
import type { SimMarker } from '../types';
import { fetchJson } from '../utils/fetch';

export function useSimulation(activeStormId: string | null) {
  const [simMode, setSimMode] = useState(false);
  const [simMarker, setSimMarker] = useState<SimMarker | null>(null);
  const [simRunning, setSimRunning] = useState(false);
  const [simResult, setSimResult] = useState<any>(null);
  const [forecastCone, setForecastCone] = useState<any>(null);
  const [forecastTrack, setForecastTrack] = useState<any[]>([]);

  const runSimulation = useCallback(async () => {
    if (!simMarker || !activeStormId) return;
    setSimRunning(true);
    setSimResult(null);
    try {
      const result = await fetchJson<any>(
        `/surgedps/api/simulate?storm_id=${encodeURIComponent(activeStormId)}&lon=${simMarker.lng}&lat=${simMarker.lat}`,
      );
      setSimResult(result);
    } catch (err) {
      console.warn('[simulation] failed:', err);
    } finally {
      setSimRunning(false);
    }
  }, [simMarker, activeStormId]);

  return {
    simMode, setSimMode,
    simMarker, setSimMarker,
    simRunning,
    simResult,
    forecastCone, setForecastCone,
    forecastTrack, setForecastTrack,
    runSimulation,
  };
}
