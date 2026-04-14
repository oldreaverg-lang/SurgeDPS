// ─────────────────────────────────────────────────────────────────────────────
// useBuildingTicks — peril toggle + time-slider state.
//
// The backend emits a cell_..._ticks.json alongside each damage.geojson
// containing per-building HAZUS runs at every tick hour for three perils
// (surge-only, rainfall-only, cumulative). We merge those into a per-building
// lookup and let the time slider + peril toggle drive map paint.
// ─────────────────────────────────────────────────────────────────────────────

import { useState, useRef, useMemo } from 'react';
import type { PerilKey, TickRow } from '../types';

const STATE_TO_CAT: Record<string, string> = {
  no: 'none', mi: 'minor', mo: 'moderate', mj: 'major', sv: 'severe',
};

const PERIL_OFFSET: Record<PerilKey, [number, number, number]> = {
  surge:      [0, 3, 6],
  rainfall:   [1, 4, 7],
  cumulative: [2, 5, 8],
};

export function useBuildingTicks(allBuildings: any) {
  const [peril, setPeril] = useState<PerilKey>('cumulative');
  const [tickIdx, setTickIdx] = useState<number>(-1);   // -1 = latest (final tick)
  const [tickHours, setTickHours] = useState<number[]>([]);
  const buildingTicksRef = useRef<Record<string, TickRow[]>>({});
  const [buildingTicksVersion, setBuildingTicksVersion] = useState(0);

  const bumpTicksVersion = () => setBuildingTicksVersion(v => v + 1);

  // When tickIdx === -1 use allBuildings as-is. Otherwise derive a new
  // feature array from the tick data at the selected hour.
  const displayBuildings = useMemo(() => {
    if (!allBuildings || tickIdx < 0 || tickHours.length === 0) return allBuildings;
    const [ftOff, stOff, lossOff] = PERIL_OFFSET[peril];
    const features = (allBuildings as any).features.map((f: any) => {
      const ticks = buildingTicksRef.current[
        String(f.properties?.id ?? f.properties?.building_id)
      ];
      if (!ticks || ticks.length === 0) return f;
      const safeIdx = Math.min(tickIdx, ticks.length - 1);
      const row = ticks[safeIdx];
      if (!Array.isArray(row) || row.length < 9) return f;
      return {
        ...f,
        properties: {
          ...f.properties,
          depth_ft:            row[ftOff] as number,
          damage_category:     STATE_TO_CAT[row[stOff] as string] ?? 'none',
          estimated_loss_usd:  row[lossOff] as number,
        },
      };
    });
    return { ...(allBuildings as any), features };
  }, [allBuildings, tickIdx, peril, buildingTicksVersion, tickHours.length]);

  return {
    peril, setPeril,
    tickIdx, setTickIdx,
    tickHours, setTickHours,
    buildingTicksRef,
    bumpTicksVersion,
    displayBuildings,
  };
}
