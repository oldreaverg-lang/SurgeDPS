// ─────────────────────────────────────────────────────────────────────────────
// Geospatial and wind/peril model utilities.
// Pure functions — no React, no side effects.
// ─────────────────────────────────────────────────────────────────────────────

// ── Haversine ─────────────────────────────────────────────────────────────────

export function haversineKm(
  lat1: number, lon1: number,
  lat2: number, lon2: number,
): number {
  const R = 6371;
  const dLat = (lat2 - lat1) * Math.PI / 180;
  const dLon = (lon2 - lon1) * Math.PI / 180;
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(lat1 * Math.PI / 180) *
    Math.cos(lat2 * Math.PI / 180) *
    Math.sin(dLon / 2) ** 2;
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

// ── Grid helpers ──────────────────────────────────────────────────────────────

const CELL_WIDTH  = 0.4;
const CELL_HEIGHT = 0.3;

export function cellBbox(
  col: number, row: number, oLon: number, oLat: number,
): [number, number, number, number] {
  return [
    oLon + col       * CELL_WIDTH,
    oLat + row       * CELL_HEIGHT,
    oLon + (col + 1) * CELL_WIDTH,
    oLat + (row + 1) * CELL_HEIGHT,
  ];
}

export function cellKey(col: number, row: number): string {
  return `${col},${row}`;
}

export function cellPolygon(
  col: number, row: number, status: string, oLon: number, oLat: number,
) {
  const [w, s, e, n] = cellBbox(col, row, oLon, oLat);
  return {
    type: 'Feature' as const,
    properties: { col, row, key: cellKey(col, row), status },
    geometry: {
      type: 'Polygon' as const,
      coordinates: [[[w, s], [e, s], [e, n], [w, n], [w, s]]],
    },
  };
}

// ── Wind model ────────────────────────────────────────────────────────────────

/** Rmax (km) by Saffir-Simpson category — for modified Rankine vortex. */
const RMAX_BY_CAT: Record<number, number> = {
  0: 100, 1: 80, 2: 60, 3: 45, 4: 35, 5: 25,
};

/** Estimate sustained wind (mph) at `distKm` from the storm centre. */
export function estimateWindMph(
  distKm: number, maxWindKt: number, category: number,
): number {
  const vMax = maxWindKt * 1.15078;
  const rMax = RMAX_BY_CAT[category] ?? 50;
  if (distKm <= 0.1) return vMax;
  if (distKm <= rMax) return vMax * (distKm / rMax);
  return vMax * Math.pow(rMax / distKm, 0.5);
}

// ── Peril attribution ─────────────────────────────────────────────────────────

/**
 * Split damage potential into wind / surge / rainfall components.
 * Returns `{ windPct, waterPct, surgePct, rainPct }` summing to 100.
 */
export function perilSplit(
  windMph: number,
  interiorSurgeFt: number,
  rainfallFt: number = 0,
): { windPct: number; waterPct: number; surgePct: number; rainPct: number } {
  const windNorm     = Math.max(0, (windMph - 74) / (180 - 74));
  const windPotential  = Math.min(1, windNorm ** 1.5);
  const surgePotential = Math.min(1, Math.max(0, interiorSurgeFt / 8));
  const rainPotential  = Math.min(1, Math.max(0, rainfallFt      / 8));
  const total = windPotential + surgePotential + rainPotential;
  if (total < 0.001) {
    return { windPct: 50, waterPct: 50, surgePct: 50, rainPct: 0 };
  }
  const windPct  = Math.round((windPotential  / total) * 100);
  const surgePct = Math.round((surgePotential / total) * 100);
  const rainPct  = Math.max(0, 100 - windPct - surgePct);
  return { windPct, waterPct: 100 - windPct, surgePct, rainPct };
}

/** Back-compat 2-way split. Prefer `perilSplit` for new code. */
export function windWaterSplit(
  windMph: number, interiorFloodFt: number,
): { windPct: number; waterPct: number } {
  const p = perilSplit(windMph, interiorFloodFt, 0);
  return { windPct: p.windPct, waterPct: p.waterPct };
}

// ── Comparable loss ───────────────────────────────────────────────────────────

export const COMP_RADIUS_KM = 0.4;

export function findComparables(
  features: any[],
  buildingType: string,
  lon: number,
  lat: number,
  radiusKm: number = COMP_RADIUS_KM,
): { count: number; avgLoss: number; minLoss: number; maxLoss: number } {
  const comps: number[] = [];
  const typePrefix = (buildingType || '').replace(/[-_].*$/, '').toUpperCase();
  for (const f of features) {
    const p = f.properties || {};
    const fType = (p.building_type || '').replace(/[-_].*$/, '').toUpperCase();
    if (fType !== typePrefix) continue;
    const [bLon, bLat] = f.geometry?.coordinates || [0, 0];
    const d = haversineKm(lat, lon, bLat, bLon);
    if (d > radiusKm || d < 0.001) continue;
    if (p.estimated_loss_usd != null) comps.push(p.estimated_loss_usd);
  }
  if (comps.length === 0) return { count: 0, avgLoss: 0, minLoss: 0, maxLoss: 0 };
  let sum = 0, lo = comps[0], hi = comps[0];
  for (const v of comps) { sum += v; if (v < lo) lo = v; if (v > hi) hi = v; }
  return {
    count: comps.length,
    avgLoss: Math.round(sum / comps.length),
    minLoss: lo,
    maxLoss: hi,
  };
}
