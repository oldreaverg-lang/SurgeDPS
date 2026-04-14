// ─────────────────────────────────────────────────────────────────────────────
// useImpactAggregates — derived impact summaries computed from the loaded
// building + flood GeoJSON. All pure useMemo derivations; no effects.
// ─────────────────────────────────────────────────────────────────────────────

import { useMemo } from 'react';
import {
  rollupByCounty, rollupToCentroidGeoJSON,
  rollupByCity, cityRollupToCentroidGeoJSON,
  AVG_HOUSEHOLD, DISPLACEMENT_HAIRCUT,
} from '../jurisdictions';
import type { CountyRollup, CityEntry } from '../jurisdictions';
import type { StormInfo, HotspotBasic } from '../types';
import { CRITICAL_ICONS, criticalPrefix } from '../utils/buildings';
import { perilSplit, estimateWindMph, haversineKm } from '../utils/geo';

export function useImpactAggregates(
  allBuildings: any,
  displayBuildings: any,
  countiesGeoJSON: any,
  citiesData: CityEntry[] | null,
  activeStorm: StormInfo | null,
) {
  const estimatedPop = useMemo(() => {
    if (!allBuildings?.features) return 0;
    return allBuildings.features.reduce((sum: number, f: any) => {
      const { building_type, occupants } = f.properties || {};
      if (occupants != null) return sum + (occupants || 0);
      const prefix = (building_type || '').replace(/[-_].*$/, '').toUpperCase();
      return sum + (prefix === 'RES' ? AVG_HOUSEHOLD : 0);
    }, 0);
  }, [allBuildings]);

  const severityCounts = useMemo(() => {
    const counts: Record<string, number> = { none: 0, minor: 0, moderate: 0, major: 0, severe: 0 };
    for (const f of (displayBuildings?.features ?? [])) {
      const cat = f.properties?.damage_category ?? 'none';
      counts[cat] = (counts[cat] ?? 0) + 1;
    }
    return counts;
  }, [displayBuildings]);

  const totalDisplaced = useMemo(() => {
    if (!displayBuildings?.features || !activeStorm) return 0;
    return displayBuildings.features.reduce((sum: number, f: any) => {
      const { damage_category, building_type, occupants } = f.properties || {};
      if (!['major', 'severe'].includes(damage_category)) return sum;
      const pop = occupants ?? (() => {
        const prefix = (building_type || '').replace(/[-_].*$/, '').toUpperCase();
        return prefix === 'RES' ? AVG_HOUSEHOLD : 0;
      })();
      return sum + Math.round(pop * DISPLACEMENT_HAIRCUT);
    }, 0);
  }, [displayBuildings, activeStorm]);

  const countyRollup = useMemo((): CountyRollup[] => {
    if (!displayBuildings?.features || !countiesGeoJSON?.features) return [];
    return rollupByCounty(displayBuildings.features, countiesGeoJSON.features);
  }, [displayBuildings, countiesGeoJSON]);

  const countyAggregatePoints = useMemo(() =>
    rollupToCentroidGeoJSON(countyRollup),
  [countyRollup]);

  const countyNameMap = useMemo<Record<string, string>>(() => {
    if (!countiesGeoJSON?.features) return {};
    return Object.fromEntries(
      countiesGeoJSON.features.map((f: any) => [
        f.properties?.GEOID ?? '',
        f.properties?.NAME ?? '',
      ]),
    );
  }, [countiesGeoJSON]);

  const cityRollup = useMemo(() => {
    if (!displayBuildings?.features || !citiesData?.length) return [];
    return rollupByCity(displayBuildings.features, citiesData, countyNameMap);
  }, [displayBuildings, citiesData, countyNameMap]);

  const cityAggregatePoints = useMemo(() =>
    cityRollupToCentroidGeoJSON(cityRollup),
  [cityRollup]);

  const criticalBreakdown = useMemo(() => {
    const counts: Record<string, number> = {};
    for (const f of (displayBuildings?.features ?? [])) {
      const p = f.properties || {};
      const icon = CRITICAL_ICONS[criticalPrefix(p)];
      if (!icon) continue;
      counts[icon] = (counts[icon] ?? 0) + 1;
    }
    return counts;
  }, [displayBuildings]);

  const criticalFacilities = useMemo(() => {
    if (!displayBuildings?.features) return { type: 'FeatureCollection', features: [] };
    const features = (displayBuildings.features as any[]).filter(f => {
      const p = f.properties || {};
      return !!CRITICAL_ICONS[criticalPrefix(p)];
    }).map(f => ({
      ...f,
      properties: {
        ...f.properties,
        critical_icon: CRITICAL_ICONS[criticalPrefix(f.properties)] ?? '',
      },
    }));
    return { type: 'FeatureCollection', features };
  }, [displayBuildings]);

  const hotspots = useMemo((): HotspotBasic[] => {
    if (!displayBuildings?.features || !activeStorm) return [];
    const GRID = 0.15;
    const grid: Record<string, { lon: number; lat: number; count: number; totalLoss: number; maxDepthFt: number; windSum: number; waterSum: number; surgeSum: number; rainSum: number }> = {};

    for (const f of displayBuildings.features) {
      const [lon, lat] = f.geometry?.coordinates ?? [0, 0];
      const p = f.properties ?? {};
      const gLon = Math.round(lon / GRID) * GRID;
      const gLat = Math.round(lat / GRID) * GRID;
      const key = `${gLon.toFixed(4)},${gLat.toFixed(4)}`;
      const depthFt = (p.depth_m ?? 0) * 3.28084;
      const distKm = haversineKm(lat, lon, activeStorm.landfall_lat, activeStorm.landfall_lon);
      const windMph = estimateWindMph(distKm, activeStorm.max_wind_kt, activeStorm.category);
      const rainfallFt = (p.rainfall_depth_m ?? 0) * 3.28084;
      const split = perilSplit(windMph, depthFt, rainfallFt);

      if (!grid[key]) {
        grid[key] = { lon: gLon, lat: gLat, count: 0, totalLoss: 0, maxDepthFt: 0, windSum: 0, waterSum: 0, surgeSum: 0, rainSum: 0 };
      }
      const cell = grid[key];
      cell.count++;
      cell.totalLoss += p.estimated_loss_usd ?? 0;
      if (depthFt > cell.maxDepthFt) cell.maxDepthFt = depthFt;
      cell.windSum  += split.windPct;
      cell.waterSum += split.waterPct;
      cell.surgeSum += split.surgePct;
      cell.rainSum  += split.rainPct;
    }

    return Object.values(grid)
      .filter(c => c.count >= 3)
      .map(c => ({
        lon: c.lon,
        lat: c.lat,
        count: c.count,
        totalLoss: c.totalLoss,
        avgLoss: c.count > 0 ? c.totalLoss / c.count : 0,
        maxDepthFt: c.maxDepthFt,
        label: `${c.count} bldgs`,
        windPct:  Math.round(c.windSum  / c.count),
        waterPct: Math.round(c.waterSum / c.count),
        surgePct: Math.round(c.surgeSum / c.count),
        rainPct:  Math.round(c.rainSum  / c.count),
      }))
      .sort((a, b) => b.totalLoss - a.totalLoss)
      .slice(0, 20);
  }, [displayBuildings, activeStorm]);

  return {
    estimatedPop,
    severityCounts,
    totalDisplaced,
    countyRollup,
    countyAggregatePoints,
    countyNameMap,
    cityRollup,
    cityAggregatePoints,
    criticalBreakdown,
    criticalFacilities,
    hotspots,
  };
}
