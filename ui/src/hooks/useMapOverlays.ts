// ─────────────────────────────────────────────────────────────────────────────
// useMapOverlays — county boundaries, cities, FEMA flood zones, stream gauges,
// and emergency shelters. Each is lazy-loaded on first enable.
// ─────────────────────────────────────────────────────────────────────────────

import { useState, useEffect, useRef, useCallback } from 'react';
import type { SheltersData, GaugesSummary } from '../types';

export function useMapOverlays(hasBuildings: boolean, _mapRef?: React.RefObject<any>) {
  // ── Counties ──────────────────────────────────────────────────────────────
  const [showCounties, setShowCounties] = useState(false);
  const [countiesGeoJSON, setCountiesGeoJSON] = useState<any>(null);
  const [countiesLoading, setCountiesLoading] = useState(false);
  const [countiesError, setCountiesError] = useState<string | null>(null);
  const countiesLoadedRef = useRef(false);

  const loadCounties = useCallback(async () => {
    if (countiesLoadedRef.current) return;
    setCountiesLoading(true);
    setCountiesError(null);
    try {
      const mod = await import('../assets/counties-coastal.json');
      const raw: any = (mod as any).default ?? mod;
      if (!raw?.features?.length) throw new Error('Empty counties dataset');
      const data = {
        type: 'FeatureCollection' as const,
        features: raw.features.map((f: any) => ({
          type: 'Feature' as const,
          geometry: f.geometry,
          properties: { ...(f.properties || {}) },
        })),
      };
      // Stable categorical color index (djb hash, 8 cool pastels).
      for (const f of data.features) {
        const g: string = f.properties?.GEOID || f.properties?.NAME || '';
        let h = 0;
        for (let i = 0; i < g.length; i++) h = (h * 31 + g.charCodeAt(i)) | 0;
        f.properties.colorIdx = (h >>> 0) % 8;
      }
      setCountiesGeoJSON(data);
      countiesLoadedRef.current = true;
    } catch (err: any) {
      console.warn('[counties] load failed:', err?.message || err);
      setCountiesError('Could not load county boundaries');
    } finally {
      setCountiesLoading(false);
    }
  }, []);

  useEffect(() => {
    if (showCounties || hasBuildings) loadCounties();
    if (!showCounties) { setCountiesError(null); setCountiesLoading(false); }
  }, [showCounties, hasBuildings, loadCounties]);

  // ── Cities ────────────────────────────────────────────────────────────────
  const [citiesData, setCitiesData] = useState<any[] | null>(null);
  const citiesLoadedRef = useRef(false);

  const loadCities = useCallback(async () => {
    if (citiesLoadedRef.current) return;
    try {
      const mod = await import('../assets/cities-coastal.json');
      const raw: any = (mod as any).default ?? mod;
      if (Array.isArray(raw) && raw.length) {
        setCitiesData(raw);
        citiesLoadedRef.current = true;
      }
    } catch (err: any) {
      console.warn('[cities] load failed:', err?.message || err);
    }
  }, []);

  useEffect(() => {
    if (hasBuildings) loadCities();
  }, [hasBuildings, loadCities]);

  // ── FEMA flood zones ──────────────────────────────────────────────────────
  const [showFloodZones, setShowFloodZones] = useState(false);
  const [floodZonesGeoJSON, setFloodZonesGeoJSON] = useState<any>(null);
  const [floodZonesLoading, setFloodZonesLoading] = useState(false);
  const [floodZonesError, setFloodZonesError] = useState<string | null>(null);
  const floodZonesFetchTimer = useRef<ReturnType<typeof setTimeout> | null>(null);

  const fetchFloodZones = useCallback((bounds: { west: number; south: number; east: number; north: number }) => {
    if (floodZonesFetchTimer.current) clearTimeout(floodZonesFetchTimer.current);
    floodZonesFetchTimer.current = setTimeout(async () => {
      const { west, south, east, north } = bounds;
      const envelope = JSON.stringify({ xmin: west, ymin: south, xmax: east, ymax: north, spatialReference: { wkid: 4326 } });
      const params = new URLSearchParams({
        where: '1=1', geometry: envelope, geometryType: 'esriGeometryEnvelope',
        inSR: '4326', outSR: '4326', spatialRel: 'esriSpatialRelIntersects',
        outFields: 'FLD_ZONE,SFHA_TF,FLOODWAY', returnGeometry: 'true',
        resultRecordCount: '2000', f: 'geojson',
      });
      const url = `https://hazards.fema.gov/arcgis/rest/services/public/NFHL/MapServer/28/query?${params}`;
      setFloodZonesLoading(true);
      setFloodZonesError(null);

      // Retry with exponential backoff — FEMA NFHL occasionally returns 502/504
      // on the first request (cold cache on their CDN).  Three attempts with
      // 2 s / 4 s delays recover the vast majority of transient failures.
      const MAX_ATTEMPTS = 3;
      let lastErr: any = null;
      for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
        const ac = new AbortController();
        const timeout = setTimeout(() => ac.abort(), 20_000);
        try {
          const res = await fetch(url, { signal: ac.signal });
          clearTimeout(timeout);
          if (!res.ok) {
            // 5xx errors are retryable; 4xx are not (bad request — bail out)
            if (res.status >= 500 && attempt < MAX_ATTEMPTS) {
              lastErr = new Error(`NFHL ${res.status}`);
              await new Promise(r => setTimeout(r, (2 ** attempt) * 1000));
              continue;
            }
            throw new Error(`NFHL ${res.status}`);
          }
          const data = await res.json();
          if (data?.error) throw new Error(data.error.message || 'NFHL error');
          setFloodZonesGeoJSON(data?.features?.length ? data : { type: 'FeatureCollection', features: [] });
          setFloodZonesError(null);
          setFloodZonesLoading(false);
          return;
        } catch (err: any) {
          clearTimeout(timeout);
          lastErr = err;
          if (err?.name === 'AbortError') break; // timeout — no point retrying
          if (attempt < MAX_ATTEMPTS) {
            await new Promise(r => setTimeout(r, (2 ** attempt) * 1000));
          }
        }
      }
      // All attempts exhausted
      setFloodZonesError(
        lastErr?.name === 'AbortError'
          ? 'FEMA flood-zone fetch timed out'
          : 'Could not load FEMA flood zones'
      );
      setFloodZonesLoading(false);
    }, 600);
  }, []);

  // ── Stream gauges ─────────────────────────────────────────────────────────
  const [showGauges, setShowGauges] = useState(false);
  const [gaugesGeoJSON, setGaugesGeoJSON] = useState<any>(null);
  const [gaugesLoading, setGaugesLoading] = useState(false);
  const [gaugesError, setGaugesError] = useState<string | null>(null);
  const [gaugesSummary, setGaugesSummary] = useState<GaugesSummary | null>(null);

  // ── Shelters ──────────────────────────────────────────────────────────────
  const [showShelters, setShowShelters] = useState(false);
  const [sheltersData, setSheltersData] = useState<SheltersData | null>(null);
  const [sheltersLoading, setSheltersLoading] = useState(false);
  const [sheltersError, setSheltersError] = useState<string | null>(null);

  // ── Misc toggles ──────────────────────────────────────────────────────────
  const [showLandUse, setShowLandUse] = useState(false);

  return {
    // counties
    showCounties, setShowCounties,
    countiesGeoJSON,
    countiesLoading, countiesError,
    // cities
    citiesData,
    // flood zones
    showFloodZones, setShowFloodZones,
    floodZonesGeoJSON,
    floodZonesLoading, floodZonesError,
    fetchFloodZones,
    // gauges
    showGauges, setShowGauges,
    gaugesGeoJSON, setGaugesGeoJSON,
    gaugesLoading, setGaugesLoading,
    gaugesError, setGaugesError,
    gaugesSummary, setGaugesSummary,
    // shelters
    showShelters, setShowShelters,
    sheltersData, setSheltersData,
    sheltersLoading, setSheltersLoading,
    sheltersError, setSheltersError,
    // misc
    showLandUse, setShowLandUse,
  };
}
