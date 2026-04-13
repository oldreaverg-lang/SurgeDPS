// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Beta data layers — Phase 5 scaffolding
//
// CAT_TEAM_PLAN §8 Phase 5 items: B7 (rainfall overlay), E5
// (shelter capacity), C6 (claims routing vendor coverage), and
// E6 (time-to-access estimate). Each of these requires a real
// backend data source that this module does not yet have.
//
// This file is intentionally frontend-only scaffolding:
//   1. It defines the *shape* of the data each panel will
//      eventually consume (the "data contract").
//   2. It exposes stub fetchers that return empty state today
//      and will become real HTTP calls once the backend ships.
//   3. Every stub has a TODO block pointing to the endpoint the
//      backend team should implement (see
//      PHASE5_DATA_CONTRACTS.md in repo root for the full spec).
//
// Keep this module pure — no React imports, no DOM, no global
// state. The UI side owns flag gating and presentation.
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

// ───────────────────────────────────────────────────────────
// B7 — Rainfall overlay
//
// Intent: show inland flooding contribution where SurgeDPS
// currently only models coastal surge. v1 source: MRMS QPE
// composite fetched via /api/rainfall (backed by NOAA S3 or
// NCEP NOMADS). The backend returns stats; tile serving for
// the map raster is a Phase 6 item (requires a COG tile server
// or XYZ endpoint conversion from the clipped GeoTIFF).
// ───────────────────────────────────────────────────────────
export type RainfallSource = 'mrms' | 'stormdps' | 'none';

export type RainfallOverlay = {
  available: boolean;
  source: RainfallSource;
  tileUrlTemplate: string | null;     // Phase 6: COG tile server URL
  validTime: string | null;           // ISO string — MRMS product valid time
  bboxInches: [number, number] | null; // [min, max] inches across storm bbox
  maxPrecipMm: number | null;
  avgPrecipMm: number | null;
  durationHr: number | null;
  product: string | null;             // e.g. "MultiSensor_QPE_72H_Pass2"
  notes: string;
};

/**
 * Fetch MRMS QPE accumulation stats for the active storm.
 *
 * Calls GET /api/rainfall?duration={durationHr}&pass={passLevel}
 * Returns available=true with stats when MRMS data is accessible;
 * falls back to available=false with notes on failure.
 *
 * @param durationHr  Accumulation window in hours (24, 48, 72). Default 72.
 * @param passLevel   MRMS pass level (1=radar-only, 2=gauge-corrected). Default 2.
 */
export async function fetchRainfallOverlay(
  _stormId: string,
  durationHr = 72,
  passLevel = 2,
): Promise<RainfallOverlay> {
  try {
    const url = `/api/rainfall?duration=${durationHr}&pass=${passLevel}&realtime=0`;
    const resp = await fetch(url, { signal: AbortSignal.timeout(15_000) });
    if (!resp.ok) {
      const text = await resp.text().catch(() => resp.statusText);
      return {
        available: false, source: 'none', tileUrlTemplate: null,
        validTime: null, bboxInches: null, maxPrecipMm: null,
        avgPrecipMm: null, durationHr: null, product: null,
        notes: `MRMS fetch error (${resp.status}): ${text}`,
      };
    }
    const data = await resp.json();
    const maxIn  = data.max_precip_mm != null ? data.max_precip_mm / 25.4 : null;
    const avgIn  = data.avg_precip_mm != null ? data.avg_precip_mm / 25.4 : null;
    return {
      available: true,
      source: 'mrms',
      tileUrlTemplate: null,   // Phase 6: wire COG/XYZ tiles here
      validTime: data.valid_time ?? null,
      bboxInches: maxIn != null && avgIn != null ? [0, +maxIn.toFixed(1)] : null,
      maxPrecipMm: data.max_precip_mm ?? null,
      avgPrecipMm: data.avg_precip_mm ?? null,
      durationHr: data.duration_hr ?? durationHr,
      product: data.product ?? null,
      notes: `MRMS ${data.product ?? ''} · source: ${data.source ?? 'unknown'} · max ${maxIn != null ? maxIn.toFixed(1) + ' in' : '—'}`,
    };
  } catch (err) {
    return {
      available: false, source: 'none', tileUrlTemplate: null,
      validTime: null, bboxInches: null, maxPrecipMm: null,
      avgPrecipMm: null, durationHr: null, product: null,
      notes: `MRMS unavailable: ${err instanceof Error ? err.message : String(err)}`,
    };
  }
}

// ───────────────────────────────────────────────────────────
// Stream gauge overlay (NOAA AHPS / NWPS)
//
// Returns active flood gauges near the storm landfall as a
// GeoJSON FeatureCollection plus summary counts by category.
// ───────────────────────────────────────────────────────────
export type GaugeSummary = {
  available: boolean;
  gaugeCount: number;
  atOrAboveMajor: number;
  atOrAboveModerate: number;
  atOrAboveMinor: number;
  geojson: GeoJSON.FeatureCollection | null;
  notes: string;
};

// Minimal GeoJSON types (avoids adding @types/geojson if not already a dep)
declare namespace GeoJSON {
  interface FeatureCollection { type: 'FeatureCollection'; features: Feature[]; }
  interface Feature { type: 'Feature'; geometry: unknown; properties: Record<string, unknown> | null; }
}

/**
 * Fetch active stream gauges near the storm's landfall.
 *
 * Calls GET /api/gauges?radius={radiusDeg}&category={minCategory}
 *
 * @param radiusDeg   Search radius in decimal degrees (~111 km/°). Default 4.
 * @param minCategory Minimum flood category: "action"|"minor"|"moderate"|"major".
 */
export async function fetchGaugeOverlay(
  _stormId: string,
  radiusDeg = 4.0,
  minCategory: 'action' | 'minor' | 'moderate' | 'major' = 'action',
): Promise<GaugeSummary> {
  try {
    const url = `/api/gauges?radius=${radiusDeg}&category=${minCategory}`;
    const resp = await fetch(url, { signal: AbortSignal.timeout(15_000) });
    if (!resp.ok) {
      return {
        available: false, gaugeCount: 0,
        atOrAboveMajor: 0, atOrAboveModerate: 0, atOrAboveMinor: 0,
        geojson: null,
        notes: `Gauge fetch error (${resp.status}): ${resp.statusText}`,
      };
    }
    const data = await resp.json();
    const count = data.gauge_count ?? data.gauges?.features?.length ?? 0;
    return {
      available: true,
      gaugeCount: count,
      atOrAboveMajor:    data.at_or_above_major    ?? 0,
      atOrAboveModerate: data.at_or_above_moderate ?? 0,
      atOrAboveMinor:    data.at_or_above_minor    ?? 0,
      geojson: data.gauges ?? null,
      notes: count > 0
        ? `${count} gauge${count === 1 ? '' : 's'} at or above ${minCategory} stage`
        : `No gauges at or above ${minCategory} stage within ${radiusDeg}° radius`,
    };
  } catch (err) {
    return {
      available: false, gaugeCount: 0,
      atOrAboveMajor: 0, atOrAboveModerate: 0, atOrAboveMinor: 0,
      geojson: null,
      notes: `Gauge data unavailable: ${err instanceof Error ? err.message : String(err)}`,
    };
  }
}

// ───────────────────────────────────────────────────────────
// E5 — Shelter capacity overlay
//
// Intent: show Red Cross / county-operated shelter locations,
// capacity, and live occupancy (if available) so an EM can
// compare bed availability against the displaced-population
// estimate from stagingPlan(). v1 source candidates: Red Cross
// Open API, FEMA OpenFEMA shelter registry, or state EM feeds.
// ───────────────────────────────────────────────────────────
export type Shelter = {
  id: string;
  name: string;
  lat: number;
  lon: number;
  capacity: number;          // total beds
  occupancy: number | null;  // current occupied, null if unknown
  operator: string;          // "Red Cross", county name, etc.
  isAccessible: boolean;     // ADA / medical needs
  isPetFriendly: boolean;
  lastUpdated: string | null;// ISO
  notes?: string;
};

export type ShelterCapacityLayer = {
  available: boolean;
  shelters: Shelter[];
  totalCapacity: number;
  totalOccupancy: number | null;
  notes: string;
};

// TODO(backend): wire to /surgedps/api/shelters?lat=&lon=&radius_km=
// Should accept a bounding box or radius around the active storm's
// landfall location and return the ShelterCapacityLayer shape.
export async function fetchShelterCapacity(
  _stormId: string,
  _center: { lat: number; lon: number } | null,
): Promise<ShelterCapacityLayer> {
  return {
    available: false,
    shelters: [],
    totalCapacity: 0,
    totalOccupancy: null,
    notes: 'Shelter capacity layer not yet integrated. Candidate sources: Red Cross Open API, FEMA OpenFEMA shelter registry, or state EM feeds. Endpoint /surgedps/api/shelters pending.',
  };
}

// ───────────────────────────────────────────────────────────
// C6 — Claims routing vendor coverage
//
// Intent: show which parts of the affected area fall within a
// known vendor's repair/mitigation coverage footprint so the CAT
// lead can skip cold-calling contractors in every parish. This
// was always flagged as "future MCP integration" in the plan.
// v1 target: a polygon layer per major national vendor
// (ServiceMaster, BELFOR, etc.) plus per-area counts.
// ───────────────────────────────────────────────────────────
export type VendorCoverage = {
  vendorId: string;
  vendorName: string;
  specialties: Array<'water' | 'wind' | 'fire' | 'mold' | 'reconstruction'>;
  coveragePct: number;       // 0..100 of analyzed area covered by this vendor
  contactUrl: string | null;
  notes?: string;
};

export type VendorCoverageLayer = {
  available: boolean;
  vendors: VendorCoverage[];
  notes: string;
};

// TODO(backend): wire to /surgedps/api/vendor_coverage?storm_id=
// Can be driven by a future MCP connector or a static GeoJSON
// coverage file per vendor. See PHASE5_DATA_CONTRACTS.md §3.
export async function fetchVendorCoverage(
  _stormId: string,
): Promise<VendorCoverageLayer> {
  return {
    available: false,
    vendors: [],
    notes: 'Vendor coverage layer not yet integrated. Will be driven by a per-vendor polygon set or an MCP connector query. Endpoint /surgedps/api/vendor_coverage pending.',
  };
}

// ───────────────────────────────────────────────────────────
// E6 — Time-to-access estimate
//
// Intent: answer "when can my assessment team actually get to
// area #3?" Driven by depth × road-network reachability: while
// major arterials are inundated above N feet, the area is
// effectively unreachable to non-amphibious vehicles. Expect
// ETA in hours from storm passage.
//
// v1 source candidates: OSM road network + current SurgeDPS
// depth rasters; or a state DOT closures feed if one exists.
// ───────────────────────────────────────────────────────────
export type AccessEstimate = {
  hotspotRank: number;
  etaHours: number | null;       // hours until area is likely accessible
  limitingFactor: 'surge' | 'road_closure' | 'debris' | 'unknown';
  confidence: 'low' | 'medium' | 'high';
  notes?: string;
};

export type TimeToAccessLayer = {
  available: boolean;
  estimates: AccessEstimate[];
  generatedAt: string | null;
  notes: string;
};

// TODO(backend): wire to /surgedps/api/time_to_access?storm_id=
// Input: the hotspot list as currently sent to the UI. Output:
// one AccessEstimate per hotspot rank. See §4 in data-contracts.
export async function fetchTimeToAccess(
  _stormId: string,
  _hotspotRanks: number[],
): Promise<TimeToAccessLayer> {
  return {
    available: false,
    estimates: [],
    generatedAt: null,
    notes: 'Time-to-access estimate not yet integrated. Requires OSM road network overlay and depth-over-road reachability analysis. Endpoint /surgedps/api/time_to_access pending.',
  };
}

// ───────────────────────────────────────────────────────────
// Shared beta-flag helpers
// ───────────────────────────────────────────────────────────
export const BETA_LAYERS_STORAGE_KEY = 'surgedps.betaDataLayers';

export function readBetaLayersEnabled(): boolean {
  if (typeof window === 'undefined') return false;
  try {
    return window.localStorage.getItem(BETA_LAYERS_STORAGE_KEY) === 'true';
  } catch {
    return false;
  }
}

export function writeBetaLayersEnabled(enabled: boolean): void {
  if (typeof window === 'undefined') return;
  try {
    window.localStorage.setItem(BETA_LAYERS_STORAGE_KEY, enabled ? 'true' : 'false');
  } catch {
    /* ignore */
  }
}
