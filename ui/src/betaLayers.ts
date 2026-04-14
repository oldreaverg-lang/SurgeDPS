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
// ───────────────────────────────────────────────────────────
// The app is mounted at /surgedps/ on stormdps.com, but the
// backend returns tile URL templates rooted at /api/... (it has
// no knowledge of the public path prefix). MapLibre resolves
// these as page-root-relative, so the tile requests go to
// stormdps.com/api/... which 404s. Prefix here so tiles load.
// ───────────────────────────────────────────────────────────
function prefixTileUrl(raw: unknown): string | null {
  if (typeof raw !== 'string' || !raw) return null;
  // Absolute (http:// or https://) URLs are left alone.
  if (/^https?:\/\//i.test(raw)) return raw;
  // Already prefixed — no-op.
  if (raw.startsWith('/surgedps/')) return raw;
  // Root-relative backend path → prepend /surgedps.
  if (raw.startsWith('/')) return `/surgedps${raw}`;
  return raw;
}

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
  // Backend now uses a background-job pattern: first response may be
  // {status:"pending"} while IEM downloads 72 GRIB2 files (~90-120 s cold).
  // We poll every 5 s for up to 3 minutes before giving up.
  const url = `/surgedps/api/rainfall?duration=${durationHr}&pass=${passLevel}&realtime=0`;
  const POLL_INTERVAL_MS = 5_000;
  const MAX_POLLS = 36; // 3 minutes total

  const _parse = (data: any): RainfallOverlay => {
    const maxIn = data.max_precip_mm != null ? data.max_precip_mm / 25.4 : null;
    const avgIn = data.avg_precip_mm != null ? data.avg_precip_mm / 25.4 : null;
    return {
      available: true,
      source: 'mrms',
      tileUrlTemplate: prefixTileUrl(data.tile_url_template),
      validTime: data.valid_time ?? null,
      bboxInches: maxIn != null && avgIn != null ? [0, +maxIn.toFixed(1)] : null,
      maxPrecipMm: data.max_precip_mm ?? null,
      avgPrecipMm: data.avg_precip_mm ?? null,
      durationHr: data.duration_hr ?? durationHr,
      product: data.product ?? null,
      notes: `MRMS ${data.product ?? ''} · source: ${data.source ?? 'unknown'} · max ${maxIn != null ? maxIn.toFixed(1) + ' in' : '—'}`,
    };
  };

  try {
    for (let poll = 0; poll < MAX_POLLS; poll++) {
      const resp = await fetch(url, { signal: AbortSignal.timeout(15_000) });
      if (!resp.ok) {
        const text = await resp.text().catch(() => resp.statusText);
        return { available: false, source: 'none', tileUrlTemplate: null,
          validTime: null, bboxInches: null, maxPrecipMm: null,
          avgPrecipMm: null, durationHr: null, product: null,
          notes: `MRMS fetch error (${resp.status}): ${text}` };
      }
      const data = await resp.json();
      if (data.status === 'pending') {
        // Background job still running — wait and retry
        await new Promise(r => setTimeout(r, POLL_INTERVAL_MS));
        continue;
      }
      if (data.available === false) {
        return { available: false, source: 'none', tileUrlTemplate: null,
          validTime: null, bboxInches: null, maxPrecipMm: null,
          avgPrecipMm: null, durationHr: null, product: null,
          notes: data.notes ?? 'MRMS data unavailable for this storm' };
      }
      return _parse(data);
    }
    return { available: false, source: 'none', tileUrlTemplate: null,
      validTime: null, bboxInches: null, maxPrecipMm: null,
      avgPrecipMm: null, durationHr: null, product: null,
      notes: 'MRMS processing timed out — data may appear after page refresh' };
  } catch (err) {
    return { available: false, source: 'none', tileUrlTemplate: null,
      validTime: null, bboxInches: null, maxPrecipMm: null,
      avgPrecipMm: null, durationHr: null, product: null,
      notes: `MRMS unavailable: ${err instanceof Error ? err.message : String(err)}` };
  }
}

// ───────────────────────────────────────────────────────────
// WPC QPF forecast overlay (72-hour precipitation forecast)
//
// Companion to fetchRainfallOverlay: MRMS is *observed* rainfall
// (what already fell); QPF is *forecast* rainfall (what WPC expects
// in the next 72 hrs). Same colormap on render because both are
// precip totals in mm.
// ───────────────────────────────────────────────────────────
export type QPFOverlay = {
  available: boolean;
  maxPrecipMm: number | null;
  durationHr: number | null;
  source: string | null;
  caveat: string | null;
  tileUrlTemplate: string | null;
  notes: string;
};

export async function fetchQPFOverlay(_stormId: string): Promise<QPFOverlay> {
  try {
    const resp = await fetch('/surgedps/api/qpf', { signal: AbortSignal.timeout(30_000) });
    if (!resp.ok) {
      const text = await resp.text().catch(() => resp.statusText);
      return {
        available: false, maxPrecipMm: null, durationHr: null,
        source: null, caveat: null, tileUrlTemplate: null,
        notes: `QPF fetch error (${resp.status}): ${text}`,
      };
    }
    const data = await resp.json();
    return {
      available: !!data.available,
      maxPrecipMm: data.max_precip_mm ?? null,
      durationHr: data.duration_hr ?? null,
      source: data.source ?? null,
      caveat: data.caveat ?? null,
      tileUrlTemplate: prefixTileUrl(data.tile_url_template),
      notes: data.caveat ?? '',
    };
  } catch (err) {
    return {
      available: false, maxPrecipMm: null, durationHr: null,
      source: null, caveat: null, tileUrlTemplate: null,
      notes: `QPF unavailable: ${err instanceof Error ? err.message : String(err)}`,
    };
  }
}

// ───────────────────────────────────────────────────────────
// Compound hazard overlay (surge ∪ rainfall, per-cell mosaic)
//
// Backend stitches per-cell compound.tif into a storm-wide
// mosaic on demand. Returns depth stats + an XYZ tile URL
// template rendered with a depth colormap (pale cyan → violet).
// ───────────────────────────────────────────────────────────
export type CompoundOverlay = {
  available: boolean;
  cellCount: number;
  maxDepthFt: number | null;
  avgDepthFt: number | null;
  tileUrlTemplate: string | null;
  notes: string;
};

export async function fetchCompoundOverlay(
  _stormId: string,
): Promise<CompoundOverlay> {
  try {
    const resp = await fetch('/surgedps/api/compound', {
      signal: AbortSignal.timeout(20_000),
    });
    if (!resp.ok) {
      const text = await resp.text().catch(() => resp.statusText);
      return {
        available: false, cellCount: 0,
        maxDepthFt: null, avgDepthFt: null, tileUrlTemplate: null,
        notes: `Compound fetch error (${resp.status}): ${text}`,
      };
    }
    const data = await resp.json();
    return {
      available: !!data.available,
      cellCount: data.cell_count ?? 0,
      maxDepthFt: data.max_depth_ft ?? null,
      avgDepthFt: data.avg_depth_ft ?? null,
      tileUrlTemplate: prefixTileUrl(data.tile_url_template),
      notes: data.notes ?? '',
    };
  } catch (err) {
    return {
      available: false, cellCount: 0,
      maxDepthFt: null, avgDepthFt: null, tileUrlTemplate: null,
      notes: `Compound unavailable: ${err instanceof Error ? err.message : String(err)}`,
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
    const url = `/surgedps/api/gauges?radius=${radiusDeg}&category=${minCategory}`;
    // 60s timeout — first hit for a storm makes a slow AHPS call, but the
    // response is then cached permanently on the Railway volume, so every
    // subsequent request for that storm returns in <100ms.
    const resp = await fetch(url, { signal: AbortSignal.timeout(60_000) });
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

/**
 * Fetch shelter capacity/occupancy for the active storm.
 * Calls GET /api/shelters?radius_km=<n>. Backend sources from a
 * shelters.geojson dropped into PERSISTENT_DIR/shelters/; a future
 * iteration will blend live feeds (Red Cross iAM, FEMA NSS).
 */
export async function fetchShelterCapacity(
  _stormId: string,
  _center: { lat: number; lon: number } | null,
  radiusKm = 200,
): Promise<ShelterCapacityLayer> {
  try {
    const resp = await fetch(`/surgedps/api/shelters?radius_km=${radiusKm}`, {
      signal: AbortSignal.timeout(15_000),
    });
    if (!resp.ok) {
      return {
        available: false, shelters: [], totalCapacity: 0, totalOccupancy: null,
        notes: `Shelter fetch error (${resp.status}): ${resp.statusText}`,
      };
    }
    const data = await resp.json();
    const shelters: Shelter[] = (data.shelters || []).map((s: any) => ({
      id: s.id, name: s.name, lat: s.lat, lon: s.lon,
      capacity: s.capacity ?? 0,
      occupancy: s.occupancy ?? null,
      operator: s.operator ?? 'Unknown',
      isAccessible: !!s.is_accessible,
      isPetFriendly: !!s.is_pet_friendly,
      lastUpdated: s.last_updated ?? null,
      notes: s.notes,
    }));
    return {
      available: !!data.available,
      shelters,
      totalCapacity: data.total_capacity ?? 0,
      totalOccupancy: data.total_occupancy ?? null,
      notes: data.notes ?? '',
    };
  } catch (err) {
    return {
      available: false, shelters: [], totalCapacity: 0, totalOccupancy: null,
      notes: `Shelter data unavailable: ${err instanceof Error ? err.message : String(err)}`,
    };
  }
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

/**
 * Fetch vendor coverage for the active storm. Hits /api/vendor_coverage
 * which reads PERSISTENT_DIR/vendors/vendors.json and computes each
 * vendor's coverage % against the storm footprint. See
 * PHASE5_DATA_CONTRACTS.md §3.
 */
export async function fetchVendorCoverage(
  _stormId: string,
): Promise<VendorCoverageLayer> {
  try {
    const resp = await fetch('/surgedps/api/vendor_coverage', {
      signal: AbortSignal.timeout(15_000),
    });
    if (!resp.ok) {
      return { available: false, vendors: [],
        notes: `Vendor coverage fetch error (${resp.status}): ${resp.statusText}` };
    }
    const data = await resp.json();
    const vendors: VendorCoverage[] = (data.vendors || []).map((v: any) => ({
      vendorId: v.vendor_id,
      vendorName: v.vendor_name,
      specialties: v.specialties || [],
      coveragePct: v.coverage_pct ?? 0,
      contactUrl: v.contact_url ?? null,
      notes: v.notes,
    }));
    return { available: !!data.available, vendors, notes: data.notes ?? '' };
  } catch (err) {
    return { available: false, vendors: [],
      notes: `Vendor coverage unavailable: ${err instanceof Error ? err.message : String(err)}` };
  }
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
  maxDepthFt?: number | null;    // deepest inundation on the best route
  miles?: number | null;         // route length (OSM reachability only)
  notes?: string;
};

export type TimeToAccessLayer = {
  available: boolean;
  estimates: AccessEstimate[];
  generatedAt: string | null;
  notes: string;
};

/**
 * Fetch time-to-access estimates for a set of hotspot ranks. Calls
 * GET /api/time_to_access?ranks=1,2,3 — the backend uses GET (not POST
 * as documented) because the payload is trivially small. v1 returns
 * heuristic ETAs (confidence='low'); OSM × depth reachability is
 * the next upgrade. See PHASE5_DATA_CONTRACTS.md §4.
 */
export type HotspotRef = { rank: number; lat: number; lon: number };

export async function fetchTimeToAccess(
  _stormId: string,
  hotspots: number[] | HotspotRef[],
): Promise<TimeToAccessLayer> {
  try {
    if (!hotspots.length) {
      return { available: false, estimates: [], generatedAt: null,
        notes: 'No hotspots to estimate.' };
    }
    // Support both call shapes: bare ranks (legacy) and {rank,lat,lon}
    // objects (preferred — lets the backend route on OSM roads).
    const first = hotspots[0] as any;
    const hasCoords = typeof first === 'object' && 'lat' in first && 'lon' in first;
    const ranks = hasCoords
      ? (hotspots as HotspotRef[]).map(h => h.rank).join(',')
      : (hotspots as number[]).join(',');
    const coords = hasCoords
      ? (hotspots as HotspotRef[]).map(h => `${h.lon.toFixed(5)},${h.lat.toFixed(5)}`).join(';')
      : '';
    const qs = coords
      ? `ranks=${encodeURIComponent(ranks)}&coords=${encodeURIComponent(coords)}`
      : `ranks=${encodeURIComponent(ranks)}`;
    const resp = await fetch(`/surgedps/api/time_to_access?${qs}`, {
      signal: AbortSignal.timeout(30_000),
    });
    if (!resp.ok) {
      return { available: false, estimates: [], generatedAt: null,
        notes: `Time-to-access fetch error (${resp.status}): ${resp.statusText}` };
    }
    const data = await resp.json();
    const estimates: AccessEstimate[] = (data.estimates || []).map((e: any) => ({
      hotspotRank: e.hotspot_rank,
      etaHours: e.eta_hours ?? null,
      limitingFactor: e.limiting_factor ?? 'unknown',
      confidence: e.confidence ?? 'low',
      maxDepthFt: e.max_depth_ft ?? null,
      miles: e.miles ?? null,
      notes: e.notes,
    }));
    return {
      available: !!data.available,
      estimates,
      generatedAt: data.generated_at ?? null,
      notes: data.notes ?? '',
    };
  } catch (err) {
    return { available: false, estimates: [], generatedAt: null,
      notes: `Time-to-access unavailable: ${err instanceof Error ? err.message : String(err)}` };
  }
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
