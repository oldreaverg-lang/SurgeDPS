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
// currently only models coastal surge. v1 target source is
// MRMS (Multi-Radar Multi-Sensor) QPE composite, falling back
// to StormDPS's rainfall service if that ships first. The
// frontend expects a raster tile URL template (MapLibre
// `raster` source) plus a legend for the color ramp.
// ───────────────────────────────────────────────────────────
export type RainfallSource = 'mrms' | 'stormdps' | 'none';

export type RainfallOverlay = {
  available: boolean;                 // false until backend ships
  source: RainfallSource;
  tileUrlTemplate: string | null;     // e.g. "/surgedps/api/rainfall/{z}/{x}/{y}.png?storm_id=…"
  validTime: string | null;           // ISO string — when the raster was generated
  bboxInches: [number, number] | null; // min/max of the color ramp for legend
  notes: string;                      // human-readable caveat
};

// TODO(backend): wire to /surgedps/api/rainfall?storm_id=…
// Response shape should match RainfallOverlay directly.
export async function fetchRainfallOverlay(
  _stormId: string,
): Promise<RainfallOverlay> {
  return {
    available: false,
    source: 'none',
    tileUrlTemplate: null,
    validTime: null,
    bboxInches: null,
    notes: 'Rainfall overlay not yet integrated. Backend endpoint /surgedps/api/rainfall pending (MRMS QPE composite or StormDPS rainfall service).',
  };
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
