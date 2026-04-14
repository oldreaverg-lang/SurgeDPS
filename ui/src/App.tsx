import { useState, useEffect, useCallback, useMemo, useRef } from 'react';
import Map, { Source, Layer, NavigationControl, Popup, Marker } from 'react-map-gl/maplibre';
import type { MapRef } from 'react-map-gl/maplibre';
import maplibregl from 'maplibre-gl';
import 'maplibre-gl/dist/maplibre-gl.css';
import {
  recommendAdjusters,
  routingHint,
  workloadSummary,
  aggregatePerilMix,
  perilHeadline,
  planDeployment,
  suggestTeamSize,
  timeToClearDays,
  formatTimeToClear,
  shelterPosture,
  worstShelterPosture,
  stagingPlan,
} from './catTeam';
import type { RoutingTag, AdjusterRecommendation, SubPersona, StagingPlan } from './catTeam';
import { buildCatDeploymentReport, buildSitRep, draftPublicAdvisory } from './catReports';
import {
  readBetaLayersEnabled,
  writeBetaLayersEnabled,
  fetchRainfallOverlay,
  fetchGaugeOverlay,
  fetchShelterCapacity,
  fetchVendorCoverage,
  fetchTimeToAccess,
} from './betaLayers';
import type {
  RainfallOverlay,
  ShelterCapacityLayer,
  VendorCoverageLayer,
  TimeToAccessLayer,
} from './betaLayers';
import { rollupByCounty, rollupToCentroidGeoJSON, rollupByCity, cityRollupToCentroidGeoJSON, AVG_HOUSEHOLD, DISPLACEMENT_HAIRCUT } from './jurisdictions';
import type { CountyRollup, CityEntry } from './jurisdictions';

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// PMTiles Protocol (cloud-native vector tiles for flood polygons)
// Graceful no-op if pmtiles package is not installed yet
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
(async () => {
  try {
    const { Protocol } = await import('pmtiles');
    const protocol = new Protocol();
    maplibregl.addProtocol('pmtiles', protocol.tile);
  } catch { /* pmtiles not installed — GeoJSON fallback */ }
})();

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Grid Constants
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
const CELL_WIDTH = 0.4;
const CELL_HEIGHT = 0.3;

function cellBbox(col: number, row: number, oLon: number, oLat: number): [number, number, number, number] {
  return [oLon + col * CELL_WIDTH, oLat + row * CELL_HEIGHT, oLon + (col + 1) * CELL_WIDTH, oLat + (row + 1) * CELL_HEIGHT];
}
function cellKey(col: number, row: number) { return `${col},${row}`; }
function cellPolygon(col: number, row: number, status: string, oLon: number, oLat: number) {
  const [w, s, e, n] = cellBbox(col, row, oLon, oLat);
  return {
    type: 'Feature' as const,
    properties: { col, row, key: cellKey(col, row), status },
    geometry: { type: 'Polygon' as const, coordinates: [[[w, s], [e, s], [e, n], [w, n], [w, s]]] },
  };
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Types
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
interface StormInfo {
  storm_id: string; name: string; year: number; category: number;
  status: string; landfall_lon: number; landfall_lat: number;
  max_wind_kt: number; min_pressure_mb: number;
  grid_origin_lon: number; grid_origin_lat: number;
  rmax_nm?: number;
  dps_score: number;
  confidence?: string;
  building_count?: number;
  population?: { county_name?: string; state_code?: string; population?: number; pop_label?: string; vintage?: number };
}
interface Season { year: number; count: number; }

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Styles
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// ── Basemap styles ──
const BASEMAPS: Record<string, any> = {
  dark: 'https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json',
  satellite: {
    version: 8, name: 'Satellite', sources: {
      'esri-sat': { type: 'raster', tiles: ['https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}'], tileSize: 256, attribution: 'Esri World Imagery' },
    }, layers: [{ id: 'esri-sat', type: 'raster', source: 'esri-sat' }],
  },
  street: {
    version: 8, name: 'Street', sources: {
      'osm': { type: 'raster', tiles: ['https://tile.openstreetmap.org/{z}/{x}/{y}.png'], tileSize: 256, attribution: '© OpenStreetMap' },
    }, layers: [{ id: 'osm', type: 'raster', source: 'osm' }],
  },
};
const BASEMAP_LABELS: Record<string, string> = { dark: 'Dark', satellite: 'Satellite', street: 'Street' };

const floodLayerStyle = {
  id: 'flood-depth-layer', type: 'fill',
  paint: {
    'fill-color': ['interpolate', ['linear'], ['get', 'depth'], 0.05, '#ffffb2', 0.3, '#fecc5c', 0.9, '#fd8d3c', 1.8, '#f03b20', 3.0, '#bd0026'],
    'fill-opacity': ['interpolate', ['linear'], ['zoom'], 10, 0.35, 13, 0.3, 15, 0.15, 17, 0.08],
  },
};
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Building Type Lookup (Hazus codes → human-readable)
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
const BUILDING_TYPES: Record<string, string> = {
  RES1: 'Single-Family Home', RES2: 'Mobile Home', RES3: 'Multi-Family Housing',
  RES4: 'Hotel / Motel', RES5: 'Dormitory', RES6: 'Nursing Home',
  COM1: 'Retail Store', COM2: 'Warehouse', COM3: 'Service Business',
  COM4: 'Office Building', COM5: 'Bank / Financial', COM6: 'Hospital',
  COM7: 'Medical Clinic', COM8: 'Entertainment Venue', COM9: 'Theater',
  COM10: 'Parking Structure', IND1: 'Heavy Industrial', IND2: 'Light Industrial',
  IND3: 'Food / Chemical Plant', IND4: 'Metal / Minerals Facility',
  IND5: 'High-Tech Industrial', IND6: 'Construction Facility',
  AGR1: 'Agricultural Building', REL1: 'Church / Place of Worship',
  GOV1: 'Government Building', GOV2: 'Emergency Services',
  EDU1: 'School', EDU2: 'College / University',
};

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Critical Facilities (for emergency management)
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Keyed off the raw NSI `occtype` prefix (NOT `building_type`). The backend
// pipeline (nsi_fetcher._nsi_to_hazus) collapses GOV/EDU/REL → "COM" in
// `building_type` because the depth-damage curves are identical, but it
// preserves the civic/school/medical distinction in the `occtype` field.
// We key off occtype so the emergency-management map can still show where
// the schools, hospitals, and government buildings actually are.
const CRITICAL_ICONS: Record<string, string> = {
  EDU1: '🏫', EDU2: '🏫',
  MED1: '➕', MED2: '➕',        // Hospitals (large + small)
  COM6: '➕', COM7: '➕',        // Medical clinics — still keyed for
                                 // pre-mapped legacy building_type data.
  GOV1: '⭐', GOV2: '⭐',
  REL1: '⛪',                    // Churches — often informal shelters
  RES6: '🛏️',                    // Nursing homes
};
// Extract the prefix the critical-icon lookup keys on. Prefers occtype
// (what NSI actually labels the building) and falls back to building_type
// for legacy cached data that only has the HAZUS-collapsed field.
function criticalPrefix(p: any): string {
  const raw = (p?.occtype || p?.building_type || '');
  return String(raw).replace(/[-_].*$/, '').toUpperCase();
}
function friendlyBuildingType(code: string): string {
  if (!code) return 'Unknown';
  const prefix = code.replace(/[-_].*$/, '').toUpperCase();
  if (BUILDING_TYPES[prefix]) return BUILDING_TYPES[prefix];
  // Backend collapses GOV/EDU/REL → "COM" and AGR → "IND" for depth-damage
  // curves. When we only have the collapsed code, return the genus name.
  if (prefix === 'COM') return 'Commercial / Civic';
  if (prefix === 'IND') return 'Industrial';
  if (prefix === 'RES') return 'Residential';
  return code;
}

// Prefer the raw NSI occtype (preserves GOV1, EDU1, MED1 detail) when
// rendering human-readable labels in the popover. Falls back to building_type.
function friendlyFacilityLabel(p: any): string {
  return friendlyBuildingType(p?.occtype || p?.building_type);
}
const CAT_COLORS: Record<number, string> = {
  0: '#5eead4', 1: '#facc15', 2: '#fb923c', 3: '#ef4444', 4: '#dc2626', 5: '#7f1d1d',
};

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Helpers (shared between components)
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
const shortName = (name: string) =>
  name.replace(/^(Hurricane|Tropical Storm|Tropical Depression)\s+/i, '');
const byDPS = (a: StormInfo, b: StormInfo) => (b.dps_score || 0) - (a.dps_score || 0);
const csvField = (v: any) => { const s = String(v ?? ''); return s.includes(',') || s.includes('"') || s.includes('\n') ? `"${s.replace(/"/g, '""')}"` : s; };

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Mode-aware formatting helpers (CAT_TEAM_PLAN §7 — false-precision cleanup)
//   Analyst Mode keeps the precise number the tool has always shown.
//   Ops Mode rounds aggressively and uses plain-language bands, because
//   CAT/EM users trust "~$80M" more than "$81.3M" from a model.
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
export type DisplayMode = 'analyst' | 'ops';

function formatLossOps(usd: number, mode: DisplayMode): string {
  if (!isFinite(usd) || usd <= 0) return '—';
  if (mode === 'analyst') {
    if (usd >= 1e9) return `$${(usd / 1e9).toLocaleString(undefined, { maximumFractionDigits: 2 })}B`;
    if (usd >= 1e6) return `$${(usd / 1e6).toLocaleString(undefined, { maximumFractionDigits: 1 })}M`;
    if (usd >= 1e3) return `$${(usd / 1e3).toLocaleString(undefined, { maximumFractionDigits: 0 })}K`;
    return `$${Math.round(usd).toLocaleString()}`;
  }
  // Ops Mode: round to a "confident" bucket so we don't oversell model precision.
  if (usd >= 1e9) {
    const b = usd / 1e9;
    if (b >= 10) return `~$${Math.round(b)}B`;
    return `~$${(Math.round(b * 2) / 2).toFixed(1)}B`; // nearest 0.5B
  }
  if (usd >= 1e8) return `~$${Math.round(usd / 1e8) * 100}M`; // nearest $100M
  if (usd >= 1e7) return `~$${Math.round(usd / 1e7) * 10}M`;  // nearest $10M
  if (usd >= 1e6) return `~$${Math.round(usd / 1e6)}M`;       // nearest $1M
  if (usd >= 1e5) return `~$${Math.round(usd / 1e5) * 100}K`; // nearest $100K
  return '<$1M';
}

function formatCountOps(n: number, mode: DisplayMode): string {
  if (!isFinite(n) || n <= 0) return '0';
  if (mode === 'analyst') return Math.round(n).toLocaleString();
  // Ops Mode: round big counts to avoid false precision
  if (n >= 100_000) return `~${Math.round(n / 1000).toLocaleString()}k`;
  if (n >= 10_000)  return `~${(Math.round(n / 100) / 10).toFixed(1)}k`;
  if (n >= 1_000)   return `~${Math.round(n / 100) * 100}`;
  if (n >= 100)     return `~${Math.round(n / 10) * 10}`;
  return Math.round(n).toLocaleString();
}

function formatDepthOps(ft: number | null | undefined, mode: DisplayMode): string {
  if (ft == null || !isFinite(ft)) return '—';
  if (mode === 'analyst') return `${ft.toFixed(1)} ft`;
  return `~${Math.round(ft)} ft`;
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Haversine distance (km) — used by comparable loss & wind model
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
function haversineKm(lat1: number, lon1: number, lat2: number, lon2: number): number {
  const R = 6371;
  const dLat = (lat2 - lat1) * Math.PI / 180;
  const dLon = (lon2 - lon1) * Math.PI / 180;
  const a = Math.sin(dLat / 2) ** 2 + Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) * Math.sin(dLon / 2) ** 2;
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Parametric wind model — estimate sustained wind (mph) at a point
// Uses modified Rankine vortex with category-scaled Rmax
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
const RMAX_BY_CAT: Record<number, number> = { 0: 100, 1: 80, 2: 60, 3: 45, 4: 35, 5: 25 }; // km

function estimateWindMph(distKm: number, maxWindKt: number, category: number): number {
  const vMax = maxWindKt * 1.15078; // kt → mph
  const rMax = RMAX_BY_CAT[category] ?? 50;
  if (distKm <= 0.1) return vMax;
  if (distKm <= rMax) return vMax * (distKm / rMax);
  // Modified Rankine decay: V ∝ (Rmax/r)^0.5
  return vMax * Math.pow(rMax / distKm, 0.5);
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Wind vs Water attribution model
// Wind damage potential: 0 below 74 mph, ramps up via cubic curve
// Water damage potential: proportional to interior flooding depth
// Returns { windPct, waterPct } summing to 100
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
function windWaterSplit(windMph: number, interiorFloodFt: number): { windPct: number; waterPct: number } {
  // Wind damage potential: 0 at ≤74 mph, cubic ramp to 1.0 at 180 mph
  const windNorm = Math.max(0, (windMph - 74) / (180 - 74));
  const windPotential = Math.min(1, windNorm ** 1.5);
  // Water damage potential: 0 at ≤0 ft, linear ramp to 1.0 at 8 ft interior flood
  const waterPotential = Math.min(1, Math.max(0, interiorFloodFt / 8));
  const total = windPotential + waterPotential;
  if (total < 0.001) return { windPct: 50, waterPct: 50 }; // no damage signal — even split
  const windPct = Math.round((windPotential / total) * 100);
  return { windPct, waterPct: 100 - windPct };
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Comparable loss evidence — find similar buildings within radius
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
const COMP_RADIUS_KM = 0.4; // ~0.25 mi — used in findComparables and UI label
function findComparables(
  features: any[], buildingType: string, lon: number, lat: number, radiusKm: number = COMP_RADIUS_KM
): { count: number; avgLoss: number; minLoss: number; maxLoss: number } {
  const comps: number[] = [];
  const typePrefix = (buildingType || '').replace(/[-_].*$/, '').toUpperCase();
  for (const f of features) {
    const p = f.properties || {};
    const fType = (p.building_type || '').replace(/[-_].*$/, '').toUpperCase();
    if (fType !== typePrefix) continue;
    const [bLon, bLat] = f.geometry?.coordinates || [0, 0];
    const d = haversineKm(lat, lon, bLat, bLon);
    if (d > radiusKm || d < 0.001) continue; // skip self (< 1m away)
    if (p.estimated_loss_usd != null) comps.push(p.estimated_loss_usd);
  }
  if (comps.length === 0) return { count: 0, avgLoss: 0, minLoss: 0, maxLoss: 0 };
  let sum = 0, lo = comps[0], hi = comps[0];
  for (const v of comps) { sum += v; if (v < lo) lo = v; if (v > hi) hi = v; }
  return { count: comps.length, avgLoss: Math.round(sum / comps.length), minLoss: lo, maxLoss: hi };
}
const dpsColor = (score: number): string => {
  if (score >= 80) return '#ef4444';
  if (score >= 60) return '#f97316';
  if (score >= 40) return '#fbbf24';
  if (score >= 20) return '#34d399';
  if (score >= 10) return '#60a5fa';
  return '#94a3b8';
};

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// StormRow (defined outside StormBrowser to avoid remount on every render)
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
function StormRow({ s, activeStormId, activating, onSelect }: {
  s: StormInfo; activeStormId: string | null; activating: boolean;
  onSelect: (id: string) => void;
}) {
  const isActive = s.storm_id === activeStormId;
  const dot = dpsColor(s.dps_score || 0);
  const catColor = CAT_COLORS[s.category] ?? '#94a3b8';
  return (
    <button
      onClick={() => onSelect(s.storm_id)}
      disabled={activating}
      className={`w-full text-left px-3 py-2 flex items-center gap-2 transition-colors rounded-md text-sm ${
        isActive ? 'bg-indigo-500/20 text-white' : 'text-slate-300 hover:bg-slate-700/60 hover:text-white'
      }`}
    >
      {/* Category dot */}
      <span style={{ background: catColor, width: 8, height: 8, borderRadius: '50%', flexShrink: 0 }} />
      <span className="truncate font-medium">{shortName(s.name)}</span>
      {/* Year chip — muted, helps disambiguate same-name storms */}
      <span className="text-[10px] text-slate-600 font-normal shrink-0">{s.year}</span>
      {/* DPS score */}
      <span className="ml-auto text-xs shrink-0">
        {s.dps_score
          ? <><span style={{ color: dot, fontWeight: 700 }}>{s.dps_score.toFixed(0)}</span><span className="text-slate-600"> DPS</span></>
          : <span className="text-slate-600">—</span>
        }
      </span>
    </button>
  );
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Storm Browser Sidebar
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

function StormBrowser({ onSelectStorm, activeStormId, activating, isOpen, onClose, activeStorm }: {
  onSelectStorm: (id: string) => void;
  activeStormId: string | null;
  activating: boolean;
  isOpen: boolean;
  onClose: () => void;
  activeStorm: StormInfo | null;
}) {
  const [seasons, setSeasons] = useState<Season[]>([]);
  const [historicStorms, setHistoricStorms] = useState<StormInfo[]>([]);
  const [activeNHC, setActiveNHC] = useState<StormInfo[]>([]);
  const [expandedYear, setExpandedYear] = useState<number | null>(null);
  const [yearStorms, setYearStorms] = useState<StormInfo[]>([]);
  const [historicOpen, setHistoricOpen] = useState(false);
  const [searchQuery, setSearchQuery] = useState('');
  const [searchResults, setSearchResults] = useState<StormInfo[] | null>(null);
  const [searchLoading, setSearchLoading] = useState(false);
  const [loadError, setLoadError] = useState(false);
  const searchTimeout = useRef<any>(null);

  useEffect(() => {
    let failed = 0;
    const check = () => { failed++; if (failed >= 3) setLoadError(true); };
    const safeArray = (setter: (v: any[]) => void) => (data: unknown) => {
      if (Array.isArray(data)) setter(data); else check();
    };
    fetch('/surgedps/api/seasons').then(r => r.json()).then(safeArray(setSeasons)).catch(check);
    fetch('/surgedps/api/storms/historic').then(r => r.json()).then(safeArray(setHistoricStorms)).catch(check);
    fetch('/surgedps/api/storms/active').then(r => r.json()).then(safeArray(setActiveNHC)).catch(check);
  }, []);

  const toggleYear = useCallback((year: number) => {
    if (expandedYear === year) { setExpandedYear(null); setYearStorms([]); }
    else { setExpandedYear(year); fetch(`/surgedps/api/season/${year}`).then(r => r.json()).then(d => setYearStorms(Array.isArray(d) ? d : [])).catch(() => setYearStorms([])); }
  }, [expandedYear]);

  const handleSearch = useCallback((q: string) => {
    setSearchQuery(q);
    if (searchTimeout.current) clearTimeout(searchTimeout.current);
    if (!q.trim()) { setSearchResults(null); return; }
    setSearchLoading(true);
    searchTimeout.current = setTimeout(() => {
      fetch(`/surgedps/api/storms/search?q=${encodeURIComponent(q)}`)
        .then(r => r.json())
        .then(data => { setSearchResults(Array.isArray(data) ? data : []); setSearchLoading(false); })
        .catch(() => { setSearchResults([]); setSearchLoading(false); });
    }, 300);
  }, []);

  // Cleanup search timeout on unmount (#13)
  useEffect(() => () => { if (searchTimeout.current) clearTimeout(searchTimeout.current); }, []);

  // Select storm AND auto-close sidebar (mobile)
  const selectAndClose = useCallback((id: string) => {
    setSearchQuery(''); setSearchResults(null);
    onSelectStorm(id);
    onClose(); // closes sidebar on mobile; no-op on desktop (sidebar is always visible via lg:relative)
  }, [onSelectStorm, onClose]);

  return (
    <div className={`w-72 shrink-0 bg-slate-900 border-r border-slate-700/50 flex flex-col h-screen overflow-hidden absolute inset-y-0 left-0 z-30 lg:relative transition-transform duration-300 ease-in-out ${isOpen ? 'translate-x-0' : '-translate-x-full lg:translate-x-0'}`}>
      {/* Header */}
      <div className="px-4 py-4 border-b border-slate-700/50 shrink-0">
        <div className="flex items-center justify-between">
          <h1 className="text-base font-bold text-white tracking-tight">SurgeDPS</h1>
          <div className="flex items-center gap-2">
            <a
              href="https://stormdps.com"
              target="_blank"
              rel="noopener noreferrer"
              className="text-[11px] font-semibold text-cyan-400 hover:text-cyan-300 transition-colors border border-cyan-700 hover:border-cyan-500 rounded px-2 py-0.5"
            >
              ← StormDPS
            </a>
            <button
              onClick={onClose}
              className="lg:hidden text-slate-400 hover:text-white transition-colors p-1 rounded"
              aria-label="Close sidebar"
            >✕</button>
          </div>
        </div>
        <p className="text-[10px] text-slate-500 uppercase tracking-widest mt-0.5">Storm Surge Analysis</p>
        {/* Active storm indicator */}
        {activeStorm && (
          <div className="mt-2 px-2 py-1.5 bg-indigo-500/15 border border-indigo-500/30 rounded-lg flex items-center gap-2">
            <span className="w-2 h-2 rounded-full bg-indigo-400 shrink-0 animate-pulse" />
            <span className="text-xs font-semibold text-indigo-300 truncate">{activeStorm.name} ({activeStorm.year})</span>
            <span className="ml-auto text-[10px] font-bold text-white px-1.5 py-0.5 rounded-full shrink-0"
              style={{ backgroundColor: CAT_COLORS[activeStorm.category] }}>Cat {activeStorm.category}</span>
          </div>
        )}
      </div>

      <div className="flex-1 overflow-y-auto">
        {loadError && (
          <div className="mx-4 mt-3 px-3 py-2 bg-red-900/40 border border-red-700/50 rounded-lg">
            <p className="text-xs text-red-300">Could not connect to the server. Check your connection and refresh the page.</p>
          </div>
        )}

        {/* ── ACTIVE STORMS ── */}
        <div className="px-4 pt-4 pb-3 border-b border-slate-700/50">
          <div className="flex items-center justify-between mb-2">
            <h2 className="text-[11px] font-bold text-indigo-400 uppercase tracking-wider">Active Storms</h2>
          </div>
          {activeNHC.length === 0 ? (
            <p className="text-xs text-slate-500 leading-relaxed">
              No active tropical cyclones in any basin. During hurricane season (Jun–Nov Atlantic, May–Nov East Pacific),
              active storms will appear here automatically.
            </p>
          ) : (
            <div className="space-y-1">
              {[...activeNHC].sort(byDPS).map(s => <StormRow key={s.storm_id} s={s} activeStormId={activeStormId} activating={activating} onSelect={selectAndClose} />)}
            </div>
          )}
        </div>

        {/* ── STORM LOOKUP ── */}
        <div className="px-4 pt-4 pb-3 border-b border-slate-700/50">
          <h2 className="text-[11px] font-bold text-slate-400 uppercase tracking-wider mb-3">Storm Lookup</h2>
          <div className="mb-2">
            <input
              type="text"
              placeholder="Search by name, e.g. Katrina, Harvey…"
              value={searchQuery}
              onChange={e => handleSearch(e.target.value)}
              className="w-full px-3 py-2 bg-slate-800 border border-slate-600 rounded-lg text-sm text-white placeholder-slate-500 focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 outline-none"
            />
          </div>

          {/* Search results dropdown */}
          {searchResults !== null && (
            <div className="bg-slate-800 rounded-lg border border-slate-600 max-h-48 overflow-y-auto mt-2">
              {searchLoading ? (
                <p className="text-xs text-slate-500 p-3 text-center">Searching...</p>
              ) : searchResults.length === 0 ? (
                <p className="text-xs text-slate-500 p-3 text-center">No storms found</p>
              ) : (
                [...searchResults].sort(byDPS).map(s => <StormRow key={s.storm_id} s={s} activeStormId={activeStormId} activating={activating} onSelect={selectAndClose} />)
              )}
            </div>
          )}
        </div>

        {/* ── STORM BROWSER ── */}
        <div className="px-4 pt-4 pb-3">
          <h2 className="text-[11px] font-bold text-slate-400 uppercase tracking-wider mb-1">Storm Browser</h2>
          <p className="text-[10px] text-slate-600 mb-3">Sorted by Damage Potential Score (DPS) — higher = more destructive surge</p>

          {/* Historic Storms (curated) */}
          <div className="mb-1">
            <button
              onClick={() => setHistoricOpen(!historicOpen)}
              className="w-full flex items-center justify-between px-3 py-2.5 bg-slate-800/50 hover:bg-slate-800 rounded-lg transition-colors"
            >
              <span className="text-sm font-semibold text-slate-200">Historic Storms</span>
              <div className="flex items-center gap-2">
                <span className="text-xs text-indigo-400 font-medium">{historicStorms.length}</span>
                <span className={`text-slate-500 text-xs transition-transform ${historicOpen ? 'rotate-90' : ''}`}>▸</span>
              </div>
            </button>
            {historicOpen && (
              <div className="mt-1 ml-1 pl-2 border-l border-slate-700/50 space-y-0.5">
                {[...historicStorms].sort(byDPS).map(s => <StormRow key={s.storm_id} s={s} activeStormId={activeStormId} activating={activating} onSelect={selectAndClose} />)}
              </div>
            )}
          </div>

          {/* Season-by-season accordion (2015+) */}
          {seasons.map(({ year, count }) => {
            const isOpen = expandedYear === year;
            return (
              <div key={year} className="mb-1">
                <button
                  onClick={() => toggleYear(year)}
                  className="w-full flex items-center justify-between px-3 py-2.5 bg-slate-800/50 hover:bg-slate-800 rounded-lg transition-colors"
                >
                  <span className="text-sm font-semibold text-slate-200">{year} Season</span>
                  <div className="flex items-center gap-2">
                    <span className="text-xs text-indigo-400 font-medium">{count}</span>
                    <span className={`text-slate-500 text-xs transition-transform ${isOpen ? 'rotate-90' : ''}`}>▸</span>
                  </div>
                </button>
                {isOpen && (
                  <div className="mt-1 ml-1 pl-2 border-l border-slate-700/50 space-y-0.5">
                    {yearStorms.length === 0 ? (
                      <p className="text-xs text-slate-500 py-2 px-3">Loading...</p>
                    ) : (
                      [...yearStorms].sort(byDPS).map(s => <StormRow key={s.storm_id} s={s} activeStormId={activeStormId} activating={activating} onSelect={selectAndClose} />)
                    )}
                  </div>
                )}
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// CAT Deployment Summary Panel (CAT_TEAM_PLAN §4a B1)
//
// Shown at the top of DashboardPanel in Ops Mode, above the existing
// Total Modeled Loss scoreboard. Purpose: give a CAT / CRT deployment
// lead a glanceable answer to "how big is this, what's the peril
// mix, where do I send people first?" — without making the numbers
// sound more precise than the model warrants.
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

function CatDeploymentSummary({
  storm,
  totals,
  estimatedPop,
  severityCounts,
  hotspots,
  mode,
  subPersona,
  teamSize,
  rollupDisplaced,
  onGenerateCatReport,
  onGenerateSitRep,
}: {
  storm: StormInfo;
  totals: { buildings: number; loss: number; totalDepth: number };
  estimatedPop: number;
  severityCounts: Record<string, number>;
  hotspots: Hotspot[];
  mode: DisplayMode;
  subPersona: SubPersona;
  teamSize: number;
  rollupDisplaced?: number;
  onGenerateCatReport: (format: 'html' | 'pdf') => void;
  onGenerateSitRep: (format: 'html' | 'pdf') => void;
}) {
  if (mode !== 'ops') return null;
  if (totals.buildings <= 0) return null;
  const isEM = subPersona === 'em';

  const wl = workloadSummary(severityCounts);
  const stormMix = aggregatePerilMix(
    hotspots.map(h => ({ windPct: h.windPct, waterPct: h.waterPct, weight: h.count })),
  );
  const headline = wl.headline;
  const top = hotspots[0];

  // Deployment-urgency color borrowed from the workloadSummary headline.
  const urgencyColor =
    headline === 'Deploy immediately' ? 'bg-red-600'
    : headline === 'Deploy CAT team' ? 'bg-orange-500'
    : headline === 'Deploy field adjusters' ? 'bg-amber-500'
    : headline === 'Standard claims handling' ? 'bg-sky-500'
    : 'bg-slate-400';

  // EM-specific aggregates: worst shelter posture across the footprint
  // and staging plan for the Top Priority callout.
  const worstPost = worstShelterPosture(hotspots.map(h => h.maxDepthFt));
  const staging = isEM
    ? stagingPlan(hotspots, estimatedPop, severityCounts, totals.buildings, rollupDisplaced)
    : null;

  const panelClass = isEM
    ? 'rounded-xl p-3 mb-3 border-2 border-emerald-300 bg-gradient-to-br from-emerald-50 to-white shadow-sm'
    : 'rounded-xl p-3 mb-3 border-2 border-orange-300 bg-gradient-to-br from-orange-50 to-white shadow-sm';
  const headerText = isEM ? 'EM Situation Summary' : 'CAT Deployment Summary';
  const headerColor = isEM ? 'text-emerald-700' : 'text-orange-700';

  return (
    <div className={panelClass}>
      <div className="flex items-center gap-2 mb-2">
        <span className={`text-[10px] font-bold uppercase tracking-wider ${headerColor}`}>{headerText}</span>
        <span className="ml-auto text-[9px] font-bold uppercase px-1.5 py-0.5 rounded-sm text-white shrink-0" style={{ backgroundColor: 'transparent' }}>
          <span className={`px-1.5 py-0.5 rounded-sm ${urgencyColor}`}>{headline}</span>
        </span>
      </div>

      {/* Exposure line */}
      <div className="text-[11px] text-slate-700 leading-snug mb-2">
        <div>
          <span className="font-bold">{storm.name}</span> — CAT {storm.category}
          {storm.year ? `, ${storm.year}` : ''}
          {estimatedPop > 0 && (
            <> · ~{formatCountOps(estimatedPop, mode)} residents in surge zone</>
          )}
        </div>
        <div className="text-slate-500">{perilHeadline(stormMix)}</div>
      </div>

      {/* Peril mix bar — storm-wide weighted aggregate */}
      <div className="flex items-center gap-2 mb-2" title={`${stormMix.waterPct}% water · ${stormMix.windPct}% wind (weighted by building count in hardest-hit areas)`}>
        <div className="flex-1 h-3 rounded-full overflow-hidden bg-slate-200 flex">
          <div className="bg-indigo-500" style={{ width: `${stormMix.waterPct}%` }} />
          <div className="bg-sky-400"    style={{ width: `${stormMix.windPct}%` }} />
        </div>
        <div className="text-[10px] text-slate-600 tabular-nums shrink-0 font-semibold">
          🌊 {stormMix.waterPct}% · 🌬️ {stormMix.windPct}%
        </div>
      </div>

      {/* Workload translation */}
      {wl.inspections_needed > 0 && (
        <div className="text-[11px] text-slate-700 leading-snug mb-2">
          <span className="font-bold">~{formatCountOps(wl.inspections_needed, mode)}</span> inspections needed
          {wl.uninhabitable > 0 && (
            <> · <span className="font-bold text-red-700">~{formatCountOps(wl.uninhabitable, mode)}</span> likely uninhabitable</>
          )}
        </div>
      )}

      {/* Top priority callout — persona-aware */}
      {top && (
        <div className={`rounded-md bg-white/80 border px-2 py-1.5 mb-2 ${isEM ? 'border-emerald-200' : 'border-orange-200'}`}>
          <div className={`text-[9px] font-bold uppercase tracking-wider ${isEM ? 'text-emerald-700' : 'text-orange-700'}`}>Top Priority</div>
          <div className="text-[11px] text-slate-800">
            <span className="font-bold">#{top.rank}</span> · {formatLossOps(top.loss, mode)} ·{' '}
            <span className="text-slate-500">{formatCountOps(top.count, mode)} bldgs</span>
          </div>
          {isEM ? (
            (() => {
              const post = shelterPosture(top.maxDepthFt);
              return (
                <div className="text-[10px] text-slate-700 mt-0.5">
                  {post.icon}{' '}
                  <span className={`text-[9px] font-bold px-1.5 py-0.5 rounded-sm ${post.classes}`} title={post.description}>
                    {post.label}
                  </span>
                  <span className="ml-1.5 text-slate-500">max ~{Math.round(top.maxDepthFt)} ft</span>
                </div>
              );
            })()
          ) : (
            <div className="text-[10px] text-slate-700 mt-0.5">
              🚗 <span className="font-semibold">{top.recommend.label}</span>
              <span className={`ml-1.5 text-[9px] font-bold px-1.5 py-0.5 rounded-sm ${top.routing.classes}`}
                title={top.routing.description}>
                {top.routing.short}
              </span>
            </div>
          )}
        </div>
      )}

      {/* EM-only: worst storm-wide shelter posture pill */}
      {isEM && (
        <div className="text-[10px] text-slate-700 mb-2 flex items-center gap-1.5">
          <span className="font-semibold">Storm-wide posture:</span>
          <span className={`text-[9px] font-bold px-1.5 py-0.5 rounded-sm ${worstPost.classes}`} title={worstPost.description}>
            {worstPost.icon} {worstPost.label}
          </span>
        </div>
      )}

      {/* EM-only: mutual-aid quick numbers (E3 preview — full panel is below) */}
      {isEM && staging && (staging.rescueTeams > 0 || staging.shelterBedsNeeded > 0) && (
        <div className="text-[10px] text-slate-700 mb-2 leading-snug">
          {staging.rescueTeams > 0 && (
            <>Request <span className="font-bold text-emerald-800">{staging.rescueTeams}</span> rescue team{staging.rescueTeams === 1 ? '' : 's'}</>
          )}
          {staging.rescueTeams > 0 && staging.shelterBedsNeeded > 0 && <> · </>}
          {staging.shelterBedsNeeded > 0 && (
            <>~<span className="font-bold text-emerald-800">{staging.shelterBedsNeeded.toLocaleString()}</span> shelter beds</>
          )}
        </div>
      )}

      {/* Time to Clear — storm-wide single-line summary (C5, CAT only) */}
      {!isEM && (() => {
        const ttc = timeToClearDays(hotspots, teamSize);
        if (!isFinite(ttc) || ttc <= 0) return null;
        return (
          <div className="text-[10px] text-slate-600 mb-2 italic">
            <span className="font-semibold not-italic">Time to clear:</span>{' '}
            {formatTimeToClear(ttc)} with a {teamSize}-adjuster team
          </div>
        );
      })()}

      {/* Action buttons — primary export reflects the active persona.
          Each button is split: main action = HTML download, adjacent
          compact button = Save as PDF (opens print dialog). §11 Q5. */}
      <div className="flex gap-1.5">
        {isEM ? (
          <>
            <div className="flex-1 flex">
              <button
                onClick={() => onGenerateSitRep('html')}
                title="Download Situation Report (HTML)"
                className="flex-1 text-[10px] font-bold px-2 py-1 rounded-l-md border border-emerald-500 text-white bg-emerald-600 hover:bg-emerald-700 transition-colors"
              >SitRep ↓</button>
              <button
                onClick={() => onGenerateSitRep('pdf')}
                title="Save Situation Report as PDF (opens print dialog)"
                aria-label="Save SitRep as PDF"
                className="text-[9px] font-bold px-1.5 py-1 rounded-r-md border-t border-r border-b border-emerald-500 text-emerald-700 bg-white hover:bg-emerald-50 transition-colors"
              >PDF</button>
            </div>
            <div className="flex-1 flex">
              <button
                onClick={() => onGenerateCatReport('html')}
                title="Download CAT Deployment Report (HTML)"
                className="flex-1 text-[10px] font-bold px-2 py-1 rounded-l-md border border-orange-300 text-orange-700 bg-white hover:bg-orange-50 transition-colors"
              >CAT ↓</button>
              <button
                onClick={() => onGenerateCatReport('pdf')}
                title="Save CAT Report as PDF (opens print dialog)"
                aria-label="Save CAT Report as PDF"
                className="text-[9px] font-bold px-1.5 py-1 rounded-r-md border-t border-r border-b border-orange-300 text-orange-700 bg-white hover:bg-orange-50 transition-colors"
              >PDF</button>
            </div>
          </>
        ) : (
          <>
            <div className="flex-1 flex">
              <button
                onClick={() => onGenerateCatReport('html')}
                title="Download CAT Deployment Report (HTML)"
                className="flex-1 text-[10px] font-bold px-2 py-1 rounded-l-md border border-orange-500 text-white bg-orange-600 hover:bg-orange-700 transition-colors"
              >CAT Report ↓</button>
              <button
                onClick={() => onGenerateCatReport('pdf')}
                title="Save CAT Report as PDF (opens print dialog)"
                aria-label="Save CAT Report as PDF"
                className="text-[9px] font-bold px-1.5 py-1 rounded-r-md border-t border-r border-b border-orange-500 text-white bg-orange-600 hover:bg-orange-700 transition-colors"
              >PDF</button>
            </div>
            <div className="flex-1 flex">
              <button
                onClick={() => onGenerateSitRep('html')}
                title="Download Situation Report (HTML)"
                className="flex-1 text-[10px] font-bold px-2 py-1 rounded-l-md border border-emerald-300 text-emerald-700 bg-white hover:bg-emerald-50 transition-colors"
              >SitRep ↓</button>
              <button
                onClick={() => onGenerateSitRep('pdf')}
                title="Save SitRep as PDF (opens print dialog)"
                aria-label="Save SitRep as PDF"
                className="text-[9px] font-bold px-1.5 py-1 rounded-r-md border-t border-r border-b border-emerald-300 text-emerald-700 bg-white hover:bg-emerald-50 transition-colors"
              >PDF</button>
            </div>
          </>
        )}
      </div>

      <div className="text-[9px] text-slate-400 mt-1.5 italic">
        Modeled estimate — not field verified. Rounded for deployment planning.
      </div>
    </div>
  );
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Deployment Planner (CAT_TEAM_PLAN §4b C3)
//
// Interactive "X adjusters over Y days" simulator. Lets the CAT
// lead drag team size / window and see per-area coverage update
// live. Pure presentation layer — all the math lives in
// planDeployment() / suggestTeamSize() in catTeam.ts.
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
function DeploymentPlanner({
  hotspots,
  teamSize,
  windowDays,
  onTeamSizeChange,
  onWindowDaysChange,
}: {
  hotspots: Hotspot[];
  teamSize: number;
  windowDays: number;
  onTeamSizeChange: (n: number) => void;
  onWindowDaysChange: (n: number) => void;
}) {
  const [expanded, setExpanded] = useState(true);
  if (hotspots.length === 0) return null;

  const plan = planDeployment(hotspots, teamSize, windowDays);

  const statusPill = (status: 'covered' | 'partial' | 'uncovered') => {
    if (status === 'covered')   return 'bg-green-100 text-green-800 border border-green-200';
    if (status === 'partial')   return 'bg-amber-100 text-amber-800 border border-amber-200';
    return 'bg-red-100 text-red-800 border border-red-200';
  };

  const barColor =
    plan.coverage_pct >= 100 ? 'bg-green-500'
    : plan.coverage_pct >= 60 ? 'bg-amber-500'
    : 'bg-red-500';

  return (
    <div className="rounded-xl p-2.5 mb-3 border border-purple-200 bg-purple-50/40">
      <button
        onClick={() => setExpanded(e => !e)}
        className="w-full flex items-center gap-2 text-left"
      >
        <span className="text-[10px] font-bold uppercase tracking-wider text-purple-700">
          Deployment Planner
        </span>
        <span className="ml-auto text-[10px] text-purple-600 font-semibold">
          {plan.coverage_pct}% coverage
        </span>
        <span className="text-purple-500 text-xs">{expanded ? '▲' : '▼'}</span>
      </button>

      {expanded && (
        <>
          {/* Team size slider */}
          <div className="mt-2">
            <div className="flex justify-between text-[10px] text-slate-600">
              <span className="font-semibold">Adjusters</span>
              <span className="tabular-nums font-bold text-slate-800">{teamSize}</span>
            </div>
            <input
              type="range"
              min={1}
              max={80}
              value={teamSize}
              onChange={e => onTeamSizeChange(Number(e.target.value))}
              className="w-full accent-purple-600"
            />
          </div>

          {/* Window slider */}
          <div className="mt-1">
            <div className="flex justify-between text-[10px] text-slate-600">
              <span className="font-semibold">Window (days)</span>
              <span className="tabular-nums font-bold text-slate-800">{windowDays}</span>
            </div>
            <input
              type="range"
              min={1}
              max={14}
              value={windowDays}
              onChange={e => onWindowDaysChange(Number(e.target.value))}
              className="w-full accent-purple-600"
            />
          </div>

          {/* Coverage bar */}
          <div className="mt-2 mb-1.5">
            <div className="h-2 rounded-full bg-slate-200 overflow-hidden">
              <div
                className={`h-full ${barColor} transition-all`}
                style={{ width: `${plan.coverage_pct}%` }}
              />
            </div>
            <div className="flex justify-between text-[9px] text-slate-500 mt-1 tabular-nums">
              <span>
                {plan.required_adjuster_days.toFixed(0)} adj-days needed
              </span>
              <span>
                {plan.capacity_adjuster_days} capacity
              </span>
            </div>
          </div>

          {/* Shortfall or full coverage indicator */}
          {plan.shortfall_days > 0 ? (
            <div className="text-[10px] text-red-700 bg-red-50 border border-red-200 rounded px-2 py-1 mb-1.5">
              <span className="font-bold">Shortfall:</span>{' '}
              {plan.shortfall_days.toFixed(0)} adjuster-days not covered
            </div>
          ) : (
            <div className="text-[10px] text-green-700 bg-green-50 border border-green-200 rounded px-2 py-1 mb-1.5">
              <span className="font-bold">✓ Full coverage</span> within {windowDays}-day window
            </div>
          )}

          {/* Per-area list */}
          <div className="space-y-1 mb-1.5">
            {plan.areas.map(a => (
              <div key={a.rank} className="flex items-center gap-1.5 text-[10px]">
                <span className="font-bold text-slate-500 w-5 shrink-0">#{a.rank}</span>
                <div className="flex-1 h-1.5 rounded-sm bg-slate-200 overflow-hidden">
                  <div
                    className={`h-full ${
                      a.status === 'covered' ? 'bg-green-500'
                      : a.status === 'partial' ? 'bg-amber-500'
                      : 'bg-red-400'
                    }`}
                    style={{ width: `${a.coverage_pct}%` }}
                  />
                </div>
                <span className={`text-[9px] font-bold px-1.5 py-0.5 rounded-sm shrink-0 ${statusPill(a.status)}`}>
                  {a.coverage_pct}%
                </span>
              </div>
            ))}
          </div>

          {/* Suggest team size button */}
          <button
            onClick={() => {
              const n = suggestTeamSize(hotspots, windowDays);
              if (n > 0) onTeamSizeChange(Math.min(80, n));
            }}
            className="w-full text-[10px] font-bold px-2 py-1 rounded-md border border-purple-400 text-purple-800 bg-white hover:bg-purple-50 transition-colors"
            title="Solve for the smallest team that fully covers all areas within the current window"
          >
            Suggest team size for full coverage
          </button>
        </>
      )}
    </div>
  );
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Resource Staging Panel (CAT_TEAM_PLAN §4c E2/E3/E4)
//
// EM-only panel showing mutual-aid sizing (rescue teams, shelter
// beds, generators) plus a copyable public-advisory draft. All
// numbers come from stagingPlan() — a pure helper — and the
// advisory text comes from draftPublicAdvisory(). Nothing here
// calls an LLM; the copy is template-driven so an EM can verify
// every field before release.
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
function ResourceStagingPanel({
  storm,
  totals,
  hotspots,
  estimatedPop,
  severityCounts,
  criticalBreakdown,
  rollupDisplaced,
}: {
  storm: StormInfo;
  totals: { buildings: number; loss: number; totalDepth: number };
  hotspots: Hotspot[];
  estimatedPop: number;
  severityCounts: Record<string, number>;
  criticalBreakdown: Array<{ icon: string; label: string; count: number }>;
  rollupDisplaced?: number;
}) {
  const [expanded, setExpanded] = useState(true);
  const [advisoryOpen, setAdvisoryOpen] = useState(false);
  const [copyToast, setCopyToast] = useState(false);

  if (hotspots.length === 0 || totals.buildings <= 0) return null;

  const plan: StagingPlan = stagingPlan(hotspots, estimatedPop, severityCounts, totals.buildings, rollupDisplaced);
  const advisory = draftPublicAdvisory({
    storm: storm as any,
    hotspots,
    estimatedPop,
    criticalBreakdown,
    shelterBedsNeeded: plan.shelterBedsNeeded,
    rescueTeams: plan.rescueTeams,
  });

  const handleCopy = async () => {
    try {
      await navigator.clipboard.writeText(advisory);
      setCopyToast(true);
      setTimeout(() => setCopyToast(false), 1800);
    } catch {
      // Fallback: select the pre text so the user can copy manually
      const el = document.getElementById('em-advisory-pre');
      if (el) {
        const range = document.createRange();
        range.selectNodeContents(el);
        const sel = window.getSelection();
        sel?.removeAllRanges();
        sel?.addRange(range);
      }
    }
  };

  return (
    <div className="rounded-xl p-2.5 mb-3 border border-emerald-200 bg-emerald-50/60">
      <button
        onClick={() => setExpanded(e => !e)}
        className="w-full flex items-center gap-2 text-left"
      >
        <span className="text-[10px] font-bold uppercase tracking-wider text-emerald-800">
          Resource Staging
        </span>
        <span className="ml-auto text-[9px] font-bold uppercase px-1.5 py-0.5 rounded-sm text-emerald-900 bg-emerald-100">
          EM
        </span>
        <span className="text-emerald-600 text-xs">{expanded ? '▲' : '▼'}</span>
      </button>

      {expanded && (
        <>
          {/* Quick numbers — mutual aid sizing (E3) */}
          <div className="grid grid-cols-3 gap-1.5 mt-2 mb-2">
            <div className="rounded bg-white/80 border border-emerald-200 px-1.5 py-1 text-center">
              <div className="text-[9px] text-emerald-700 font-bold uppercase tracking-wider">Rescue</div>
              <div className="text-sm font-black text-emerald-900 tabular-nums">{plan.rescueTeams}</div>
              <div className="text-[9px] text-slate-500">team{plan.rescueTeams === 1 ? '' : 's'}</div>
            </div>
            <div className="rounded bg-white/80 border border-emerald-200 px-1.5 py-1 text-center">
              <div className="text-[9px] text-emerald-700 font-bold uppercase tracking-wider">Shelter</div>
              <div className="text-sm font-black text-emerald-900 tabular-nums">{plan.shelterBedsNeeded.toLocaleString()}</div>
              <div className="text-[9px] text-slate-500">beds</div>
            </div>
            <div className="rounded bg-white/80 border border-emerald-200 px-1.5 py-1 text-center">
              <div className="text-[9px] text-emerald-700 font-bold uppercase tracking-wider">Gens</div>
              <div className="text-sm font-black text-emerald-900 tabular-nums">{plan.generatorsRecommended}</div>
              <div className="text-[9px] text-slate-500">units</div>
            </div>
          </div>

          {/* Displaced pop summary */}
          <div className="text-[10px] text-slate-700 mb-2">
            Est. displaced: <span className="font-bold text-emerald-900">~{plan.displacedPop.toLocaleString()}</span>
            {plan.topStagingArea && (
              <> · Stage {plan.topStagingArea}</>
            )}
          </div>

          {/* Narrative notes (E2) */}
          {plan.notes.length > 0 && (
            <ul className="text-[10px] text-slate-600 mb-2 space-y-0.5 list-disc pl-4">
              {plan.notes.map((n, i) => <li key={i}>{n}</li>)}
            </ul>
          )}

          {/* Public advisory toggle (E4) */}
          <button
            onClick={() => setAdvisoryOpen(o => !o)}
            className="w-full text-[10px] font-bold px-2 py-1 rounded-md border border-emerald-400 text-emerald-800 bg-white hover:bg-emerald-50 transition-colors"
            title="Show / hide the draft public advisory — copy and adapt before release"
          >
            {advisoryOpen ? '▲' : '▼'} Draft public advisory
          </button>

          {advisoryOpen && (
            <div className="mt-2">
              <pre
                id="em-advisory-pre"
                className="whitespace-pre-wrap bg-white border border-emerald-200 rounded-md p-2 text-[10px] leading-snug text-slate-800 font-mono max-h-48 overflow-y-auto"
              >{advisory}</pre>
              <div className="flex items-center gap-2 mt-1.5">
                <button
                  onClick={handleCopy}
                  className="text-[10px] font-bold px-2 py-1 rounded-md border border-emerald-500 text-white bg-emerald-600 hover:bg-emerald-700 transition-colors"
                >📋 Copy advisory</button>
                {copyToast && (
                  <span className="text-[10px] text-emerald-700 font-semibold">✓ Copied</span>
                )}
                <span className="ml-auto text-[9px] text-slate-400 italic">
                  Template only — verify before release
                </span>
              </div>
            </div>
          )}

          <div className="text-[9px] text-slate-400 mt-1.5 italic">
            Rule-of-thumb sizing — tune against your agency's playbook.
          </div>
        </>
      )}
    </div>
  );
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Phase 5 — Beta Data Layers panel
//
// Placeholder surface area for the four Phase 5 data layers.
// Each sub-panel calls its stub fetcher in betaLayers.ts, which
// currently returns { available: false, notes: '...' }. Once the
// backend ships, the stub becomes a real fetch and the panels
// light up without further UI changes.
//
// Persona gating (per CAT_TEAM_PLAN §8):
//   Rainfall overlay (B7)  — both CAT and EM
//   Shelter capacity (E5)  — EM only
//   Vendor coverage (C6)   — CAT only
//   Time-to-access (E6)    — EM only
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

function BetaSection({ title, badge, notes }: { title: string; badge: string; notes: string }) {
  return (
    <div className="rounded-md border border-dashed border-purple-300 bg-white/70 px-2 py-1.5 mb-1.5">
      <div className="flex items-center gap-1.5 mb-0.5">
        <span className="text-[10px] font-bold text-purple-900">{title}</span>
        <span className="ml-auto text-[8px] font-bold uppercase tracking-wider px-1 py-0.5 rounded-sm bg-purple-100 text-purple-800 border border-purple-200">
          {badge}
        </span>
      </div>
      <div className="text-[9px] text-slate-600 italic leading-snug">
        {notes}
      </div>
      <div className="text-[8px] text-purple-500 mt-0.5 uppercase tracking-wider font-bold">
        Data layer pending
      </div>
    </div>
  );
}

function BetaDataLayersPanel({
  storm,
  hotspots,
  subPersona,
}: {
  storm: StormInfo;
  hotspots: Hotspot[];
  subPersona: SubPersona;
}) {
  const [expanded, setExpanded] = useState(false);
  const [rainfall, setRainfall] = useState<RainfallOverlay | null>(null);
  const [shelters, setShelters] = useState<ShelterCapacityLayer | null>(null);
  const [vendors, setVendors] = useState<VendorCoverageLayer | null>(null);
  const [access, setAccess] = useState<TimeToAccessLayer | null>(null);

  const stormId = (storm as any)?.id || (storm as any)?.storm_id || storm.name || 'unknown';

  // Fetch on storm or persona change. The stubs return quickly so there's
  // no need for loading indicators today — add them when real fetches land.
  useEffect(() => {
    let cancelled = false;
    (async () => {
      const [r, s, v, a] = await Promise.all([
        fetchRainfallOverlay(stormId),
        subPersona === 'em' ? fetchShelterCapacity(stormId, { lat: (storm as any).landfall_lat ?? 0, lon: (storm as any).landfall_lon ?? 0 }) : Promise.resolve(null),
        subPersona === 'cat' ? fetchVendorCoverage(stormId) : Promise.resolve(null),
        subPersona === 'em' ? fetchTimeToAccess(stormId, hotspots.map(h => h.rank)) : Promise.resolve(null),
      ]);
      if (cancelled) return;
      setRainfall(r);
      setShelters(s as ShelterCapacityLayer | null);
      setVendors(v as VendorCoverageLayer | null);
      setAccess(a as TimeToAccessLayer | null);
    })();
    return () => { cancelled = true; };
  }, [stormId, subPersona, hotspots.length]);

  return (
    <div className="rounded-xl p-2.5 mb-3 border border-purple-200 bg-purple-50/60">
      <button
        onClick={() => setExpanded(e => !e)}
        className="w-full flex items-center gap-2 text-left"
      >
        <span className="text-[10px] font-bold uppercase tracking-wider text-purple-800">
          🧪 Beta data layers
        </span>
        <span className="ml-auto text-[9px] font-bold uppercase px-1.5 py-0.5 rounded-sm text-purple-900 bg-purple-100">
          Preview
        </span>
        <span className="text-purple-600 text-xs">{expanded ? '▲' : '▼'}</span>
      </button>

      {expanded && (
        <div className="mt-2">
          {/* B7 — Rainfall overlay (both personas) */}
          {rainfall && (
            <BetaSection
              title="Rainfall overlay (B7)"
              badge="both"
              notes={rainfall.notes}
            />
          )}

          {/* E5 — Shelter capacity (EM only) */}
          {subPersona === 'em' && shelters && (
            <BetaSection
              title="Shelter capacity (E5)"
              badge="EM"
              notes={shelters.notes}
            />
          )}

          {/* C6 — Vendor coverage (CAT only) */}
          {subPersona === 'cat' && vendors && (
            <BetaSection
              title="Vendor coverage (C6)"
              badge="CAT"
              notes={vendors.notes}
            />
          )}

          {/* E6 — Time-to-access (EM only) */}
          {subPersona === 'em' && access && (
            <BetaSection
              title="Time-to-access (E6)"
              badge="EM"
              notes={access.notes}
            />
          )}

          <div className="text-[9px] text-purple-500 mt-1 italic leading-snug">
            These layers are scaffolding only — real data ships as each backend
            endpoint lands. See PHASE5_DATA_CONTRACTS.md.
          </div>
        </div>
      )}
    </div>
  );
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Dashboard Panel (right overlay on map)
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

const CONFIDENCE_STYLES: Record<string, { bg: string; text: string; label: string }> = {
  high:        { bg: 'bg-green-100', text: 'text-green-800', label: 'High Confidence' },
  medium:      { bg: 'bg-yellow-100', text: 'text-yellow-800', label: 'Medium Confidence' },
  low:         { bg: 'bg-red-100', text: 'text-red-800', label: 'Low Confidence' },
  unvalidated: { bg: 'bg-gray-100', text: 'text-gray-500', label: 'Unvalidated' },
};

// Hotspot row — shared between App computation and DashboardPanel rendering.
// Phase 2 adds per-area peril mix (windPct/waterPct), severity breakdown,
// adjuster recommendation, and routing hint.
interface Hotspot {
  rank: number;
  loss: number;
  count: number;
  lat: number;
  lon: number;
  avgLoss: number;
  maxDepthFt: number;
  windPct: number;
  waterPct: number;
  severity: { severe: number; major: number; moderate: number; minor: number; none: number };
  recommend: AdjusterRecommendation;
  routing: RoutingTag;
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Jurisdictions Panel — per-county rollup when the Counties
// overlay is on. Shows EM the slice-by-slice picture so they
// can allocate resources to independently-managed counties.
// CAT persona gets the same panel but with loss-first framing.
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
function JurisdictionsPanel({
  rollup,
  subPersona,
  onFlyTo,
  counties,
}: {
  rollup: CountyRollup[];
  subPersona: SubPersona;
  onFlyTo?: (lon: number, lat: number) => void;
  counties: any;
}) {
  const [expanded, setExpanded] = useState(true);
  if (!rollup.length) return null;
  const isEM = subPersona === 'em';

  // Build a geoid → centroid map so we can fly to a county when clicked
  const centroids = useMemo(() => {
    const out: Record<string, [number, number]> = {};
    if (!counties?.features) return out;
    for (const f of counties.features) {
      const geoid = f.properties?.GEOID || f.properties?.NAME || '?';
      let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
      const scan = (rings: any[][]) => {
        for (const ring of rings) for (const [x, y] of ring) {
          if (x < minX) minX = x; if (y < minY) minY = y;
          if (x > maxX) maxX = x; if (y > maxY) maxY = y;
        }
      };
      if (f.geometry?.type === 'Polygon') scan(f.geometry.coordinates);
      else if (f.geometry?.type === 'MultiPolygon') {
        for (const poly of f.geometry.coordinates) scan(poly);
      }
      if (isFinite(minX)) out[geoid] = [(minX + maxX) / 2, (minY + maxY) / 2];
    }
    return out;
  }, [counties]);

  // Panel-wide totals, for the "X% of total loss in 2 counties" insight
  const totalLoss = rollup.reduce((s, r) => s + r.loss, 0);
  const totalBldgs = rollup.reduce((s, r) => s + r.buildings, 0);
  const totalDisplaced = rollup.reduce((s, r) => s + r.estDisplaced, 0);
  const topTwoPct = rollup.length >= 2 && totalLoss > 0
    ? Math.round((rollup[0].loss + rollup[1].loss) / totalLoss * 100)
    : 0;

  const panelClass = isEM
    ? 'rounded-xl p-2.5 mb-3 border border-emerald-200 bg-emerald-50/40'
    : 'rounded-xl p-2.5 mb-3 border border-blue-200 bg-blue-50/40';
  const headerColor = isEM ? 'text-emerald-800' : 'text-blue-800';

  return (
    <div className={panelClass}>
      <button
        onClick={() => setExpanded(e => !e)}
        className="w-full flex items-center gap-2 text-left"
      >
        <span className={`text-[10px] font-bold uppercase tracking-wider ${headerColor}`}>
          Jurisdictions ({rollup.length})
        </span>
        <span className={`ml-auto text-[9px] font-bold uppercase px-1.5 py-0.5 rounded-sm ${isEM ? 'text-emerald-900 bg-emerald-100' : 'text-blue-900 bg-blue-100'}`}>
          {isEM ? 'EM' : 'CAT'}
        </span>
        <span className={`${isEM ? 'text-emerald-600' : 'text-blue-600'} text-xs`}>{expanded ? '▲' : '▼'}</span>
      </button>

      {expanded && (
        <>
          {/* Totals line */}
          <div className="text-[10px] text-slate-600 mt-1.5 mb-2 italic">
            {totalBldgs.toLocaleString()} bldgs · ${(totalLoss / 1e6).toFixed(0)}M total loss
            {isEM && totalDisplaced > 0 && <> · ~{totalDisplaced.toLocaleString()} displaced</>}
            {topTwoPct > 60 && <> · <span className="font-semibold text-red-700">{topTwoPct}% concentrated in top 2 counties</span></>}
          </div>

          {/* Per-county rows — click to fly */}
          <div className="space-y-1 max-h-64 overflow-y-auto">
            {rollup.map((r) => {
              const lossPct = totalLoss > 0 ? Math.round(r.loss / totalLoss * 100) : 0;
              const uninhab = r.severe + r.major;
              return (
                <button
                  key={r.geoid}
                  onClick={() => {
                    const c = centroids[r.geoid];
                    if (c && onFlyTo) onFlyTo(c[0], c[1]);
                  }}
                  className="w-full text-left bg-white/70 hover:bg-white rounded-md border border-slate-200 px-2 py-1.5 transition-colors"
                  title={`Fly to ${r.name} County · ${r.buildings.toLocaleString()} bldgs · $${(r.loss / 1e6).toFixed(1)}M loss`}
                >
                  {/* Row 1: name + loss */}
                  <div className="flex items-baseline gap-2">
                    <span className="text-xs font-bold text-slate-800 truncate flex-1">
                      {r.name}{r.state ? `, ${r.state}` : ''}
                    </span>
                    <span className="text-xs font-black text-red-700 tabular-nums">
                      ${(r.loss / 1e6).toFixed(1)}M
                    </span>
                    <span className="text-[9px] text-slate-400 tabular-nums w-7 text-right">{lossPct}%</span>
                  </div>

                  {/* Row 2: EM-flavored (displaced + critical) or CAT-flavored (uninhabitable + adjusters) */}
                  {isEM ? (
                    <div className="text-[10px] text-slate-600 mt-0.5 flex items-center gap-2">
                      <span>{r.buildings.toLocaleString()} bldgs</span>
                      {r.estDisplaced > 0 && (
                        <span>· 🏠 ~{r.estDisplaced.toLocaleString()} displaced</span>
                      )}
                      {r.criticalFacilities > 0 && (
                        <span>· ⭐ {r.criticalFacilities} critical</span>
                      )}
                      {r.maxDepthFt > 0 && (
                        <span className="ml-auto text-slate-400">max ~{Math.round(r.maxDepthFt)} ft</span>
                      )}
                    </div>
                  ) : (
                    <div className="text-[10px] text-slate-600 mt-0.5 flex items-center gap-2">
                      <span>{r.buildings.toLocaleString()} bldgs</span>
                      {uninhab > 0 && (
                        <span className="text-red-700 font-semibold">· {uninhab} uninhabitable</span>
                      )}
                      {r.criticalFacilities > 0 && (
                        <span>· ⭐ {r.criticalFacilities} critical</span>
                      )}
                    </div>
                  )}

                  {/* Severity bar */}
                  {r.buildings > 0 && (
                    <div className="mt-1 h-1 rounded-sm overflow-hidden bg-slate-100 flex">
                      <div className="bg-[#7f1d1d]" style={{ width: `${r.severe / r.buildings * 100}%` }} />
                      <div className="bg-[#ef4444]" style={{ width: `${r.major / r.buildings * 100}%` }} />
                      <div className="bg-[#fb923c]" style={{ width: `${r.moderate / r.buildings * 100}%` }} />
                      <div className="bg-[#facc15]" style={{ width: `${r.minor / r.buildings * 100}%` }} />
                    </div>
                  )}
                </button>
              );
            })}
          </div>

          <div className="text-[9px] text-slate-400 mt-1.5 italic">
            {isEM
              ? 'Allocate rescue, shelter, and mutual-aid requests per-county using these numbers. Displaced = (severe + major) residential × avg household.'
              : 'Per-county adjuster routing will follow the same pattern — drag the planner to see per-jurisdiction coverage.'}
          </div>
        </>
      )}
    </div>
  );
}

function DashboardPanel({ storm, totals, loadedCells, loadingCells, confidence, eli: _eli, validatedDps: _validatedDps, onOpenSidebar, zoom, onClearStorm, estimatedPop, severityCounts, criticalCount, criticalBreakdown, hotspots, onFlyTo, mode, onModeChange, subPersona, onSubPersonaChange, onGenerateCatReport, onGenerateSitRep, teamSize, windowDays, onTeamSizeChange, onWindowDaysChange, betaLayersEnabled, countyRollup, countiesGeoJSON, totalDisplaced, showCounties }: {
  storm: StormInfo | null;
  totals: { buildings: number; loss: number; totalDepth: number };
  loadedCells: Set<string>;
  loadingCells: Set<string>;
  confidence: { level: string; count: number };
  eli: { value: number; tier: string };
  validatedDps: { value: number; adj: number; reason: string };
  onOpenSidebar: () => void;
  zoom: number;
  onClearStorm: () => void;
  estimatedPop: number;
  severityCounts: Record<string, number>;
  criticalCount: number;
  criticalBreakdown: Array<{ icon: string; label: string; count: number }>;
  hotspots: Hotspot[];
  onFlyTo?: (lon: number, lat: number) => void;
  mode: DisplayMode;
  onModeChange: (m: DisplayMode) => void;
  subPersona: SubPersona;
  onSubPersonaChange: (p: SubPersona) => void;
  onGenerateCatReport: (format: 'html' | 'pdf') => void;
  onGenerateSitRep: (format: 'html' | 'pdf') => void;
  teamSize: number;
  windowDays: number;
  onTeamSizeChange: (n: number) => void;
  onWindowDaysChange: (n: number) => void;
  betaLayersEnabled: boolean;
  countyRollup: CountyRollup[] | null;
  countiesGeoJSON: any;
  totalDisplaced: number;
  showCounties: boolean;
}) {
  // Auto-expand on desktop, collapsed by default on mobile
  const [expanded, setExpanded] = useState(false);
  useEffect(() => {
    const mq = window.matchMedia('(min-width: 1024px)');
    setExpanded(mq.matches);
    const handler = (e: MediaQueryListEvent) => setExpanded(e.matches);
    mq.addEventListener('change', handler);
    return () => mq.removeEventListener('change', handler);
  }, []);

  // Authoritative displaced count from the jurisdictions rollup (if present),
  // so the Ops panel reconciles with the city/county bubbles and Analyst
  // tab totals instead of the legacy pop-fraction estimate in stagingPlan().
  // Falls back to `totalDisplaced` (computed in App from allBuildings) when
  // the county rollup hasn't been computed yet (countiesGeoJSON is lazy-loaded).
  const rollupDisplaced = countyRollup
    ? countyRollup.reduce((s, r) => s + r.estDisplaced, 0)
    : totalDisplaced;

  if (!storm) return null;

  return (
    <div className="absolute top-4 right-14 bg-white/95 backdrop-blur shadow-2xl rounded-lg w-72 max-w-[calc(100vw-2rem)] border border-gray-100 z-10">

      {/* ── Always-visible compact header ── */}
      <div className="flex items-center gap-2 px-3 py-2.5">
        {/* Sidebar toggle (mobile only) */}
        <button
          onClick={onOpenSidebar}
          className="lg:hidden text-slate-400 hover:text-slate-700 transition-colors p-1 rounded text-base leading-none shrink-0"
          aria-label="Open storm browser"
        >☰</button>

        {/* Storm name + category */}
        <div className="flex items-center gap-1.5 flex-1 min-w-0">
          <span className="font-bold text-gray-800 text-sm truncate">{storm.name}</span>
          <span className="text-[10px] font-bold px-1.5 py-0.5 rounded-full text-white shrink-0"
            style={{ backgroundColor: CAT_COLORS[storm.category] }}>CAT {storm.category}</span>
        </div>

        {/* Loss summary — visible only when collapsed */}
        {!expanded && totals.loss > 0 && (
          <span className="text-red-600 font-black text-sm shrink-0">
            {formatLossOps(totals.loss, mode)}
          </span>
        )}

        {/* Clear storm */}
        <button
          onClick={onClearStorm}
          className="text-gray-300 hover:text-red-500 transition-colors shrink-0 text-xs px-0.5"
          aria-label="Close storm"
          title="Close storm"
        >✕</button>

        {/* Expand / collapse toggle */}
        <button
          onClick={() => setExpanded(e => !e)}
          className="text-gray-400 hover:text-gray-600 transition-colors shrink-0 text-xs px-1"
          aria-label={expanded ? 'Collapse panel' : 'Expand panel'}
        >{expanded ? '▲' : '▼'}</button>
      </div>

      {/* ── Mode toggle — Analyst | Ops (CAT_TEAM_PLAN §3) ── */}
      {expanded && (
        <div className="px-3 pb-2 -mt-1">
          <div
            role="tablist"
            aria-label="Display mode"
            className="inline-flex w-full items-center rounded-md bg-slate-100 p-0.5 text-[10px] font-bold uppercase tracking-wider"
          >
            <button
              role="tab"
              aria-selected={mode === 'analyst'}
              onClick={() => onModeChange('analyst')}
              className={`flex-1 px-2 py-1 rounded transition-colors ${
                mode === 'analyst'
                  ? 'bg-white text-slate-800 shadow-sm'
                  : 'text-slate-500 hover:text-slate-700'
              }`}
              title="Analyst Mode — precise dollar losses and technical detail"
            >Analyst</button>
            <button
              role="tab"
              aria-selected={mode === 'ops'}
              onClick={() => onModeChange('ops')}
              className={`flex-1 px-2 py-1 rounded transition-colors ${
                mode === 'ops'
                  ? subPersona === 'em'
                    ? 'bg-white text-emerald-700 shadow-sm'
                    : 'bg-white text-orange-700 shadow-sm'
                  : 'text-slate-500 hover:text-slate-700'
              }`}
              title="Ops Mode — deployment-focused with confidence bands and rounded numbers"
            >Ops</button>
          </div>

          {/* ── Sub-persona pill — only meaningful in Ops Mode (Phase 4 §16) ── */}
          {mode === 'ops' && (
            <div
              role="tablist"
              aria-label="Ops sub-persona"
              className="inline-flex w-full items-center rounded-md bg-slate-50 border border-slate-200 p-0.5 text-[9px] font-bold uppercase tracking-wider mt-1"
            >
              <button
                role="tab"
                aria-selected={subPersona === 'cat'}
                onClick={() => onSubPersonaChange('cat')}
                className={`flex-1 px-2 py-1 rounded transition-colors ${
                  subPersona === 'cat'
                    ? 'bg-orange-500 text-white shadow-sm'
                    : 'text-slate-500 hover:text-orange-600'
                }`}
                title="Insurance CAT / CRT lens — adjuster deployment, claims routing, CAT Report"
              >🏢 Insurance CAT</button>
              <button
                role="tab"
                aria-selected={subPersona === 'em'}
                onClick={() => onSubPersonaChange('em')}
                className={`flex-1 px-2 py-1 rounded transition-colors ${
                  subPersona === 'em'
                    ? 'bg-emerald-600 text-white shadow-sm'
                    : 'text-slate-500 hover:text-emerald-700'
                }`}
                title="Emergency Manager lens — shelter/evac posture, resource staging, SitRep"
              >🚨 Emergency Mgr</button>
            </div>
          )}
        </div>
      )}

      {/* ── Expandable detail content ── */}
      {expanded && (
      <div className="px-4 pb-4 overflow-y-auto max-h-[70vh] flex flex-col">

      {/* Storm info card */}
      <div className="rounded-xl p-3 mb-3 border shadow-sm"
        style={{ backgroundColor: `${CAT_COLORS[storm.category]}10`, borderColor: `${CAT_COLORS[storm.category]}40` }}
      >
        <div className="grid grid-cols-2 gap-x-3 gap-y-0.5 text-xs text-gray-600">
          <span>Wind: <strong className="text-gray-800">{Math.round(storm.max_wind_kt * 1.15078)} mph</strong></span>
          <span>Pressure: <strong className="text-gray-800">{storm.min_pressure_mb} mb</strong></span>
          <span>Year: <strong className="text-gray-800">{storm.year}</strong></span>
        </div>
        {storm.population?.pop_label && (
          <div className="mt-1.5 pt-1.5 border-t border-gray-200/50 flex items-center gap-1.5 text-xs text-gray-600">
            <span className="text-sm">👥</span>
            <span><strong className="text-gray-800">{storm.population.pop_label}</strong> in {storm.population.county_name}, {storm.population.state_code}</span>
            {storm.population.vintage && <span className="text-[10px] text-gray-400">({storm.population.vintage})</span>}
          </div>
        )}
        <div className="mt-1.5 pt-1.5 border-t border-gray-200/50 text-[10px] text-gray-500">
          <span className="font-semibold">Surge note:</span> Modeled depths reflect SLOSH maximum-of-maximums (worst-case tidal alignment). Actual depths may have been lower if landfall did not coincide with local high tide.
        </div>
      </div>

      {/* CAT Deployment Summary (Ops Mode only — CAT_TEAM_PLAN §4a B1) */}
      <CatDeploymentSummary
        storm={storm}
        totals={totals}
        estimatedPop={estimatedPop}
        severityCounts={severityCounts}
        hotspots={hotspots}
        mode={mode}
        subPersona={subPersona}
        teamSize={teamSize}
        rollupDisplaced={rollupDisplaced}
        onGenerateCatReport={onGenerateCatReport}
        onGenerateSitRep={onGenerateSitRep}
      />

      {/* Deployment Planner (CAT_TEAM_PLAN §4b C3) — CAT persona only */}
      {mode === 'ops' && subPersona === 'cat' && (
        <DeploymentPlanner
          hotspots={hotspots}
          teamSize={teamSize}
          windowDays={windowDays}
          onTeamSizeChange={onTeamSizeChange}
          onWindowDaysChange={onWindowDaysChange}
        />
      )}

      {/* Resource Staging (CAT_TEAM_PLAN §4c E2/E3/E4) — EM persona only */}
      {mode === 'ops' && subPersona === 'em' && (
        <ResourceStagingPanel
          storm={storm}
          totals={totals}
          hotspots={hotspots}
          estimatedPop={estimatedPop}
          severityCounts={severityCounts}
          criticalBreakdown={criticalBreakdown}
          rollupDisplaced={rollupDisplaced}
        />
      )}

      {/* Jurisdictions (per-county rollup) — shown whenever the Counties overlay is on
          and we have damage data. EM uses this to allocate resources per jurisdiction;
          CAT uses it to see which counties carry the biggest loss share. */}
      {mode === 'ops' && showCounties && countyRollup && countyRollup.length > 0 && (
        <JurisdictionsPanel
          rollup={countyRollup}
          subPersona={subPersona}
          onFlyTo={onFlyTo}
          counties={countiesGeoJSON}
        />
      )}

      {/* Phase 5 — Beta data layers (CAT_TEAM_PLAN §8) — gated by More-menu flag, Ops only */}
      {mode === 'ops' && betaLayersEnabled && (
        <BetaDataLayersPanel
          storm={storm}
          hotspots={hotspots}
          subPersona={subPersona}
        />
      )}

      {/* R5: Confidence badge + sub-component pips (CAT_TEAM_PLAN B5) */}
      {(() => {
        const cs = CONFIDENCE_STYLES[confidence.level] || CONFIDENCE_STYLES.unvalidated;
        const tip = confidence.level === 'high' ? 'Strong building data coverage in the affected area'
          : confidence.level === 'medium' ? 'Moderate building data — some gaps possible'
          : confidence.level === 'low' ? 'Limited building data — estimates may be incomplete'
          : 'Model estimate only — building data not yet loaded';

        // Derive sub-component confidence from the data we already have.
        // Surge: SLOSH MoMs is always high-quality for historical storms we ship.
        // Buildings: mirrors the existing badge level.
        // Population: high if we have a county count, medium if only a label, low otherwise.
        const surgeLevel: 'high' | 'medium' | 'low' = 'high';
        const buildingsLevel: 'high' | 'medium' | 'low' =
          confidence.level === 'high' ? 'high'
            : confidence.level === 'medium' ? 'medium'
            : confidence.level === 'low' ? 'low'
            : 'low';
        const popLevel: 'high' | 'medium' | 'low' =
          storm.population?.population != null ? 'high'
            : storm.population?.pop_label ? 'medium'
            : 'low';

        const pipColor = (lv: 'high' | 'medium' | 'low') =>
          lv === 'high' ? 'bg-emerald-500'
            : lv === 'medium' ? 'bg-amber-400'
            : 'bg-rose-400';
        const pipFill = (lv: 'high' | 'medium' | 'low') =>
          lv === 'high' ? 5 : lv === 'medium' ? 3 : 2;

        const Pip = ({ label, lv, title }: { label: string; lv: 'high' | 'medium' | 'low'; title: string }) => (
          <div className="flex items-center gap-1.5 text-[10px] text-slate-600" title={title}>
            <span className="font-semibold w-[52px]">{label}</span>
            <div className="flex gap-0.5">
              {[0,1,2,3,4].map(i => (
                <span
                  key={i}
                  className={`w-1.5 h-2 rounded-sm ${i < pipFill(lv) ? pipColor(lv) : 'bg-slate-200'}`}
                />
              ))}
            </div>
          </div>
        );

        return (
          <div className={`${cs.bg} rounded-lg px-3 py-2 mb-3`}>
            <div className="flex items-center justify-between">
              <span className={`text-xs font-bold ${cs.text}`}>{cs.label}</span>
              <span className={`text-[10px] ${cs.text}`}>{confidence.count.toLocaleString()} buildings</span>
            </div>
            <p className={`text-[10px] mt-0.5 ${cs.text} opacity-75`}>{tip}</p>
            <div className="mt-1.5 pt-1.5 border-t border-white/60 grid grid-cols-1 gap-0.5">
              <Pip label="Surge"     lv={surgeLevel}     title="SLOSH maximum-of-maximums modeling for this event" />
              <Pip label="Buildings" lv={buildingsLevel} title="Building inventory coverage in the loaded grid cells" />
              <Pip label="Populatn." lv={popLevel}       title="County-level population data availability for the affected area" />
            </div>
          </div>
        );
      })()}

      {/* Critical Facilities in Surge Zone */}
      {criticalCount > 0 && (
        <div className="bg-orange-50 rounded-lg px-3 py-2 mb-3 border border-orange-200">
          <div className="text-[10px] text-orange-800 font-bold uppercase tracking-wider">Critical Facilities in Surge Zone</div>
          <div className="text-xs text-orange-900 mt-1 space-y-0.5">
            {criticalBreakdown.map(({ icon, label, count }: any) => count > 0 && (
              <div key={label} className="flex items-center gap-1.5">
                <span>{icon}</span>
                <span className="flex-1">{label}</span>
                <span className="font-bold">{count}</span>
              </div>
            ))}
          </div>
        </div>
      )}



      {/* Scoreboard */}
      {totals.buildings > 0 && (
        <div className="bg-gray-100/50 rounded-xl p-3 text-center border border-gray-200/60 shadow-sm mb-3">
          <div className="text-[10px] text-gray-500 font-bold uppercase tracking-wider mb-0.5">
            {mode === 'ops' ? 'Modeled Loss (rounded)' : 'Total Modeled Loss'}
          </div>
          <div className="text-2xl font-black text-red-600 tracking-tighter">
            {totals.loss > 0 ? formatLossOps(totals.loss, mode) : '...'}
          </div>
          <div className="text-xs text-gray-500 mt-0.5">
            Across {formatCountOps(totals.buildings, mode)} properties
          </div>
          {(estimatedPop > 0 || storm.population?.population) && (
            <div className="text-[10px] text-gray-400 mt-0.5">
              {storm.population?.population
                ? `${storm.population.county_name} county pop: ${formatCountOps(storm.population.population, mode)} · ~${formatCountOps(estimatedPop, mode)} in surge zone`
                : `~${formatCountOps(estimatedPop, mode)} estimated residents in surge zone`}
            </div>
          )}
          {mode === 'ops' && (
            <div className="mt-1 text-[9px] text-gray-400 italic">
              Rounded for deployment planning — see Analyst Mode for precise figures.
            </div>
          )}
        </div>
      )}

      {/* Damage Severity Breakdown */}
      {totals.buildings > 0 && (
        <div className="bg-gray-50 rounded-lg p-2.5 mb-3 border border-gray-200">
          <div className="text-[10px] text-gray-500 font-bold uppercase tracking-wider mb-1.5">Damage Breakdown</div>
          <div className="space-y-1">
            {[
              { key: 'severe', color: '#7f1d1d', label: 'Severe' },
              { key: 'major', color: '#ef4444', label: 'Major' },
              { key: 'moderate', color: '#fb923c', label: 'Moderate' },
              { key: 'minor', color: '#facc15', label: 'Minor' },
              { key: 'none', color: '#4ade80', label: 'No Damage' },
            ].map(({ key, color, label }) => {
              const count = severityCounts[key] || 0;
              const pct = totals.buildings > 0 ? (count / totals.buildings * 100) : 0;
              return (
                <div key={key} className="flex items-center gap-2 text-xs">
                  <span className="w-3 h-3 rounded-full shrink-0" style={{ backgroundColor: color }} />
                  <span className="text-gray-600 flex-1">{label}</span>
                  <span className="font-bold text-gray-800">{count.toLocaleString()}</span>
                  <span className="text-gray-400 w-10 text-right">{count > 0 && pct < 1 ? '<1' : pct.toFixed(0)}%</span>
                </div>
              );
            })}
          </div>
          {(() => {
            // CAT_TEAM_PLAN §4a B3 — severity → workload translation footer
            const wl = workloadSummary(severityCounts);
            if (wl.inspections_needed === 0) return null;
            const show = wl.uninhabitable > 0 || wl.inspections_needed >= 100;
            if (!show) return null;
            return (
              <div className="mt-2 bg-red-50 rounded px-2 py-1 border border-red-200">
                <div className="text-[10px] font-bold text-red-700">
                  {formatCountOps(wl.inspections_needed, mode)} inspections needed
                  {wl.uninhabitable > 0 && (
                    <> · {formatCountOps(wl.uninhabitable, mode)} likely uninhabitable</>
                  )}
                </div>
                <div className="text-[9px] text-red-500 mt-0.5 italic">{wl.headline}</div>
              </div>
            );
          })()}
        </div>
      )}

      {/* R9: Nuisance Flood Flag */}
      {totals.buildings > 2000 && totals.totalDepth > 0 && (totals.totalDepth / totals.buildings) < 1.5 && (
        <div className="bg-amber-50 rounded-lg px-3 py-2 mb-3 border border-amber-300">
          <div className="text-[10px] text-amber-800 font-bold uppercase tracking-wider">Nuisance Flood Warning</div>
          <div className="text-xs text-amber-700 mt-0.5">
            Avg. depth of {formatDepthOps(totals.totalDepth / totals.buildings, mode)} across {formatCountOps(totals.buildings, mode)} buildings — widespread shallow flooding can cause significant aggregate damage even when individual losses appear modest.
          </div>
        </div>
      )}

      {/* Hardest-Hit Areas (CAT_TEAM_PLAN §4b C1/C2/B2 — peril bar, routing, adjusters) */}
      {hotspots.length > 0 && (
        <div className="bg-red-50/50 rounded-lg p-2.5 mb-3 border border-red-100">
          <div className="text-[10px] text-red-600 font-bold uppercase tracking-wider mb-1.5">Hardest-Hit Areas</div>
          <div className="space-y-2">
            {hotspots.map((h) => {
              const isEM = mode === 'ops' && subPersona === 'em';
              const post = isEM ? shelterPosture(h.maxDepthFt) : null;
              return (
                <button
                  key={h.rank}
                  onClick={() => onFlyTo?.(h.lon, h.lat)}
                  className="w-full text-left hover:bg-red-100/50 rounded px-1 py-1 transition-colors"
                >
                  {/* Top line: rank, loss, routing tag (CAT) or shelter posture (EM) */}
                  <div className="flex items-center gap-2">
                    <span className="text-xs font-black text-red-400 w-4">#{h.rank}</span>
                    <div className="flex-1 min-w-0">
                      <div className="text-xs font-bold text-red-700">{formatLossOps(h.loss, mode)}</div>
                      <div className="text-[10px] text-red-400">
                        {mode === 'ops'
                          ? `${formatCountOps(h.count, mode)} bldgs · avg ${formatLossOps(h.avgLoss, mode)}`
                          : `${h.count} bldgs · avg $${h.avgLoss.toLocaleString()}`}
                      </div>
                    </div>
                    {isEM && post ? (
                      <span
                        className={`text-[9px] font-bold px-1.5 py-0.5 rounded-sm shrink-0 ${post.classes}`}
                        title={post.description}
                      >{post.short}</span>
                    ) : (
                      <span
                        className={`text-[9px] font-bold px-1.5 py-0.5 rounded-sm shrink-0 ${h.routing.classes}`}
                        title={h.routing.description}
                      >{h.routing.short}</span>
                    )}
                  </div>

                  {/* Peril mix bar (B2) — shown in both personas */}
                  <div className="mt-1 ml-6 flex items-center gap-1.5">
                    <div className="flex-1 h-1.5 rounded-sm overflow-hidden bg-slate-200 flex" title={`${h.waterPct}% water · ${h.windPct}% wind`}>
                      <div className="bg-indigo-500" style={{ width: `${h.waterPct}%` }} />
                      <div className="bg-sky-400"    style={{ width: `${h.windPct}%` }} />
                    </div>
                    <span className="text-[9px] text-slate-500 tabular-nums shrink-0">
                      🌊 {h.waterPct}% · 🌬️ {h.windPct}%
                    </span>
                  </div>

                  {/* Sub-line: adjuster recommendation (CAT) or shelter posture detail (EM) — E1 */}
                  {isEM && post ? (
                    <div className="mt-0.5 ml-6 text-[10px] text-slate-600">
                      <span>{post.icon}</span>{' '}
                      <span className="font-semibold">{post.label}</span>
                      <span className="text-slate-400"> · max ~{Math.round(h.maxDepthFt)} ft</span>
                    </div>
                  ) : (
                    h.recommend.adjusters > 0 && (
                      <div className="mt-0.5 ml-6 text-[10px] text-slate-600">
                        <span className="font-semibold">🚗 {h.recommend.label}</span>
                        {(h.severity.severe + h.severity.major) > 0 && (
                          <span className="text-slate-400"> · {h.severity.severe + h.severity.major} uninhabitable</span>
                        )}
                      </div>
                    )
                  )}
                </button>
              );
            })}
          </div>
        </div>
      )}

      {/* Grid status */}
      <div className="bg-blue-50/50 rounded-lg p-2.5 mb-3 border border-blue-100">
        <div className="text-[10px] text-blue-600 font-bold uppercase tracking-wider mb-0.5">Map Coverage</div>
        <div className="text-sm text-blue-800 font-semibold">
          {totals.buildings > 0 ? `${totals.buildings.toLocaleString()} buildings` : `${loadedCells.size} area${loadedCells.size !== 1 ? 's' : ''}`} analyzed
        </div>
        <div className="text-xs text-blue-500 mt-0.5">
          {loadingCells.size > 0
            ? `Fetching data for ${loadingCells.size} more area${loadingCells.size !== 1 ? 's' : ''}…`
            : zoom >= 13 ? 'Zoom out to see grid borders and expand coverage'
            : 'Click the dashed borders on the map to expand coverage'}
        </div>
      </div>

      {/* Building Damage legend removed — redundant with Damage Breakdown above */}
      </div>
      )}
    </div>
  );
}

// Average persons per residential unit (HAZUS defaults)
const POP_PER_UNIT: Record<string, number> = {
  RES1: 2.5, RES2: 2.0, RES3: 6.0, RES4: 2.0, RES5: 2.0, RES6: 30.0,
};

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// App
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

function App() {
  const mapRef = useRef<MapRef>(null);

  const [sidebarOpen, setSidebarOpen] = useState(true);
  const [activeStorm, setActiveStorm] = useState<StormInfo | null>(null);
  const [activating, setActivating] = useState(false);
  const [hoverInfo, setHoverInfo] = useState<any>(null);
  const [pinnedInfo, setPinnedInfo] = useState<any>(null);
  const [impactTotals, setImpactTotals] = useState({ buildings: 0, loss: 0, totalDepth: 0 });
  const [loadedCells, setLoadedCells] = useState<Set<string>>(new Set());
  const [loadingCells, setLoadingCells] = useState<Set<string>>(new Set());
  const [allBuildings, setAllBuildings] = useState<any>(null);
  const [allFlood, setAllFlood] = useState<any>(null);
  const [zoom, setZoom] = useState(10);
  const [basemap, setBasemap] = useState<string>('dark');
  const [imageryDate, setImageryDate] = useState<string | null>(null);
  const imageryFetchTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  // Map view mode — toggles the bubble pipeline between damage-weighted
  // (buildings × severity, loss) and population-weighted (est. displaced,
  // occupants). Same county → city → building zoom hierarchy either way.
  const [mapView, setMapView] = useState<'damage' | 'population'>('damage');
  // ── Time-series peril state ───────────────────────────────────────────────
  // The backend emits a cell_..._ticks.json alongside each damage.geojson
  // containing per-building HAZUS runs at every tick hour for three perils
  // (surge-only, rainfall-only, cumulative). We fetch those bundles async
  // after the main cell response lands, merge into a per-building lookup,
  // and let the time slider + peril toggle drive the map paint.
  type PerilKey = 'surge' | 'rainfall' | 'cumulative';
  type TickRow = [number, number, number, string, string, string, number, number, number];
  interface TicksBundle {
    schema_version: string;
    tick_hours: number[];
    duration_h: number;
    peril_fields: string[];
    buildings: { id: string; lat: number; lon: number; ticks: TickRow[] }[];
  }
  // `Map` here is shadowed by the react-map-gl import above, so we use
  // a plain Record<string, …> for the per-building tick lookup.
  const [peril, setPeril] = useState<PerilKey>('cumulative');
  const [tickIdx, setTickIdx] = useState<number>(-1); // -1 = "latest" (final tick)
  const [tickHours, setTickHours] = useState<number[]>([]);
  const buildingTicksRef = useRef<Record<string, TickRow[]>>({});
  const [buildingTicksVersion, setBuildingTicksVersion] = useState(0);
  // `noUnusedLocals` guard — these are consumed by the slider + peril toggle
  // UI added in a follow-up commit. Referenced here so strict TS is happy.
  void peril; void setPeril; void tickIdx; void setTickIdx;
  void tickHours; void buildingTicksVersion;
  const [showCounties, setShowCounties] = useState(false);
  const [countiesGeoJSON, setCountiesGeoJSON] = useState<any>(null);
  const [countiesLoading, setCountiesLoading] = useState(false);
  const [countiesError, setCountiesError] = useState<string | null>(null);
  // City-level data — lazy-loaded alongside buildings (no toggle needed;
  // city bubbles are always on between county zoom and building zoom).
  const [citiesData, setCitiesData] = useState<CityEntry[] | null>(null);
  const citiesLoadedRef = useRef(false);
  const [showFloodZones, setShowFloodZones] = useState(false);
  // Land-use overlay — served live from the USGS/MRLC NLCD 2021 WMS.
  // 30 m CONUS raster, reclassified on the server side into 16 Anderson
  // Level II classes that we roll up in the legend to residential /
  // commercial / agricultural / open / water. No bundling, no per-run
  // bake — the MRLC geoserver is a stable public endpoint used by the
  // National Map. Can be replaced with a baked PMTiles later (see
  // scripts/build_landuse_pmtiles.py) if we want offline or faster load.
  const [showLandUse, setShowLandUse] = useState(false);
  const [floodZonesGeoJSON, setFloodZonesGeoJSON] = useState<any>(null);
  const [floodZonesLoading, setFloodZonesLoading] = useState(false);
  const [floodZonesError, setFloodZonesError] = useState<string | null>(null);
  const floodZonesFetchTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  // ── Rainfall hazard view (Option A: fold rainfall into the hazard control) ──
  // 'surge'    → existing surge-driven polygons/bubbles (default)
  // 'rainfall' → show MRMS observed accumulation stats badge (raster tiles Phase 6)
  // 'compound' → surge + rainfall + fluvial (same as damage model input); raster Phase 6
  const [hazardView, setHazardView] = useState<'surge' | 'rainfall' | 'compound'>('surge');
  const [rainfallStats, setRainfallStats] = useState<{ maxIn: number | null; avgIn: number | null; product: string | null; validTime: string | null; notes: string; tileUrl: string | null } | null>(null);
  const [rainfallLoading, setRainfallLoading] = useState(false);
  // ── AHPS / NWPS stream gauges layer ──
  // Cheap, immediate value: GeoJSON points colored by flood category.
  const [showGauges, setShowGauges] = useState(false);
  const [gaugesGeoJSON, setGaugesGeoJSON] = useState<any>(null);
  const [gaugesLoading, setGaugesLoading] = useState(false);
  const [gaugesError, setGaugesError] = useState<string | null>(null);
  const [gaugesSummary, setGaugesSummary] = useState<{ major: number; moderate: number; minor: number; count: number } | null>(null);
  const [moreMenuOpen, setMoreMenuOpen] = useState(false);
  // Building flags — keyed by "lon,lat" for uniqueness. Values: 'confirmed_destroyed' | 'shelter_in_place' | 'inspected' | 'inaccessible' | ''
  const [buildingFlags, setBuildingFlags] = useState<Record<string, string>>({});
  const [nuisanceDismissed, setNuisanceDismissed] = useState(false);
  const [gridHintDismissed, setGridHintDismissed] = useState(false);
  const gridHintTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const activateAbortRef = useRef<AbortController | null>(null);
  const batchAbortRef = useRef(false);
  const handleResetView = useCallback(() => {
    if (activeStorm && mapRef.current) {
      mapRef.current.flyTo({ center: [activeStorm.landfall_lon, activeStorm.landfall_lat], zoom: 10, pitch: 30, duration: 2000 });
    }
  }, [activeStorm]);

  // ── County boundaries (bundled coastal-states GeoJSON) ──
  // The overlay ships as a static asset (~1.2 MB, Census cartographic boundary,
  // coastal + Gulf + Pacific states only). Lazy-loaded on first toggle via dynamic
  // import so the initial bundle isn't penalized. No upstream dependency = no
  // WAF/CORS/timeout failure modes. Once loaded, the same GeoJSON is reused.
  const countiesLoadedRef = useRef(false);
  const loadCounties = useCallback(async () => {
    if (countiesLoadedRef.current) return;
    setCountiesLoading(true);
    setCountiesError(null);
    try {
      const mod = await import('./assets/counties-coastal.json');
      const raw: any = (mod as any).default ?? mod;
      if (!raw?.features?.length) throw new Error('Empty counties dataset');
      // CLONE before mutating — dynamic imports are cached at the module
      // level, so writing into raw.features would pollute the singleton
      // for any other importer. Shallow-clone features + properties; the
      // geometry arrays are passed by reference (safe, we never mutate them).
      const data = {
        type: 'FeatureCollection' as const,
        features: raw.features.map((f: any) => ({
          type: 'Feature' as const,
          geometry: f.geometry,
          properties: { ...(f.properties || {}) },
        })),
      };
      // Assign a stable categorical color index to every feature so the
      // county-fill layer can render a distinct-color choropleth. Uses a
      // djb-style hash of GEOID forced to unsigned 32-bit (>>> 0) so the
      // Math.abs edge case on INT32_MIN can't collapse to 0. With 8 cool
      // pastels the jurisdictions read at a glance.
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

  // ── City data (Census Places, pop ≥ 2500, 18 coastal states) ──
  // Lazy-loaded as soon as buildings are available — no toggle needed because
  // city bubbles are always shown between county zoom (<8) and individual
  // building zoom (≥11). 423 KB minified, loads in ~100 ms on LTE.
  const loadCities = useCallback(async () => {
    if (citiesLoadedRef.current) return;
    try {
      const mod = await import('./assets/cities-coastal.json');
      const raw: any = (mod as any).default ?? mod;
      if (Array.isArray(raw) && raw.length) {
        setCitiesData(raw as CityEntry[]);
        citiesLoadedRef.current = true;
      }
    } catch (err: any) {
      console.warn('[cities] load failed:', err?.message || err);
    }
  }, []);

  // ── FEMA NFHL flood zone fetch ──
  // Queries FEMA's National Flood Hazard Layer MapServer (layer 28 = Flood
  // Hazard Zones) for the current map view. Returns GeoJSON with FLD_ZONE
  // attribute used for color-coding.
  //
  // Notes on the endpoint: FEMA deprecated the old `/gis/nfhl/rest/...
  // /FeatureServer/28` path (now 404). The current public endpoint is
  // `/arcgis/rest/services/public/NFHL/MapServer/28`, and it only accepts
  // `f=geojson` when `geometry` is encoded as a JSON envelope object, not
  // as a comma-separated bbox string (the comma form errors with HTTP 400).
  const fetchFloodZones = useCallback((bounds: { west: number; south: number; east: number; north: number }) => {
    if (floodZonesFetchTimer.current) clearTimeout(floodZonesFetchTimer.current);
    floodZonesFetchTimer.current = setTimeout(async () => {
      const { west, south, east, north } = bounds;
      const envelope = JSON.stringify({
        xmin: west, ymin: south, xmax: east, ymax: north,
        spatialReference: { wkid: 4326 },
      });
      const params = new URLSearchParams({
        where: '1=1',
        geometry: envelope,
        geometryType: 'esriGeometryEnvelope',
        inSR: '4326',
        outSR: '4326',
        spatialRel: 'esriSpatialRelIntersects',
        outFields: 'FLD_ZONE,SFHA_TF,FLOODWAY',
        returnGeometry: 'true',
        resultRecordCount: '2000',
        f: 'geojson',
      });
      const url = `https://hazards.fema.gov/arcgis/rest/services/public/NFHL/MapServer/28/query?${params}`;
      setFloodZonesLoading(true);
      setFloodZonesError(null);
      const ac = new AbortController();
      const timeout = setTimeout(() => ac.abort(), 20_000);
      try {
        const res = await fetch(url, { signal: ac.signal });
        clearTimeout(timeout);
        if (!res.ok) throw new Error(`NFHL ${res.status}`);
        const data = await res.json();
        if (data?.error) throw new Error(data.error.message || 'NFHL error');
        if (data?.features?.length) {
          setFloodZonesGeoJSON(data);
        } else {
          setFloodZonesGeoJSON({ type: 'FeatureCollection', features: [] });
        }
      } catch (err: any) {
        clearTimeout(timeout);
        console.warn('[flood-zones] fetch failed:', err?.message || err);
        setFloodZonesError(err?.name === 'AbortError' ? 'FEMA flood-zone fetch timed out' : 'Could not load FEMA flood zones');
      } finally {
        setFloodZonesLoading(false);
      }
    }, 600);
  }, []);

  // ── ESRI World Imagery date lookup ──
  // Queries the ESRI identify endpoint for the acquisition date of the satellite
  // tile at the current map center. Debounced 800 ms so it only fires after the
  // user stops panning. Clears when switching away from satellite basemap.
  const fetchImageryMeta = useCallback((lat: number, lon: number) => {
    if (imageryFetchTimer.current) clearTimeout(imageryFetchTimer.current);
    imageryFetchTimer.current = setTimeout(async () => {
      try {
        const m = 0.05;
        const params = new URLSearchParams({
          geometry: `${lon},${lat}`,
          geometryType: 'esriGeometryPoint',
          sr: '4326',
          layers: 'all',
          returnGeometry: 'false',
          tolerance: '2',
          mapExtent: `${lon-m},${lat-m},${lon+m},${lat+m}`,
          imageDisplay: '800,600,96',
          f: 'json',
        });
        const res = await fetch(
          `https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/identify?${params}`
        );
        const data = await res.json();
        const attrs = data.results?.[0]?.attributes;
        if (attrs) {
          const ts = attrs.SRC_DATE ?? attrs.SRC_DATE2 ?? attrs.SAMP_DATE;
          if (ts) {
            const epoch = parseInt(ts);
            // Guard: ESRI returns 0 or negative for missing dates → shows "Dec 1969"
            // Valid satellite imagery dates are after 2000 (epoch > 946684800000)
            if (!isNaN(epoch) && epoch > 946684800000) {
              const d = new Date(epoch);
              if (!isNaN(d.getTime())) {
                setImageryDate(d.toLocaleDateString('en-US', { year: 'numeric', month: 'short' }));
                return;
              }
            }
          }
        }
        setImageryDate(null);
      } catch {
        setImageryDate(null);
      }
    }, 800);
  }, []);
  const [confidence, setConfidence] = useState<{ level: string; count: number }>({ level: 'unvalidated', count: 0 });
  // Analyst vs Ops display mode (CAT_TEAM_PLAN §3). Persisted best-effort in localStorage.
  const [mode, setMode] = useState<DisplayMode>(() => {
    if (typeof window === 'undefined') return 'analyst';
    try {
      const stored = window.localStorage.getItem('surgedps.mode');
      return stored === 'ops' ? 'ops' : 'analyst';
    } catch { return 'analyst'; }
  });
  useEffect(() => {
    try { window.localStorage.setItem('surgedps.mode', mode); } catch { /* ignore */ }
  }, [mode]);
  // Ops Mode sub-persona toggle — Insurance CAT vs Emergency Manager
  // (CAT_TEAM_PLAN §3, Phase 4 §16). Only meaningful when mode === 'ops'.
  const [subPersona, setSubPersona] = useState<SubPersona>(() => {
    if (typeof window === 'undefined') return 'cat';
    try {
      const stored = window.localStorage.getItem('surgedps.subpersona');
      return stored === 'em' ? 'em' : 'cat';
    } catch { return 'cat'; }
  });
  useEffect(() => {
    try { window.localStorage.setItem('surgedps.subpersona', subPersona); } catch { /* ignore */ }
  }, [subPersona]);
  // Phase 5 — Beta data layers flag (CAT_TEAM_PLAN §8 Phase 5).
  // Single shared toggle that unlocks the B7/E5/C6/E6 placeholder
  // panels. Real data hooks live in ui/src/betaLayers.ts and return
  // empty shapes until the backend endpoints ship.
  const [betaLayersEnabled, setBetaLayersEnabled] = useState<boolean>(() => readBetaLayersEnabled());
  useEffect(() => { writeBetaLayersEnabled(betaLayersEnabled); }, [betaLayersEnabled]);
  // Deployment Planner state (CAT_TEAM_PLAN §4b C3) — lifted to App
  // so the planner and CAT summary see the same numbers.
  const [teamSize, setTeamSize] = useState<number>(20);
  const [windowDays, setWindowDays] = useState<number>(5);
  const [eli, setEli] = useState<{ value: number; tier: string }>({ value: 0, tier: 'unavailable' });
  const [validatedDps, setValidatedDps] = useState<{ value: number; adj: number; reason: string }>({ value: 0, adj: 0, reason: '' });
  const [manifest, setManifest] = useState<Record<string, any>>({});

  // ── Simulator state (active storms with forecast track) ──
  const [simMode, setSimMode] = useState(false);
  const [simMarker, setSimMarker] = useState<{ lng: number; lat: number } | null>(null);
  const [simRunning, setSimRunning] = useState(false);
  const [simResult, setSimResult] = useState<any>(null);
  const [forecastCone, setForecastCone] = useState<any>(null);       // GeoJSON polygon
  const [forecastTrack, setForecastTrack] = useState<any[]>([]);     // Forecast points
  // Toast notifications (error = red, success = green)
  const [cellError, setCellError] = useState<string | null>(null);
  const [toastSuccess, setToastSuccess] = useState<string | null>(null);
  const [retryStormId, setRetryStormId] = useState<string | null>(null);
  useEffect(() => { if (cellError) { const t = setTimeout(() => { setCellError(null); setRetryStormId(null); }, 8000); return () => clearTimeout(t); } }, [cellError]);
  useEffect(() => { if (toastSuccess) { const t = setTimeout(() => setToastSuccess(null), 3000); return () => clearTimeout(t); } }, [toastSuccess]);

  // Load counties on first enable OR as soon as the first cell loads (needed
  // for the low-zoom county-aggregate bubble layer, which runs regardless of
  // the overlay toggle). Only depends on a *flag* for "buildings exist" so
  // the effect doesn't re-run on every cell load — the ref short-circuits
  // re-imports, but the work is still wasteful.
  const hasBuildings = !!allBuildings?.features?.length;
  useEffect(() => {
    if (showCounties || hasBuildings) {
      loadCounties();
    }
    if (!showCounties) {
      setCountiesError(null);
      setCountiesLoading(false);
    }
  }, [showCounties, hasBuildings, loadCounties]);

  // Load city data as soon as buildings are present (city bubbles are always-on
  // between county and building zoom levels — no user toggle needed).
  useEffect(() => {
    if (hasBuildings) loadCities();
  }, [hasBuildings, loadCities]);

  // Fetch FEMA flood zones when showFloodZones is enabled; clear when disabled.
  useEffect(() => {
    if (showFloodZones) {
      const tryFetch = (attempt: number) => {
        if (mapRef.current) {
          const b = mapRef.current.getBounds();
          fetchFloodZones({ west: b.getWest(), south: b.getSouth(), east: b.getEast(), north: b.getNorth() });
        } else if (attempt < 10) {
          setTimeout(() => tryFetch(attempt + 1), 150);
        }
      };
      tryFetch(0);
    } else {
      if (floodZonesFetchTimer.current) clearTimeout(floodZonesFetchTimer.current);
      setFloodZonesGeoJSON(null);
      setFloodZonesError(null);
      setFloodZonesLoading(false);
    }
  }, [showFloodZones, fetchFloodZones]);

  // ── Stream gauges (AHPS / NWPS) ──
  // Fires when user toggles the gauges overlay on AND a storm is active.
  // Backend /api/gauges returns GeoJSON + category counts within ~4° of landfall.
  useEffect(() => {
    if (!showGauges || !activeStorm) {
      setGaugesGeoJSON(null);
      setGaugesSummary(null);
      setGaugesError(null);
      setGaugesLoading(false);
      return;
    }
    let cancelled = false;
    setGaugesLoading(true);
    setGaugesError(null);
    fetchGaugeOverlay(activeStorm.storm_id, 4.0, 'action')
      .then((res: Awaited<ReturnType<typeof fetchGaugeOverlay>>) => {
        if (cancelled) return;
        if (!res.available) {
          setGaugesError(res.notes);
          setGaugesGeoJSON(null);
          setGaugesSummary(null);
        } else {
          setGaugesGeoJSON(res.geojson);
          setGaugesSummary({
            major: res.atOrAboveMajor,
            moderate: res.atOrAboveModerate,
            minor: res.atOrAboveMinor,
            count: res.gaugeCount,
          });
        }
      })
      .catch((err: unknown) => {
        if (!cancelled) setGaugesError(err instanceof Error ? err.message : String(err));
      })
      .finally(() => {
        if (!cancelled) setGaugesLoading(false);
      });
    return () => { cancelled = true; };
  }, [showGauges, activeStorm]);

  // ── Rainfall stats (only fetched when user picks Rainfall/Compound view) ──
  // Phase 5: returns accumulation stats only (tile raster comes in Phase 6).
  useEffect(() => {
    if (hazardView === 'surge' || !activeStorm) {
      setRainfallStats(null);
      setRainfallLoading(false);
      return;
    }
    let cancelled = false;
    setRainfallLoading(true);
    fetchRainfallOverlay(activeStorm.storm_id, 72, 2)
      .then((res: RainfallOverlay) => {
        if (cancelled) return;
        if (!res.available) {
          setRainfallStats({ maxIn: null, avgIn: null, product: null, validTime: null, notes: res.notes, tileUrl: null });
        } else {
          setRainfallStats({
            maxIn: res.maxPrecipMm != null ? +(res.maxPrecipMm / 25.4).toFixed(1) : null,
            avgIn: res.avgPrecipMm != null ? +(res.avgPrecipMm / 25.4).toFixed(1) : null,
            product: res.product,
            validTime: res.validTime,
            notes: res.notes,
            tileUrl: res.tileUrlTemplate,
          });
        }
      })
      .finally(() => { if (!cancelled) setRainfallLoading(false); });
    return () => { cancelled = true; };
  }, [hazardView, activeStorm]);

  // Fetch imagery acquisition date when switching to satellite, clear when switching away
  useEffect(() => {
    if (basemap === 'satellite' && mapRef.current) {
      const c = mapRef.current.getCenter();
      fetchImageryMeta(c.lat, c.lng);
    } else {
      if (imageryFetchTimer.current) clearTimeout(imageryFetchTimer.current);
      setImageryDate(null);
    }
  }, [basemap, fetchImageryMeta]);

  // Progress tracking for loading overlay
  const [loadProgress, setLoadProgress] = useState<{ step: string; step_num: number; total_steps: number; elapsed: number }>({ step: '', step_num: 0, total_steps: 4, elapsed: 0 });
  const progressIntervalRef = useRef<ReturnType<typeof setInterval> | null>(null);

  // ── Address search (geocoding via Nominatim) ──
  const [addressQuery, setAddressQuery] = useState('');
  const [addressSearching, setAddressSearching] = useState(false);
  const [addressError, setAddressError] = useState('');
  const flyToPopupTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  useEffect(() => () => { if (flyToPopupTimer.current) { clearTimeout(flyToPopupTimer.current); flyToPopupTimer.current = null; } }, []);
  const handleAddressSearch = useCallback(() => {
    const q = addressQuery.trim();
    if (!q || !mapRef.current) return;
    setAddressSearching(true);
    setAddressError('');
    if (flyToPopupTimer.current) { clearTimeout(flyToPopupTimer.current); flyToPopupTimer.current = null; }
    fetch(`/surgedps/api/geocode/search?q=${encodeURIComponent(q)}`)
      .then(r => r.json())
      .then((data: any) => { const results: any[] = data?.results || [];
        if (results.length === 0) { setAddressError('Address not found'); return; }
        const gLon = parseFloat(results[0].lon), gLat = parseFloat(results[0].lat);
        // Find nearest building within 200m and auto-select it
        let nearest: any = null, minDist = Infinity;
        if (allBuildings?.features?.length) {
          for (const f of allBuildings.features) {
            const [bLon, bLat] = f.geometry?.coordinates || [0, 0];
            const d = haversineKm(gLat, gLon, bLat, bLon);
            if (d < minDist) { minDist = d; nearest = f; }
          }
        }
        if (nearest && minDist < 0.2) {
          const [nLon, nLat] = nearest.geometry.coordinates;
          mapRef.current?.flyTo({ center: [nLon, nLat], zoom: 17, duration: 2000 });
          flyToPopupTimer.current = setTimeout(() => { flyToPopupTimer.current = null; setPinnedInfo({ lng: nLon, lat: nLat, type: 'damage', feature: { properties: nearest.properties, geometry: nearest.geometry } }); }, 2200);
        } else {
          mapRef.current?.flyTo({ center: [gLon, gLat], zoom: 16, duration: 2000 });
        }
      })
      .catch(() => setAddressError('Search failed — try again'))
      .finally(() => setAddressSearching(false));
  }, [addressQuery, allBuildings]);

  // ── Batch address lookup ──
  const [batchOpen, setBatchOpen] = useState(false);
  const [batchInput, setBatchInput] = useState('');
  const [batchResults, setBatchResults] = useState<any[]>([]);
  const [batchLoading, setBatchLoading] = useState(false);
  const handleBatchLookup = useCallback(async () => {
    const lines = batchInput.split('\n').map(l => l.trim()).filter(Boolean);
    if (lines.length === 0 || !allBuildings?.features?.length) return;
    setBatchLoading(true);
    setBatchResults([]);
    batchAbortRef.current = false;
    const results: any[] = [];
    for (let idx = 0; idx < lines.length; idx++) {
      if (batchAbortRef.current) break;
      const addr = lines[idx];
      try {
        const r = await fetch(`/surgedps/api/geocode/search?q=${encodeURIComponent(addr)}`);
        const geoData = await r.json();
        const geoResults = geoData?.results || [];
        if (!geoResults.length) { results.push({ address: addr, status: 'not found' }); continue; }
        const lon = parseFloat(geoResults[0].lon), lat = parseFloat(geoResults[0].lat);
        // Find nearest building within ~200m (haversine)
        let nearest: any = null, minDist = Infinity;
        for (const f of allBuildings.features) {
          const [bLon, bLat] = f.geometry?.coordinates || [0, 0];
          const d = haversineKm(lat, lon, bLat, bLon);
          if (d < minDist) { minDist = d; nearest = f; }
        }
        if (nearest && minDist < 0.2) {
          const p = nearest.properties;
          results.push({ address: addr, status: 'matched', distance_m: Math.round(minDist * 1000), ...p });
        } else {
          results.push({ address: addr, status: 'no building nearby', lat, lon });
        }
      } catch { results.push({ address: addr, status: 'geocode error' }); }
      // Server handles Nominatim rate limiting; small delay for UI responsiveness
      if (idx < lines.length - 1) await new Promise(r => setTimeout(r, 200));
    }
    setBatchResults(results);
    setBatchLoading(false);
  }, [batchInput, allBuildings]);

  const handleBatchExport = useCallback(() => {
    if (!batchResults.length) return;
    const header = 'address,status,surge_depth_ft,found_ht,interior_flood_ft,structure_dmg_pct,contents_dmg_pct,total_dmg_pct,estimated_loss_usd,damage_category,deductible_flag';
    const rows = batchResults.map(r => {
      if (r.status !== 'matched') return `${csvField(r.address)},${r.status},,,,,,,,,`;
      const depthFt = r.depth_ft ?? 0;
      const interior = r.found_ht != null ? Math.max(0, depthFt - r.found_ht) : '';
      const lossVal = r.estimated_loss_usd ?? 0;
      const dedFlag = lossVal < 1250 ? 'below_min' : lossVal < 10000 ? 'below_typical' : 'above';
      return `${csvField(r.address)},${r.status},${depthFt},${r.found_ht ?? ''},${interior},${r.structure_damage_pct ?? ''},${r.contents_damage_pct ?? ''},${r.total_damage_pct ?? ''},${r.estimated_loss_usd ?? ''},${csvField(r.damage_category ?? '')},${dedFlag}`;
    });
    const csv = [header, ...rows].join('\n');
    const blob = new Blob([csv], { type: 'text/csv' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url; a.download = `surgedps_batch_${activeStorm?.storm_id || 'results'}.csv`; a.click();
    URL.revokeObjectURL(url);
  }, [batchResults, activeStorm]);

  // ── PDA (Preliminary Damage Assessment) export ──
  const handleExportPDA = useCallback(() => {
    if (!allBuildings?.features?.length || !activeStorm) {
      setCellError('Load a storm with damage data before exporting a PDA Report.');
      return;
    }

    // FEMA PDA categories
    const PDA_MAP: Record<string, string> = { minor: 'Affected', moderate: 'Minor Damage', major: 'Major Damage', severe: 'Destroyed' };

    // Critical facility occupancy codes (GOV, EDU, MED, REL → tagged in NSI occtype)
    const CRITICAL_OCCTYPES = new Set(['GOV1','GOV2','EDU1','EDU2','MED1','MED2','COM8','COM9','COM10']);
    const isCritical = (p: any) => p.occtype && CRITICAL_OCCTYPES.has((p.occtype || '').toUpperCase().split('-')[0] + (p.occtype || '').replace(/[^0-9]/g,'').slice(0,1));

    // Average household size for displacement estimate (FEMA default)
    const AVG_HOUSEHOLD = 2.53;

    type JurisData = { affected: number; minor: number; major: number; destroyed: number; totalLoss: number; displaced: number; critical: number };
    const counts: Record<string, JurisData> = {};
    const overall = { affected: 0, minor: 0, major: 0, destroyed: 0, totalLoss: 0, totalBuildings: 0, displaced: 0, critical: 0 };
    const criticalList: { type: string; occtype: string; loss: number; lat: number; lon: number }[] = [];

    for (const f of allBuildings.features) {
      const p = f.properties || {};
      const cat = p.damage_category || 'none';
      if (cat === 'none') continue;
      if (!PDA_MAP[cat]) continue;

      // Jurisdiction: prefer reverse-geocoded county from population context,
      // fall back to loaded-cell grid coordinates as a zone label
      const [lon, lat] = f.geometry?.coordinates || [0, 0];
      // Round to 1° to create coarse zone buckets when county isn't available
      const jurisdiction = activeStorm.population?.county_name
        ? activeStorm.population.county_name
        : `Zone ${Math.round(lat)}N-${Math.round(Math.abs(lon))}W`;

      if (!counts[jurisdiction]) counts[jurisdiction] = { affected:0, minor:0, major:0, destroyed:0, totalLoss:0, displaced:0, critical:0 };

      if (cat === 'minor')    { counts[jurisdiction].affected++;  overall.affected++; }
      else if (cat === 'moderate') { counts[jurisdiction].minor++;     overall.minor++; }
      else if (cat === 'major')    { counts[jurisdiction].major++;     overall.major++; }
      else if (cat === 'severe')   { counts[jurisdiction].destroyed++; overall.destroyed++; }

      counts[jurisdiction].totalLoss += p.estimated_loss_usd || 0;
      overall.totalLoss += p.estimated_loss_usd || 0;
      overall.totalBuildings++;

      // Displaced persons: Major + Destroyed residential buildings × avg household
      const isRes = (p.building_type || '').startsWith('RES');
      if (isRes && (cat === 'major' || cat === 'severe')) {
        const disp = Math.round(AVG_HOUSEHOLD);
        counts[jurisdiction].displaced += disp;
        overall.displaced += disp;
      }

      // Critical facilities
      if (isCritical(p) && (cat === 'major' || cat === 'severe')) {
        counts[jurisdiction].critical++;
        overall.critical++;
        criticalList.push({ type: p.building_type || '', occtype: p.occtype || '', loss: p.estimated_loss_usd || 0, lat, lon });
      }
    }

    const nsiCount  = allBuildings.features.filter((f: any) => f.properties?.source === 'NSI').length;
    const cellCount = loadedCells.size;
    const totalDamaged = overall.affected + overall.minor + overall.major + overall.destroyed;

    const lines = [
      `PRELIMINARY DAMAGE ASSESSMENT SUMMARY`,
      `Storm: ${activeStorm.name} (${activeStorm.year}) — Category ${activeStorm.category}`,
      `Generated: ${new Date().toISOString()}`,
      `Source: SurgeDPS (stormdps.com/surgedps) — MODELED ESTIMATE NOT FIELD VERIFIED`,
      ``,
      `STORM PARAMETERS`,
      `Max Wind,${Math.round(activeStorm.max_wind_kt * 1.15078)} mph`,
      `Min Pressure,${activeStorm.min_pressure_mb} mb`,
      `Landfall,${activeStorm.landfall_lat?.toFixed(4) ?? '?'}°N  ${activeStorm.landfall_lon?.toFixed(4) ?? '?'}°W`,
      ``,
      `DATA COVERAGE`,
      `Cells Loaded,${cellCount} (export covers loaded cells only — full storm footprint may be larger)`,
      `Buildings in Export,${allBuildings.features.length.toLocaleString()}`,
      `NSI-sourced Buildings,${nsiCount.toLocaleString()} (${allBuildings.features.length ? Math.round(nsiCount/allBuildings.features.length*100) : 0}% — higher = more reliable valuations)`,
      ``,
      `DAMAGE SUMMARY`,
      `Total Structures Damaged,${totalDamaged.toLocaleString()}`,
      `  Affected (cosmetic),${overall.affected.toLocaleString()}`,
      `  Minor Damage (repairable),${overall.minor.toLocaleString()}`,
      `  Major Damage (uninhabitable),${overall.major.toLocaleString()}`,
      `  Destroyed (total loss),${overall.destroyed.toLocaleString()}`,
      `Total Modeled Loss,$${(overall.totalLoss / 1e6).toFixed(1)}M`,
      `Est. Displaced Persons,${overall.displaced.toLocaleString()} (residential Major+Destroyed × ${AVG_HOUSEHOLD} avg household)`,
      `Critical Facilities (Major+Destroyed),${overall.critical}`,
      ``,
      `JURISDICTION BREAKDOWN (FEMA PDA FORMAT)`,
      `Jurisdiction,Affected,Minor Damage,Major Damage,Destroyed,Total Damaged,Estimated Loss,Est. Displaced,Critical Facilities`,
    ];

    for (const [juris, c] of Object.entries(counts)) {
      const tot = c.affected + c.minor + c.major + c.destroyed;
      lines.push(`${juris},${c.affected},${c.minor},${c.major},${c.destroyed},${tot},$${Math.round(c.totalLoss).toLocaleString()},${c.displaced},${c.critical}`);
    }
    lines.push(`TOTAL,${overall.affected},${overall.minor},${overall.major},${overall.destroyed},${totalDamaged},$${Math.round(overall.totalLoss).toLocaleString()},${overall.displaced},${overall.critical}`);

    if (criticalList.length > 0) {
      lines.push('');
      lines.push('CRITICAL FACILITIES — MAJOR/DESTROYED');
      lines.push('occupancy_type,hazus_type,estimated_loss,lat,lon');
      for (const cf of criticalList.slice(0, 50)) {
        lines.push(`${cf.occtype},${cf.type},$${Math.round(cf.loss).toLocaleString()},${cf.lat.toFixed(5)},${cf.lon.toFixed(5)}`);
      }
    }

    lines.push('');
    lines.push('CATEGORY DEFINITIONS');
    lines.push('"Affected","Cosmetic/minor damage (<10% structure loss) — HAZUS minor"');
    lines.push('"Minor Damage","Repairable structural damage (10–30%) — habitable with repairs"');
    lines.push('"Major Damage","Significant structural damage (30–50%) — likely uninhabitable"');
    lines.push('"Destroyed","Total or near-total loss (>50%) — demolition likely"');
    lines.push('');
    lines.push('METHODOLOGY NOTES');
    lines.push('"Surge depths from parametric SLOSH-based model. Losses use FEMA HAZUS depth-damage curves."');
    lines.push('"Building valuations from FEMA NSI where available; OSM footprints used as fallback."');
    lines.push('"Inundation mask applied: only buildings where surge exceeds foundation height are assessed."');
    lines.push('"Displaced persons estimate uses FEMA default of 2.53 persons/household for residential structures."');

    const csv = lines.join('\n');
    const blob = new Blob([csv], { type: 'text/csv' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `surgedps_PDA_${activeStorm.storm_id}.csv`;
    a.click();
    URL.revokeObjectURL(url);
  }, [allBuildings, activeStorm, loadedCells]);

  // ── Print / Share ──
  const handlePrint = useCallback(() => { window.print(); }, []);
  const handleShareLink = useCallback(() => {
    if (!activeStorm || !mapRef.current) return;
    const c = mapRef.current.getCenter();
    const z = mapRef.current.getZoom().toFixed(1);
    const url = `${window.location.origin}/surgedps?storm=${activeStorm.storm_id}&lat=${c.lat.toFixed(5)}&lng=${c.lng.toFixed(5)}&z=${z}`;
    navigator.clipboard.writeText(url).then(() => setToastSuccess('Link copied to clipboard')).catch(() => setCellError('Could not copy link — try again'));
  }, [activeStorm]);

  // ── Methodology panel ──
  const [methodologyOpen, setMethodologyOpen] = useState(false);

  // ── CSV export of visible buildings ──
  const handleExportCSV = useCallback(() => {
    if (!allBuildings?.features?.length) { setCellError('No building data loaded — select a storm and load cells first.'); return; }
    const totalLoss = allBuildings.features.reduce((s: number, f: any) => s + (f.properties?.estimated_loss_usd || 0), 0);
    const nsiCount  = allBuildings.features.filter((f: any) => f.properties?.source === 'NSI').length;
    const osmCount  = allBuildings.features.filter((f: any) => f.properties?.source === 'OSM').length;
    const cellsLoaded = loadedCells.size;
    const summaryLines = [
      `# SurgeDPS Building Export — ${activeStorm?.name || 'Unknown'} (${activeStorm?.year || ''})`,
      `# Category ${activeStorm?.category || '?'} | Max wind ${activeStorm ? Math.round(activeStorm.max_wind_kt * 1.15078) : '?'} mph | ${activeStorm?.min_pressure_mb || '?'} mb`,
      `# Landfall: ${activeStorm?.landfall_lat?.toFixed(4) ?? '?'}°N ${activeStorm?.landfall_lon?.toFixed(4) ?? '?'}°W`,
      `# Buildings exported: ${allBuildings.features.length.toLocaleString()} (NSI: ${nsiCount.toLocaleString()}, OSM: ${osmCount.toLocaleString()})`,
      `# Cells loaded: ${cellsLoaded} of full storm footprint — export covers loaded cells only`,
      `# Total modeled loss: $${(totalLoss / 1e6).toFixed(1)}M`,
      `# Exported: ${new Date().toISOString()} | Source: SurgeDPS (stormdps.com/surgedps)`,
      `# MODELED ESTIMATE — NOT FIELD VERIFIED. Losses use FEMA HAZUS depth-damage curves applied to NSI/OSM inventory.`,
      `# data_quality: 0.0–1.0 reliability score (1.0 = full NSI record with all attributes; 0.1 = ML footprint only)`,
      `# source: NSI = FEMA National Structure Inventory | OSM = OpenStreetMap | MSFT = Microsoft ML footprints`,
    ];
    const rows = allBuildings.features.map((f: any) => {
      const p = f.properties || {};
      const [lon, lat] = f.geometry?.coordinates || [0, 0];
      const flagKey = `${lon.toFixed(5)},${lat.toFixed(5)}`;
      const flag = buildingFlags[flagKey] || '';
      // Interior flooding = surge depth above first finished floor
      const depthFt  = p.depth_ft  != null ? Number(p.depth_ft)  : null;
      const foundHt  = p.found_ht  != null ? Number(p.found_ht)  : null;
      const interiorFt = (depthFt != null && foundHt != null) ? Math.max(0, depthFt - foundHt).toFixed(2) : '';
      return [
        lat, lon,
        csvField(p.building_id || p.id || ''),   // NSI fd_id for cross-reference
        csvField(p.source || ''),                  // NSI | OSM | MSFT
        p.data_quality ?? '',                      // 0.0–1.0 reliability
        csvField(p.building_type || ''),
        csvField(p.occtype || ''),                 // raw NSI occupancy code
        p.depth_ft ?? '',
        p.found_ht ?? '',
        interiorFt,                                // interior flooding above floor
        p.structure_damage_pct ?? '',
        p.contents_damage_pct ?? '',
        p.total_damage_pct ?? '',
        p.estimated_loss_usd ?? '',
        p.loss_low_usd ?? '',
        p.loss_high_usd ?? '',
        p.val_struct ?? '',
        p.val_cont ?? '',
        p.replacement_value_usd ?? '',
        p.med_yr_blt ?? '',                        // year built
        p.num_story ?? '',                         // stories
        csvField(p.damage_category || ''),
        csvField(flag),
        p.ihp_eligible != null ? (p.ihp_eligible ? 'Y' : 'N') : '',
        csvField(p.ihp_category || ''),
        p.ihp_est_amount ?? '',
        p.wind_speed_mph ?? '',
        p.wind_damage_pct ?? '',
        p.wind_loss_usd ?? '',
        p.combined_loss_usd ?? '',
      ].join(',');
    });
    const header = 'lat,lon,building_id,source,data_quality,building_type,occupancy_type,surge_depth_ft,foundation_ht_ft,interior_flood_ft,structure_dmg_pct,contents_dmg_pct,total_dmg_pct,estimated_loss_usd,loss_low_usd,loss_high_usd,val_struct,val_cont,replacement_value_usd,year_built,num_stories,damage_category,field_flag,ihp_eligible,ihp_category,ihp_est_amount,wind_speed_mph,wind_damage_pct,wind_loss_usd,combined_loss_usd';
    const csv = [...summaryLines, header, ...rows].join('\n');
    const blob = new Blob([csv], { type: 'text/csv' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `surgedps_${activeStorm?.storm_id || 'export'}_buildings.csv`;
    a.click();
    URL.revokeObjectURL(url);
    setToastSuccess(`Exported ${allBuildings.features.length.toLocaleString()} buildings to CSV`);
  }, [allBuildings, activeStorm, buildingFlags, loadedCells]);

  // ── Per-building Claims Documentation Generator ──
  const generateClaimDoc = useCallback((feature: any, address?: string | null) => {
    if (!feature || !activeStorm) return;
    const p = feature.properties || {};
    const [lon, lat] = feature.geometry?.coordinates || [0, 0];
    const depthFt = p.depth_ft != null ? Number(p.depth_ft) : null;
    const foundHt = p.found_ht != null ? Number(p.found_ht) : null;
    const interiorFt = (depthFt != null && foundHt != null) ? Math.max(0, depthFt - foundHt) : null;
    const structPct = p.structure_damage_pct ?? 0;
    const contPct = p.contents_damage_pct ?? 0;
    const structLoss = p.val_struct != null ? Math.round(p.val_struct * structPct / 100) : null;
    const contLoss = p.val_cont != null ? Math.round(p.val_cont * contPct / 100) : null;

    // Wind vs water attribution
    let windMph: number | null = null;
    let wwSplit: { windPct: number; waterPct: number } | null = null;
    if (activeStorm.landfall_lat && activeStorm.landfall_lon) {
      const distKm = haversineKm(lat, lon, activeStorm.landfall_lat, activeStorm.landfall_lon);
      windMph = Math.round(estimateWindMph(distKm, activeStorm.max_wind_kt, activeStorm.category));
      const floodForWind = interiorFt != null ? interiorFt : (depthFt != null ? Math.max(0, depthFt - 1) : 0);
      wwSplit = windWaterSplit(windMph, floodForWind);
    }

    // Comparable properties
    const comps = allBuildings?.features
      ? findComparables(allBuildings.features, p.building_type, lon, lat)
      : { count: 0, avgLoss: 0, minLoss: 0, maxLoss: 0 };

    const flagKey = `${lon.toFixed(5)},${lat.toFixed(5)}`;
    const fieldFlag = buildingFlags[flagKey] || '';

    const locationStr = address || `${lat.toFixed(5)}°N, ${Math.abs(lon).toFixed(5)}°W`;
    const severity = !p.damage_category || p.damage_category === 'none' ? 'No Damage' : p.damage_category;
    const severityLabel: Record<string, string> = { minor: 'Affected — Cosmetic damage', moderate: 'Minor — Repairable structural damage', major: 'Major — Uninhabitable', severe: 'Destroyed — Total loss' };
    const qualityLabel = p.data_quality != null
      ? p.data_quality >= 0.8 ? 'High (full NSI record)' : p.data_quality >= 0.5 ? 'Medium (partial NSI)' : 'Low (footprint only)'
      : 'Unknown';

    const now = new Date();
    const html = `<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8"><title>Claim Report — ${locationStr}</title>
<style>
  body { font-family: 'Segoe UI', system-ui, -apple-system, sans-serif; max-width: 800px; margin: 0 auto; padding: 24px; color: #1e293b; line-height: 1.5; }
  h1 { font-size: 20px; color: #0f172a; margin-bottom: 4px; border-bottom: 3px solid #4f46e5; padding-bottom: 8px; }
  h2 { font-size: 15px; color: #334155; margin-top: 24px; margin-bottom: 8px; border-bottom: 1px solid #e2e8f0; padding-bottom: 4px; text-transform: uppercase; letter-spacing: 0.05em; }
  .meta { font-size: 12px; color: #64748b; margin-bottom: 16px; }
  table { width: 100%; border-collapse: collapse; margin: 8px 0; font-size: 13px; }
  td { padding: 5px 10px; border-bottom: 1px solid #f1f5f9; }
  td:first-child { font-weight: 600; color: #475569; width: 45%; }
  .severity-badge { display: inline-block; padding: 3px 12px; border-radius: 4px; font-weight: 700; font-size: 13px; }
  .severity-minor { background: #dcfce7; color: #166534; }
  .severity-moderate { background: #fef9c3; color: #854d0e; }
  .severity-major { background: #fed7aa; color: #9a3412; }
  .severity-severe { background: #fecaca; color: #991b1b; }
  .severity-none { background: #f1f5f9; color: #475569; }
  .loss-total { font-size: 20px; font-weight: 800; color: #dc2626; }
  .peril-bar { display: flex; height: 18px; border-radius: 9px; overflow: hidden; margin: 4px 0; }
  .peril-wind { background: #0ea5e9; }
  .peril-water { background: #4f46e5; }
  .comps-box { background: #eff6ff; border: 1px solid #bfdbfe; border-radius: 6px; padding: 10px 14px; margin: 8px 0; font-size: 12px; color: #1e40af; }
  .disclaimer { background: #fefce8; border: 1px solid #fde68a; border-radius: 6px; padding: 10px 14px; margin-top: 24px; font-size: 11px; color: #92400e; }
  .footer { margin-top: 24px; padding-top: 12px; border-top: 1px solid #e2e8f0; font-size: 11px; color: #94a3b8; text-align: center; }
  @media print { body { padding: 0; } .no-print { display: none; } }
</style></head><body>
<h1>Claims Documentation Report</h1>
<p class="meta">
  <strong>${activeStorm.name} (${activeStorm.year})</strong> — Category ${activeStorm.category} |
  Generated ${now.toLocaleDateString('en-US', { year:'numeric', month:'long', day:'numeric' })} at ${now.toLocaleTimeString('en-US', { hour:'2-digit', minute:'2-digit' })} |
  Claim Reference: <strong>SDC-${activeStorm.storm_id}-${(p.building_id || '').slice(-6).toUpperCase() || lat.toFixed(3).replace('.','') + lon.toFixed(3).replace('.','').replace('-','')}</strong>
</p>

<h2>Property Identification</h2>
<table>
  <tr><td>Location</td><td>${locationStr}</td></tr>
  <tr><td>Coordinates</td><td>${lat.toFixed(6)}°N, ${lon.toFixed(6)}°W</td></tr>
  <tr><td>Building Type</td><td>${friendlyBuildingType(p.building_type)} (${p.building_type || 'Unknown'})</td></tr>
  ${p.occtype ? `<tr><td>Occupancy Code</td><td>${p.occtype}</td></tr>` : ''}
  ${p.num_story ? `<tr><td>Stories</td><td>${p.num_story}</td></tr>` : ''}
  ${p.med_yr_blt ? `<tr><td>Year Built</td><td>${p.med_yr_blt}</td></tr>` : ''}
  ${p.area_sqft ? `<tr><td>Footprint</td><td>${Number(p.area_sqft).toLocaleString()} sq ft</td></tr>` : ''}
  <tr><td>Foundation Height</td><td>${foundHt != null ? foundHt.toFixed(1) + ' ft above grade' : 'Not available'}</td></tr>
  <tr><td>Building ID</td><td>${p.building_id || 'N/A'} (${p.source || 'Unknown'} inventory)</td></tr>
  <tr><td>Data Reliability</td><td>${qualityLabel}${p.data_quality != null ? ` (${p.data_quality.toFixed(2)})` : ''}</td></tr>
  <tr><td>Google Maps</td><td><a href="https://www.google.com/maps/@${lat},${lon},19z/data=!3m1!1e1" target="_blank">Satellite View ↗</a> · <a href="https://maps.google.com/maps?q=&layer=c&cbll=${lat},${lon}" target="_blank">Street View ↗</a></td></tr>
</table>

<h2>Damage Assessment</h2>
<table>
  <tr><td>Damage Severity</td><td><span class="severity-badge severity-${p.damage_category || 'none'}">${severity.charAt(0).toUpperCase() + severity.slice(1)}</span>${severityLabel[p.damage_category] ? ' — ' + severityLabel[p.damage_category].split('—')[1] : ''}</td></tr>
  <tr><td>Storm Surge Depth</td><td>${depthFt != null ? depthFt.toFixed(1) + ' ft' : 'N/A'}</td></tr>
  <tr><td>Interior Flooding</td><td style="color: ${interiorFt && interiorFt > 0 ? '#dc2626' : '#16a34a'}; font-weight: 700">${interiorFt != null ? (interiorFt > 0 ? interiorFt.toFixed(1) + ' ft above first floor' : 'None — surge below foundation') : 'N/A'}</td></tr>
</table>

<h2>Loss Estimate</h2>
<table>
  <tr><td>Structure Damage</td><td>${structPct}% of replacement value${structLoss != null ? ' — $' + structLoss.toLocaleString() : ''}</td></tr>
  <tr><td>Contents Damage</td><td>${contPct}% of replacement value${contLoss != null ? ' — $' + contLoss.toLocaleString() : ''}</td></tr>
  <tr><td>Structure Replacement Value</td><td>${p.val_struct != null ? '$' + Number(p.val_struct).toLocaleString() : 'Not available'}</td></tr>
  <tr><td>Contents Value</td><td>${p.val_cont != null ? '$' + Number(p.val_cont).toLocaleString() : 'Not available'}</td></tr>
  <tr><td colspan="2" style="text-align: center; padding-top: 12px">
    <span class="loss-total">Total Modeled Loss: $${(p.estimated_loss_usd ?? 0).toLocaleString()}</span>
    ${p.loss_low_usd != null && p.loss_high_usd != null ? `<br><span style="font-size:11px;color:#64748b">Loss range (±30% depth uncertainty): $${p.loss_low_usd.toLocaleString()} – $${p.loss_high_usd.toLocaleString()}</span>` : ''}
  </td></tr>
</table>

${p.ihp_eligible ? `
<h2>FEMA IHP Eligibility Estimate</h2>
<table>
  <tr><td>Eligibility Status</td><td style="color:#059669;font-weight:700">Likely Eligible</td></tr>
  <tr><td>Damage Category</td><td>${(p.ihp_category || '').charAt(0).toUpperCase() + (p.ihp_category || '').slice(1)}</td></tr>
  <tr><td>Estimated IHP Award</td><td style="font-weight:700">$${(p.ihp_est_amount ?? 0).toLocaleString()}</td></tr>
  <tr><td>FY2025 Maximum</td><td>$42,500</td></tr>
</table>
<p style="font-size:10px;color:#94a3b8;margin-top:4px">IHP assists owner-occupied primary residences. Actual award depends on FEMA inspection, insurance coverage, and other factors. This is a modeled estimate only.</p>
` : ''}

${wwSplit && windMph != null ? `
<h2>Peril Attribution (Wind vs. Water)</h2>
<table>
  <tr><td>Estimated Wind Speed at Property</td><td>${windMph} mph (modeled from ${activeStorm.max_wind_kt ? Math.round(activeStorm.max_wind_kt * 1.15078) : '?'} mph max sustained)</td></tr>
  <tr><td>Wind Damage Attribution</td><td>${wwSplit.windPct}%${p.estimated_loss_usd ? ' — $' + Math.round(p.estimated_loss_usd * wwSplit.windPct / 100).toLocaleString() : ''}</td></tr>
  <tr><td>Water Damage Attribution</td><td>${wwSplit.waterPct}%${p.estimated_loss_usd ? ' — $' + Math.round(p.estimated_loss_usd * wwSplit.waterPct / 100).toLocaleString() : ''}</td></tr>
</table>
<div class="peril-bar">
  <div class="peril-wind" style="width:${wwSplit.windPct}%"></div>
  <div class="peril-water" style="width:${wwSplit.waterPct}%"></div>
</div>
<div style="display:flex;justify-content:space-between;font-size:11px;color:#64748b">
  <span style="color:#0ea5e9;font-weight:700">Wind ${wwSplit.windPct}%</span>
  <span style="color:#4f46e5;font-weight:700">Water ${wwSplit.waterPct}%</span>
</div>
` : ''}

${comps.count >= 2 ? `
<h2>Comparable Properties</h2>
<div class="comps-box">
  <strong>${comps.count}</strong> similar ${friendlyBuildingType(p.building_type).toLowerCase()}s within ${(COMP_RADIUS_KM / 1.609).toFixed(2)} miles
  averaged <strong>$${comps.avgLoss.toLocaleString()}</strong> in modeled losses (range: $${comps.minLoss.toLocaleString()} – $${comps.maxLoss.toLocaleString()}).
  ${p.estimated_loss_usd != null && comps.avgLoss > 0 ? `This property's loss is ${Math.round((p.estimated_loss_usd / comps.avgLoss) * 100)}% of the area average.` : ''}
</div>
` : ''}

${fieldFlag ? `
<h2>Field Assessment</h2>
<table>
  <tr><td>Field Flag</td><td><strong>${fieldFlag.replace(/_/g, ' ').replace(/\b\w/g, (l: string) => l.toUpperCase())}</strong> (set by adjuster in SurgeDPS)</td></tr>
</table>
` : ''}

<h2>Storm Parameters</h2>
<table>
  <tr><td>Storm</td><td>${activeStorm.name} (${activeStorm.year}) — Category ${activeStorm.category}</td></tr>
  <tr><td>Maximum Sustained Winds</td><td>${Math.round(activeStorm.max_wind_kt * 1.15078)} mph (${activeStorm.max_wind_kt} kt)</td></tr>
  <tr><td>Minimum Central Pressure</td><td>${activeStorm.min_pressure_mb} mb</td></tr>
  <tr><td>Landfall Location</td><td>${activeStorm.landfall_lat?.toFixed(4) ?? '?'}°N, ${activeStorm.landfall_lon?.toFixed(4) ?? '?'}°W</td></tr>
</table>

<div class="disclaimer">
  <strong>MODELED ESTIMATE — NOT FIELD VERIFIED.</strong> Losses computed using FEMA HAZUS depth-damage curves applied to ${p.source || 'NSI/OSM'} building inventory.
  Inundation mask applied: only buildings where surge depth exceeds foundation height are assessed. Actual losses may differ based on construction quality,
  flood-proofing measures, contents specifics, and other factors not captured in the model. This report is intended to support — not replace — field inspection.
</div>

<div class="footer">
  Generated by SurgeDPS (stormdps.com/surgedps) — ${now.toISOString()}<br>
  Claim Ref: SDC-${activeStorm.storm_id}-${(p.building_id || '').slice(-6).toUpperCase() || lat.toFixed(3).replace('.','') + lon.toFixed(3).replace('.','').replace('-','')}
</div>
</body></html>`;

    const blob = new Blob([html], { type: 'text/html' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `claim_${activeStorm.storm_id}_${(p.building_id || 'bldg').slice(-8)}.html`;
    a.click();
    URL.revokeObjectURL(url);
    setToastSuccess('Claims report downloaded');
  }, [activeStorm, allBuildings, buildingFlags]);

  // ── Batch Claims Package Export ──
  const handleExportClaimsPackage = useCallback(() => {
    if (!allBuildings?.features?.length || !activeStorm) {
      setCellError('Load a storm with damage data before exporting a Claims Package.');
      return;
    }
    const damaged = allBuildings.features.filter((f: any) => {
      const cat = f.properties?.damage_category;
      return cat && cat !== 'none';
    });
    if (!damaged.length) { setCellError('No damaged buildings to include in claims package.'); return; }

    const now = new Date();
    const totalLoss = damaged.reduce((s: number, f: any) => s + (f.properties?.estimated_loss_usd || 0), 0);

    // Build per-building claim rows
    const rows = damaged.map((f: any, i: number) => {
      const p = f.properties || {};
      const [lon, lat] = f.geometry?.coordinates || [0, 0];
      const depthFt = p.depth_ft != null ? Number(p.depth_ft) : null;
      const foundHt = p.found_ht != null ? Number(p.found_ht) : null;
      const interiorFt = (depthFt != null && foundHt != null) ? Math.max(0, depthFt - foundHt) : null;
      const structLoss = p.val_struct != null && p.structure_damage_pct != null ? Math.round(p.val_struct * p.structure_damage_pct / 100) : null;
      const contLoss = p.val_cont != null && p.contents_damage_pct != null ? Math.round(p.val_cont * p.contents_damage_pct / 100) : null;
      const flagKey = `${lon.toFixed(5)},${lat.toFixed(5)}`;
      const flag = buildingFlags[flagKey] || '';

      // Wind vs water
      let windPct = '', waterPct = '';
      if (activeStorm.landfall_lat && activeStorm.landfall_lon) {
        const distKm = haversineKm(lat, lon, activeStorm.landfall_lat, activeStorm.landfall_lon);
        const windMph = estimateWindMph(distKm, activeStorm.max_wind_kt, activeStorm.category);
        const floodForWind = interiorFt != null ? interiorFt : (depthFt != null ? Math.max(0, depthFt - 1) : 0);
        const ww = windWaterSplit(windMph, floodForWind);
        windPct = String(ww.windPct);
        waterPct = String(ww.waterPct);
      }

      const sevMap: Record<string, string> = { minor: 'Affected', moderate: 'Minor Damage', major: 'Major Damage', severe: 'Destroyed' };
      return [
        i + 1,
        `SDC-${activeStorm.storm_id}-${(p.building_id || '').slice(-6).toUpperCase() || String(i+1).padStart(4,'0')}`,
        lat.toFixed(6), lon.toFixed(6),
        csvField(friendlyBuildingType(p.building_type)),
        csvField(p.occtype || ''),
        csvField(sevMap[p.damage_category] || p.damage_category || ''),
        depthFt != null ? depthFt.toFixed(1) : '',
        foundHt != null ? foundHt.toFixed(1) : '',
        interiorFt != null ? interiorFt.toFixed(1) : '',
        p.structure_damage_pct ?? '',
        p.contents_damage_pct ?? '',
        p.estimated_loss_usd ?? '',
        p.loss_low_usd ?? '',
        p.loss_high_usd ?? '',
        structLoss ?? '',
        contLoss ?? '',
        p.val_struct ?? '',
        p.val_cont ?? '',
        windPct,
        waterPct,
        p.source || '',
        p.data_quality ?? '',
        p.med_yr_blt ?? '',
        p.num_story ?? '',
        csvField(flag),
        p.ihp_eligible != null ? (p.ihp_eligible ? 'Y' : 'N') : '',
        csvField(p.ihp_category || ''),
        p.ihp_est_amount ?? '',
        p.wind_speed_mph ?? '',
        p.wind_damage_pct ?? '',
        p.wind_loss_usd ?? '',
        p.combined_loss_usd ?? '',
      ].join(',');
    });

    // Category counts and IHP totals
    const catCounts: Record<string, number> = { minor: 0, moderate: 0, major: 0, severe: 0 };
    let ihpTotal = 0;
    let ihpCount = 0;
    damaged.forEach((f: any) => {
      const c = f.properties?.damage_category;
      if (c && catCounts[c] !== undefined) catCounts[c]++;
      if (f.properties?.ihp_est_amount) { ihpTotal += f.properties.ihp_est_amount; ihpCount++; }
    });

    const totalLossLow = damaged.reduce((s: number, f: any) => s + (f.properties?.loss_low_usd || f.properties?.estimated_loss_usd || 0), 0);
    const totalLossHigh = damaged.reduce((s: number, f: any) => s + (f.properties?.loss_high_usd || f.properties?.estimated_loss_usd || 0), 0);

    const lines = [
      `# ═══════════════════════════════════════════════════════════════`,
      `# CLAIMS DOCUMENTATION PACKAGE`,
      `# ${activeStorm.name} (${activeStorm.year}) — Category ${activeStorm.category}`,
      `# ═══════════════════════════════════════════════════════════════`,
      `# Generated: ${now.toISOString()} | SurgeDPS (stormdps.com/surgedps)`,
      `# Max Wind: ${Math.round(activeStorm.max_wind_kt * 1.15078)} mph | Min Pressure: ${activeStorm.min_pressure_mb} mb`,
      `# Landfall: ${activeStorm.landfall_lat?.toFixed(4) ?? '?'}°N ${activeStorm.landfall_lon?.toFixed(4) ?? '?'}°W`,
      `#`,
      `# DAMAGE SUMMARY`,
      `# Damaged Buildings: ${damaged.length.toLocaleString()} of ${allBuildings.features.length.toLocaleString()} assessed`,
      `#   Affected (cosmetic): ${catCounts.minor.toLocaleString()}`,
      `#   Minor Damage (repairable): ${catCounts.moderate.toLocaleString()}`,
      `#   Major Damage (uninhabitable): ${catCounts.major.toLocaleString()}`,
      `#   Destroyed (total loss): ${catCounts.severe.toLocaleString()}`,
      `# Total Modeled Loss: $${(totalLoss / 1e6).toFixed(1)}M (range: $${(totalLossLow / 1e6).toFixed(1)}M – $${(totalLossHigh / 1e6).toFixed(1)}M)`,
      `# Est. FEMA IHP Assistance: $${(ihpTotal / 1e6).toFixed(1)}M across ${ihpCount.toLocaleString()} eligible residences`,
      `#`,
      `# COLUMN GUIDE`,
      `# claim_ref: Unique identifier for cross-referencing (SDC = SurgeDPS Claim)`,
      `# interior_flood_ft: Surge depth above first finished floor (the real damage driver)`,
      `# loss_low/high_usd: Loss range from ±30% depth uncertainty bracketing`,
      `# wind_pct / water_pct: Modeled peril attribution for coverage determination`,
      `# wind_speed_mph: StormDPS modeled wind speed at building location`,
      `# wind_damage_pct / wind_loss_usd: Wind-only damage estimate from StormDPS vulnerability curves`,
      `# combined_loss_usd: Surge + 30% × wind (avoids double-counting co-located perils)`,
      `# data_quality: 0.0–1.0 reliability of source building data (higher = more trustworthy)`,
      `# ihp_eligible: FEMA Individual & Households Program eligibility estimate (Y/N)`,
      `# ihp_est_amount: Estimated IHP payout (capped at FY2025 maximum $42,500)`,
      `# field_flag: Adjuster annotation set in SurgeDPS (blank = not yet inspected)`,
      `#`,
      `# MODELED ESTIMATE — NOT FIELD VERIFIED. Use as initial triage; confirm with field inspection.`,
      `# Cells loaded: ${loadedCells.size} — export covers loaded cells only.`,
      `#`,
      `seq,claim_ref,lat,lon,building_type,occupancy_type,damage_severity,surge_depth_ft,foundation_ht_ft,interior_flood_ft,structure_dmg_pct,contents_dmg_pct,total_loss_usd,loss_low_usd,loss_high_usd,structure_loss_usd,contents_loss_usd,val_struct,val_cont,wind_pct,water_pct,data_source,data_quality,year_built,num_stories,field_flag,ihp_eligible,ihp_category,ihp_est_amount,wind_speed_mph,wind_damage_pct,wind_loss_usd,combined_loss_usd`,
      ...rows,
    ];

    const csv = lines.join('\n');
    const blob = new Blob([csv], { type: 'text/csv' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `surgedps_claims_${activeStorm.storm_id}.csv`;
    a.click();
    URL.revokeObjectURL(url);
    setToastSuccess(`Claims package exported — ${damaged.length.toLocaleString()} damaged buildings`);
  }, [allBuildings, activeStorm, buildingFlags, loadedCells]);

  // Reverse geocoding for building hover
  const geocodeCache = useRef<Record<string, string>>({});
  const [hoverAddress, setHoverAddress] = useState<string | null>(null);

  // Activate a storm (use ref to avoid recreating callback on activating changes)
  const activatingRef = useRef(false);
  const activateStorm = useCallback(async (stormId: string) => {
    if (activatingRef.current) return;
    activatingRef.current = true;
    setActivating(true);
    setAllBuildings(null); setAllFlood(null);
    setLoadedCells(new Set()); setLoadingCells(new Set());
    setImpactTotals({ buildings: 0, loss: 0, totalDepth: 0 }); setHoverInfo(null);
    setPinnedInfo(null);
    setConfidence({ level: 'unvalidated', count: 0 });
    setEli({ value: 0, tier: 'unavailable' });
    setValidatedDps({ value: 0, adj: 0, reason: '' });
    setManifest({});
    setNuisanceDismissed(false);
    setGridHintDismissed(false);
    setBatchOpen(false); setBatchResults([]); setBatchInput('');
    setAddressQuery(''); setAddressError('');
    setMethodologyOpen(false); setMoreMenuOpen(false);
    setLoadProgress({ step: 'Connecting to server', step_num: 0, total_steps: 4, elapsed: 0 });

    // Start polling server for real progress updates
    if (progressIntervalRef.current) clearInterval(progressIntervalRef.current);
    progressIntervalRef.current = setInterval(async () => {
      try {
        const r = await fetch('/surgedps/api/progress');
        if (r.ok) {
          const p = await r.json();
          if (p.storm_id === stormId && p.step) setLoadProgress(p);
        }
      } catch { /* ignore polling errors */ }
    }, 2000);

    let timedOut = false;
    try {
      const ac = new AbortController();
      activateAbortRef.current = ac;
      const timeout = setTimeout(() => { timedOut = true; ac.abort(); }, 300_000); // 5 min timeout (loading 3×3 grid)
      const resp = await fetch(`/surgedps/api/storm/${stormId}/activate`, { signal: ac.signal });
      clearTimeout(timeout);
      if (!resp.ok) throw new Error(`${resp.status}`);
      const data = await resp.json();
      const { storm, center_cell } = data;
      setActiveStorm(storm);
      if (storm.confidence) setConfidence({ level: storm.confidence, count: storm.building_count || 0 });
      if (storm.eli) setEli({ value: storm.eli, tier: storm.eli_tier || 'unavailable' });
      if (storm.validated_dps) setValidatedDps({ value: storm.validated_dps, adj: storm.dps_adjustment || 0, reason: storm.dps_adj_reason || '' });

      // Fetch pre-computed cell manifest (non-blocking — shades grid cells as "ready")
      const manifestStormId = stormId;
      fetch(`/surgedps/api/manifest?storm_id=${stormId}`)
        .then(r => r.ok ? r.json() : {})
        .then((m: any) => { if (activeStormRef.current?.storm_id === manifestStormId) setManifest(m?.cells || {}); })
        .catch(() => { if (activeStormRef.current?.storm_id === manifestStormId) setManifest({}); });

      // Load all grid cells returned by the server (3×3 when pre-cached)
      const gridCells = data.grid_cells || (center_cell ? { '0,0': center_cell } : {});
      const loadedKeys = new Set<string>();
      let mergedBuildings: any[] = [];
      let mergedFlood: any[] = [];
      let totalBuildings = 0, totalLoss = 0, totalDepthSum = 0;

      for (const [key, cellData] of Object.entries(gridCells) as [string, any][]) {
        loadedKeys.add(key);
        if (cellData.buildings?.features) mergedBuildings = mergedBuildings.concat(cellData.buildings.features);
        if (cellData.flood?.features) mergedFlood = mergedFlood.concat(cellData.flood.features);
        const feats = cellData.buildings?.features || [];
        totalBuildings += feats.length;
        totalLoss += feats.reduce((s: number, f: any) => s + (f.properties.estimated_loss_usd || 0), 0);
        totalDepthSum += feats.reduce((s: number, f: any) => s + (f.properties.depth_ft || 0), 0);
      }

      setAllBuildings({ type: 'FeatureCollection', features: mergedBuildings });
      setAllFlood({ type: 'FeatureCollection', features: mergedFlood });
      setLoadedCells(loadedKeys);
      setImpactTotals({ buildings: totalBuildings, loss: totalLoss, totalDepth: totalDepthSum });

      mapRef.current?.flyTo({ center: [storm.landfall_lon, storm.landfall_lat], zoom: 10, pitch: 30, duration: 2500 });

      // Server may return partial data with a cell_error flag
      if (storm.cell_error) setCellError(storm.cell_error);
    } catch (err: any) {
      if (err?.name === 'AbortError' && !timedOut) {
        console.log('Storm activation cancelled by user');
      } else if (err?.name === 'AbortError' && timedOut) {
        console.warn('Storm activation timed out after 2 minutes');
        setRetryStormId(stormId);
        setCellError('Storm data is still being generated on the server. Please wait a moment and try again — the data will be cached for next time.');
      } else {
        console.error('Failed to activate storm:', err);
        setRetryStormId(stormId);
        setCellError('Failed to load storm data. The server may be warming up — try again in a moment.');
      }
    } finally {
      if (progressIntervalRef.current) { clearInterval(progressIntervalRef.current); progressIntervalRef.current = null; }
      setActivating(false);
      activatingRef.current = false;
    }
  }, []); // stable — no dependencies

  // ── Fetch forecast track for active storms ──
  useEffect(() => {
    if (!activeStorm || activeStorm.status !== 'active') {
      setForecastCone(null);
      setForecastTrack([]);
      setSimMode(false);
      return;
    }
    (async () => {
      try {
        const r = await fetch('/surgedps/api/forecast/track');
        if (!r.ok) return;
        const tracks = await r.json();
        if (!tracks?.length) return;
        // Match by storm name
        const name = activeStorm.name.replace(/^Hurricane\s+/i, '').replace(/^Tropical Storm\s+/i, '').toUpperCase();
        const match = tracks.find((t: any) => t.storm_name?.toUpperCase() === name);
        if (match) {
          setForecastTrack(match.points || []);
          if (match.cone) setForecastCone(match.cone);
          if (match.predicted_landfall) {
            setSimMarker({ lng: match.predicted_landfall.lon, lat: match.predicted_landfall.lat });
          }
        }
      } catch { }
    })();
  }, [activeStorm]);

  // ── Run simulation at custom landfall point ──
  const runSimulation = useCallback(async () => {
    if (!simMarker || !activeStorm) return;
    setSimRunning(true);
    setSimResult(null);
    try {
      const r = await fetch(`/surgedps/api/simulate?lat=${simMarker.lat}&lon=${simMarker.lng}&wind=${activeStorm.max_wind_kt}&pressure=${activeStorm.min_pressure_mb}`);
      if (!r.ok) throw new Error('Simulation failed');
      const data = await r.json();
      setSimResult(data);
    } catch (err) {
      console.error('Simulation error:', err);
      setCellError('Simulation failed — try adjusting the landfall point.');
    } finally {
      setSimRunning(false);
    }
  }, [simMarker, activeStorm]);

  // ── Restore shared-link params on mount ──
  useEffect(() => {
    const params = new URLSearchParams(window.location.search);
    const stormId = params.get('storm');
    if (stormId) {
      activateStorm(stormId).then(() => {
        const lat = parseFloat(params.get('lat') || '');
        const lng = parseFloat(params.get('lng') || '');
        const z = parseFloat(params.get('z') || '');
        if (!isNaN(lat) && !isNaN(lng) && mapRef.current) {
          setTimeout(() => mapRef.current?.flyTo({ center: [lng, lat], zoom: isNaN(z) ? 12 : z, duration: 1500 }), 3000);
        }
        // Clean URL only after successful activation
        window.history.replaceState({}, '', window.location.pathname);
      });
    }
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  // Grid GeoJSON — includes "ready" status for pre-computed cells from manifest
  const gridGeoJson = useMemo(() => {
    if (!activeStorm) return { type: 'FeatureCollection' as const, features: [] };
    const oLon = activeStorm.grid_origin_lon, oLat = activeStorm.grid_origin_lat;
    const allKeys = new Set([...loadedCells, ...loadingCells]);
    if (allKeys.size === 0) allKeys.add(cellKey(0, 0));
    const parsed: [number, number][] = [];
    allKeys.forEach(k => { const [c, r] = k.split(',').map(Number); parsed.push([c, r]); });
    const features: any[] = [], seen = new Set<string>();
    for (const [c, r] of parsed) {
      const k = cellKey(c, r); seen.add(k);
      features.push(cellPolygon(c, r, loadedCells.has(k) ? 'loaded' : 'loading', oLon, oLat));
    }
    for (const [c, r] of parsed) {
      if (!loadedCells.has(cellKey(c, r))) continue;
      for (const [dc, dr] of [[-1, 0], [1, 0], [0, -1], [0, 1]]) {
        const nc = c + dc, nr = r + dr;
        const nk = cellKey(nc, nr);
        if (!seen.has(nk)) {
          seen.add(nk);
          // Pre-computed cells in manifest get "ready" status (solid border, instant load)
          const status = manifest[nk] ? 'ready' : 'available';
          features.push(cellPolygon(nc, nr, status, oLon, oLat));
        }
      }
    }
    return { type: 'FeatureCollection' as const, features };
  }, [activeStorm, loadedCells, loadingCells, manifest]);

  // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  // Emergency Management Metrics
  // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  // Estimated population
  const estimatedPop = useMemo(() => {
    if (!allBuildings?.features?.length) return 0;
    let pop = 0;
    for (const f of allBuildings.features) {
      const bt = (f.properties?.building_type || '').replace(/[-_].*$/, '').toUpperCase();
      pop += POP_PER_UNIT[bt] || 0;
    }
    return Math.round(pop);
  }, [allBuildings]);

  // Damage severity breakdown
  const severityCounts = useMemo(() => {
    const counts: Record<string, number> = { none: 0, minor: 0, moderate: 0, major: 0, severe: 0 };
    if (!allBuildings?.features?.length) return counts;
    for (const f of allBuildings.features) {
      const cat = f.properties?.damage_category || 'none';
      if (cat in counts) counts[cat]++;
    }
    return counts;
  }, [allBuildings]);

  // Total displaced — computed directly from allBuildings so the Ops panel
  // has a reconciled number before countiesGeoJSON lazy-loads and before
  // the rollupByCounty point-in-polygon pass finishes. Same formula as
  // rollupByCounty: residential major/severe × haircut × avg household.
  const totalDisplaced = useMemo(() => {
    if (!allBuildings?.features?.length) return 0;
    let resMajorSevere = 0;
    for (const f of allBuildings.features) {
      const p = f.properties || {};
      const cat = p.damage_category;
      const isRes = (p.building_type || '').startsWith('RES');
      if (isRes && (cat === 'major' || cat === 'severe')) resMajorSevere++;
    }
    return Math.round(resMajorSevere * DISPLACEMENT_HAIRCUT * AVG_HOUSEHOLD);
  }, [allBuildings]);

  // Per-county jurisdictional rollup — only computed when the Counties
  // overlay is on. EM uses this to allocate rescue teams/shelter beds
  // per jurisdiction since counties are independently managed.
  // Null when the overlay is off or there's no data yet.
  // Compute county rollup whenever counties data and buildings are both present.
  // This drives both the JurisdictionsPanel (when Counties overlay is on) and
  // the low-zoom county-aggregated bubble layer (always on, so the EM sees
  // one bubble per county at max zoom out instead of geographically arbitrary
  // supercluster blobs).
  const countyRollup = useMemo(() => {
    if (!countiesGeoJSON || !allBuildings) return null;
    return rollupByCounty(allBuildings, countiesGeoJSON);
  }, [countiesGeoJSON, allBuildings]);

  const countyAggregatePoints = useMemo(() => {
    if (!countyRollup || countyRollup.length === 0) return null;
    return rollupToCentroidGeoJSON(countyRollup);
  }, [countyRollup]);

  // GEOID → county name lookup — drives the "Unincorp. Harris" labels
  // inside the city rollup for buildings that fall outside any city boundary.
  const countyNameMap = useMemo<Record<string, string>>(() => {
    if (!countiesGeoJSON?.features?.length) return {};
    const map: Record<string, string> = {};
    for (const f of countiesGeoJSON.features) {
      const geoid: string = f.properties?.GEOID;
      const name: string  = f.properties?.NAME;
      if (geoid && name) map[geoid] = name;
    }
    return map;
  }, [countiesGeoJSON]);

  // City-level rollup — groups buildings into Census Places (pop ≥ 2500)
  // with an "Unincorporated" bucket per county for buildings outside any place.
  const cityRollup = useMemo(() => {
    if (!citiesData || !allBuildings) return null;
    return rollupByCity(allBuildings, citiesData, countyNameMap);
  }, [citiesData, allBuildings, countyNameMap]);

  const cityAggregatePoints = useMemo(() => {
    if (!cityRollup || cityRollup.length === 0) return null;
    return cityRollupToCentroidGeoJSON(cityRollup);
  }, [cityRollup]);

  // Critical facilities breakdown
  const criticalBreakdown = useMemo(() => {
    if (!allBuildings?.features?.length) return [];
    const counts: Record<string, number> = {};
    for (const f of allBuildings.features) {
      const bt = criticalPrefix(f.properties);
      if (bt in CRITICAL_ICONS) counts[bt] = (counts[bt] || 0) + 1;
    }
    return [
      { icon: '➕', label: 'Hospitals / Clinics', count: (counts.MED1 || 0) + (counts.MED2 || 0) + (counts.COM6 || 0) + (counts.COM7 || 0) },
      { icon: '🏫', label: 'Schools / Universities', count: (counts.EDU1 || 0) + (counts.EDU2 || 0) },
      { icon: '⭐', label: 'Government / Emergency', count: (counts.GOV1 || 0) + (counts.GOV2 || 0) },
      { icon: '⛪', label: 'Places of Worship', count: counts.REL1 || 0 },
      { icon: '🛏️', label: 'Nursing Homes', count: counts.RES6 || 0 },
    ];
  }, [allBuildings]);

  const criticalCount = criticalBreakdown.reduce((s: number, b: any) => s + b.count, 0);

  // Critical facilities GeoJSON
  const criticalFacilities = useMemo(() => {
    if (!allBuildings?.features?.length) return null;
    const critical = allBuildings.features
      .filter((f: any) => criticalPrefix(f.properties) in CRITICAL_ICONS)
      .map((f: any) => ({
        ...f,
        properties: {
          ...f.properties,
          critical_icon: CRITICAL_ICONS[criticalPrefix(f.properties)],
        },
      }));
    if (critical.length === 0) return null;
    return { type: 'FeatureCollection' as const, features: critical };
  }, [allBuildings]);

  // Hotspots ranking
  const hotspots = useMemo(() => {
    if (!allBuildings?.features?.length) return [];
    const BIN = 0.005; // ~0.5km grid

    // Bin aggregates include peril-mix accumulators and per-severity counts
    // so we can drive CAT_TEAM_PLAN §4b C1/C2 without a second pass.
    type Bin = {
      loss: number;
      count: number;
      lat: number;
      lon: number;
      // Weighted peril accumulators — the weight is building count so that
      // a cluster of 200 water-damaged homes outweighs a single wind-hit house.
      windSum: number;
      waterSum: number;
      windWeight: number;
      maxDepthFt: number;
      severity: { severe: number; major: number; moderate: number; minor: number; none: number };
    };
    const bins: Record<string, Bin> = {};

    const hasLandfall = activeStorm?.landfall_lat != null && activeStorm?.landfall_lon != null;

    for (const f of allBuildings.features) {
      const [lon, lat] = f.geometry?.coordinates || [0, 0];
      const p = f.properties || {};
      const bLon = Math.floor(lon / BIN) * BIN;
      const bLat = Math.floor(lat / BIN) * BIN;
      const key = `${bLon.toFixed(4)},${bLat.toFixed(4)}`;
      if (!bins[key]) {
        bins[key] = {
          loss: 0, count: 0, lat: bLat + BIN / 2, lon: bLon + BIN / 2,
          windSum: 0, waterSum: 0, windWeight: 0, maxDepthFt: 0,
          severity: { severe: 0, major: 0, moderate: 0, minor: 0, none: 0 },
        };
      }
      const b = bins[key];
      b.loss += p.estimated_loss_usd || 0;
      b.count += 1;

      // Severity tally
      const cat = (p.damage_category as string | undefined) || 'none';
      if (cat === 'severe' || cat === 'major' || cat === 'moderate' || cat === 'minor' || cat === 'none') {
        b.severity[cat] += 1;
      }

      // Depth / peril contribution — only when we have landfall + depth data
      const depthFt = p.depth_ft != null ? Number(p.depth_ft) : null;
      const foundHt = p.found_ht != null ? Number(p.found_ht) : null;
      if (depthFt != null && depthFt > b.maxDepthFt) b.maxDepthFt = depthFt;
      if (hasLandfall && activeStorm && depthFt != null) {
        const interiorFt = (foundHt != null) ? Math.max(0, depthFt - foundHt) : Math.max(0, depthFt - 1);
        const distKm = haversineKm(lat, lon, activeStorm.landfall_lat!, activeStorm.landfall_lon!);
        const windMph = estimateWindMph(distKm, activeStorm.max_wind_kt, activeStorm.category);
        const ww = windWaterSplit(windMph, interiorFt);
        b.windSum  += ww.windPct;
        b.waterSum += ww.waterPct;
        b.windWeight += 1;
      }
    }

    return Object.values(bins)
      .filter(b => b.count >= 5 && b.loss > 0)
      .sort((a, b) => b.loss - a.loss)
      .slice(0, 5)
      .map((b, i) => {
        const windPct  = b.windWeight > 0 ? Math.round(b.windSum  / b.windWeight) : 50;
        const waterPct = 100 - windPct;
        const recommend = recommendAdjusters(b.severity);
        const routing   = routingHint(windPct, waterPct);
        return {
          rank: i + 1,
          loss: b.loss,
          count: b.count,
          lat: b.lat,
          lon: b.lon,
          avgLoss: Math.round(b.loss / b.count),
          maxDepthFt: b.maxDepthFt,
          windPct,
          waterPct,
          severity: b.severity,
          recommend,
          routing,
        };
      });
  }, [allBuildings, activeStorm]);

  // Callback to fly to hotspot
  const handleFlyToHotspot = useCallback((lon: number, lat: number) => {
    mapRef.current?.flyTo({ center: [lon, lat], zoom: 16, duration: 2000 });
  }, []);

  // ── Shared helper: deliver a report HTML string as either a
  //    downloadable .html file or an auto-print window the user can
  //    "Save as PDF" from. (CAT_TEAM_PLAN §11 Q5 — user-selectable
  //    format.) We don't bundle a client-side HTML→PDF library
  //    because the browser's built-in print-to-PDF already works
  //    with the @media print styles in catReports.ts.
  // Returns true on success, false if the PDF pop-up was blocked
  // (so the caller knows not to overwrite the "pop-ups blocked"
  // error toast with its own success toast).
  const deliverReport = useCallback((html: string, baseName: string, format: 'html' | 'pdf'): boolean => {
    if (format === 'html') {
      const blob = new Blob([html], { type: 'text/html' });
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = `${baseName}.html`;
      a.click();
      URL.revokeObjectURL(url);
      return true;
    }
    // PDF path: open the report in a new tab and trigger the
    // browser print dialog. The user picks "Save as PDF" as the
    // destination. Works offline, no extra dependencies.
    const w = window.open('', '_blank');
    if (!w) {
      setCellError('Pop-ups blocked — allow pop-ups for this site to export as PDF.');
      return false;
    }
    w.document.open();
    w.document.write(html);
    w.document.close();
    // Delay the print() call until the new window has laid the
    // content out. Too-early calls print a blank page in Safari.
    const fireprint = () => { try { w.focus(); w.print(); } catch { /* ignore */ } };
    if (w.document.readyState === 'complete') setTimeout(fireprint, 350);
    else w.addEventListener('load', () => setTimeout(fireprint, 200));
    return true;
  }, []);

  // ── CAT Deployment Report export (CAT_TEAM_PLAN §4b C4, §11 Q5) ──
  const handleGenerateCatReport = useCallback((format: 'html' | 'pdf' = 'html') => {
    if (!activeStorm || !hotspots.length) {
      setCellError('Load a storm with damage data before generating a CAT Report.');
      return;
    }
    const html = buildCatDeploymentReport({
      storm: activeStorm as any,
      totals: impactTotals,
      severityCounts,
      hotspots,
      estimatedPop,
      confidence,
      teamSize,
      windowDays,
    });
    const baseName = `cat_report_${activeStorm.storm_id}_${new Date().toISOString().slice(0,10)}`;
    const ok = deliverReport(html, baseName, format);
    if (!ok) return; // deliverReport has already set an error toast
    setToastSuccess(format === 'pdf' ? 'CAT Report opened — save as PDF from the print dialog' : 'CAT Deployment Report downloaded');
  }, [activeStorm, hotspots, impactTotals, severityCounts, estimatedPop, confidence, teamSize, windowDays, deliverReport]);

  // ── Situation Report export (CAT_TEAM_PLAN §4a B8, §11 Q5) ──
  const handleGenerateSitRep = useCallback((format: 'html' | 'pdf' = 'html') => {
    if (!activeStorm || !hotspots.length) {
      setCellError('Load a storm with damage data before generating a SitRep.');
      return;
    }
    const html = buildSitRep({
      storm: activeStorm as any,
      totals: impactTotals,
      severityCounts,
      hotspots,
      estimatedPop,
      confidence,
      criticalBreakdown,
    });
    const baseName = `sitrep_${activeStorm.storm_id}_${new Date().toISOString().slice(0,10)}`;
    const ok = deliverReport(html, baseName, format);
    if (!ok) return; // deliverReport has already set an error toast
    setToastSuccess(format === 'pdf' ? 'SitRep opened — save as PDF from the print dialog' : 'Situation Report downloaded');
  }, [activeStorm, hotspots, impactTotals, severityCounts, estimatedPop, confidence, criticalBreakdown, deliverReport]);

  // Refs to avoid stale closures in loadCell callback
  const loadingCellsRef = useRef(loadingCells);
  loadingCellsRef.current = loadingCells;
  const loadedCellsRef = useRef(loadedCells);
  loadedCellsRef.current = loadedCells;
  const activeStormRef = useRef(activeStorm);
  activeStormRef.current = activeStorm;

  // Load cell — stable callback with no state dependencies (uses refs)
  const loadCell = useCallback(async (col: number, row: number) => {
    const key = cellKey(col, row);
    if (loadedCellsRef.current.has(key) || loadingCellsRef.current.has(key)) return;
    const stormId = activeStormRef.current?.storm_id || '';
    if (!stormId) return; // no active storm
    setLoadingCells(prev => new Set([...prev, key]));
    try {
      const ac = new AbortController();
      const timeout = setTimeout(() => ac.abort(), 90_000); // 90s timeout for cell generation
      const resp = await fetch(`/surgedps/api/cell?col=${col}&row=${row}&storm_id=${encodeURIComponent(stormId)}`, { signal: ac.signal });
      clearTimeout(timeout);
      // Guard: if user switched storms while waiting, discard stale response
      if (activeStormRef.current?.storm_id !== stormId) return;
      if (!resp.ok) throw new Error(`${resp.status}`);
      const cellData = await resp.json();
      const { buildings, flood } = cellData;
      if (cellData.confidence) setConfidence({ level: cellData.confidence, count: cellData.building_count || 0 });
      if (cellData.eli) setEli({ value: cellData.eli, tier: cellData.eli_tier || 'unavailable' });
      if (cellData.validated_dps) setValidatedDps({ value: cellData.validated_dps, adj: cellData.dps_adjustment || 0, reason: cellData.dps_adj_reason || '' });

      // ── Flood first: render immediately for fast visual feedback ──
      setAllFlood((p: any) => {
        if (!p) return flood;
        return { type: 'FeatureCollection', features: p.features.concat(flood.features) };
      });
      setLoadedCells(prev => new Set([...prev, key]));

      // ── Buildings deferred: push to next tick so flood paints first ──
      setTimeout(() => {
        // Guard: discard if user switched storms during the deferred tick
        if (activeStormRef.current?.storm_id !== stormId) return;
        setAllBuildings((p: any) => {
          if (!p) return buildings;
          return { type: 'FeatureCollection', features: p.features.concat(buildings.features) };
        });
        const cellFeats = buildings?.features || [];
        setImpactTotals(p => ({
          buildings: p.buildings + cellFeats.length,
          loss: p.loss + cellFeats.reduce((s: number, f: any) => s + (f.properties.estimated_loss_usd || 0), 0),
          totalDepth: p.totalDepth + cellFeats.reduce((s: number, f: any) => s + (f.properties.depth_ft || 0), 0),
        }));
      }, 0);

      // ── Ticks bundle (peril time-series): fetch in background ──
      // Returns 404 on legacy cells generated pre-peril pipeline; we
      // silently ignore so the slider stays disabled for those.
      fetch(`/surgedps/api/cell_ticks?col=${col}&row=${row}&storm_id=${encodeURIComponent(stormId)}`)
        .then(r => (r.ok ? r.json() : null))
        .then((bundle: TicksBundle | null) => {
          if (!bundle || activeStormRef.current?.storm_id !== stormId) return;
          // First bundle for this session sets the tick schedule.
          // Using functional setState so the empty-deps useCallback closure
          // doesn't stall a stale tickHours value here.
          if (Array.isArray(bundle.tick_hours)) {
            setTickHours(prev => prev.length ? prev : bundle.tick_hours);
          }
          const m = buildingTicksRef.current;
          for (const b of bundle.buildings) m[b.id] = b.ticks;
          setBuildingTicksVersion(v => v + 1);
        })
        .catch(() => { /* 404s / network blips are expected & non-fatal */ });
    } catch (err) { console.error(`Failed cell (${col},${row}):`, err); setCellError('Could not load this area — the data source may be temporarily unavailable. Try again in a moment.'); }
    finally { setLoadingCells(prev => { const n = new Set([...prev]); n.delete(key); return n; }); }
  }, []); // stable — all state accessed via refs

  // Reverse-geocode building popup via Nominatim (debounced 300ms to avoid hammering the API)
  // Uses pinnedInfo if set (click-to-pin), otherwise hoverInfo
  const displayedPopup = pinnedInfo ?? hoverInfo;
  useEffect(() => {
    if (displayedPopup?.type !== 'damage') {
      setHoverAddress(null);
      return;
    }
    const { lng, lat } = displayedPopup;
    const cacheKey = `${lng.toFixed(5)},${lat.toFixed(5)}`;
    if (cacheKey in geocodeCache.current) {
      setHoverAddress(geocodeCache.current[cacheKey] || null);
      return;
    }
    setHoverAddress(null);
    const controller = new AbortController();
    const timer = setTimeout(() => {
      // Pre-populate so we don't fire duplicate requests
      geocodeCache.current[cacheKey] = '';
      fetch(
        `/surgedps/api/geocode/reverse?lat=${lat}&lon=${lng}`,
        { signal: controller.signal }
      )
        .then(r => r.json())
        .then(data => {
          const label = data?.label || null;
          geocodeCache.current[cacheKey] = label || '';
          setHoverAddress(label);
        })
        .catch(() => { delete geocodeCache.current[cacheKey]; });
    }, 300);
    return () => { clearTimeout(timer); controller.abort(); };
  }, [displayedPopup]);

  // Events
  const onHover = useCallback((event: any) => {
    const { features, lngLat: { lng, lat } } = event;
    if (!features || !features.length) { setHoverInfo(null); return; }
    for (const [layerId, type] of [['grid-available-fill', 'grid'], ['grid-ready-fill', 'grid'], ['damage-clusters', 'cluster'], ['damage-points', 'damage'], ['population-points', 'population'], ['city-aggregate-circle', 'city'], ['county-aggregate-circle', 'county']] as const) {
      const f = features.find((f: any) => f.layer.id === layerId);
      if (f) { setHoverInfo({ lng, lat, type, feature: f }); return; }
    }
    setHoverInfo(null);
  }, []);

  const onClick = useCallback((event: any) => {
    // Close any open menus when map is clicked
    setMoreMenuOpen(false);
    // City aggregate bubble click → fly in to zoom 11 (individual building zoom).
    const cityBubble = event.features?.find((f: any) => f.layer.id === 'city-aggregate-circle');
    if (cityBubble && mapRef.current) {
      mapRef.current.flyTo({
        center: [event.lngLat.lng, event.lngLat.lat],
        zoom: 11,
        duration: 700,
      });
      return;
    }
    // County aggregate bubble click → fly in to zoom 9 (above the minzoom
    // threshold for city-aggregate bubbles) so the user can drill down.
    const countyBubble = event.features?.find((f: any) => f.layer.id === 'county-aggregate-circle');
    if (countyBubble && mapRef.current) {
      mapRef.current.flyTo({
        center: [event.lngLat.lng, event.lngLat.lat],
        zoom: 9,
        duration: 900,
      });
      return;
    }
    // Cluster click → zoom in
    const cluster = event.features?.find((f: any) => f.layer.id === 'damage-clusters');
    if (cluster && mapRef.current) {
      const src = mapRef.current.getSource('damage-data') as any;
      if (src?.getClusterExpansionZoom) {
        src.getClusterExpansionZoom(cluster.properties.cluster_id, (err: any, z: number) => {
          if (!err) mapRef.current?.flyTo({ center: [event.lngLat.lng, event.lngLat.lat], zoom: z, duration: 500 });
        });
      }
      return;
    }
    // Damage point click → pin popup
    const damagePoint = event.features?.find((f: any) => f.layer.id === 'damage-points');
    if (damagePoint) {
      setPinnedInfo({ lng: event.lngLat.lng, lat: event.lngLat.lat, type: 'damage', feature: damagePoint });
      return;
    }
    // Population point click → pin population popup (property-level occupancy + displacement status)
    const popPoint = event.features?.find((f: any) => f.layer.id === 'population-points');
    if (popPoint) {
      setPinnedInfo({ lng: event.lngLat.lng, lat: event.lngLat.lat, type: 'population', feature: popPoint });
      return;
    }
    // Grid cell click → load data (available or pre-computed "ready" cells)
    const f = event.features?.find((f: any) => f.layer.id === 'grid-available-fill' || f.layer.id === 'grid-ready-fill');
    if (f) loadCell(f.properties.col, f.properties.row);
    else setPinnedInfo(null); // Click on empty space → clear pinned popup
  }, [loadCell]);

  const showGrid = zoom < 13;

  // Grid hint auto-dismiss after 8 seconds
  useEffect(() => {
    if (showGrid && activeStorm && !gridHintDismissed && loadedCells.size > 0) {
      if (gridHintTimerRef.current) clearTimeout(gridHintTimerRef.current);
      const timer = setTimeout(() => setGridHintDismissed(true), 8000);
      gridHintTimerRef.current = timer;
      return () => { clearTimeout(timer); gridHintTimerRef.current = null; };
    }
  }, [showGrid, activeStorm, gridHintDismissed, loadedCells.size]);

  return (
    <div className="flex h-screen w-full relative overflow-hidden">
      {/* Left Sidebar — Storm Browser */}
      <StormBrowser
        onSelectStorm={activateStorm}
        activeStormId={activeStorm?.storm_id || null}
        activating={activating}
        isOpen={sidebarOpen}
        onClose={() => setSidebarOpen(false)}
        activeStorm={activeStorm}
      />

      {/* Backdrop — taps to close sidebar on mobile/tablet */}
      {sidebarOpen && (
        <div
          className="fixed inset-0 z-20 bg-black/50 lg:hidden"
          onClick={() => setSidebarOpen(false)}
        />
      )}

      {/* Map Area */}
      <div className="relative flex-1">
        <Map
          ref={mapRef}
          initialViewState={{ longitude: -85, latitude: 30, zoom: 5, pitch: 0 }}
          mapStyle={BASEMAPS[basemap]}
          interactiveLayerIds={['damage-points', 'damage-clusters', 'population-points', 'county-aggregate-circle', 'city-aggregate-circle', ...(showGrid ? ['grid-available-fill', 'grid-ready-fill'] : [])]}
          cursor={hoverInfo?.type === 'cluster' || hoverInfo?.type === 'grid' || hoverInfo?.type === 'damage' || hoverInfo?.type === 'population' || hoverInfo?.type === 'county' || hoverInfo?.type === 'city' ? 'pointer' : ''}
          onMouseMove={onHover} onClick={onClick}
          onMoveEnd={e => {
            setZoom(e.viewState.zoom);
            if (basemap === 'satellite') fetchImageryMeta(e.viewState.latitude, e.viewState.longitude);
            // Counties are bundled so no per-pan refetch needed.
            if (showFloodZones && mapRef.current) {
              const b = mapRef.current.getBounds();
              fetchFloodZones({ west: b.getWest(), south: b.getSouth(), east: b.getEast(), north: b.getNorth() });
            }
          }}
        >
          <NavigationControl position="top-right" />

          {showCounties && (
            countiesGeoJSON && (
              <Source id="county-boundaries" type="geojson" data={countiesGeoJSON}>
                {/* Categorical choropleth fill — one of 8 cool pastels per
                    county so adjacent jurisdictions read as distinct at a
                    glance. Cool palette deliberately avoids the damage-bubble
                    hues (green/yellow/orange/red) so bubbles stay legible. */}
                <Layer
                  id="county-fill"
                  type="fill"
                  paint={{
                    'fill-color': ['match', ['get', 'colorIdx'],
                      0, '#93c5fd',  // blue-300
                      1, '#a5b4fc',  // indigo-300
                      2, '#c4b5fd',  // violet-300
                      3, '#d8b4fe',  // purple-300
                      4, '#f0abfc',  // fuchsia-300
                      5, '#7dd3fc',  // sky-300
                      6, '#67e8f9',  // cyan-300
                      7, '#5eead4',  // teal-300
                      '#cbd5e1',     // slate fallback
                    ],
                    'fill-opacity': 0.32,
                  }}
                />
                <Layer
                  id="county-line"
                  type="line"
                  paint={{ 'line-color': '#ffffff', 'line-width': 1, 'line-opacity': 0.7 }}
                />
                {/* County labels — append " County" / " Parish" (Louisiana)
                    so the label reads as a full jurisdiction name instead of
                    just a word. Larger size + heavier halo so it stays legible
                    on top of the surge grid, which otherwise washes out the
                    pastel fill. STATE "22" = Louisiana FIPS. */}
                <Layer
                  id="county-labels"
                  type="symbol"
                  minzoom={6}
                  layout={{
                    'text-field': ['concat',
                      ['get', 'NAME'],
                      ['case', ['==', ['get', 'STATE'], '22'], ' Parish', ' County'],
                    ],
                    'text-font': ['Open Sans Semibold', 'Arial Unicode MS Regular'],
                    'text-size': ['interpolate', ['linear'], ['zoom'], 6, 11, 10, 15, 14, 18],
                    'text-anchor': 'center',
                    'text-max-width': 10,
                    'text-letter-spacing': 0.02,
                    'text-transform': 'uppercase',
                  }}
                  paint={{
                    'text-color': '#ffffff',
                    'text-halo-color': '#0f172a',
                    'text-halo-width': 2,
                    'text-halo-blur': 0.5,
                  }}
                />
              </Source>
            )
          )}

          {/* ── NLCD 2021 Land Cover (Esri Living Atlas ImageServer) ──
              30 m CONUS raster served live from Esri's hosted copy of
              USGS/MRLC NLCD 2021. We originally pointed at mrlc.gov's
              GeoServer WMS but it doesn't set CORS headers, so the
              browser blocks the tiles. Esri's ImageServer `exportImage`
              endpoint is CORS-enabled and returns the canonical
              16-class Anderson Level II palette (red = developed,
              yellow = cropland, green = forest, blue = water). Layered
              beneath the damage bubbles at 55 % opacity. */}
          {showLandUse && (
            <Source
              id="nlcd-landuse"
              type="raster"
              tiles={[
                'https://landscape11.arcgis.com/arcgis/rest/services/' +
                'USA_NLCD_Land_Cover/ImageServer/exportImage' +
                '?bbox={bbox-epsg-3857}&bboxSR=3857&imageSR=3857' +
                '&size=256,256&format=png32&transparent=true&f=image',
              ]}
              tileSize={256}
              attribution="Land cover © USGS/MRLC NLCD 2021 (via Esri Living Atlas)"
            >
              <Layer
                id="nlcd-landuse-raster"
                type="raster"
                paint={{ 'raster-opacity': 0.6, 'raster-fade-duration': 200 }}
              />
            </Source>
          )}

          {showFloodZones && floodZonesGeoJSON && (
            <Source id="fema-flood-zones" type="geojson" data={floodZonesGeoJSON}>
              {/* Fill — color-coded by FEMA zone type */}
              <Layer
                id="fema-zones-fill"
                type="fill"
                paint={{
                  'fill-color': [
                    'match', ['get', 'FLD_ZONE'],
                    ['VE', 'V'],  '#dc2626',   // Coastal high-hazard — deep red
                    ['AE', 'AO', 'AH', 'A'], '#f97316',  // High-risk — orange
                    ['X'],        '#facc15',   // Moderate / minimal — yellow
                    '#94a3b8',                // Unknown / D — slate
                  ],
                  'fill-opacity': 0.30,
                }}
              />
              {/* Outline */}
              <Layer
                id="fema-zones-line"
                type="line"
                paint={{
                  'line-color': [
                    'match', ['get', 'FLD_ZONE'],
                    ['VE', 'V'],  '#ef4444',
                    ['AE', 'AO', 'AH', 'A'], '#fb923c',
                    ['X'],        '#fde047',
                    '#cbd5e1',
                  ],
                  'line-width': 1,
                  'line-opacity': 0.7,
                }}
              />
              {/* Zone labels at higher zoom */}
              <Layer
                id="fema-zones-labels"
                type="symbol"
                minzoom={10}
                layout={{
                  'text-field': ['get', 'FLD_ZONE'],
                  'text-font': ['Open Sans Bold', 'Arial Unicode MS Bold'],
                  'text-size': 10,
                  'text-anchor': 'center',
                }}
                paint={{ 'text-color': '#fff', 'text-halo-color': '#000', 'text-halo-width': 1 }}
              />
            </Source>
          )}

          {/* AHPS / NWPS stream gauges — color-coded by active flood category.
              Cheap high-value layer: no tile server needed, just GeoJSON points.
              Categories ordered worst→best so the halo ring stays readable. */}
          {showGauges && gaugesGeoJSON && (
            <Source id="stream-gauges" type="geojson" data={gaugesGeoJSON}>
              <Layer
                id="stream-gauges-halo"
                type="circle"
                paint={{
                  'circle-radius': ['interpolate', ['linear'], ['zoom'], 6, 8, 10, 14, 14, 20],
                  'circle-color': [
                    'match', ['get', 'category'],
                    'major',    '#7f1d1d',
                    'moderate', '#ef4444',
                    'minor',    '#fb923c',
                    'action',   '#facc15',
                    '#94a3b8',
                  ],
                  'circle-opacity': 0.25,
                  'circle-stroke-width': 0,
                }}
              />
              <Layer
                id="stream-gauges-dot"
                type="circle"
                paint={{
                  'circle-radius': ['interpolate', ['linear'], ['zoom'], 6, 3, 10, 5, 14, 7],
                  'circle-color': [
                    'match', ['get', 'category'],
                    'major',    '#7f1d1d',
                    'moderate', '#ef4444',
                    'minor',    '#fb923c',
                    'action',   '#facc15',
                    '#94a3b8',
                  ],
                  'circle-stroke-color': '#fff',
                  'circle-stroke-width': 1.5,
                }}
              />
              <Layer
                id="stream-gauges-label"
                type="symbol"
                minzoom={9}
                layout={{
                  'text-field': ['coalesce', ['get', 'name'], ['get', 'nws_lid'], ''],
                  'text-font': ['Open Sans Bold', 'Arial Unicode MS Bold'],
                  'text-size': 10,
                  'text-anchor': 'top',
                  'text-offset': [0, 0.8],
                  'text-allow-overlap': false,
                }}
                paint={{
                  'text-color': '#0f172a',
                  'text-halo-color': '#fff',
                  'text-halo-width': 1.5,
                }}
              />
            </Source>
          )}

          {/* Rainfall raster overlay — served on-demand as PNG tiles by the
              /api/rainfall_tile endpoint (rio-tiler + NWS precipitation ramp).
              Rendered below the damage bubbles so loss points remain visible;
              opacity tuned so the basemap and FEMA zones still read through. */}
          {hazardView === 'rainfall' && rainfallStats?.tileUrl && (
            <Source
              id="rainfall-raster"
              type="raster"
              tiles={[rainfallStats.tileUrl]}
              tileSize={256}
              minzoom={3}
              maxzoom={12}
            >
              <Layer
                id="rainfall-raster-layer"
                type="raster"
                paint={{
                  'raster-opacity': 0.72,
                  'raster-fade-duration': 250,
                  'raster-resampling': 'linear',
                }}
              />
            </Source>
          )}

          {allFlood && <Source id="flood-data" type="geojson" data={allFlood} tolerance={0.5}><Layer {...(floodLayerStyle as any)} /></Source>}

          {allBuildings && mapView === 'damage' && (
            <Source id="damage-data" type="geojson" data={allBuildings}
              cluster={true} clusterMaxZoom={14} clusterRadius={50}
              clusterProperties={{ total_loss: ['+', ['get', 'estimated_loss_usd']] }}
            >
              {/* Individual-building and supercluster bubbles only at zoom ≥ 11.
                  Below zoom 11 city-aggregate bubbles take over (zoom 8–11),
                  and below zoom 8 county-aggregate bubbles are shown instead,
                  so the EM sees one clearly-labeled marker per jurisdiction. */}
              <Layer id="damage-points" type="circle" filter={['!', ['has', 'point_count']]}
                minzoom={11}
                paint={{
                  'circle-radius': ['interpolate', ['linear'], ['zoom'], 10, 5, 13, 8, 14, 12, 16, 16, 18, 22],
                  'circle-color': ['match', ['get', 'damage_category'], 'none', '#4ade80', 'minor', '#facc15', 'moderate', '#fb923c', 'major', '#ef4444', 'severe', '#7f1d1d', '#9ca3af'],
                  'circle-opacity': 0.85,
                  'circle-stroke-width': 2, 'circle-stroke-color': '#fff',
                }} />
              <Layer id="damage-clusters" type="circle" filter={['has', 'point_count']}
                minzoom={11}
                paint={{
                  'circle-color': ['step', ['/', ['get', 'total_loss'], ['get', 'point_count']],
                    '#4ade80', 5000, '#facc15', 25000, '#fb923c', 75000, '#ef4444', 200000, '#7f1d1d'],
                  'circle-radius': ['step', ['get', 'point_count'], 16, 5, 22, 10, 30],
                  'circle-stroke-width': 3, 'circle-stroke-color': '#fff',
                }} />
              <Layer id="damage-cluster-count" type="symbol" filter={['has', 'point_count']}
                minzoom={11}
                layout={{
                  'text-field': ['concat', '$', ['to-string', ['round', ['/', ['get', 'total_loss'], 1000]]], 'K'],
                  'text-size': 11,
                }}
                paint={{ 'text-color': '#fff', 'text-halo-color': 'rgba(0,0,0,0.8)', 'text-halo-width': 1.5 }} />
            </Source>
          )}

          {/* Population mode building layer — non-clustered so every residential
              dwelling shows up distinctly. Radius scales with zoom; color
              encodes displacement state derived from damage_category × building_type
              (RES + major/severe = red "displaced", RES + minor/none = green "safe",
              non-residential = gray). */}
          {allBuildings && mapView === 'population' && (
            <Source id="population-data" type="geojson" data={allBuildings}>
              <Layer id="population-points" type="circle"
                minzoom={11}
                paint={{
                  'circle-radius': ['interpolate', ['linear'], ['zoom'], 11, 4, 13, 7, 14, 10, 16, 14, 18, 20],
                  // Color: if building_type starts with "RES" → color by damage severity;
                  // otherwise gray (non-residential buildings don't have overnight residents).
                  'circle-color': [
                    'case',
                    ['==', ['index-of', 'RES', ['get', 'building_type']], 0],
                    ['match', ['get', 'damage_category'],
                      'severe', '#7f1d1d',
                      'major', '#ef4444',
                      'moderate', '#fb923c',
                      'minor', '#facc15',
                      '#4ade80'],
                    '#6b7280',
                  ],
                  'circle-opacity': [
                    'case',
                    ['==', ['index-of', 'RES', ['get', 'building_type']], 0],
                    0.9,
                    0.45,
                  ],
                  'circle-stroke-width': [
                    'case',
                    ['==', ['index-of', 'RES', ['get', 'building_type']], 0],
                    2,
                    1,
                  ],
                  'circle-stroke-color': '#fff',
                }} />
            </Source>
          )}

          {/* County-aggregate bubbles — one per county at the bbox center.
              Visible only at low zoom (zoom < 8). Color by worst-severity,
              radius by building count, label with county name + count. */}
          {countyAggregatePoints && (
            <Source id="county-aggregate-data" type="geojson" data={countyAggregatePoints}>
              <Layer id="county-aggregate-circle" type="circle"
                maxzoom={8}
                paint={mapView === 'damage' ? {
                  'circle-radius': ['interpolate', ['linear'], ['get', 'buildings'],
                    1, 10, 100, 14, 1000, 20, 10000, 30, 50000, 42],
                  'circle-color': ['match', ['get', 'worstCategory'],
                    'severe', '#7f1d1d',
                    'major', '#ef4444',
                    'moderate', '#fb923c',
                    'minor', '#facc15',
                    '#4ade80'],
                  'circle-opacity': 0.9,
                  'circle-stroke-width': 3,
                  'circle-stroke-color': '#fff',
                } : {
                  // Population mode: radius = est. displaced, color stepped by displaced count.
                  'circle-radius': ['interpolate', ['linear'], ['get', 'estDisplaced'],
                    0, 8, 50, 12, 500, 18, 5000, 28, 25000, 40, 100000, 50],
                  'circle-color': ['step', ['get', 'estDisplaced'],
                    '#4ade80',     // 0 displaced = green (safe)
                    1,    '#facc15',   // 1–99
                    100,  '#fb923c',   // 100–999
                    1000, '#ef4444',   // 1k–9.9k
                    10000,'#7f1d1d'],  // 10k+
                  'circle-opacity': 0.9,
                  'circle-stroke-width': 3,
                  'circle-stroke-color': '#fff',
                }} />
              <Layer id="county-aggregate-label" type="symbol"
                maxzoom={8}
                layout={mapView === 'damage' ? {
                  'text-field': ['get', 'label'],
                  'text-size': 11,
                  'text-offset': [0, 1.8],
                  'text-anchor': 'top',
                  'text-allow-overlap': false,
                } : {
                  'text-field': ['concat', ['get', 'name'], '  ',
                    ['case', ['>=', ['get', 'estDisplaced'], 1000],
                      ['concat', ['to-string', ['round', ['/', ['get', 'estDisplaced'], 1000]]], 'k displ.'],
                      ['concat', ['to-string', ['get', 'estDisplaced']], ' displ.']]],
                  'text-size': 11,
                  'text-offset': [0, 1.8],
                  'text-anchor': 'top',
                  'text-allow-overlap': false,
                }}
                paint={{
                  'text-color': '#fff',
                  'text-halo-color': 'rgba(0,0,0,0.85)',
                  'text-halo-width': 1.5,
                }} />
            </Source>
          )}

          {/* City-aggregate bubbles — one per Census Place (or Unincorporated
              county bucket) at zoom 8–11. Sits between the county overview
              (maxzoom 8) and individual building dots (minzoom 11) so the EM
              can identify which cities carry the most exposure before drilling
              into individual properties. Click flies to zoom 11. */}
          {cityAggregatePoints && (
            <Source id="city-aggregate-data" type="geojson" data={cityAggregatePoints}>
              <Layer id="city-aggregate-circle" type="circle"
                minzoom={8} maxzoom={11}
                paint={mapView === 'damage' ? {
                  'circle-radius': ['interpolate', ['linear'], ['get', 'buildings'],
                    1, 8, 50, 12, 500, 18, 5000, 28, 20000, 38],
                  'circle-color': ['match', ['get', 'worstCategory'],
                    'severe', '#7f1d1d',
                    'major', '#ef4444',
                    'moderate', '#fb923c',
                    'minor', '#facc15',
                    '#4ade80'],
                  'circle-opacity': 0.88,
                  'circle-stroke-width': 2,
                  'circle-stroke-color': '#fff',
                } : {
                  // Population mode: radius by displaced, color stepped by displaced count.
                  // Narrower thresholds than county (cities are smaller).
                  'circle-radius': ['interpolate', ['linear'], ['get', 'estDisplaced'],
                    0, 6, 25, 10, 250, 16, 2500, 26, 10000, 36],
                  'circle-color': ['step', ['get', 'estDisplaced'],
                    '#4ade80',
                    1,   '#facc15',
                    50,  '#fb923c',
                    500, '#ef4444',
                    5000,'#7f1d1d'],
                  'circle-opacity': 0.88,
                  'circle-stroke-width': 2,
                  'circle-stroke-color': '#fff',
                }} />
              <Layer id="city-aggregate-label" type="symbol"
                minzoom={8} maxzoom={11}
                layout={mapView === 'damage' ? {
                  'text-field': ['get', 'label'],
                  'text-size': 10,
                  'text-offset': [0, 1.6],
                  'text-anchor': 'top',
                  'text-allow-overlap': false,
                } : {
                  'text-field': ['concat', ['get', 'name'], '  ',
                    ['case', ['>=', ['get', 'estDisplaced'], 1000],
                      ['concat', ['to-string', ['round', ['/', ['get', 'estDisplaced'], 1000]]], 'k displ.'],
                      ['concat', ['to-string', ['get', 'estDisplaced']], ' displ.']]],
                  'text-size': 10,
                  'text-offset': [0, 1.6],
                  'text-anchor': 'top',
                  'text-allow-overlap': false,
                }}
                paint={{
                  'text-color': '#fff',
                  'text-halo-color': 'rgba(0,0,0,0.85)',
                  'text-halo-width': 1.5,
                }} />
            </Source>
          )}

          {criticalFacilities && (
            <Source id="critical-facilities" type="geojson" data={criticalFacilities}>
              {/* Colored circle behind each icon — category glance-read.
                  Hospitals=red, gov/emergency=gold, schools=blue,
                  nursing=teal, churches=purple. MinZoom 15 keeps them
                  hidden until the user is fully drilled into the
                  individual-building view, so the neighbourhood/city
                  sweep isn't cluttered. */}
              <Layer id="critical-icon-halos" type="circle"
                minzoom={15}
                paint={{
                  'circle-radius': ['interpolate', ['linear'], ['zoom'], 15, 10, 17, 14, 19, 20],
                  'circle-color': ['match', ['get', 'critical_icon'],
                    '➕',  '#dc2626',   // Hospitals/Clinics — red
                    '⭐',  '#f59e0b',   // Government / Emergency — gold
                    '🏫',  '#2563eb',   // Schools — blue
                    '🛏️',  '#0d9488',  // Nursing homes — teal
                    '⛪',  '#7c3aed',   // Places of worship — purple
                    '#475569',         // fallback — slate
                  ],
                  'circle-stroke-color': '#ffffff',
                  'circle-stroke-width': 2,
                  'circle-opacity': 0.95,
                }}
              />
              <Layer id="critical-icons" type="symbol"
                minzoom={15}
                layout={{
                  'text-field': ['get', 'critical_icon'],
                  'text-size': ['interpolate', ['linear'], ['zoom'], 15, 12, 17, 18, 19, 24],
                  'text-allow-overlap': true,
                  'text-ignore-placement': true,
                  'symbol-sort-key': ['case',
                    ['==', ['get', 'critical_icon'], '➕'], 0,
                    ['==', ['get', 'critical_icon'], '⭐'], 1,
                    ['==', ['get', 'critical_icon'], '🏫'], 2,
                    ['==', ['get', 'critical_icon'], '🛏️'], 3,
                    ['==', ['get', 'critical_icon'], '⛪'], 4,
                    5,
                  ],
                }}
                paint={{
                  'text-color': '#ffffff',
                  'text-halo-color': 'rgba(0,0,0,0.5)',
                  'text-halo-width': 0.5,
                }}
              />
            </Source>
          )}

          {showGrid && activeStorm && (
            <Source id="grid-data" type="geojson" data={gridGeoJson}>
              <Layer id="grid-loaded-border" type="line" filter={['==', ['get', 'status'], 'loaded']}
                paint={{ 'line-color': '#4ade80', 'line-width': 2, 'line-opacity': 0.6, 'line-dasharray': [4, 2] }} />
              <Layer id="grid-available-fill" type="fill" filter={['==', ['get', 'status'], 'available']}
                paint={{ 'fill-color': '#6366f1', 'fill-opacity': 0.05 }} />
              <Layer id="grid-available-border" type="line" filter={['==', ['get', 'status'], 'available']}
                paint={{ 'line-color': '#a5b4fc', 'line-width': 1.5, 'line-opacity': 0.6, 'line-dasharray': [6, 3] }} />
              <Layer id="grid-loading-fill" type="fill" filter={['==', ['get', 'status'], 'loading']}
                paint={{ 'fill-color': '#facc15', 'fill-opacity': 0.1 }} />
              <Layer id="grid-loading-border" type="line" filter={['==', ['get', 'status'], 'loading']}
                paint={{ 'line-color': '#facc15', 'line-width': 2.5, 'line-opacity': 0.9 }} />
              <Layer id="grid-available-label" type="symbol" filter={['==', ['get', 'status'], 'available']}
                layout={{ 'text-field': '+ Click to load', 'text-size': 13, 'text-font': ['Open Sans Semibold'] }}
                paint={{ 'text-color': '#c7d2fe', 'text-opacity': 0.85, 'text-halo-color': '#000', 'text-halo-width': 1.2 }} />
              {/* Pre-computed "ready" cells — solid green border (instant load from cache) */}
              <Layer id="grid-ready-fill" type="fill" filter={['==', ['get', 'status'], 'ready']}
                paint={{ 'fill-color': '#4ade80', 'fill-opacity': 0.06 }} />
              <Layer id="grid-ready-border" type="line" filter={['==', ['get', 'status'], 'ready']}
                paint={{ 'line-color': '#4ade80', 'line-width': 2, 'line-opacity': 0.7 }} />
              <Layer id="grid-ready-label" type="symbol" filter={['==', ['get', 'status'], 'ready']}
                layout={{ 'text-field': 'Cached \u2713', 'text-size': 12, 'text-font': ['Open Sans Regular'] }}
                paint={{ 'text-color': '#4ade80', 'text-opacity': 0.8, 'text-halo-color': '#000', 'text-halo-width': 1 }} />
              <Layer id="grid-loading-label" type="symbol" filter={['==', ['get', 'status'], 'loading']}
                layout={{ 'text-field': 'Loading...', 'text-size': 13, 'text-font': ['Open Sans Regular'] }}
                paint={{ 'text-color': '#facc15', 'text-opacity': 0.9, 'text-halo-color': '#000', 'text-halo-width': 1 }} />
            </Source>
          )}

          {/* ── Forecast cone overlay (active storms only) ── */}
          {simMode && forecastCone && (
            <Source id="forecast-cone" type="geojson" data={forecastCone}>
              <Layer id="cone-fill" type="fill" paint={{ 'fill-color': '#ffffff', 'fill-opacity': 0.12 }} />
              <Layer id="cone-border" type="line" paint={{ 'line-color': '#ffffff', 'line-width': 2, 'line-opacity': 0.5, 'line-dasharray': [4, 3] }} />
            </Source>
          )}

          {/* ── Forecast track line (active storms only) ── */}
          {simMode && forecastTrack.length > 1 && (
            <Source id="forecast-track-line" type="geojson" data={{
              type: 'Feature', geometry: { type: 'LineString', coordinates: forecastTrack.map((p: any) => [p.lon, p.lat]) }, properties: {}
            }}>
              <Layer id="track-line" type="line" paint={{ 'line-color': '#ef4444', 'line-width': 3, 'line-dasharray': [6, 4] }} />
            </Source>
          )}

          {/* ── Forecast track points ── */}
          {simMode && forecastTrack.map((p: any, i: number) => (
            <Marker key={`fpt-${i}`} longitude={p.lon} latitude={p.lat}>
              <div className="w-3 h-3 rounded-full bg-red-500 border-2 border-white shadow" title={`${p.date_label} — ${p.max_wind_kt} kt`} />
            </Marker>
          ))}

          {/* ── Draggable landfall marker (simulator) ── */}
          {simMode && simMarker && (
            <Marker
              longitude={simMarker.lng}
              latitude={simMarker.lat}
              draggable
              onDragEnd={(e) => setSimMarker({ lng: e.lngLat.lng, lat: e.lngLat.lat })}
            >
              <div className="flex flex-col items-center cursor-grab active:cursor-grabbing" title="Drag to simulate landfall at a different location">
                <div className="w-6 h-6 rounded-full bg-red-600 border-3 border-white shadow-lg flex items-center justify-center">
                  <div className="w-2 h-2 rounded-full bg-white" />
                </div>
                <div className="text-[10px] font-bold text-white bg-red-600/90 px-1.5 py-0.5 rounded mt-0.5 whitespace-nowrap shadow">
                  LANDFALL
                </div>
              </div>
            </Marker>
          )}

          {(pinnedInfo || hoverInfo) && (
            <Popup longitude={pinnedInfo?.lng ?? hoverInfo?.lng} latitude={pinnedInfo?.lat ?? hoverInfo?.lat} closeButton={pinnedInfo ? true : false} closeOnClick={false} anchor="bottom" className="z-50" onClose={() => pinnedInfo && setPinnedInfo(null)}>
              <div className="p-2 min-w-[200px]">
                {(pinnedInfo ?? hoverInfo).type === 'grid' ? (
                  <div className="text-center">
                    <p className="text-sm font-semibold text-gray-800">
                      {(pinnedInfo ?? hoverInfo).feature?.properties?.status === 'ready' ? 'Pre-computed Region' : 'Unexplored Region'}
                    </p>
                    <p className="text-xs text-gray-500 mt-1">
                      {(pinnedInfo ?? hoverInfo).feature?.properties?.status === 'ready'
                        ? 'Click to load instantly from cache'
                        : 'Click to load buildings & damage data'}
                    </p>
                  </div>
                ) : (pinnedInfo ?? hoverInfo).type === 'cluster' ? (
                  <><h3 className="font-semibold text-gray-800 text-sm border-b pb-1 mb-1 border-gray-200">Neighborhood Impact</h3>
                  <div className="text-xs space-y-1">
                    <p className="flex justify-between"><span className="text-gray-500">Properties:</span> <span className="font-medium">{(pinnedInfo ?? hoverInfo).feature.properties.point_count}</span></p>
                    <p className="flex justify-between text-sm"><span className="text-gray-500">Loss:</span> <span className="font-bold text-red-600">${(pinnedInfo ?? hoverInfo).feature.properties.total_loss?.toLocaleString() || 0}</span></p>
                    {(() => {
                      const mPerPx = 156543.03 * Math.cos((pinnedInfo ?? hoverInfo).lat * Math.PI / 180) / Math.pow(2, zoom);
                      const radiusM = 50 * mPerPx;
                      const areaSqMi = Math.PI * radiusM * radiusM / (1609.34 * 1609.34);
                      const density = areaSqMi > 0 ? Math.round((pinnedInfo ?? hoverInfo).feature.properties.point_count / areaSqMi) : 0;
                      const avgLoss = Math.round((pinnedInfo ?? hoverInfo).feature.properties.total_loss / (pinnedInfo ?? hoverInfo).feature.properties.point_count);
                      return (
                        <>
                          <p className="flex justify-between"><span className="text-gray-500">Avg loss:</span> <span className="font-medium">${avgLoss.toLocaleString()}</span></p>
                          <p className="flex justify-between"><span className="text-gray-500">Density:</span> <span className="font-medium">~{density.toLocaleString()} bldgs/sq mi</span></p>
                        </>
                      );
                    })()}
                  </div></>
                ) : (pinnedInfo ?? hoverInfo).type === 'flood' ? (
                  <><h3 className="font-semibold text-gray-800 text-sm mb-1">Storm Surge Depth</h3>
                  <p className="text-gray-800 font-bold text-base">{((pinnedInfo ?? hoverInfo).feature.properties.depth_ft != null ? (pinnedInfo ?? hoverInfo).feature.properties.depth_ft : ((pinnedInfo ?? hoverInfo).feature.properties.depth != null ? (pinnedInfo ?? hoverInfo).feature.properties.depth * 3.28084 : 0)).toFixed(1)} ft</p>
                  <p className="text-gray-400 text-[10px] mt-0.5">Modeled inundation at this location</p></>
                ) : (pinnedInfo ?? hoverInfo).type === 'county' ? (() => {
                  const p = (pinnedInfo ?? hoverInfo).feature.properties;
                  const isPop = mapView === 'population';
                  return (
                    <><h3 className="font-semibold text-gray-800 text-sm border-b pb-1 mb-1 border-gray-200">{p.name}, {p.state}</h3>
                    <div className="text-xs space-y-1">
                      {isPop ? (
                        <>
                          <p className="flex justify-between"><span className="text-gray-500">Est. displaced:</span> <span className="font-bold text-indigo-600">{(p.estDisplaced ?? 0).toLocaleString()}</span></p>
                          <p className="flex justify-between"><span className="text-gray-500">Severe damage:</span> <span className="font-medium">{(p.severe ?? 0).toLocaleString()}</span></p>
                          <p className="flex justify-between"><span className="text-gray-500">Major damage:</span> <span className="font-medium">{(p.major ?? 0).toLocaleString()}</span></p>
                          <p className="flex justify-between"><span className="text-gray-500">Buildings exposed:</span> <span className="font-medium">{(p.buildings ?? 0).toLocaleString()}</span></p>
                        </>
                      ) : (
                        <>
                          <p className="flex justify-between"><span className="text-gray-500">Buildings:</span> <span className="font-medium">{(p.buildings ?? 0).toLocaleString()}</span></p>
                          <p className="flex justify-between"><span className="text-gray-500">Est. loss:</span> <span className="font-bold text-red-600">${((p.loss ?? 0) / 1e6).toFixed(1)}M</span></p>
                          {p.criticalFacilities > 0 && <p className="flex justify-between"><span className="text-gray-500">Critical:</span> <span className="font-medium">{p.criticalFacilities}</span></p>}
                        </>
                      )}
                      <p className="text-gray-400 text-[10px] mt-0.5">Click to zoom in</p>
                    </div></>
                  );
                })()
                : (pinnedInfo ?? hoverInfo).type === 'city' ? (() => {
                  const p = (pinnedInfo ?? hoverInfo).feature.properties;
                  const isPop = mapView === 'population';
                  const pct = isPop && p.pop > 0 ? Math.min(100, Math.round(100 * (p.estDisplaced ?? 0) / p.pop)) : null;
                  return (
                    <><h3 className="font-semibold text-gray-800 text-sm border-b pb-1 mb-1 border-gray-200">{p.name}, {p.state}</h3>
                    <div className="text-xs space-y-1">
                      {isPop ? (
                        <>
                          <p className="flex justify-between"><span className="text-gray-500">Est. displaced:</span> <span className="font-bold text-indigo-600">{(p.estDisplaced ?? 0).toLocaleString()}</span></p>
                          {p.pop > 0 && <p className="flex justify-between"><span className="text-gray-500">City population<span className="text-gray-400"> · 2020 Census</span>:</span> <span className="font-medium">{p.pop.toLocaleString()}</span></p>}
                          {pct != null && <p className="flex justify-between"><span className="text-gray-500">Displacement rate:</span> <span className={`font-bold ${pct >= 25 ? 'text-red-600' : pct >= 5 ? 'text-orange-600' : 'text-gray-700'}`}>{pct}%</span></p>}
                          <p className="flex justify-between"><span className="text-gray-500">Buildings exposed:</span> <span className="font-medium">{(p.buildings ?? 0).toLocaleString()}</span></p>
                        </>
                      ) : (
                        <>
                          <p className="flex justify-between"><span className="text-gray-500">Buildings:</span> <span className="font-medium">{(p.buildings ?? 0).toLocaleString()}</span></p>
                          <p className="flex justify-between"><span className="text-gray-500">Est. loss:</span> <span className="font-bold text-red-600">${((p.loss ?? 0) / 1e6).toFixed(1)}M</span></p>
                          {p.criticalFacilities > 0 && <p className="flex justify-between"><span className="text-gray-500">Critical:</span> <span className="font-medium">{p.criticalFacilities}</span></p>}
                          {p.pop > 0 && <p className="flex justify-between"><span className="text-gray-500">Population<span className="text-gray-400"> · 2020 Census</span>:</span> <span className="font-medium">{p.pop.toLocaleString()}</span></p>}
                        </>
                      )}
                      <p className="text-gray-400 text-[10px] mt-0.5">Click to zoom in</p>
                    </div></>
                  );
                })()
                : (pinnedInfo ?? hoverInfo).type === 'population' ? (() => {
                  const p = (pinnedInfo ?? hoverInfo).feature.properties;
                  const isRes = (p.building_type || '').startsWith('RES');
                  const cat = (p.damage_category || 'none') as string;
                  const displaced = isRes && (cat === 'severe' || cat === 'major');
                  const estOccupants = isRes ? 2.5 : 0;
                  return (
                    <><h3 className="font-semibold text-gray-800 text-sm border-b pb-1 mb-1 border-gray-200">Property — Population Impact</h3>
                    <div className="text-xs space-y-1">
                      <p className="flex justify-between gap-2"><span className="text-gray-500">Type:</span> <span className="font-medium text-right">{friendlyFacilityLabel(p)}{p.occtype && p.occtype.replace(/[-_].*$/, '').toUpperCase() !== (p.building_type || '').replace(/[-_].*$/, '').toUpperCase() ? <span className="text-[10px] text-gray-400 ml-1">({p.occtype})</span> : null}</span></p>
                      <p className="flex justify-between"><span className="text-gray-500">Damage:</span> <span className="font-medium capitalize">{cat === 'none' ? 'No damage' : cat}</span></p>
                      {isRes ? (
                        <>
                          <p className="flex justify-between"><span className="text-gray-500">Est. occupants:</span> <span className="font-medium">{estOccupants}</span></p>
                          <p className="flex justify-between"><span className="text-gray-500">Status:</span>
                            <span className={`font-bold ${displaced ? 'text-red-600' : 'text-green-700'}`}>
                              {displaced ? 'Displaced' : 'Sheltering in place'}
                            </span>
                          </p>
                        </>
                      ) : (
                        <p className="text-gray-500 italic">Non-residential — no overnight occupancy</p>
                      )}
                    </div></>
                  );
                })()
                : (
                  (() => {
                    const p = (pinnedInfo ?? hoverInfo).feature.properties;
                    const foundHt = p.found_ht != null ? p.found_ht : null;
                    const depthFt = p.depth_ft != null ? Number(p.depth_ft) : null;
                    const interiorFt = foundHt != null && depthFt != null ? Math.max(0, depthFt - foundHt) : null;
                    const structPct = p.structure_damage_pct ?? 0;
                    const contPct = p.contents_damage_pct ?? 0;
                    const structLoss = p.val_struct != null && p.structure_damage_pct != null ? Math.round(p.val_struct * structPct / 100) : null;
                    const contLoss = p.val_cont != null && p.contents_damage_pct != null ? Math.round(p.val_cont * contPct / 100) : null;

                    // ── Comparable Loss Evidence ──
                    const popupInfo = pinnedInfo ?? hoverInfo;
                    const comps = allBuildings?.features
                      ? findComparables(allBuildings.features, p.building_type, popupInfo.lng, popupInfo.lat)
                      : { count: 0, avgLoss: 0, minLoss: 0, maxLoss: 0 };

                    // ── Wind vs Water Attribution ──
                    let wwSplit: { windPct: number; waterPct: number } | null = null;
                    let estWindMph: number | null = null;
                    if (activeStorm) {
                      const distKm = haversineKm(popupInfo.lat, popupInfo.lng, activeStorm.landfall_lat, activeStorm.landfall_lon);
                      estWindMph = Math.round(estimateWindMph(distKm, activeStorm.max_wind_kt, activeStorm.category));
                      const floodForWind = interiorFt != null ? interiorFt : (depthFt != null ? Math.max(0, depthFt - 1) : 0); // fallback: assume 1ft foundation
                      wwSplit = windWaterSplit(estWindMph, floodForWind);
                    }

                    return (
                    <><h3 className="font-semibold text-gray-800 text-sm border-b pb-1 mb-1 border-gray-200">Property Damage</h3>
                    <a
                      href={`https://www.google.com/maps/@${popupInfo.lat},${popupInfo.lng},19z/data=!3m1!1e1`}
                      target="_blank"
                      rel="noopener noreferrer"
                      title="Open in Google Maps (satellite view — Street View may be unavailable on military bases)"
                      className="flex items-center gap-1 text-[11px] text-indigo-700 font-semibold mb-1.5 pb-1 border-b border-gray-100 hover:text-indigo-500 transition-colors group"
                    >
                      <span className="truncate">{hoverAddress ?? `${popupInfo.lat.toFixed(5)}, ${popupInfo.lng.toFixed(5)}`}</span>
                      <span className="shrink-0 opacity-60 group-hover:opacity-100 transition-opacity" title="Google Maps">↗</span>
                    </a>
                    <div className="text-xs space-y-1">
                      <p className="flex justify-between gap-2"><span className="text-gray-500">Type:</span> <span className="font-medium text-right">{friendlyFacilityLabel(p)}{p.occtype && p.occtype.replace(/[-_].*$/, '').toUpperCase() !== (p.building_type || '').replace(/[-_].*$/, '').toUpperCase() ? <span className="text-[10px] text-gray-400 ml-1">({p.occtype})</span> : null}</span></p>
                      <p className="flex justify-between"><span className="text-gray-500">Severity:</span> <span className="font-medium capitalize">{!p.damage_category || p.damage_category === 'none' ? 'No Damage' : p.damage_category}</span></p>
                      {/* Surge depth + interior flooding */}
                      <p className="flex justify-between"><span className="text-gray-500">Surge depth:</span> <span className="font-medium">{p.depth_ft != null ? Number(p.depth_ft).toFixed(1) : '—'} ft</span></p>
                      {foundHt != null && (
                        <p className="flex justify-between"><span className="text-gray-500">Foundation:</span> <span className="font-medium">{foundHt.toFixed(1)} ft above grade</span></p>
                      )}
                      {interiorFt != null && (
                        <p className="flex justify-between"><span className="text-gray-500">Interior flooding:</span> <span className={`font-bold ${interiorFt > 0 ? 'text-red-600' : 'text-green-600'}`}>{interiorFt > 0 ? `${interiorFt.toFixed(1)} ft` : 'None'}</span></p>
                      )}
                      {/* Structure vs Contents breakdown */}
                      <hr className="border-gray-200 !my-1.5" />
                      <p className="flex justify-between"><span className="text-gray-500">Structure:</span> <span className="font-medium">{structPct}%{structLoss != null ? ` ($${structLoss.toLocaleString()})` : ''}</span></p>
                      <p className="flex justify-between"><span className="text-gray-500">Contents:</span> <span className="font-medium">{contPct}%{contLoss != null ? ` ($${contLoss.toLocaleString()})` : ''}</span></p>
                      <p className="flex justify-between text-sm"><span className="text-gray-500">Total loss:</span> <span className="font-bold text-red-600">${(p.estimated_loss_usd ?? 0).toLocaleString()}</span></p>
                      {/* Loss confidence interval from ±30% depth uncertainty */}
                      {p.loss_low_usd != null && p.loss_high_usd != null && (
                        <div className="bg-gray-50 rounded px-2 py-1 mt-0.5 border border-gray-200">
                          <div className="flex justify-between text-[10px]">
                            <span className="text-gray-500">Loss range (±30% depth):</span>
                          </div>
                          <div className="flex items-center gap-1 mt-0.5">
                            <span className="text-[10px] text-green-700 font-bold">${p.loss_low_usd.toLocaleString()}</span>
                            <div className="flex-1 h-1.5 rounded-full bg-gradient-to-r from-green-300 via-yellow-300 to-red-400 relative">
                              {p.estimated_loss_usd != null && p.loss_high_usd > p.loss_low_usd && (
                                <div className="absolute top-[-1px] w-1.5 h-2 bg-white border border-gray-600 rounded-sm" style={{ left: `${Math.min(100, ((p.estimated_loss_usd - p.loss_low_usd) / (p.loss_high_usd - p.loss_low_usd)) * 100)}%` }} />
                              )}
                            </div>
                            <span className="text-[10px] text-red-700 font-bold">${p.loss_high_usd.toLocaleString()}</span>
                          </div>
                        </div>
                      )}
                      <p className="text-[10px] text-gray-400 mt-0.5">Surge/flood loss (FEMA HAZUS depth-damage curves)</p>
                      {/* Wind damage from StormDPS wind model */}
                      {p.wind_damage_pct != null && p.wind_damage_pct > 0 && (
                        <div className="bg-sky-50 rounded px-2 py-1 mt-1 border border-sky-200">
                          <div className="flex justify-between text-[10px]">
                            <span className="text-sky-700 font-bold">Wind Damage: {p.wind_damage_pct}%</span>
                            <span className="text-sky-700 font-bold">${(p.wind_loss_usd ?? 0).toLocaleString()}</span>
                          </div>
                          {p.combined_loss_usd != null && (
                            <div className="flex justify-between text-[11px] mt-0.5 pt-0.5 border-t border-sky-200">
                              <span className="font-bold text-gray-700">Combined Loss (Surge + Wind):</span>
                              <span className="font-bold text-red-700">${p.combined_loss_usd.toLocaleString()}</span>
                            </div>
                          )}
                          <p className="text-[9px] text-sky-500 mt-0.5">Wind model: StormDPS Emanuel/HAZUS curve{p.wind_speed_mph ? ` at ${p.wind_speed_mph} mph` : ''}</p>
                        </div>
                      )}
                      {/* FEMA IHP eligibility estimate */}
                      {p.ihp_eligible != null && (
                        <div className={`mt-1 rounded px-2 py-1 border text-[10px] ${p.ihp_eligible ? 'bg-emerald-50 border-emerald-200' : 'bg-gray-50 border-gray-200'}`}>
                          <div className="flex justify-between items-center">
                            <span className={`font-bold uppercase tracking-wider ${p.ihp_eligible ? 'text-emerald-700' : 'text-gray-500'}`}>
                              FEMA IHP: {p.ihp_eligible ? 'Likely Eligible' : 'Not Eligible'}
                            </span>
                            {p.ihp_est_amount != null && (
                              <span className="font-bold text-emerald-700">Est. ${p.ihp_est_amount.toLocaleString()}</span>
                            )}
                          </div>
                          {p.ihp_category && (
                            <p className="text-gray-500 mt-0.5">Category: {p.ihp_category.charAt(0).toUpperCase() + p.ihp_category.slice(1)} — Owner-occupied residential only</p>
                          )}
                        </div>
                      )}
                      {/* Deductible threshold flag */}
                      {p.estimated_loss_usd != null && (() => {
                        const loss = p.estimated_loss_usd;
                        const [label, bg, text] = loss < 1250
                          ? ['Below Min Deductible', 'bg-green-100', 'text-green-700']
                          : loss < 10000
                          ? ['Below Typical Deductible', 'bg-yellow-100', 'text-yellow-700']
                          : ['Exceeds Deductible', 'bg-red-100', 'text-red-700'];
                        return (
                          <div className={`${bg} ${text} text-[10px] font-bold rounded px-2 py-0.5 text-center mt-1`}>{label}</div>
                        );
                      })()}

                      {/* ── Wind vs Water Attribution ── */}
                      {wwSplit && estWindMph != null && (
                        <>
                        <hr className="border-gray-200 !my-1.5" />
                        <div className="text-[10px] font-bold text-gray-500 uppercase tracking-wider">Peril Attribution</div>
                        <p className="text-[9px] text-gray-500 mb-0.5">Peril split uses StormDPS wind model + HAZUS flood curves.</p>
                        <p className="flex justify-between"><span className="text-gray-500">Est. wind:</span> <span className="font-medium">{estWindMph} mph</span></p>
                        <div className="flex items-center gap-1.5 mt-0.5">
                          <div className="flex-1 h-3 rounded-full overflow-hidden bg-gray-200 flex">
                            <div className="h-full bg-sky-500 transition-all" style={{ width: `${wwSplit.windPct}%` }} />
                            <div className="h-full bg-indigo-600 transition-all" style={{ width: `${wwSplit.waterPct}%` }} />
                          </div>
                        </div>
                        <div className="flex justify-between text-[10px] mt-0.5">
                          <span className="text-sky-600 font-bold">Wind {wwSplit.windPct}%</span>
                          <span className="text-indigo-700 font-bold">Water {wwSplit.waterPct}%</span>
                        </div>
                        {p.estimated_loss_usd != null && (
                          <div className="flex justify-between text-[10px] text-gray-500">
                            <span>${Math.round(p.estimated_loss_usd * wwSplit.windPct / 100).toLocaleString()}</span>
                            <span>${Math.round(p.estimated_loss_usd * wwSplit.waterPct / 100).toLocaleString()}</span>
                          </div>
                        )}
                        </>
                      )}

                      {/* ── Comparable Loss Evidence ── */}
                      {comps.count >= 2 && (
                        <>
                        <hr className="border-gray-200 !my-1.5" />
                        <div className="bg-blue-50 rounded-lg px-2 py-1.5 border border-blue-200">
                          <div className="text-[10px] font-bold text-blue-700 uppercase tracking-wider mb-0.5">Comparable Properties</div>
                          <p className="text-[11px] text-blue-900">
                            <strong>{comps.count}</strong> similar {friendlyBuildingType(p.building_type).toLowerCase()}s within {(COMP_RADIUS_KM / 1.609).toFixed(2)} mi averaged{' '}
                            <strong className="text-blue-700">${comps.avgLoss.toLocaleString()}</strong> in modeled losses
                          </p>
                          <p className="text-[10px] text-blue-500 mt-0.5">
                            Range: ${comps.minLoss.toLocaleString()} – ${comps.maxLoss.toLocaleString()}
                          </p>
                        </div>
                        </>
                      )}

                      {/* ── Building Flag / Annotation ── */}
                      {pinnedInfo && (() => {
                        const flagKey = `${popupInfo.lng.toFixed(5)},${popupInfo.lat.toFixed(5)}`;
                        const currentFlag = buildingFlags[flagKey] || '';
                        const FLAG_OPTIONS = [
                          { value: '', label: 'No flag', color: 'bg-gray-100 text-gray-600' },
                          { value: 'confirmed_destroyed', label: 'Confirmed Destroyed', color: 'bg-red-100 text-red-700' },
                          { value: 'major_damage', label: 'Major Damage', color: 'bg-orange-100 text-orange-700' },
                          { value: 'shelter_in_place', label: 'Shelter-in-Place', color: 'bg-yellow-100 text-yellow-700' },
                          { value: 'inspected_ok', label: 'Inspected OK', color: 'bg-green-100 text-green-700' },
                          { value: 'inaccessible', label: 'Inaccessible', color: 'bg-purple-100 text-purple-700' },
                        ];
                        return (
                          <>
                          <hr className="border-gray-200 !my-1.5" />
                          <div className="text-[10px] font-bold text-gray-500 uppercase tracking-wider mb-1">Field Assessment Flag</div>
                          <div className="flex flex-wrap gap-1">
                            {FLAG_OPTIONS.map(opt => (
                              <button
                                key={opt.value}
                                onClick={() => setBuildingFlags(prev => {
                                  const next = { ...prev };
                                  if (opt.value === '') delete next[flagKey];
                                  else next[flagKey] = opt.value;
                                  return next;
                                })}
                                className={`text-[10px] font-bold px-2 py-0.5 rounded transition-colors ${currentFlag === opt.value ? opt.color + ' ring-2 ring-offset-1 ring-indigo-400' : 'bg-gray-50 text-gray-400 hover:bg-gray-100'}`}
                              >{opt.label}</button>
                            ))}
                          </div>
                          </>
                        );
                      })()}

                      {/* ── Generate Claim Report ── */}
                      {pinnedInfo && (
                        <>
                        <hr className="border-gray-200 !my-1.5" />
                        <button
                          onClick={() => generateClaimDoc(popupInfo.feature, hoverAddress)}
                          className="w-full text-center text-[11px] font-bold text-white bg-indigo-600 hover:bg-indigo-500 active:bg-indigo-700 rounded-md py-1.5 transition-colors"
                          title="Download a formatted claims documentation report for this property"
                        >📄 Generate Claims Report</button>
                        </>
                      )}
                      <p
                        className="text-[9px] text-gray-400 mt-1 pt-1 border-t border-gray-100 text-center"
                        title="Per-building valuations from USACE National Structure Inventory v2 (released Oct 2023). Depth-damage and wind fragility curves from FEMA HAZUS 5.1 (2020). Replacement values are not inflation-adjusted and may underestimate current rebuild costs."
                      >
                        NSI v2 (2023) · HAZUS 5.1 (2020)
                      </p>
                    </div></>
                    );
                  })()
                )}
              </div>
            </Popup>
          )}
        </Map>

        {/* ── Mobile sidebar toggle — always visible on small screens ── */}
        {!sidebarOpen && (
          <button
            onClick={() => setSidebarOpen(true)}
            className="lg:hidden absolute top-3 left-3 z-20 bg-slate-900/95 backdrop-blur text-white rounded-lg shadow-lg border border-slate-700 w-10 h-10 flex items-center justify-center text-lg hover:bg-slate-800 transition-colors"
            aria-label="Open storm browser"
          >☰</button>
        )}

        {/* ── Address search bar (top-center of map) — always visible ── */}
        <div className="absolute top-3 left-1/2 -translate-x-1/2 z-10 flex items-center gap-2">
          <div className="flex bg-white/95 backdrop-blur shadow-lg rounded-lg overflow-hidden border border-gray-200">
            <input
              type="text"
              placeholder={activeStorm ? "Search address, e.g. 412 N Austin St, Rockport TX" : "Load a storm to search addresses"}
              value={addressQuery}
              onChange={e => { setAddressQuery(e.target.value); setAddressError(''); }}
              onKeyDown={e => e.key === 'Enter' && activeStorm && handleAddressSearch()}
              disabled={!activeStorm || addressSearching}
              className="px-3 py-2 text-sm text-gray-800 placeholder-gray-400 w-72 outline-none disabled:bg-gray-100 disabled:text-gray-400"
            />
            <button
              onClick={handleAddressSearch}
              disabled={!activeStorm || addressSearching}
              className="px-3 py-2 bg-indigo-500 hover:bg-indigo-400 text-white text-sm font-medium transition-colors disabled:opacity-50 disabled:bg-gray-300"
            >{addressSearching ? '...' : 'Go'}</button>
          </div>
          {addressError && <span className="text-xs text-red-500 bg-white/90 px-2 py-1 rounded shadow">{addressError}</span>}
          {/* Action buttons */}
          {activeStorm && allBuildings?.features?.length > 0 && (
            <>
              <button
                onClick={handleExportCSV}
                className="bg-white/95 backdrop-blur shadow-lg rounded-lg px-3 py-2 text-sm font-medium text-gray-700 hover:text-indigo-600 hover:bg-white border border-gray-200 transition-colors"
                title="Download all loaded buildings as CSV"
              >Export CSV</button>
              <button
                onClick={() => setBatchOpen(true)}
                className="bg-white/95 backdrop-blur shadow-lg rounded-lg px-3 py-2 text-sm font-medium text-gray-700 hover:text-indigo-600 hover:bg-white border border-gray-200 transition-colors"
                title="Look up multiple addresses at once"
              >Batch Lookup</button>
              {/* Flag count indicator */}
              {Object.keys(buildingFlags).length > 0 && (
                <span className="bg-purple-100 text-purple-700 text-xs font-bold px-2 py-2 rounded-lg shadow-lg border border-purple-200">
                  {Object.keys(buildingFlags).length} flagged
                </span>
              )}
              {/* More dropdown menu */}
              <div className="relative">
                <button
                  onClick={() => setMoreMenuOpen(!moreMenuOpen)}
                  className="bg-white/95 backdrop-blur shadow-lg rounded-lg px-3 py-2 text-sm font-medium text-gray-700 hover:text-indigo-600 hover:bg-white border border-gray-200 transition-colors"
                  title="More actions"
                >More</button>
                {moreMenuOpen && (
                  <>
                    <div className="fixed inset-0 z-30" onClick={() => setMoreMenuOpen(false)} />
                    <div className="absolute right-0 mt-1 bg-white/95 backdrop-blur shadow-lg rounded-lg border border-gray-200 overflow-hidden z-40 w-40">
                      <button
                        onClick={() => { handlePrint(); setMoreMenuOpen(false); }}
                        className="w-full text-left px-4 py-2 text-sm text-gray-700 hover:bg-gray-100 transition-colors flex items-center gap-2"
                        title="Print current map view"
                      >🖨 Print</button>
                      <button
                        onClick={() => { handleShareLink(); setMoreMenuOpen(false); }}
                        className="w-full text-left px-4 py-2 text-sm text-gray-700 hover:bg-gray-100 transition-colors flex items-center gap-2"
                        title="Copy share link to clipboard"
                      >🔗 Share</button>
                      <button
                        onClick={() => { setMethodologyOpen(m => !m); setMoreMenuOpen(false); }}
                        className="w-full text-left px-4 py-2 text-sm text-gray-700 hover:bg-gray-100 transition-colors flex items-center gap-2"
                        title="View methodology and data sources"
                      >ℹ️ Methodology</button>
                      <button
                        onClick={() => { handleExportPDA(); setMoreMenuOpen(false); }}
                        className="w-full text-left px-4 py-2 text-sm text-gray-700 hover:bg-gray-100 transition-colors flex items-center gap-2"
                        title="Export FEMA Preliminary Damage Assessment summary — data will only include loaded cells"
                      >📋 PDA Report (.csv)</button>
                      <button
                        onClick={() => { handleExportClaimsPackage(); setMoreMenuOpen(false); }}
                        className="w-full text-left px-4 py-2 text-sm text-gray-700 hover:bg-gray-100 transition-colors flex items-center gap-2"
                        title="Export claims documentation package with peril attribution and triage data for all damaged buildings"
                      >📄 Claims Package (.csv)</button>
                      <div className="border-t border-gray-200" />
                      <button
                        onClick={() => { setShowLandUse(v => !v); setMoreMenuOpen(false); }}
                        className={`w-full text-left px-4 py-2 text-sm transition-colors flex items-center gap-2 ${showLandUse ? 'bg-emerald-50 text-emerald-900 hover:bg-emerald-100' : 'text-gray-700 hover:bg-gray-100'}`}
                        title="Toggle USGS/MRLC NLCD 2021 land cover overlay. 30 m CONUS raster with residential, commercial, agricultural, and natural land classes."
                      >🗺 Land use {showLandUse ? '✓' : ''}</button>
                      <button
                        onClick={() => { setBetaLayersEnabled(v => !v); setMoreMenuOpen(false); }}
                        className={`w-full text-left px-4 py-2 text-sm transition-colors flex items-center gap-2 ${betaLayersEnabled ? 'bg-purple-50 text-purple-900 hover:bg-purple-100' : 'text-gray-700 hover:bg-gray-100'}`}
                        title="Toggle Phase 5 beta data layers (Rainfall, Shelter capacity, Vendor coverage, Time-to-access). These panels show the UX shape; real data is pending backend integration."
                      >🧪 Beta layers {betaLayersEnabled ? '✓' : ''}</button>
                    </div>
                  </>
                )}
              </div>
            </>
          )}
        </div>

        {/* Nuisance flood warning banner */}
        {activeStorm && impactTotals.buildings > 2000 && impactTotals.totalDepth > 0 && (impactTotals.totalDepth / impactTotals.buildings) < 1.5 && !nuisanceDismissed && (
          <div className="absolute top-20 left-1/2 -translate-x-1/2 z-10 bg-amber-100 border border-amber-300 rounded-lg px-4 py-2.5 text-sm text-amber-900 shadow-lg flex items-center justify-between max-w-sm gap-3">
            <span className="flex-1"><strong>Nuisance Flood Warning:</strong> Many minor tidal inundations with very shallow depths.</span>
            <button
              onClick={() => setNuisanceDismissed(true)}
              className="text-amber-700 hover:text-amber-900 font-bold text-lg shrink-0"
            >✕</button>
          </div>
        )}

        {/* Dashboard overlay */}
        <DashboardPanel storm={activeStorm} totals={impactTotals} loadedCells={loadedCells} loadingCells={loadingCells} confidence={confidence} eli={eli} validatedDps={validatedDps} mode={mode} onModeChange={setMode} subPersona={subPersona} onSubPersonaChange={setSubPersona} onOpenSidebar={() => setSidebarOpen(true)} zoom={zoom} estimatedPop={estimatedPop} severityCounts={severityCounts} criticalCount={criticalCount} criticalBreakdown={criticalBreakdown} hotspots={hotspots} onFlyTo={handleFlyToHotspot} onGenerateCatReport={handleGenerateCatReport} onGenerateSitRep={handleGenerateSitRep} teamSize={teamSize} windowDays={windowDays} onTeamSizeChange={setTeamSize} onWindowDaysChange={setWindowDays} betaLayersEnabled={betaLayersEnabled} countyRollup={countyRollup} countiesGeoJSON={countiesGeoJSON} totalDisplaced={totalDisplaced} showCounties={showCounties} onClearStorm={() => {
          setActiveStorm(null); setAllBuildings(null); setAllFlood(null);
          setLoadedCells(new Set()); setLoadingCells(new Set());
          setImpactTotals({ buildings: 0, loss: 0, totalDepth: 0 }); setHoverInfo(null);
          setPinnedInfo(null);
          setConfidence({ level: 'unvalidated', count: 0 }); setEli({ value: 0, tier: 'unavailable' });
          setValidatedDps({ value: 0, adj: 0, reason: '' }); setManifest({});
          setBatchResults([]); setBatchOpen(false); setAddressQuery(''); setAddressError(''); setMethodologyOpen(false);
          setShowCounties(false); setShowFloodZones(false); setShowLandUse(false); setShowGauges(false); setHazardView('surge'); setBuildingFlags({});
          setSimMode(false); setSimResult(null); setForecastCone(null); setForecastTrack([]);
        }} />

        {/* ── Simulator panel (active storms with forecast track) ── */}
        {activeStorm && activeStorm.status === 'active' && forecastTrack.length > 0 && (
          <div className="absolute bottom-4 left-1/2 -translate-x-1/2 z-30">
            {!simMode ? (
              <button
                onClick={() => setSimMode(true)}
                className="bg-red-600 hover:bg-red-700 text-white text-sm font-bold px-5 py-2.5 rounded-full shadow-lg transition-all hover:scale-105 flex items-center gap-2"
              >
                <span className="text-lg">🎯</span> Open Landfall Simulator
              </button>
            ) : (
              <div className="bg-gray-900/95 backdrop-blur rounded-xl shadow-2xl border border-gray-700 px-5 py-4 max-w-md">
                <div className="flex items-center justify-between mb-3">
                  <h3 className="text-white font-bold text-sm flex items-center gap-2">
                    <span className="text-lg">🎯</span> Landfall Simulator
                  </h3>
                  <button onClick={() => { setSimMode(false); setSimResult(null); }}
                    className="text-gray-400 hover:text-white text-xs px-1">✕</button>
                </div>

                <p className="text-gray-400 text-xs mb-3">
                  Drag the red marker along the coastline to test different landfall scenarios within the NHC forecast cone.
                </p>

                {simMarker && (
                  <div className="text-xs text-gray-300 mb-3 flex gap-4">
                    <span>Lat: <strong>{simMarker.lat.toFixed(3)}</strong></span>
                    <span>Lon: <strong>{simMarker.lng.toFixed(3)}</strong></span>
                  </div>
                )}

                <button
                  onClick={runSimulation}
                  disabled={simRunning || !simMarker}
                  className={`w-full py-2.5 rounded-lg font-bold text-sm transition-all ${
                    simRunning
                      ? 'bg-gray-700 text-gray-400 cursor-wait'
                      : 'bg-red-600 hover:bg-red-700 text-white hover:scale-[1.02]'
                  }`}
                >
                  {simRunning ? 'Simulating...' : 'Simulate Losses'}
                </button>

                {/* Simulation results */}
                {simResult && (
                  <div className="mt-3 pt-3 border-t border-gray-700 space-y-2">
                    <div className="text-center">
                      <div className="text-[10px] text-gray-500 uppercase font-bold tracking-wider">Simulated Loss (Center Cell)</div>
                      <div className="text-2xl font-black text-red-500">
                        ${simResult.summary?.total_loss_M?.toLocaleString()}M
                      </div>
                      <div className="text-xs text-gray-400">
                        {simResult.summary?.buildings_assessed?.toLocaleString()} properties · {simResult.summary?.buildings_damaged?.toLocaleString()} damaged
                      </div>
                    </div>
                    {simResult.population?.pop_label && (
                      <div className="text-xs text-gray-400 text-center">
                        {simResult.population.pop_label} in {simResult.population.county_name}, {simResult.population.state_code}
                      </div>
                    )}
                    {simResult.prediction && (
                      <div className="text-[10px] text-gray-500 text-center mt-1">
                        Confidence range: ${(simResult.prediction.low / 1e6).toFixed(0)}M – ${(simResult.prediction.high / 1e6).toFixed(0)}M
                      </div>
                    )}
                  </div>
                )}
              </div>
            )}
          </div>
        )}

        {/* Cell loading progress indicator */}
        {loadingCells.size > 0 && (
          <div className="absolute top-20 right-4 z-30 bg-slate-900/95 backdrop-blur border border-indigo-500/40 text-indigo-300 text-xs font-semibold px-4 py-2.5 rounded-lg shadow-xl flex items-center gap-2.5">
            <span className="w-3 h-3 rounded-full border-2 border-indigo-400 border-t-transparent animate-spin shrink-0" />
            <span>Loading {loadingCells.size} area{loadingCells.size !== 1 ? 's' : ''}…</span>
            {loadedCells.size > 0 && <span className="text-slate-500">· {loadedCells.size} done</span>}
          </div>
        )}

        {/* Cell error toast with optional retry */}
        {cellError && (
          <div className="absolute bottom-6 left-1/2 -translate-x-1/2 z-30 bg-red-600 text-white text-sm px-4 py-2.5 rounded-lg shadow-xl max-w-md text-center flex items-center gap-3">
            <span>{cellError}</span>
            {retryStormId && (
              <button
                onClick={() => { setCellError(null); setRetryStormId(null); activateStorm(retryStormId); }}
                className="shrink-0 bg-white/20 hover:bg-white/30 text-white text-xs font-semibold px-3 py-1 rounded transition-colors"
              >Retry</button>
            )}
          </div>
        )}
        {/* Success toast */}
        {toastSuccess && (
          <div className="absolute bottom-6 left-1/2 -translate-x-1/2 z-30 bg-green-600 text-white text-sm px-4 py-2.5 rounded-lg shadow-xl max-w-sm text-center">
            {toastSuccess}
          </div>
        )}

        {/* Empty-state overlay — shown when no storm is active */}
        {!activeStorm && !activating && (
          <div className="absolute inset-0 flex items-center justify-center z-10 pointer-events-none">
            <div className="bg-slate-900/95 backdrop-blur-sm rounded-2xl px-8 py-6 text-center shadow-2xl border border-slate-700 max-w-sm w-full mx-4 pointer-events-auto">
              <img src="/surgedps/logo-180.png" alt="SurgeDPS" className="w-16 h-16 mx-auto mb-3 rounded-2xl" style={{ boxShadow: '0 4px 20px rgba(99,102,241,0.4)', filter: 'brightness(1.15)' }} />
              <p className="text-white font-bold text-lg">Select a storm to begin</p>
              <p className="text-slate-400 text-sm mt-1 mb-4">Choose a hurricane from the list on the left to see surge depths and damage estimates across the impact zone.</p>
              {/* Quick-launch notable storms */}
              <div className="text-[10px] text-slate-500 uppercase tracking-widest font-bold mb-2">Notable Storms</div>
              <div className="grid grid-cols-2 gap-2">
                {([
                  { id: 'harvey_2017',  label: 'Harvey 2017',  cat: 4, color: CAT_COLORS[4] },
                  { id: 'katrina_2005', label: 'Katrina 2005', cat: 3, color: CAT_COLORS[3] },
                  { id: 'ian_2022',     label: 'Ian 2022',     cat: 4, color: CAT_COLORS[4] },
                  { id: 'michael_2018', label: 'Michael 2018', cat: 5, color: CAT_COLORS[5] },
                ] as { id: string; label: string; cat: number; color: string }[]).map(({ id, label, cat, color }) => (
                  <button
                    key={id}
                    onClick={() => activateStorm(id)}
                    className="flex items-center gap-2 bg-slate-800 hover:bg-slate-700 border border-slate-700 hover:border-slate-500 text-slate-200 text-xs font-semibold px-3 py-2.5 rounded-lg transition-colors text-left"
                  >
                    <span className="w-2 h-2 rounded-full shrink-0" style={{ backgroundColor: color }} />
                    <span className="flex-1 truncate">{label}</span>
                    <span className="text-[10px] font-bold text-white px-1 py-0.5 rounded shrink-0" style={{ backgroundColor: color }}>C{cat}</span>
                  </button>
                ))}
              </div>
              <button
                onClick={() => setSidebarOpen(true)}
                className="lg:hidden mt-3 w-full bg-indigo-600/30 hover:bg-indigo-600/50 border border-indigo-500/40 text-indigo-300 text-sm font-semibold px-4 py-2 rounded-lg transition-colors"
              >☰ Browse All Storms</button>
              <p className="text-slate-600 text-[10px] mt-4">DPS = Damage Potential Score · higher score = more destructive surge</p>
            </div>
          </div>
        )}

        {/* Loading overlay with real progress */}
        {activating && (
          <div className="absolute inset-0 bg-black/60 backdrop-blur-sm flex items-center justify-center z-20" style={{ animation: 'fadeIn 0.2s ease-out' }}>
            <div className="bg-slate-900 border border-slate-700 rounded-2xl p-7 shadow-2xl text-center min-w-[300px] max-w-sm">
              {/* Animated ring */}
              <div className="relative w-14 h-14 mx-auto mb-4">
                <div className="absolute inset-0 animate-spin rounded-full border-4 border-slate-700 border-t-indigo-500"></div>
                <div className="absolute inset-2 rounded-full bg-slate-900 flex items-center justify-center">
                  <span className="text-indigo-400 text-lg">🌀</span>
                </div>
              </div>
              <p className="font-bold text-white text-base">
                {loadProgress.step_num === 0 ? 'Loading storm data…' : 'Analyzing storm…'}
              </p>
              <p className="text-xs text-indigo-300 mt-1 font-medium">{loadProgress.step || 'Connecting to server…'}</p>
              {/* Progress bar */}
              <div className="mt-4 w-full bg-slate-700 rounded-full h-1.5 overflow-hidden">
                <div
                  className="h-full bg-indigo-500 rounded-full transition-all duration-700 ease-out"
                  style={{ width: `${Math.max(4, (loadProgress.step_num / loadProgress.total_steps) * 100)}%` }}
                />
              </div>
              <p className="text-[10px] text-slate-500 mt-2">
                Step {loadProgress.step_num} of {loadProgress.total_steps}
                {loadProgress.elapsed > 0 ? ` · ${Math.round(loadProgress.elapsed)}s elapsed` : ''}
              </p>
              <button
                onClick={() => activateAbortRef.current?.abort()}
                className="mt-4 text-xs text-slate-500 hover:text-slate-300 transition-colors font-medium border border-slate-700 hover:border-slate-500 rounded-lg px-4 py-1.5"
              >Cancel</button>
            </div>
          </div>
        )}

        {/* ── Batch Address Lookup Modal ── */}
        {batchOpen && (
          <div className="absolute inset-0 bg-black/50 flex items-center justify-center z-40">
            <div className="bg-white rounded-xl shadow-2xl w-[560px] max-w-[95vw] max-h-[85vh] flex flex-col">
              {/* Header */}
              <div className="flex items-center justify-between px-5 py-3.5 border-b border-gray-200">
                <h2 className="font-bold text-gray-800 text-base">Batch Address Lookup</h2>
                <button onClick={() => setBatchOpen(false)} className="text-gray-400 hover:text-gray-600 text-lg">✕</button>
              </div>
              {/* Body */}
              <div className="px-5 py-4 flex-1 overflow-y-auto">
                <p className="text-xs text-gray-500 mb-2">Paste one address per line. Each will be geocoded and matched to the nearest loaded building within 200 m.</p>
                <textarea
                  value={batchInput}
                  onChange={e => setBatchInput(e.target.value)}
                  placeholder={"412 N Austin St, Rockport, TX\n1024 Main St, Port Aransas, TX\n..."}
                  rows={6}
                  className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm text-gray-800 placeholder-gray-400 outline-none focus:ring-2 focus:ring-indigo-500 resize-none"
                />
                <div className="flex gap-2 mt-3">
                  <button
                    onClick={handleBatchLookup}
                    disabled={batchLoading || !batchInput.trim()}
                    className="bg-indigo-500 hover:bg-indigo-400 disabled:opacity-50 text-white text-sm font-semibold px-4 py-2 rounded-lg transition-colors"
                  >{batchLoading ? 'Looking up...' : 'Look Up Addresses'}</button>
                  {batchLoading && (
                    <button
                      onClick={() => { batchAbortRef.current = true; }}
                      className="bg-gray-100 hover:bg-gray-200 text-gray-700 text-sm font-medium px-4 py-2 rounded-lg transition-colors"
                    >Cancel</button>
                  )}
                  {batchResults.length > 0 && (
                    <button
                      onClick={handleBatchExport}
                      className="bg-gray-100 hover:bg-gray-200 text-gray-700 text-sm font-medium px-4 py-2 rounded-lg transition-colors"
                    >Export CSV</button>
                  )}
                </div>

                {/* Results table */}
                {batchResults.length > 0 && (
                  <div className="mt-4 border border-gray-200 rounded-lg overflow-hidden">
                    <table className="w-full text-xs">
                      <thead className="bg-gray-50 text-gray-500 uppercase">
                        <tr>
                          <th className="text-left px-3 py-2">Address</th>
                          <th className="text-left px-3 py-2">Status</th>
                          <th className="text-right px-3 py-2">Surge (ft)</th>
                          <th className="text-right px-3 py-2">Loss</th>
                          <th className="text-center px-3 py-2">Deductible</th>
                        </tr>
                      </thead>
                      <tbody className="divide-y divide-gray-100">
                        {batchResults.map((r, i) => {
                          const loss = r.estimated_loss_usd ?? 0;
                          const dedFlag = r.status !== 'matched' ? '—'
                            : loss < 1250 ? 'Below Min'
                            : loss < 10000 ? 'Below Typical'
                            : 'Exceeds';
                          const dedColor = r.status !== 'matched' ? 'text-gray-400'
                            : loss < 1250 ? 'text-green-600'
                            : loss < 10000 ? 'text-yellow-600'
                            : 'text-red-600';
                          return (
                            <tr key={i} className="hover:bg-gray-50">
                              <td className="px-3 py-1.5 truncate max-w-[180px]" title={r.address}>{r.address}</td>
                              <td className="px-3 py-1.5">
                                <span className={`font-medium ${r.status === 'matched' ? 'text-green-600' : 'text-gray-400'}`}>{r.status}</span>
                              </td>
                              <td className="px-3 py-1.5 text-right font-medium">{r.status === 'matched' ? (r.depth_ft != null ? r.depth_ft.toFixed(1) : '—') : '—'}</td>
                              <td className="px-3 py-1.5 text-right font-bold text-red-600">{r.status === 'matched' ? `$${loss.toLocaleString()}` : '—'}</td>
                              <td className={`px-3 py-1.5 text-center font-bold text-[10px] ${dedColor}`}>{dedFlag}</td>
                            </tr>
                          );
                        })}
                      </tbody>
                    </table>
                  </div>
                )}
              </div>
            </div>
          </div>
        )}

        {/* ── Methodology Disclosure Panel ── */}
        {methodologyOpen && (
          <div className="absolute bottom-4 right-4 z-30 bg-white/95 backdrop-blur shadow-2xl rounded-xl w-80 max-w-[90vw] max-h-[60vh] overflow-y-auto border border-gray-200">
            <div className="flex items-center justify-between px-4 py-3 border-b border-gray-200 sticky top-0 bg-white/95 backdrop-blur rounded-t-xl">
              <h3 className="font-bold text-gray-800 text-sm">Methodology & Data Sources</h3>
              <button onClick={() => setMethodologyOpen(false)} className="text-gray-400 hover:text-gray-600 text-sm">✕</button>
            </div>
            <div className="px-4 py-3 text-xs text-gray-600 space-y-3">
              <div>
                <h4 className="font-bold text-gray-800 mb-1">Storm Surge Model</h4>
                <p>Surge inundation depths are derived from NOAA SLOSH (Sea, Lake, and Overland Surges from Hurricanes) maximum-of-maximums (MOM) grids, interpolated to building footprints using bilinear sampling.</p>
              </div>
              <div>
                <h4 className="font-bold text-gray-800 mb-1">Damage Estimation</h4>
                <p>Building damage percentages use FEMA HAZUS depth-damage functions. Separate curves are applied for structure and contents based on building occupancy type (e.g., RES1 = single-family residential). Foundation height is subtracted from surge depth to estimate interior flooding before applying the damage curve.</p>
              </div>
              <div>
                <h4 className="font-bold text-gray-800 mb-1">Building Data</h4>
                <p>Building footprints, occupancy types, and replacement values are sourced from Microsoft Building Footprints and NSI (National Structure Inventory). Coverage varies by region — the confidence badge reflects data completeness for the loaded area.</p>
              </div>
              <div>
                <h4 className="font-bold text-gray-800 mb-1">Damage Potential Score (DPS)</h4>
                <p>DPS combines Integrated Kinetic Energy (IKE), maximum surge depth, and regional building exposure into a single 0–100 index. Scores above 60 indicate historically severe surge events.</p>
              </div>
              <div>
                <h4 className="font-bold text-gray-800 mb-1">Limitations</h4>
                <p>This is a modeled estimate, not a field assessment. Actual damage depends on construction quality, mitigation measures, debris impact, and other factors not captured in the model. Loss figures should be treated as order-of-magnitude guidance, not precise valuations.</p>
              </div>
              {showFloodZones && (
                <div>
                  <h4 className="font-bold text-gray-800 mb-1">FEMA Flood Zones</h4>
                  <p>Sourced from FEMA's National Flood Hazard Layer (NFHL). Zone colors: <span className="text-red-500 font-bold">■ VE/V</span> coastal high-hazard (wave action + surge), <span className="text-orange-400 font-bold">■ AE/A</span> high-risk 100-year floodplain, <span className="text-yellow-400 font-bold">■ X</span> moderate/minimal risk. Zone labels appear at zoom ≥ 10.</p>
                </div>
              )}
              {showLandUse && (
                <div>
                  <h4 className="font-bold text-gray-800 mb-1">Land Use (NLCD 2021)</h4>
                  <p>USGS/MRLC National Land Cover Database 2021, 30 m CONUS raster served live via the MRLC WMS. Sixteen Anderson Level II classes are rolled up into the bottom-right legend. Note this is <em>land cover</em>, not municipal zoning — a vacant R-1 lot classifies as "Developed Open Space," not residential. For regulatory zoning (R-1, C-2, M-1) consult the jurisdiction's planning department or the National Zoning Atlas.</p>
                </div>
              )}
              {basemap === 'satellite' && (
                <div>
                  <h4 className="font-bold text-gray-800 mb-1">Satellite Imagery</h4>
                  <p>Basemap tiles are sourced from <strong>ESRI World Imagery</strong> (Maxar/DigitalGlobe). Acquisition date for the current view: <strong>{imageryDate ?? 'loading…'}</strong>. Date updates as you pan the map. Imagery age matters — post-storm captures will show debris and damage visible from above.</p>
                </div>
              )}
              <div className="text-[10px] text-gray-400 pt-2 border-t border-gray-200">
                SurgeDPS v1.0 — stormdps.com/surgedps
              </div>
            </div>
          </div>
        )}

        {/* Grid onboarding hint */}
        {showGrid && activeStorm && !gridHintDismissed && loadedCells.size > 0 && (
          <div className="absolute bottom-24 left-1/2 -translate-x-1/2 z-20 bg-slate-900/95 backdrop-blur shadow-lg rounded-lg px-4 py-3 border border-slate-700 max-w-xs text-center"
            style={{ animation: 'fadeInUp 0.4s ease-out' }}
          >
            <p className="text-sm text-slate-200 font-medium">Click the dashed borders around the loaded area to expand coverage</p>
            <button
              onClick={() => setGridHintDismissed(true)}
              className="mt-2 text-xs font-semibold text-indigo-400 hover:text-indigo-300 transition-colors"
            >Got it ✓</button>
          </div>
        )}

        {/* ── NLCD land-use legend (only when overlay is active) ──
            Maps the canonical MRLC palette down to the five categories
            stakeholders actually plan around. Colors are the ones MRLC
            renders server-side — we're just decoding them. */}
        {showLandUse && (
          <div className="absolute bottom-24 right-4 z-20 bg-white/95 backdrop-blur shadow-lg rounded-lg border border-gray-200 px-3 py-2 text-xs text-gray-700 max-w-[190px]">
            <div className="flex items-center justify-between mb-1.5">
              <span className="font-semibold text-gray-800">Land Use (NLCD 2021)</span>
              <button
                onClick={() => setShowLandUse(false)}
                className="text-gray-400 hover:text-gray-700 text-sm leading-none"
                title="Hide land-use overlay"
              >✕</button>
            </div>
            <div className="space-y-1">
              <div className="flex items-center gap-1.5"><span className="inline-block w-3 h-3 rounded-sm" style={{ background: '#AB0000' }} /><span>Developed — High</span></div>
              <div className="flex items-center gap-1.5"><span className="inline-block w-3 h-3 rounded-sm" style={{ background: '#D99282' }} /><span>Residential</span></div>
              <div className="flex items-center gap-1.5"><span className="inline-block w-3 h-3 rounded-sm" style={{ background: '#DCD939' }} /><span>Cropland</span></div>
              <div className="flex items-center gap-1.5"><span className="inline-block w-3 h-3 rounded-sm" style={{ background: '#AB7028' }} /><span>Pasture / Hay</span></div>
              <div className="flex items-center gap-1.5"><span className="inline-block w-3 h-3 rounded-sm" style={{ background: '#68AB5F' }} /><span>Forest</span></div>
              <div className="flex items-center gap-1.5"><span className="inline-block w-3 h-3 rounded-sm" style={{ background: '#5475A8' }} /><span>Water / Wetland</span></div>
            </div>
            <div className="mt-1.5 pt-1.5 border-t border-gray-200 text-[10px] text-gray-500 leading-tight">
              Source: USGS / MRLC 30 m raster
            </div>
          </div>
        )}

        {/* ── Satellite imagery date badge (bottom-right, above MapLibre attribution) ── */}
        {basemap === 'satellite' && (
          <div className="absolute bottom-7 right-2 z-20 pointer-events-none">
            <div className="bg-black/55 backdrop-blur-sm text-white/80 text-[10px] px-1.5 py-0.5 rounded-sm flex items-center gap-1">
              <span>📅</span>
              <span>{imageryDate ?? '…'}</span>
            </div>
          </div>
        )}

        {/* ── Basemap toggle + map controls (bottom-left of map) — always visible ── */}
        <div className="absolute bottom-4 left-4 z-20 flex flex-col gap-2">
          {/* View mode pill: Damage vs Population. Swaps the bubble pipeline
              (same county → city → building hierarchy) between damage-weighted
              and population-displaced metrics. */}
          {activeStorm && (
            <div className="flex bg-white/90 backdrop-blur rounded-lg shadow-lg border border-gray-200 overflow-hidden">
              <button
                onClick={() => setMapView('damage')}
                title="Color/size bubbles by building damage & loss"
                className={`px-3 py-1.5 text-xs font-semibold transition-colors ${mapView === 'damage' ? 'bg-rose-600 text-white' : 'text-gray-600 hover:bg-gray-100'}`}
              >🏚️ Damage</button>
              <button
                onClick={() => setMapView('population')}
                title="Color/size bubbles by estimated displaced population"
                className={`px-3 py-1.5 text-xs font-semibold transition-colors ${mapView === 'population' ? 'bg-indigo-600 text-white' : 'text-gray-600 hover:bg-gray-100'}`}
              >👥 Population</button>
            </div>
          )}
          {/* Hazard view pill: Surge | Rainfall | Compound.
              Surge is the current default; Rainfall and Compound currently
              show an informational badge with MRMS accumulation stats —
              full raster tiles land in Phase 6 via a COG tile server. */}
          {activeStorm && (
            <div className="flex bg-white/90 backdrop-blur rounded-lg shadow-lg border border-gray-200 overflow-hidden">
              <button
                onClick={() => setHazardView('surge')}
                title="Coastal storm surge depth (default)"
                className={`px-3 py-1.5 text-xs font-semibold transition-colors ${hazardView === 'surge' ? 'bg-sky-600 text-white' : 'text-gray-600 hover:bg-gray-100'}`}
              >🌊 Surge</button>
              <button
                onClick={() => setHazardView('rainfall')}
                title="MRMS observed rainfall accumulation (stats only — raster tiles coming Phase 6)"
                className={`px-3 py-1.5 text-xs font-semibold transition-colors ${hazardView === 'rainfall' ? 'bg-indigo-600 text-white' : 'text-gray-600 hover:bg-gray-100'}`}
              >🌧️ Rainfall</button>
              <button
                onClick={() => setHazardView('compound')}
                title="Surge + rainfall + fluvial combined (the actual damage-model input)"
                className={`px-3 py-1.5 text-xs font-semibold transition-colors ${hazardView === 'compound' ? 'bg-violet-600 text-white' : 'text-gray-600 hover:bg-gray-100'}`}
              >💧 Compound</button>
            </div>
          )}
          {/* Rainfall / Compound informational badge — shown only when
              those views are selected. Gives immediate value today (max/avg
              accumulation for the storm bbox) while the raster pipeline is
              still stubbed. When tileUrlTemplate lands this whole badge can
              be replaced by the real legend. */}
          {activeStorm && hazardView !== 'surge' && (
            <div className="bg-white/95 backdrop-blur rounded-lg shadow-lg border border-gray-200 px-3 py-2 text-[11px] text-gray-700 max-w-[240px] leading-tight">
              <div className="font-semibold text-gray-900 mb-0.5">
                {hazardView === 'rainfall' ? '🌧️ Observed rainfall' : '💧 Compound hazard'}
              </div>
              {rainfallLoading && <div className="text-gray-500">Loading MRMS…</div>}
              {!rainfallLoading && rainfallStats && rainfallStats.maxIn != null && (
                <>
                  <div>Max accumulation: <span className="font-semibold">{rainfallStats.maxIn} in</span></div>
                  {rainfallStats.avgIn != null && <div>Avg across storm bbox: {rainfallStats.avgIn} in</div>}
                  {rainfallStats.product && <div className="text-gray-500 mt-0.5">{rainfallStats.product}</div>}
                </>
              )}
              {!rainfallLoading && rainfallStats && rainfallStats.maxIn == null && (
                <div className="text-gray-500">{rainfallStats.notes}</div>
              )}
              {/* NWS rainfall legend — shown only when the raster is
                  actually mounted. Each row: color swatch + inch range.
                  Keeps the colors in sync with _NWS_RAIN_* in api_server.py. */}
              {hazardView === 'rainfall' && rainfallStats?.tileUrl && (
                <div className="mt-1.5 pt-1.5 border-t border-gray-200">
                  <div className="text-[10px] font-semibold text-gray-600 mb-0.5">Precipitation (in)</div>
                  <div className="flex items-center gap-0.5">
                    {[
                      { c: '#c8ffc8', l: '.01' },
                      { c: '#64e664', l: '.1' },
                      { c: '#32b432', l: '.25' },
                      { c: '#008200', l: '.5' },
                      { c: '#aac83c', l: '.75' },
                      { c: '#ffff00', l: '1' },
                      { c: '#ffc800', l: '1.5' },
                      { c: '#ff8c00', l: '2' },
                      { c: '#ff3c00', l: '3' },
                      { c: '#c80000', l: '4' },
                      { c: '#960064', l: '6' },
                      { c: '#6e00b4', l: '8' },
                      { c: '#4600c8', l: '10' },
                      { c: '#ffffff', l: '15+' },
                    ].map(stop => (
                      <div key={stop.l} className="flex flex-col items-center" style={{ width: 14 }}>
                        <div style={{ backgroundColor: stop.c, width: 12, height: 10, border: '1px solid rgba(0,0,0,0.15)' }} />
                        <div className="text-[8px] text-gray-500">{stop.l}</div>
                      </div>
                    ))}
                  </div>
                </div>
              )}
              {hazardView === 'rainfall' && !rainfallStats?.tileUrl && !rainfallLoading && (
                <div className="text-gray-400 mt-1 italic">
                  Stats only — raster GeoTIFF not on disk for this storm yet.
                </div>
              )}
              {hazardView === 'compound' && (
                <div className="text-gray-400 mt-1 italic">
                  Compound raster (surge + rain + fluvial) already drives loss estimates; standalone map overlay coming next.
                </div>
              )}
            </div>
          )}
          <div className="flex bg-white/90 backdrop-blur rounded-lg shadow-lg border border-gray-200 overflow-hidden">
            {Object.entries(BASEMAP_LABELS).map(([key, label]) => (
              <button
                key={key}
                onClick={() => setBasemap(key)}
                className={`px-3 py-1.5 text-xs font-medium transition-colors ${basemap === key ? 'bg-blue-600 text-white' : 'text-gray-600 hover:bg-gray-100'}`}
              >{label}</button>
            ))}
          </div>
          {activeStorm && (
            <>
              <button
                onClick={handleResetView}
                className="bg-white/90 backdrop-blur rounded-lg shadow-lg border border-gray-200 px-3 py-1.5 text-xs font-medium text-gray-600 hover:bg-gray-100 transition-colors text-center"
              >↺ Reset View</button>
              <div className="flex gap-1.5">
                <button
                  onClick={() => setShowCounties(c => !c)}
                  title={countiesError ?? (countiesLoading ? 'Loading county boundaries…' : showCounties ? `${countiesGeoJSON?.features?.length ?? 0} counties in view — click to hide` : 'Show county boundaries (Census TIGER)')}
                  className={`rounded-lg shadow-lg border px-3 py-1.5 text-xs font-medium transition-colors flex items-center gap-1.5 ${
                    countiesError ? 'bg-red-600 text-white border-red-700'
                    : showCounties ? 'bg-blue-600 text-white border-gray-200'
                    : 'bg-white/90 backdrop-blur text-gray-600 hover:bg-gray-100 border-gray-200'
                  }`}
                >
                  <span>Counties</span>
                  {countiesLoading && <span className="w-2.5 h-2.5 rounded-full border-2 border-white border-t-transparent animate-spin" />}
                  {countiesError && !countiesLoading && <span className="text-[10px]">⚠</span>}
                </button>
                <button
                  onClick={() => setShowFloodZones(f => !f)}
                  title={floodZonesError ?? (floodZonesLoading ? 'Loading FEMA flood zones…' : showFloodZones ? `${floodZonesGeoJSON?.features?.length ?? 0} zones in view — click to hide` : 'Show FEMA National Flood Hazard Layer')}
                  className={`rounded-lg shadow-lg border px-3 py-1.5 text-xs font-medium transition-colors flex items-center gap-1.5 ${
                    floodZonesError ? 'bg-red-600 text-white border-red-700'
                    : showFloodZones ? 'bg-blue-600 text-white border-gray-200'
                    : 'bg-white/90 backdrop-blur text-gray-600 hover:bg-gray-100 border-gray-200'
                  }`}
                >
                  <span>FEMA Zones</span>
                  {floodZonesLoading && <span className="w-2.5 h-2.5 rounded-full border-2 border-white border-t-transparent animate-spin" />}
                  {floodZonesError && !floodZonesLoading && <span className="text-[10px]">⚠</span>}
                </button>
                <button
                  onClick={() => setShowGauges(g => !g)}
                  title={gaugesError ?? (gaugesLoading ? 'Loading AHPS stream gauges…' : showGauges
                    ? `${gaugesSummary?.count ?? 0} gauges in view${gaugesSummary?.major ? ` · ${gaugesSummary.major} at major stage` : ''} — click to hide`
                    : 'Show NOAA AHPS/NWPS stream gauges colored by active flood stage')}
                  className={`rounded-lg shadow-lg border px-3 py-1.5 text-xs font-medium transition-colors flex items-center gap-1.5 ${
                    gaugesError ? 'bg-red-600 text-white border-red-700'
                    : showGauges ? 'bg-cyan-600 text-white border-gray-200'
                    : 'bg-white/90 backdrop-blur text-gray-600 hover:bg-gray-100 border-gray-200'
                  }`}
                >
                  <span>Gauges</span>
                  {gaugesLoading && <span className="w-2.5 h-2.5 rounded-full border-2 border-white border-t-transparent animate-spin" />}
                  {showGauges && gaugesSummary && gaugesSummary.count > 0 && !gaugesLoading && (
                    <span className="text-[10px] bg-white/25 rounded px-1">{gaugesSummary.count}</span>
                  )}
                  {gaugesError && !gaugesLoading && <span className="text-[10px]">⚠</span>}
                </button>
              </div>
              {(countiesError || floodZonesError || gaugesError) && (
                <div className="bg-red-50 border border-red-200 text-red-700 text-[10px] rounded-lg px-2 py-1.5 max-w-[220px] leading-tight">
                  {countiesError && <div>{countiesError}</div>}
                  {floodZonesError && <div>{floodZonesError}</div>}
                  {gaugesError && <div>{gaugesError}</div>}
                  <div className="text-red-500 mt-0.5">Check browser console for details, or pan the map to retry.</div>
                </div>
              )}
            </>
          )}
        </div>
      </div>
    </div>
  );
}

export default App;
