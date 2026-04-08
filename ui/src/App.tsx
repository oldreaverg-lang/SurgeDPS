import { useState, useEffect, useCallback, useMemo, useRef } from 'react';
import Map, { Source, Layer, NavigationControl, Popup } from 'react-map-gl/maplibre';
import type { MapRef } from 'react-map-gl/maplibre';
import maplibregl from 'maplibre-gl';
import 'maplibre-gl/dist/maplibre-gl.css';

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
  dps_score: number;
  confidence?: string;
  building_count?: number;
}
interface Season { year: number; count: number; }

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Styles
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
const MAP_STYLE = 'https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json';
const floodLayerStyle = {
  id: 'flood-depth-layer', type: 'fill',
  paint: {
    'fill-color': ['step', ['get', 'depth'], '#40E0D0', 0.11, '#6495ED', 0.45, '#4169E1', 1.0, '#8A2BE2', 1.7, '#FF00FF'],
    'fill-opacity': 0.25,
  },
};
const LEGEND_ITEMS = [
  { color: '#40E0D0', label: '< 0.5 ft' },
  { color: '#6495ED', label: '0.5 – 1.5 ft' },
  { color: '#4169E1', label: '1.5 – 3.5 ft' },
  { color: '#8A2BE2', label: '3.5 – 6 ft' },
  { color: '#FF00FF', label: '> 6 ft' },
];

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
function friendlyBuildingType(code: string): string {
  if (!code) return 'Unknown';
  const prefix = code.replace(/[-_].*$/, '').toUpperCase();
  return BUILDING_TYPES[prefix] || code;
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
  return {
    windPct: Math.round((windPotential / total) * 100),
    waterPct: Math.round((waterPotential / total) * 100),
  };
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Comparable loss evidence — find similar buildings within radius
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
function findComparables(
  features: any[], buildingType: string, lon: number, lat: number, radiusKm: number = 0.4
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
  const sum = comps.reduce((a, b) => a + b, 0);
  return {
    count: comps.length,
    avgLoss: Math.round(sum / comps.length),
    minLoss: Math.min(...comps),
    maxLoss: Math.max(...comps),
  };
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
  return (
    <button
      onClick={() => onSelect(s.storm_id)}
      disabled={activating}
      className={`w-full text-left px-3 py-2 flex items-center gap-2 transition-colors rounded-md text-sm ${
        isActive ? 'bg-indigo-500/20 text-white' : 'text-slate-300 hover:bg-slate-700/60 hover:text-white'
      }`}
    >
      <span style={{ background: dot, width: 8, height: 8, borderRadius: '50%', flexShrink: 0 }} />
      <span className="truncate font-medium">{shortName(s.name)}</span>
      <span className="ml-auto text-xs text-slate-500 shrink-0">
        {s.dps_score ? <><span style={{ color: dot, fontWeight: 600 }}>{s.dps_score.toFixed(1)}</span>{' '}<span className="text-slate-600">DPS</span></> : '—'}
      </span>
    </button>
  );
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Storm Browser Sidebar
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

function StormBrowser({ onSelectStorm, activeStormId, activating, isOpen, onClose }: {
  onSelectStorm: (id: string) => void;
  activeStormId: string | null;
  activating: boolean;
  isOpen: boolean;
  onClose: () => void;
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
              StormDPS →
            </a>
            <button
              onClick={onClose}
              className="lg:hidden text-slate-400 hover:text-white transition-colors p-1 rounded"
              aria-label="Close sidebar"
            >✕</button>
          </div>
        </div>
        <p className="text-[10px] text-slate-500 uppercase tracking-widest mt-0.5">Storm Surge Analysis</p>
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
// Dashboard Panel (right overlay on map)
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

const CONFIDENCE_STYLES: Record<string, { bg: string; text: string; label: string }> = {
  high:        { bg: 'bg-green-100', text: 'text-green-800', label: 'High Confidence' },
  medium:      { bg: 'bg-yellow-100', text: 'text-yellow-800', label: 'Medium Confidence' },
  low:         { bg: 'bg-red-100', text: 'text-red-800', label: 'Low Confidence' },
  unvalidated: { bg: 'bg-gray-100', text: 'text-gray-500', label: 'Unvalidated' },
};

const ELI_STYLES: Record<string, { bg: string; text: string; border: string; label: string }> = {
  extreme:   { bg: 'bg-red-50', text: 'text-red-900', border: 'border-red-300', label: 'Extreme' },
  very_high: { bg: 'bg-orange-50', text: 'text-orange-900', border: 'border-orange-300', label: 'Very High' },
  high:      { bg: 'bg-yellow-50', text: 'text-yellow-900', border: 'border-yellow-300', label: 'High' },
  moderate:  { bg: 'bg-blue-50', text: 'text-blue-900', border: 'border-blue-300', label: 'Moderate' },
  low:       { bg: 'bg-gray-50', text: 'text-gray-700', border: 'border-gray-300', label: 'Low' },
  unavailable: { bg: 'bg-gray-50', text: 'text-gray-400', border: 'border-gray-200', label: 'Pending' },
};

function DashboardPanel({ storm, totals, loadedCells, loadingCells, confidence, eli, validatedDps, onOpenSidebar, zoom, onClearStorm }: {
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

  if (!storm) return null;

  return (
    <div className="absolute top-4 left-4 bg-white/95 backdrop-blur shadow-2xl rounded-lg w-72 max-w-[calc(100vw-2rem)] border border-gray-100 z-10">

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
            ${(totals.loss / 1e6).toLocaleString(undefined, { maximumFractionDigits: 1 })}M
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
      </div>

      {/* R5: Confidence badge */}
      {(() => {
        const cs = CONFIDENCE_STYLES[confidence.level] || CONFIDENCE_STYLES.unvalidated;
        const tip = confidence.level === 'high' ? 'Strong building data coverage in the affected area'
          : confidence.level === 'medium' ? 'Moderate building data — some gaps possible'
          : confidence.level === 'low' ? 'Limited building data — estimates may be incomplete'
          : 'Model estimate only — building data not yet loaded';
        return (
          <div className={`${cs.bg} rounded-lg px-3 py-2 mb-3`}>
            <div className="flex items-center justify-between">
              <span className={`text-xs font-bold ${cs.text}`}>{cs.label}</span>
              <span className={`text-[10px] ${cs.text}`}>{confidence.count.toLocaleString()} buildings</span>
            </div>
            <p className={`text-[10px] mt-0.5 ${cs.text} opacity-75`}>{tip}</p>
          </div>
        );
      })()}

      {/* R11: Validated DPS adjustment */}
      {validatedDps.adj !== 0 && (
        <div className={`${validatedDps.adj > 0 ? 'bg-orange-50 border-orange-200' : 'bg-blue-50 border-blue-200'} rounded-lg px-3 py-1.5 mb-3 border`}>
          <div className="flex items-center justify-between">
            <span className="text-[10px] font-bold text-gray-600 uppercase">Adjusted Damage Score</span>
            <span className={`text-sm font-black ${validatedDps.adj > 0 ? 'text-orange-700' : 'text-blue-700'}`}>{validatedDps.value.toFixed(1)}</span>
          </div>
          <div className="text-[10px] text-gray-500 mt-0.5">{validatedDps.reason}</div>
        </div>
      )}

      {/* R8: Expected Loss Index */}
      {eli.value > 0 && (() => {
        const es = ELI_STYLES[eli.tier] || ELI_STYLES.unavailable;
        return (
          <div className={`${es.bg} rounded-xl p-3 text-center border ${es.border} shadow-sm mb-3`}>
            <div className="text-[10px] text-gray-500 font-bold uppercase tracking-wider mb-0.5">Expected Loss Index</div>
            <div className={`text-2xl font-black ${es.text} tracking-tighter`}>
              {eli.value.toFixed(0)}
            </div>
            <div className={`text-xs ${es.text} mt-0.5 font-semibold`}>
              {es.label} Damage Potential
            </div>
            <div className="text-[10px] text-gray-400 mt-1">
              Storm intensity × scale of exposure
            </div>
          </div>
        );
      })()}

      {/* Scoreboard */}
      {totals.buildings > 0 && (
        <div className="bg-gray-100/50 rounded-xl p-3 text-center border border-gray-200/60 shadow-sm mb-3">
          <div className="text-[10px] text-gray-500 font-bold uppercase tracking-wider mb-0.5">Total Modeled Loss</div>
          <div className="text-2xl font-black text-red-600 tracking-tighter">
            ${totals.loss > 0 ? (totals.loss / 1e6).toLocaleString(undefined, { maximumFractionDigits: 1 }) + 'M' : '...'}
          </div>
          <div className="text-xs text-gray-500 mt-0.5">
            Across {totals.buildings.toLocaleString()} properties
          </div>
        </div>
      )}

      {/* R9: Nuisance Flood Flag */}
      {totals.buildings > 2000 && totals.buildings > 0 && (totals.totalDepth / totals.buildings) < 1.5 && (
        <div className="bg-amber-50 rounded-lg px-3 py-2 mb-3 border border-amber-300">
          <div className="text-[10px] text-amber-800 font-bold uppercase tracking-wider">Nuisance Flood Warning</div>
          <div className="text-xs text-amber-700 mt-0.5">
            Avg. depth of {(totals.totalDepth / totals.buildings).toFixed(1)} ft across {totals.buildings.toLocaleString()} buildings — widespread shallow flooding can cause significant aggregate damage even when individual losses appear modest.
          </div>
        </div>
      )}

      {/* Grid status */}
      <div className="bg-blue-50/50 rounded-lg p-2.5 mb-3 border border-blue-100">
        <div className="text-[10px] text-blue-600 font-bold uppercase tracking-wider mb-0.5">Map Coverage</div>
        <div className="text-sm text-blue-800 font-semibold">
          {loadedCells.size} area{loadedCells.size !== 1 ? 's' : ''} analyzed
        </div>
        <div className="text-xs text-blue-500 mt-0.5">
          {loadingCells.size > 0
            ? `Fetching data for ${loadingCells.size} more area${loadingCells.size !== 1 ? 's' : ''}…`
            : zoom >= 13 ? 'Zoom out to see grid borders and expand coverage'
            : 'Click the dashed borders on the map to expand coverage'}
        </div>
      </div>

      {/* Legend */}
      <div className="text-[10px] text-gray-400 font-bold uppercase tracking-wider mb-1.5">Surge Depth</div>
      <div className="space-y-1.5">
        {LEGEND_ITEMS.map((item, i) => (
          <div key={i} className="flex items-center gap-2">
            <span className="w-4 h-4 rounded shadow-inner border border-gray-200 shrink-0" style={{ backgroundColor: item.color, opacity: 0.35 }}></span>
            <span className="text-xs font-medium text-gray-600">{item.label}</span>
          </div>
        ))}
      </div>
      <hr className="my-3 border-gray-200" />
      <div className="text-[10px] text-gray-400 font-bold uppercase tracking-wider mb-1.5">Building Damage</div>
      <div className="grid grid-cols-2 gap-2 text-[11px]">
        <div className="flex items-center gap-1.5"><span className="w-3 h-3 rounded-full bg-[#4ade80] border border-gray-300"></span> No Damage</div>
        <div className="flex items-center gap-1.5"><span className="w-3 h-3 rounded-full bg-[#facc15] border border-gray-300"></span> Minor</div>
        <div className="flex items-center gap-1.5"><span className="w-3 h-3 rounded-full bg-[#fb923c] border border-gray-300"></span> Moderate</div>
        <div className="flex items-center gap-1.5"><span className="w-3 h-3 rounded-full bg-[#ef4444] border border-gray-300"></span> Major</div>
        <div className="flex items-center gap-1.5"><span className="w-3 h-3 rounded-full bg-[#7f1d1d] border border-gray-300"></span> Severe</div>
      </div>
      </div>
      )}
    </div>
  );
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// App
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

function App() {
  const mapRef = useRef<MapRef>(null);

  const [sidebarOpen, setSidebarOpen] = useState(true);
  const [activeStorm, setActiveStorm] = useState<StormInfo | null>(null);
  const [activating, setActivating] = useState(false);
  const [hoverInfo, setHoverInfo] = useState<any>(null);
  const [impactTotals, setImpactTotals] = useState({ buildings: 0, loss: 0, totalDepth: 0 });
  const [loadedCells, setLoadedCells] = useState<Set<string>>(new Set());
  const [loadingCells, setLoadingCells] = useState<Set<string>>(new Set());
  const [allBuildings, setAllBuildings] = useState<any>(null);
  const [allFlood, setAllFlood] = useState<any>(null);
  const [zoom, setZoom] = useState(10);
  const [confidence, setConfidence] = useState<{ level: string; count: number }>({ level: 'unvalidated', count: 0 });
  const [eli, setEli] = useState<{ value: number; tier: string }>({ value: 0, tier: 'unavailable' });
  const [validatedDps, setValidatedDps] = useState<{ value: number; adj: number; reason: string }>({ value: 0, adj: 0, reason: '' });
  const [manifest, setManifest] = useState<Record<string, any>>({});

  // Cell load error toast
  const [cellError, setCellError] = useState<string | null>(null);
  useEffect(() => { if (cellError) { const t = setTimeout(() => setCellError(null), 5000); return () => clearTimeout(t); } }, [cellError]);

  // ── Address search (geocoding via Nominatim) ──
  const [addressQuery, setAddressQuery] = useState('');
  const [addressSearching, setAddressSearching] = useState(false);
  const [addressError, setAddressError] = useState('');
  const handleAddressSearch = useCallback(() => {
    const q = addressQuery.trim();
    if (!q || !mapRef.current) return;
    setAddressSearching(true);
    setAddressError('');
    fetch(`https://nominatim.openstreetmap.org/search?q=${encodeURIComponent(q)}&format=json&limit=1`, {
      headers: { 'User-Agent': 'SurgeDPS/1.0 (surgedps.com)' },
    })
      .then(r => r.json())
      .then((results: any[]) => {
        if (results.length === 0) { setAddressError('Address not found'); return; }
        const { lon, lat } = results[0];
        mapRef.current?.flyTo({ center: [parseFloat(lon), parseFloat(lat)], zoom: 16, duration: 2000 });
      })
      .catch(() => setAddressError('Search failed — try again'))
      .finally(() => setAddressSearching(false));
  }, [addressQuery]);

  // ── Batch address lookup ──
  const [batchOpen, setBatchOpen] = useState(false);
  const [batchInput, setBatchInput] = useState('');
  const [batchResults, setBatchResults] = useState<any[]>([]);
  const [batchLoading, setBatchLoading] = useState(false);
  const handleBatchLookup = useCallback(async () => {
    const lines = batchInput.split('\n').map(l => l.trim()).filter(Boolean);
    if (lines.length === 0 || !allBuildings?.features?.length) return;
    setBatchLoading(true);
    const results: any[] = [];
    for (const addr of lines) {
      try {
        const r = await fetch(`https://nominatim.openstreetmap.org/search?q=${encodeURIComponent(addr)}&format=json&limit=1`, {
          headers: { 'User-Agent': 'SurgeDPS/1.0 (surgedps.com)' },
        });
        const data = await r.json();
        if (!data.length) { results.push({ address: addr, status: 'not found' }); continue; }
        const lon = parseFloat(data[0].lon), lat = parseFloat(data[0].lat);
        // Find nearest building within ~200m
        let nearest: any = null, minDist = Infinity;
        for (const f of allBuildings.features) {
          const [bLon, bLat] = f.geometry?.coordinates || [0, 0];
          const d = Math.sqrt((bLon - lon) ** 2 + (bLat - lat) ** 2);
          if (d < minDist) { minDist = d; nearest = f; }
        }
        const distM = minDist * 111_000; // rough degrees→meters
        if (nearest && distM < 200) {
          const p = nearest.properties;
          results.push({ address: addr, status: 'matched', distance_m: Math.round(distM), ...p });
        } else {
          results.push({ address: addr, status: 'no building nearby', lat, lon });
        }
      } catch { results.push({ address: addr, status: 'geocode error' }); }
      // Nominatim rate limit: 1 req/sec
      if (lines.indexOf(addr) < lines.length - 1) await new Promise(r => setTimeout(r, 1100));
    }
    setBatchResults(results);
    setBatchLoading(false);
  }, [batchInput, allBuildings]);

  const handleBatchExport = useCallback(() => {
    if (!batchResults.length) return;
    const header = 'address,status,surge_depth_ft,found_ht,interior_flood_ft,structure_dmg_pct,contents_dmg_pct,total_dmg_pct,estimated_loss_usd,damage_category,deductible_flag';
    const rows = batchResults.map(r => {
      if (r.status !== 'matched') return `"${r.address}",${r.status},,,,,,,,, `;
      const interior = r.found_ht != null ? Math.max(0, r.depth_ft - r.found_ht) : '';
      const dedFlag = r.estimated_loss_usd < 1250 ? 'below_min' : r.estimated_loss_usd < 10000 ? 'below_typical' : 'above';
      return `"${r.address}",${r.status},${r.depth_ft ?? ''},${r.found_ht ?? ''},${interior},${r.structure_damage_pct ?? ''},${r.contents_damage_pct ?? ''},${r.total_damage_pct ?? ''},${r.estimated_loss_usd ?? ''},${r.damage_category ?? ''},${dedFlag}`;
    });
    const csv = [header, ...rows].join('\n');
    const blob = new Blob([csv], { type: 'text/csv' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url; a.download = `surgedps_batch_${activeStorm?.storm_id || 'results'}.csv`; a.click();
    URL.revokeObjectURL(url);
  }, [batchResults, activeStorm]);

  // ── Print / Share ──
  const handlePrint = useCallback(() => { window.print(); }, []);
  const handleShareLink = useCallback(() => {
    if (!activeStorm || !mapRef.current) return;
    const c = mapRef.current.getCenter();
    const z = mapRef.current.getZoom().toFixed(1);
    const url = `${window.location.origin}/surgedps?storm=${activeStorm.storm_id}&lat=${c.lat.toFixed(5)}&lng=${c.lng.toFixed(5)}&z=${z}`;
    navigator.clipboard.writeText(url).then(() => setCellError('Link copied to clipboard'));
  }, [activeStorm]);

  // ── Methodology panel ──
  const [methodologyOpen, setMethodologyOpen] = useState(false);

  // ── CSV export of visible buildings ──
  const handleExportCSV = useCallback(() => {
    if (!allBuildings?.features?.length) return;
    const rows = allBuildings.features.map((f: any) => {
      const p = f.properties || {};
      const [lon, lat] = f.geometry?.coordinates || [0, 0];
      return [
        lat, lon, p.building_type || '', p.depth_ft ?? '', p.found_ht ?? '',
        p.structure_damage_pct ?? '', p.contents_damage_pct ?? '', p.total_damage_pct ?? '',
        p.estimated_loss_usd ?? '', p.val_struct ?? '', p.val_cont ?? '',
        p.damage_category || '', p.replacement_value_usd ?? '',
      ].join(',');
    });
    const header = 'lat,lon,building_type,surge_depth_ft,foundation_ht_ft,structure_dmg_pct,contents_dmg_pct,total_dmg_pct,estimated_loss_usd,val_struct,val_cont,damage_category,replacement_value_usd';
    const csv = [header, ...rows].join('\n');
    const blob = new Blob([csv], { type: 'text/csv' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `surgedps_${activeStorm?.storm_id || 'export'}_buildings.csv`;
    a.click();
    URL.revokeObjectURL(url);
  }, [allBuildings, activeStorm]);

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
    setConfidence({ level: 'unvalidated', count: 0 });
    setEli({ value: 0, tier: 'unavailable' });
    setValidatedDps({ value: 0, adj: 0, reason: '' });
    setManifest({});

    try {
      const ac = new AbortController();
      const timeout = setTimeout(() => ac.abort(), 120_000); // 2 min timeout for activation
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
      fetch(`/surgedps/api/manifest?storm_id=${stormId}`)
        .then(r => r.ok ? r.json() : {})
        .then((m: any) => setManifest(m?.cells || {}))
        .catch(() => setManifest({}));

      if (center_cell) {
        setAllBuildings(center_cell.buildings);
        setAllFlood(center_cell.flood);
        setLoadedCells(new Set([cellKey(0, 0)]));
        const feats = center_cell.buildings?.features || [];
        const buildings = feats.length;
        const loss = feats.reduce((s: number, f: any) => s + (f.properties.estimated_loss_usd || 0), 0);
        const totalDepth = feats.reduce((s: number, f: any) => s + (f.properties.depth_ft || 0), 0);
        setImpactTotals({ buildings, loss, totalDepth });
      }

      mapRef.current?.flyTo({ center: [storm.landfall_lon, storm.landfall_lat], zoom: 10, pitch: 30, duration: 2500 });

      // Server may return partial data with a cell_error flag
      if (storm.cell_error) setCellError(storm.cell_error);
    } catch (err) {
      console.error('Failed to activate storm:', err);
      setCellError('Failed to load storm data. The server may be warming up — try again in a moment.');
    } finally {
      setActivating(false);
      activatingRef.current = false;
    }
  }, []); // stable — no dependencies

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
        const nk = cellKey(c + dc, r + dr);
        if (!seen.has(nk)) {
          seen.add(nk);
          // Pre-computed cells in manifest get "ready" status (solid border, instant load)
          const status = manifest[nk] ? 'ready' : 'available';
          features.push(cellPolygon(c + dc, r + dr, status, oLon, oLat));
        }
      }
    }
    return { type: 'FeatureCollection' as const, features };
  }, [activeStorm, loadedCells, loadingCells, manifest]);

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
    setLoadingCells(prev => new Set([...prev, key]));
    try {
      const stormId = activeStormRef.current?.storm_id || '';
      const ac = new AbortController();
      const timeout = setTimeout(() => ac.abort(), 90_000); // 90s timeout for cell generation
      const resp = await fetch(`/surgedps/api/cell?col=${col}&row=${row}&storm_id=${encodeURIComponent(stormId)}`, { signal: ac.signal });
      clearTimeout(timeout);
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
    } catch (err) { console.error(`Failed cell (${col},${row}):`, err); setCellError('Could not load this area — the data source may be temporarily unavailable. Try again in a moment.'); }
    finally { setLoadingCells(prev => { const n = new Set([...prev]); n.delete(key); return n; }); }
  }, []); // stable — all state accessed via refs

  // Reverse-geocode building hover via Nominatim (debounced 300ms to avoid hammering the API)
  useEffect(() => {
    if (hoverInfo?.type !== 'damage') {
      setHoverAddress(null);
      return;
    }
    const { lng, lat } = hoverInfo;
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
        `https://nominatim.openstreetmap.org/reverse?lat=${lat}&lon=${lng}&format=json`,
        { signal: controller.signal, headers: { 'User-Agent': 'SurgeDPS/1.0 (surgedps.com)' } }
      )
        .then(r => r.json())
        .then(data => {
          const addr = data?.address || {};
          const parts = [
            addr.house_number,
            addr.road,
            addr.city || addr.town || addr.village || addr.hamlet,
          ].filter(Boolean);
          const label = parts.length > 1 ? parts.join(', ') : null;
          geocodeCache.current[cacheKey] = label || '';
          setHoverAddress(label);
        })
        .catch(() => {});
    }, 300);
    return () => { clearTimeout(timer); controller.abort(); };
  }, [hoverInfo]);

  // Events
  const onHover = useCallback((event: any) => {
    const { features, lngLat: { lng, lat } } = event;
    if (!features || !features.length) { setHoverInfo(null); return; }
    for (const [layerId, type] of [['grid-available-fill', 'grid'], ['grid-ready-fill', 'grid'], ['damage-clusters', 'cluster'], ['damage-points', 'damage'], ['flood-depth-layer', 'flood']] as const) {
      const f = features.find((f: any) => f.layer.id === layerId);
      if (f) { setHoverInfo({ lng, lat, type, feature: f }); return; }
    }
    setHoverInfo(null);
  }, []);

  const onClick = useCallback((event: any) => {
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
    // Grid cell click → load data (available or pre-computed "ready" cells)
    const f = event.features?.find((f: any) => f.layer.id === 'grid-available-fill' || f.layer.id === 'grid-ready-fill');
    if (f) loadCell(f.properties.col, f.properties.row);
  }, [loadCell]);

  const showGrid = zoom < 13;

  return (
    <div className="flex h-screen w-full relative overflow-hidden">
      {/* Left Sidebar — Storm Browser */}
      <StormBrowser
        onSelectStorm={activateStorm}
        activeStormId={activeStorm?.storm_id || null}
        activating={activating}
        isOpen={sidebarOpen}
        onClose={() => setSidebarOpen(false)}
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
          mapStyle={MAP_STYLE}
          interactiveLayerIds={['flood-depth-layer', 'damage-points', 'damage-clusters', ...(showGrid ? ['grid-available-fill', 'grid-ready-fill'] : [])]}
          cursor={hoverInfo?.type === 'cluster' || hoverInfo?.type === 'grid' ? 'pointer' : ''}
          onMouseMove={onHover} onClick={onClick}
          onZoomEnd={e => setZoom(e.viewState.zoom)}
        >
          <NavigationControl position="top-right" />

          {allFlood && <Source id="flood-data" type="geojson" data={allFlood} tolerance={0.5}><Layer {...(floodLayerStyle as any)} /></Source>}

          {allBuildings && (
            <Source id="damage-data" type="geojson" data={allBuildings}
              cluster={true} clusterMaxZoom={14} clusterRadius={50}
              clusterProperties={{ total_loss: ['+', ['get', 'estimated_loss_usd']] }}
            >
              <Layer id="damage-points" type="circle" filter={['!', ['has', 'point_count']]}
                paint={{
                  'circle-radius': 10,
                  'circle-color': ['match', ['get', 'damage_category'], 'none', '#4ade80', 'minor', '#facc15', 'moderate', '#fb923c', 'major', '#ef4444', 'severe', '#7f1d1d', '#9ca3af'],
                  'circle-opacity': ['coalesce', ['get', 'data_quality'], 0.7],
                  'circle-stroke-width': 2, 'circle-stroke-color': '#fff',
                }} />
              <Layer id="damage-clusters" type="circle" filter={['has', 'point_count']}
                paint={{
                  'circle-color': ['step', ['/', ['get', 'total_loss'], ['get', 'point_count']],
                    '#4ade80', 5000, '#facc15', 25000, '#fb923c', 75000, '#ef4444', 200000, '#7f1d1d'],
                  'circle-radius': ['step', ['get', 'point_count'], 16, 5, 22, 10, 30],
                  'circle-stroke-width': 3, 'circle-stroke-color': '#fff',
                }} />
              <Layer id="damage-cluster-count" type="symbol" filter={['has', 'point_count']}
                layout={{
                  'text-field': ['concat', '$', ['to-string', ['round', ['/', ['get', 'total_loss'], 1000]]], 'K'],
                  'text-size': 11,
                }}
                paint={{ 'text-color': '#fff' }} />
            </Source>
          )}

          {showGrid && activeStorm && (
            <Source id="grid-data" type="geojson" data={gridGeoJson}>
              <Layer id="grid-loaded-border" type="line" filter={['==', ['get', 'status'], 'loaded']}
                paint={{ 'line-color': '#4ade80', 'line-width': 2, 'line-opacity': 0.6, 'line-dasharray': [4, 2] }} />
              <Layer id="grid-available-fill" type="fill" filter={['==', ['get', 'status'], 'available']}
                paint={{ 'fill-color': '#ffffff', 'fill-opacity': 0.06 }} />
              <Layer id="grid-available-border" type="line" filter={['==', ['get', 'status'], 'available']}
                paint={{ 'line-color': '#ffffff', 'line-width': 2, 'line-opacity': 0.7, 'line-dasharray': [6, 3] }} />
              <Layer id="grid-loading-fill" type="fill" filter={['==', ['get', 'status'], 'loading']}
                paint={{ 'fill-color': '#facc15', 'fill-opacity': 0.1 }} />
              <Layer id="grid-loading-border" type="line" filter={['==', ['get', 'status'], 'loading']}
                paint={{ 'line-color': '#facc15', 'line-width': 2.5, 'line-opacity': 0.9 }} />
              <Layer id="grid-available-label" type="symbol" filter={['==', ['get', 'status'], 'available']}
                layout={{ 'text-field': 'Click to load', 'text-size': 13, 'text-font': ['Open Sans Regular'] }}
                paint={{ 'text-color': '#fff', 'text-opacity': 0.7, 'text-halo-color': '#000', 'text-halo-width': 1 }} />
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

          {hoverInfo && (
            <Popup longitude={hoverInfo.lng} latitude={hoverInfo.lat} closeButton={false} closeOnClick={false} anchor="bottom" className="z-50">
              <div className="p-2 min-w-[200px]">
                {hoverInfo.type === 'grid' ? (
                  <div className="text-center">
                    <p className="text-sm font-semibold text-gray-800">
                      {hoverInfo.feature?.properties?.status === 'ready' ? 'Pre-computed Region' : 'Unexplored Region'}
                    </p>
                    <p className="text-xs text-gray-500 mt-1">
                      {hoverInfo.feature?.properties?.status === 'ready'
                        ? 'Click to load instantly from cache'
                        : 'Click to load buildings & damage data'}
                    </p>
                  </div>
                ) : hoverInfo.type === 'cluster' ? (
                  <><h3 className="font-semibold text-gray-800 text-sm border-b pb-1 mb-1 border-gray-200">Neighborhood Impact</h3>
                  <div className="text-xs space-y-1">
                    <p className="flex justify-between"><span className="text-gray-500">Properties:</span> <span className="font-medium">{hoverInfo.feature.properties.point_count}</span></p>
                    <p className="flex justify-between text-sm"><span className="text-gray-500">Loss:</span> <span className="font-bold text-red-600">${hoverInfo.feature.properties.total_loss?.toLocaleString() || 0}</span></p>
                  </div></>
                ) : hoverInfo.type === 'flood' ? (
                  <><h3 className="font-semibold text-gray-800 text-sm mb-1">Storm Surge Depth</h3>
                  <p className="text-gray-800 font-bold text-base">{(hoverInfo.feature.properties.depth_ft != null ? hoverInfo.feature.properties.depth_ft : hoverInfo.feature.properties.depth * 3.28084).toFixed(1)} ft</p>
                  <p className="text-gray-400 text-[10px] mt-0.5">Modeled inundation at this location</p></>
                ) : (
                  (() => {
                    const p = hoverInfo.feature.properties;
                    const foundHt = p.found_ht != null ? p.found_ht : null;
                    const interiorFt = foundHt != null ? Math.max(0, p.depth_ft - foundHt) : null;
                    const structLoss = p.val_struct != null ? Math.round(p.val_struct * p.structure_damage_pct / 100) : null;
                    const contLoss = p.val_cont != null ? Math.round(p.val_cont * p.contents_damage_pct / 100) : null;

                    // ── Comparable Loss Evidence ──
                    const comps = allBuildings?.features
                      ? findComparables(allBuildings.features, p.building_type, hoverInfo.lng, hoverInfo.lat)
                      : { count: 0, avgLoss: 0, minLoss: 0, maxLoss: 0 };

                    // ── Wind vs Water Attribution ──
                    let wwSplit: { windPct: number; waterPct: number } | null = null;
                    let estWindMph: number | null = null;
                    if (activeStorm) {
                      const distKm = haversineKm(hoverInfo.lat, hoverInfo.lng, activeStorm.landfall_lat, activeStorm.landfall_lon);
                      estWindMph = Math.round(estimateWindMph(distKm, activeStorm.max_wind_kt, activeStorm.category));
                      const floodForWind = interiorFt != null ? interiorFt : Math.max(0, p.depth_ft - 1); // fallback: assume 1ft foundation
                      wwSplit = windWaterSplit(estWindMph, floodForWind);
                    }

                    return (
                    <><h3 className="font-semibold text-gray-800 text-sm border-b pb-1 mb-1 border-gray-200">Property Damage</h3>
                    {hoverAddress && (
                      <p className="text-[11px] text-indigo-700 font-semibold mb-1.5 pb-1 border-b border-gray-100 truncate" title={hoverAddress}>{hoverAddress}</p>
                    )}
                    <div className="text-xs space-y-1">
                      <p className="flex justify-between"><span className="text-gray-500">Type:</span> <span className="font-medium">{friendlyBuildingType(p.building_type)}</span></p>
                      <p className="flex justify-between"><span className="text-gray-500">Severity:</span> <span className="font-medium capitalize">{p.damage_category === 'none' ? 'No Damage' : p.damage_category}</span></p>
                      {/* Surge depth + interior flooding */}
                      <p className="flex justify-between"><span className="text-gray-500">Surge depth:</span> <span className="font-medium">{p.depth_ft} ft</span></p>
                      {foundHt != null && (
                        <p className="flex justify-between"><span className="text-gray-500">Foundation:</span> <span className="font-medium">{foundHt.toFixed(1)} ft above grade</span></p>
                      )}
                      {interiorFt != null && (
                        <p className="flex justify-between"><span className="text-gray-500">Interior flooding:</span> <span className={`font-bold ${interiorFt > 0 ? 'text-red-600' : 'text-green-600'}`}>{interiorFt > 0 ? `${interiorFt.toFixed(1)} ft` : 'None'}</span></p>
                      )}
                      {/* Structure vs Contents breakdown */}
                      <hr className="border-gray-200 !my-1.5" />
                      <p className="flex justify-between"><span className="text-gray-500">Structure:</span> <span className="font-medium">{p.structure_damage_pct}%{structLoss != null ? ` ($${structLoss.toLocaleString()})` : ''}</span></p>
                      <p className="flex justify-between"><span className="text-gray-500">Contents:</span> <span className="font-medium">{p.contents_damage_pct}%{contLoss != null ? ` ($${contLoss.toLocaleString()})` : ''}</span></p>
                      <p className="flex justify-between text-sm"><span className="text-gray-500">Total loss:</span> <span className="font-bold text-red-600">${p.estimated_loss_usd?.toLocaleString()}</span></p>
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
                            <strong>{comps.count}</strong> similar {friendlyBuildingType(p.building_type).toLowerCase()}s within 0.25 mi averaged{' '}
                            <strong className="text-blue-700">${comps.avgLoss.toLocaleString()}</strong> in modeled losses
                          </p>
                          <p className="text-[10px] text-blue-500 mt-0.5">
                            Range: ${comps.minLoss.toLocaleString()} – ${comps.maxLoss.toLocaleString()}
                          </p>
                        </div>
                        </>
                      )}
                    </div></>
                    );
                  })()
                )}
              </div>
            </Popup>
          )}
        </Map>

        {/* ── Address search bar (top-center of map) ── */}
        {activeStorm && (
          <div className="absolute top-3 left-1/2 -translate-x-1/2 z-10 flex items-center gap-2">
            <div className="flex bg-white/95 backdrop-blur shadow-lg rounded-lg overflow-hidden border border-gray-200">
              <input
                type="text"
                placeholder="Search address, e.g. 412 N Austin St, Rockport TX"
                value={addressQuery}
                onChange={e => { setAddressQuery(e.target.value); setAddressError(''); }}
                onKeyDown={e => e.key === 'Enter' && handleAddressSearch()}
                className="px-3 py-2 text-sm text-gray-800 placeholder-gray-400 w-72 outline-none"
              />
              <button
                onClick={handleAddressSearch}
                disabled={addressSearching}
                className="px-3 py-2 bg-indigo-500 hover:bg-indigo-400 text-white text-sm font-medium transition-colors disabled:opacity-50"
              >{addressSearching ? '...' : 'Go'}</button>
            </div>
            {addressError && <span className="text-xs text-red-500 bg-white/90 px-2 py-1 rounded shadow">{addressError}</span>}
            {/* Action buttons */}
            {allBuildings?.features?.length > 0 && (
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
                <button
                  onClick={handlePrint}
                  className="bg-white/95 backdrop-blur shadow-lg rounded-lg px-2.5 py-2 text-sm text-gray-700 hover:text-indigo-600 hover:bg-white border border-gray-200 transition-colors"
                  title="Print current map view"
                >🖨</button>
                <button
                  onClick={handleShareLink}
                  className="bg-white/95 backdrop-blur shadow-lg rounded-lg px-2.5 py-2 text-sm text-gray-700 hover:text-indigo-600 hover:bg-white border border-gray-200 transition-colors"
                  title="Copy share link to clipboard"
                >🔗</button>
                <button
                  onClick={() => setMethodologyOpen(m => !m)}
                  className="bg-white/95 backdrop-blur shadow-lg rounded-lg px-2.5 py-2 text-sm text-gray-700 hover:text-indigo-600 hover:bg-white border border-gray-200 transition-colors"
                  title="View methodology and data sources"
                >ℹ️</button>
              </>
            )}
          </div>
        )}

        {/* Dashboard overlay */}
        <DashboardPanel storm={activeStorm} totals={impactTotals} loadedCells={loadedCells} loadingCells={loadingCells} confidence={confidence} eli={eli} validatedDps={validatedDps} onOpenSidebar={() => setSidebarOpen(true)} zoom={zoom} onClearStorm={() => {
          setActiveStorm(null); setAllBuildings(null); setAllFlood(null);
          setLoadedCells(new Set()); setLoadingCells(new Set());
          setImpactTotals({ buildings: 0, loss: 0, totalDepth: 0 }); setHoverInfo(null);
          setConfidence({ level: 'unvalidated', count: 0 }); setEli({ value: 0, tier: 'unavailable' });
          setValidatedDps({ value: 0, adj: 0, reason: '' }); setManifest({});
        }} />

        {/* Cell error toast */}
        {cellError && (
          <div className="absolute bottom-6 left-1/2 -translate-x-1/2 z-30 bg-red-600 text-white text-sm px-4 py-2.5 rounded-lg shadow-xl max-w-sm text-center">
            {cellError}
          </div>
        )}

        {/* Empty-state overlay — shown when no storm is active */}
        {!activeStorm && !activating && (
          <div className="absolute inset-0 flex items-center justify-center z-10 pointer-events-none">
            <div className="bg-black/60 backdrop-blur-sm rounded-2xl px-8 py-6 text-center shadow-2xl max-w-sm pointer-events-auto">
              <div className="text-4xl mb-3">🌀</div>
              <p className="text-white font-bold text-lg">Select a storm to begin</p>
              <p className="text-slate-300 text-sm mt-1">Choose a hurricane from the browser on the left to load surge data and damage estimates.</p>
              <button
                onClick={() => setSidebarOpen(true)}
                className="lg:hidden mt-4 bg-indigo-500 hover:bg-indigo-400 text-white text-sm font-semibold px-4 py-2 rounded-lg transition-colors"
              >☰ Browse Storms</button>
            </div>
          </div>
        )}

        {/* Loading overlay */}
        {activating && (
          <div className="absolute inset-0 bg-black/40 flex items-center justify-center z-20">
            <div className="bg-white rounded-xl p-6 shadow-2xl text-center">
              <div className="animate-spin w-8 h-8 border-4 border-indigo-500 border-t-transparent rounded-full mx-auto mb-3"></div>
              <p className="font-semibold text-gray-800">Analyzing storm...</p>
              <p className="text-xs text-gray-500 mt-1">Fetching buildings & running damage model</p>
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
                              <td className="px-3 py-1.5 text-right font-medium">{r.status === 'matched' ? r.depth_ft?.toFixed(1) : '—'}</td>
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
              <div className="text-[10px] text-gray-400 pt-2 border-t border-gray-200">
                SurgeDPS v1.0 — stormdps.com/surgedps
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

export default App;
