import { useState, useEffect, useCallback, useMemo, useRef } from 'react';
import Map, { Source, Layer, NavigationControl, Popup } from 'react-map-gl/maplibre';
import type { MapRef } from 'react-map-gl/maplibre';
import 'maplibre-gl/dist/maplibre-gl.css';

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
  const searchTimeout = useRef<any>(null);

  useEffect(() => {
    fetch('/surgedps/api/seasons').then(r => r.json()).then(setSeasons).catch(() => {});
    fetch('/surgedps/api/storms/historic').then(r => r.json()).then(setHistoricStorms).catch(() => {});
    fetch('/surgedps/api/storms/active').then(r => r.json()).then(setActiveNHC).catch(() => {});
  }, []);

  const toggleYear = useCallback((year: number) => {
    if (expandedYear === year) { setExpandedYear(null); setYearStorms([]); }
    else { setExpandedYear(year); fetch(`/surgedps/api/season/${year}`).then(r => r.json()).then(setYearStorms).catch(() => setYearStorms([])); }
  }, [expandedYear]);

  const handleSearch = useCallback((q: string) => {
    setSearchQuery(q);
    if (searchTimeout.current) clearTimeout(searchTimeout.current);
    if (!q.trim()) { setSearchResults(null); return; }
    setSearchLoading(true);
    searchTimeout.current = setTimeout(() => {
      fetch(`/surgedps/api/storms/search?q=${encodeURIComponent(q)}`)
        .then(r => r.json())
        .then(data => { setSearchResults(data); setSearchLoading(false); })
        .catch(() => { setSearchResults([]); setSearchLoading(false); });
    }, 300);
  }, []);

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

  // Cell load error toast
  const [cellError, setCellError] = useState<string | null>(null);
  useEffect(() => { if (cellError) { const t = setTimeout(() => setCellError(null), 5000); return () => clearTimeout(t); } }, [cellError]);

  // Reverse geocoding for building hover
  const geocodeCache = useRef<Record<string, string>>({});
  const [hoverAddress, setHoverAddress] = useState<string | null>(null);

  // Activate a storm
  const activateStorm = useCallback(async (stormId: string) => {
    if (activating) return;
    setActivating(true);
    setAllBuildings(null); setAllFlood(null);
    setLoadedCells(new Set()); setLoadingCells(new Set());
    setImpactTotals({ buildings: 0, loss: 0, totalDepth: 0 }); setHoverInfo(null);
    setConfidence({ level: 'unvalidated', count: 0 });
    setEli({ value: 0, tier: 'unavailable' });
    setValidatedDps({ value: 0, adj: 0, reason: '' });

    try {
      const resp = await fetch(`/surgedps/api/storm/${stormId}/activate`);
      if (!resp.ok) throw new Error(`${resp.status}`);
      const data = await resp.json();
      const { storm, center_cell, grid_cells } = data;
      setActiveStorm(storm);
      if (storm.confidence) setConfidence({ level: storm.confidence, count: storm.building_count || 0 });
      if (storm.eli) setEli({ value: storm.eli, tier: storm.eli_tier || 'unavailable' });
      if (storm.validated_dps) setValidatedDps({ value: storm.validated_dps, adj: storm.dps_adjustment || 0, reason: storm.dps_adj_reason || '' });

      // R6: If server returned a 3x3 grid, merge all cells
      if (grid_cells) {
        let mergedBuildings: any[] = [];
        let mergedFlood: any[] = [];
        const keys = new Set<string>();
        let totalBldgs = 0, totalLoss = 0, totalDepth = 0;
        for (const [k, cellData] of Object.entries(grid_cells) as [string, any][]) {
          const [c, r] = k.split(',').map(Number);
          keys.add(cellKey(c, r));
          const feats = cellData.buildings?.features || [];
          mergedBuildings.push(...feats);
          mergedFlood.push(...(cellData.flood?.features || []));
          totalBldgs += feats.length;
          totalLoss += feats.reduce((s: number, f: any) => s + (f.properties.estimated_loss_usd || 0), 0);
          totalDepth += feats.reduce((s: number, f: any) => s + (f.properties.depth_ft || 0), 0);
        }
        setAllBuildings({ type: 'FeatureCollection', features: mergedBuildings });
        setAllFlood({ type: 'FeatureCollection', features: mergedFlood });
        setLoadedCells(keys);
        setImpactTotals({ buildings: totalBldgs, loss: totalLoss, totalDepth });
      } else if (center_cell) {
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
    } catch (err) {
      console.error('Failed to activate storm:', err);
    } finally {
      setActivating(false);
    }
  }, [activating]);

  // Grid GeoJSON
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
        if (!seen.has(nk)) { seen.add(nk); features.push(cellPolygon(c + dc, r + dr, 'available', oLon, oLat)); }
      }
    }
    return { type: 'FeatureCollection' as const, features };
  }, [activeStorm, loadedCells, loadingCells]);

  // Load cell
  const loadCell = useCallback(async (col: number, row: number) => {
    const key = cellKey(col, row);
    if (loadedCells.has(key) || loadingCells.has(key)) return;
    setLoadingCells(prev => new Set([...prev, key]));
    try {
      const resp = await fetch(`/surgedps/api/cell?col=${col}&row=${row}`);
      if (!resp.ok) throw new Error(`${resp.status}`);
      const cellData = await resp.json();
      const { buildings, flood } = cellData;
      if (cellData.confidence) setConfidence({ level: cellData.confidence, count: cellData.building_count || 0 });
      if (cellData.eli) setEli({ value: cellData.eli, tier: cellData.eli_tier || 'unavailable' });
      if (cellData.validated_dps) setValidatedDps({ value: cellData.validated_dps, adj: cellData.dps_adjustment || 0, reason: cellData.dps_adj_reason || '' });
      setAllBuildings((p: any) => p ? { type: 'FeatureCollection', features: [...p.features, ...buildings.features] } : buildings);
      setAllFlood((p: any) => p ? { type: 'FeatureCollection', features: [...p.features, ...flood.features] } : flood);
      const cellFeats = buildings?.features || [];
      setImpactTotals(p => ({
        buildings: p.buildings + cellFeats.length,
        loss: p.loss + cellFeats.reduce((s: number, f: any) => s + (f.properties.estimated_loss_usd || 0), 0),
        totalDepth: p.totalDepth + cellFeats.reduce((s: number, f: any) => s + (f.properties.depth_ft || 0), 0),
      }));
      setLoadedCells(prev => new Set([...prev, key]));
    } catch (err) { console.error(`Failed cell (${col},${row}):`, err); setCellError('Could not load this area — the data source may be temporarily unavailable. Try again in a moment.'); }
    finally { setLoadingCells(prev => { const n = new Set([...prev]); n.delete(key); return n; }); }
  }, [loadedCells, loadingCells]);

  // Reverse-geocode building hover via Nominatim
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
    // Pre-populate so we don't fire duplicate requests while waiting
    geocodeCache.current[cacheKey] = '';
    setHoverAddress(null);
    const controller = new AbortController();
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
    return () => controller.abort();
  }, [hoverInfo]);

  // Events
  const onHover = useCallback((event: any) => {
    const { features, lngLat: { lng, lat } } = event;
    if (!features || !features.length) { setHoverInfo(null); return; }
    for (const [layerId, type] of [['grid-available-fill', 'grid'], ['damage-clusters', 'cluster'], ['damage-points', 'damage'], ['flood-depth-layer', 'flood']] as const) {
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
    // Grid cell click → load data
    const f = event.features?.find((f: any) => f.layer.id === 'grid-available-fill');
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
          interactiveLayerIds={['flood-depth-layer', 'damage-points', 'damage-clusters', ...(showGrid ? ['grid-available-fill'] : [])]}
          cursor={hoverInfo?.type === 'cluster' || hoverInfo?.type === 'grid' ? 'pointer' : ''}
          onMouseMove={onHover} onClick={onClick}
          onZoom={e => setZoom(e.viewState.zoom)}
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
                    <p className="text-sm font-semibold text-gray-800">Unexplored Region</p>
                    <p className="text-xs text-gray-500 mt-1">Click to load buildings & damage data</p>
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
                  <><h3 className="font-semibold text-gray-800 text-sm border-b pb-1 mb-1 border-gray-200">Property Damage</h3>
                  {hoverAddress && (
                    <p className="text-[11px] text-indigo-700 font-semibold mb-1.5 pb-1 border-b border-gray-100 truncate" title={hoverAddress}>{hoverAddress}</p>
                  )}
                  <div className="text-xs space-y-1">
                    <p className="flex justify-between"><span className="text-gray-500">Type:</span> <span className="font-medium">{friendlyBuildingType(hoverInfo.feature.properties.building_type)}</span></p>
                    <p className="flex justify-between"><span className="text-gray-500">Severity:</span> <span className="font-medium capitalize">{hoverInfo.feature.properties.damage_category === 'none' ? 'No Damage' : hoverInfo.feature.properties.damage_category}</span></p>
                    <p className="flex justify-between"><span className="text-gray-500">Damage:</span> <span className="font-medium">{hoverInfo.feature.properties.total_damage_pct}%</span></p>
                    <p className="flex justify-between text-sm"><span className="text-gray-500">Loss:</span> <span className="font-bold text-red-600">${hoverInfo.feature.properties.estimated_loss_usd?.toLocaleString()}</span></p>
                  </div></>
                )}
              </div>
            </Popup>
          )}
        </Map>

        {/* Dashboard overlay */}
        <DashboardPanel storm={activeStorm} totals={impactTotals} loadedCells={loadedCells} loadingCells={loadingCells} confidence={confidence} eli={eli} validatedDps={validatedDps} onOpenSidebar={() => setSidebarOpen(true)} zoom={zoom} onClearStorm={() => {
          setActiveStorm(null); setAllBuildings(null); setAllFlood(null);
          setLoadedCells(new Set()); setLoadingCells(new Set());
          setImpactTotals({ buildings: 0, loss: 0, totalDepth: 0 }); setHoverInfo(null);
          setConfidence({ level: 'unvalidated', count: 0 }); setEli({ value: 0, tier: 'unavailable' });
          setValidatedDps({ value: 0, adj: 0, reason: '' });
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
      </div>
    </div>
  );
}

export default App;
