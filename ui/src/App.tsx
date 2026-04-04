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
  { color: '#40E0D0', label: '< Ankle' }, { color: '#6495ED', label: 'Ankle–Knee' },
  { color: '#4169E1', label: 'Knee–Waist' }, { color: '#8A2BE2', label: 'Waist–Head' },
  { color: '#FF00FF', label: '> Head' },
];
const CAT_COLORS: Record<number, string> = {
  0: '#5eead4', 1: '#facc15', 2: '#fb923c', 3: '#ef4444', 4: '#dc2626', 5: '#7f1d1d',
};

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Storm Browser Sidebar
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

function StormBrowser({ onSelectStorm, activeStormId, activating }: {
  onSelectStorm: (id: string) => void;
  activeStormId: string | null;
  activating: boolean;
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

  // Load sidebar data on mount
  useEffect(() => {
    fetch('/api/seasons').then(r => r.json()).then(setSeasons).catch(() => {});
    fetch('/api/storms/historic').then(r => r.json()).then(setHistoricStorms).catch(() => {});
    fetch('/api/storms/active').then(r => r.json()).then(setActiveNHC).catch(() => {});
  }, []);

  // Expand a season year
  const toggleYear = useCallback((year: number) => {
    if (expandedYear === year) {
      setExpandedYear(null);
      setYearStorms([]);
    } else {
      setExpandedYear(year);
      fetch(`/api/season/${year}`).then(r => r.json()).then(setYearStorms).catch(() => setYearStorms([]));
    }
  }, [expandedYear]);

  // Live search
  const handleSearch = useCallback((q: string) => {
    setSearchQuery(q);
    if (searchTimeout.current) clearTimeout(searchTimeout.current);
    if (!q.trim()) { setSearchResults(null); return; }
    setSearchLoading(true);
    searchTimeout.current = setTimeout(() => {
      fetch(`/api/storms/search?q=${encodeURIComponent(q)}`)
        .then(r => r.json())
        .then(data => { setSearchResults(data); setSearchLoading(false); })
        .catch(() => { setSearchResults([]); setSearchLoading(false); });
    }, 300);
  }, []);

  /** Strip "Hurricane " / "Tropical Storm " / "Tropical Depression " prefixes */
  const shortName = (name: string) =>
    name.replace(/^(Hurricane|Tropical Storm|Tropical Depression)\s+/i, '');

  /** Sort storms by DPS score, highest first */
  const byDPS = (a: StormInfo, b: StormInfo) => (b.dps_score || 0) - (a.dps_score || 0);

  /** DPS severity color matching StormDPS thresholds */
  const dpsColor = (score: number): string => {
    if (score >= 80) return '#ef4444'; // Red — Catastrophic
    if (score >= 60) return '#f97316'; // Orange — Extreme
    if (score >= 40) return '#fbbf24'; // Yellow — Severe
    if (score >= 20) return '#34d399'; // Green — Moderate
    if (score >= 10) return '#60a5fa'; // Blue — Minor
    return '#94a3b8';                  // Gray — Below Minor
  };

  const StormRow = ({ s }: { s: StormInfo }) => {
    const isActive = s.storm_id === activeStormId;
    const dot = dpsColor(s.dps_score || 0);
    return (
      <button
        onClick={() => onSelectStorm(s.storm_id)}
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
  };

  return (
    <div className="w-72 bg-slate-900 border-r border-slate-700/50 flex flex-col h-screen overflow-hidden">
      {/* Header */}
      <div className="px-4 py-4 border-b border-slate-700/50 shrink-0">
        <h1 className="text-base font-bold text-white tracking-tight">SurgeDPS</h1>
        <p className="text-[10px] text-slate-500 uppercase tracking-widest mt-0.5">Storm Surge Analysis</p>
      </div>

      <div className="flex-1 overflow-y-auto">

        {/* ── ACTIVE STORMS ── */}
        <div className="px-4 pt-4 pb-3 border-b border-slate-700/50">
          <div className="flex items-center justify-between mb-2">
            <h2 className="text-[11px] font-bold text-indigo-400 uppercase tracking-wider">Active Storms</h2>
            <span className="text-[10px] text-slate-500">
              {new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}
            </span>
          </div>
          {activeNHC.length === 0 ? (
            <p className="text-xs text-slate-500 leading-relaxed">
              No active tropical cyclones in any basin. During hurricane season (Jun–Nov Atlantic, May–Nov East Pacific),
              active storms will appear here automatically with live DPS tracking.
            </p>
          ) : (
            <div className="space-y-1">
              {[...activeNHC].sort(byDPS).map(s => <StormRow key={s.storm_id} s={s} />)}
            </div>
          )}
        </div>

        {/* ── STORM LOOKUP ── */}
        <div className="px-4 pt-4 pb-3 border-b border-slate-700/50">
          <h2 className="text-[11px] font-bold text-slate-400 uppercase tracking-wider mb-3">Storm Lookup</h2>
          <div className="mb-2">
            <label className="text-xs text-slate-400 block mb-1">Storm Name</label>
            <input
              type="text"
              placeholder="e.g. Katrina, Harvey, Ike"
              value={searchQuery}
              onChange={e => handleSearch(e.target.value)}
              className="w-full px-3 py-2 bg-slate-800 border border-slate-600 rounded-lg text-sm text-white placeholder-slate-500 focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 outline-none"
            />
            <p className="text-[10px] text-slate-600 mt-1.5">Also accepts ATCF IDs (e.g. AL092008)</p>
          </div>

          {/* Search results dropdown */}
          {searchResults !== null && (
            <div className="bg-slate-800 rounded-lg border border-slate-600 max-h-48 overflow-y-auto mt-2">
              {searchLoading ? (
                <p className="text-xs text-slate-500 p-3 text-center">Searching...</p>
              ) : searchResults.length === 0 ? (
                <p className="text-xs text-slate-500 p-3 text-center">No storms found</p>
              ) : (
                [...searchResults].sort(byDPS).map(s => <StormRow key={s.storm_id} s={s} />)
              )}
            </div>
          )}
        </div>

        {/* ── STORM BROWSER ── */}
        <div className="px-4 pt-4 pb-3">
          <h2 className="text-[11px] font-bold text-slate-400 uppercase tracking-wider mb-3">Storm Browser</h2>

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
                {[...historicStorms].sort(byDPS).map(s => <StormRow key={s.storm_id} s={s} />)}
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
                      [...yearStorms].sort(byDPS).map(s => <StormRow key={s.storm_id} s={s} />)
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

function DashboardPanel({ storm, totals, loadedCells, loadingCells, confidence, eli, validatedDps }: {
  storm: StormInfo | null;
  totals: { buildings: number; loss: number; totalDepth: number };
  loadedCells: Set<string>;
  loadingCells: Set<string>;
  confidence: { level: string; count: number };
  eli: { value: number; tier: string };
  validatedDps: { value: number; adj: number; reason: string };
}) {
  if (!storm) return null;

  return (
    <div className="absolute top-4 left-[19.5rem] bg-white/95 backdrop-blur shadow-2xl rounded-lg p-4 w-72 border border-gray-100 z-10 flex flex-col max-h-[90vh] overflow-y-auto">
      {/* Storm info card */}
      <div className="rounded-xl p-3 mb-3 border shadow-sm"
        style={{ backgroundColor: `${CAT_COLORS[storm.category]}10`, borderColor: `${CAT_COLORS[storm.category]}40` }}
      >
        <div className="flex items-center justify-between mb-1.5">
          <h3 className="font-bold text-gray-800 text-sm">{storm.name}</h3>
          <span className="text-[10px] font-bold px-2 py-0.5 rounded-full text-white"
            style={{ backgroundColor: CAT_COLORS[storm.category] }}
          >CAT {storm.category}</span>
        </div>
        <div className="grid grid-cols-2 gap-x-3 gap-y-0.5 text-xs text-gray-600">
          <span>Wind: <strong className="text-gray-800">{storm.max_wind_kt} kt</strong></span>
          <span>Pressure: <strong className="text-gray-800">{storm.min_pressure_mb} mb</strong></span>
          <span>Year: <strong className="text-gray-800">{storm.year}</strong></span>
          <span>Status: <strong className="text-gray-800 capitalize">{storm.status}</strong></span>
        </div>
      </div>

      {/* R5: Confidence badge */}
      {(() => {
        const cs = CONFIDENCE_STYLES[confidence.level] || CONFIDENCE_STYLES.unvalidated;
        return (
          <div className={`${cs.bg} rounded-lg px-3 py-1.5 mb-3 flex items-center justify-between`}>
            <span className={`text-xs font-bold ${cs.text}`}>{cs.label}</span>
            <span className={`text-[10px] ${cs.text}`}>{confidence.count.toLocaleString()} buildings</span>
          </div>
        );
      })()}

      {/* R11: Validated DPS adjustment */}
      {validatedDps.adj !== 0 && (
        <div className={`${validatedDps.adj > 0 ? 'bg-orange-50 border-orange-200' : 'bg-blue-50 border-blue-200'} rounded-lg px-3 py-1.5 mb-3 border`}>
          <div className="flex items-center justify-between">
            <span className="text-[10px] font-bold text-gray-600 uppercase">Validated DPS</span>
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
              ELI = sqrt(DPS) × sqrt(buildings)
            </div>
          </div>
        );
      })()}

      {/* Scoreboard */}
      {totals.buildings > 0 && (
        <div className="bg-gray-100/50 rounded-xl p-3 text-center border border-gray-200/60 shadow-sm mb-3">
          <div className="text-[10px] text-gray-500 font-bold uppercase tracking-wider mb-0.5">Total Modeled Deficit</div>
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
            Low avg depth ({(totals.totalDepth / totals.buildings).toFixed(1)} ft) across {totals.buildings.toLocaleString()} buildings — aggregate damage may exceed DPS severity level.
          </div>
        </div>
      )}

      {/* Grid status */}
      <div className="bg-blue-50/50 rounded-lg p-2.5 mb-3 border border-blue-100">
        <div className="text-[10px] text-blue-600 font-bold uppercase tracking-wider mb-0.5">Coverage</div>
        <div className="text-sm text-blue-800 font-semibold">
          {loadedCells.size} cell{loadedCells.size !== 1 ? 's' : ''} loaded
        </div>
        <div className="text-xs text-blue-500 mt-0.5">
          {loadingCells.size > 0
            ? `Loading ${loadingCells.size}...`
            : 'Click white borders to expand'}
        </div>
      </div>

      {/* Legend */}
      <div className="space-y-2">
        {LEGEND_ITEMS.map((item, i) => (
          <div key={i} className="flex items-center gap-2">
            <span className="w-4 h-4 rounded shadow-inner border border-gray-200" style={{ backgroundColor: item.color }}></span>
            <span className="text-xs font-medium text-gray-600">{item.label}</span>
          </div>
        ))}
      </div>
      <hr className="my-3 border-gray-200" />
      <div className="grid grid-cols-2 gap-2 text-[11px]">
        <div className="flex items-center gap-1.5"><span className="w-3 h-3 rounded-full bg-[#facc15] border border-gray-300"></span> Minor</div>
        <div className="flex items-center gap-1.5"><span className="w-3 h-3 rounded-full bg-[#fb923c] border border-gray-300"></span> Moderate</div>
        <div className="flex items-center gap-1.5"><span className="w-3 h-3 rounded-full bg-[#ef4444] border border-gray-300"></span> Major</div>
        <div className="flex items-center gap-1.5"><span className="w-3 h-3 rounded-full bg-[#7f1d1d] border border-gray-300"></span> Severe</div>
      </div>
    </div>
  );
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// App
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

function App() {
  const mapRef = useRef<MapRef>(null);

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
      const resp = await fetch(`/api/storm/${stormId}/activate`);
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
      const resp = await fetch(`/api/cell?col=${col}&row=${row}`);
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
    } catch (err) { console.error(`Failed cell (${col},${row}):`, err); }
    finally { setLoadingCells(prev => { const n = new Set([...prev]); n.delete(key); return n; }); }
  }, [loadedCells, loadingCells]);

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
    const f = event.features?.find((f: any) => f.layer.id === 'grid-available-fill');
    if (f) loadCell(f.properties.col, f.properties.row);
  }, [loadCell]);

  const showGrid = zoom < 13;

  return (
    <div className="flex h-screen w-full">
      {/* Left Sidebar — Storm Browser */}
      <StormBrowser
        onSelectStorm={activateStorm}
        activeStormId={activeStorm?.storm_id || null}
        activating={activating}
      />

      {/* Map Area */}
      <div className="relative flex-1">
        <Map
          ref={mapRef}
          initialViewState={{ longitude: -85, latitude: 30, zoom: 5, pitch: 0 }}
          mapStyle={MAP_STYLE}
          interactiveLayerIds={['flood-depth-layer', 'damage-points', 'damage-clusters', ...(showGrid ? ['grid-available-fill'] : [])]}
          onMouseMove={onHover} onClick={onClick}
          onZoom={e => setZoom(e.viewState.zoom)}
        >
          <NavigationControl position="top-right" />

          {allFlood && <Source id="flood-data" type="geojson" data={allFlood}><Layer {...(floodLayerStyle as any)} /></Source>}

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
                paint={{ 'circle-color': '#3b82f6', 'circle-radius': ['step', ['get', 'point_count'], 16, 5, 22, 10, 30], 'circle-stroke-width': 3, 'circle-stroke-color': '#fff' }} />
              <Layer id="damage-cluster-count" type="symbol" filter={['has', 'point_count']}
                layout={{ 'text-field': '{point_count_abbreviated}', 'text-size': 13 }}
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
                  <><h3 className="font-semibold text-gray-800 text-sm mb-1">Inundation</h3>
                  <p className="text-gray-600 text-xs">Depth: {(hoverInfo.feature.properties.depth_ft || hoverInfo.feature.properties.depth * 3.28084).toFixed(1)} ft</p></>
                ) : (
                  <><h3 className="font-semibold text-gray-800 text-sm border-b pb-1 mb-1 border-gray-200">Property Damage</h3>
                  <div className="text-xs space-y-1">
                    <p className="flex justify-between"><span className="text-gray-500">Facility:</span> <span className="font-medium">{hoverInfo.feature.properties.building_type}</span></p>
                    <p className="flex justify-between"><span className="text-gray-500">Category:</span> <span className="font-medium capitalize">{hoverInfo.feature.properties.damage_category}</span></p>
                    <p className="flex justify-between"><span className="text-gray-500">Damage:</span> <span className="font-medium">{hoverInfo.feature.properties.total_damage_pct}%</span></p>
                    <p className="flex justify-between text-sm"><span className="text-gray-500">Loss:</span> <span className="font-bold text-red-600">${hoverInfo.feature.properties.estimated_loss_usd?.toLocaleString()}</span></p>
                  </div></>
                )}
              </div>
            </Popup>
          )}
        </Map>

        {/* Dashboard overlay */}
        <DashboardPanel storm={activeStorm} totals={impactTotals} loadedCells={loadedCells} loadingCells={loadingCells} confidence={confidence} eli={eli} validatedDps={validatedDps} />

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
