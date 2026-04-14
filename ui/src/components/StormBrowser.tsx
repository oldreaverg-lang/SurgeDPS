import { useState, useEffect, useCallback, useRef } from 'react';
import type { StormInfo, Season } from '../types';
import { fetchJsonArray } from '../utils/fetch';
import { byDPS, dpsColor, shortName } from '../utils/format';

const CAT_COLORS: Record<number, string> = {
  0: '#5eead4', 1: '#facc15', 2: '#fb923c', 3: '#ef4444', 4: '#dc2626', 5: '#7f1d1d',
};

// ── StormRow ──────────────────────────────────────────────────────────────────

function StormRow({ s, activeStormId, activating, onSelect }: {
  s: StormInfo;
  activeStormId: string | null;
  activating: boolean;
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
      <span style={{ background: catColor, width: 8, height: 8, borderRadius: '50%', flexShrink: 0 }} />
      <span className="truncate font-medium">{shortName(s.name)}</span>
      <span className="text-[10px] text-slate-600 font-normal shrink-0">{s.year}</span>
      <span className="ml-auto text-xs shrink-0">
        {s.dps_score
          ? <><span style={{ color: dot, fontWeight: 700 }}>{s.dps_score.toFixed(0)}</span><span className="text-slate-600"> DPS</span></>
          : <span className="text-slate-600">—</span>
        }
      </span>
    </button>
  );
}

// ── StormBrowser ──────────────────────────────────────────────────────────────

interface Props {
  onSelectStorm: (id: string) => void;
  activeStormId: string | null;
  activating: boolean;
  isOpen: boolean;
  onClose: () => void;
  activeStorm: StormInfo | null;
}

export function StormBrowser({ onSelectStorm, activeStormId, activating, isOpen, onClose, activeStorm }: Props) {
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
    const onErr = () => { failed++; if (failed >= 3) setLoadError(true); };
    fetchJsonArray<Season>('/surgedps/api/seasons', undefined, onErr).then(setSeasons);
    fetchJsonArray<StormInfo>('/surgedps/api/storms/historic', undefined, onErr).then(setHistoricStorms);
    fetchJsonArray<StormInfo>('/surgedps/api/storms/active', undefined, onErr).then(setActiveNHC);
  }, []);

  const toggleYear = useCallback((year: number) => {
    if (expandedYear === year) { setExpandedYear(null); setYearStorms([]); }
    else {
      setExpandedYear(year);
      fetchJsonArray<StormInfo>(`/surgedps/api/season/${year}`).then(setYearStorms);
    }
  }, [expandedYear]);

  const handleSearch = useCallback((q: string) => {
    setSearchQuery(q);
    if (searchTimeout.current) clearTimeout(searchTimeout.current);
    if (!q.trim()) { setSearchResults(null); return; }
    setSearchLoading(true);
    searchTimeout.current = setTimeout(() => {
      fetchJsonArray<StormInfo>(`/surgedps/api/storms/search?q=${encodeURIComponent(q)}`)
        .then(data => { setSearchResults(data); setSearchLoading(false); })
        .catch(() => { setSearchResults([]); setSearchLoading(false); });
    }, 300);
  }, []);

  useEffect(() => () => { if (searchTimeout.current) clearTimeout(searchTimeout.current); }, []);

  const selectAndClose = useCallback((id: string) => {
    setSearchQuery(''); setSearchResults(null);
    onSelectStorm(id);
    onClose();
  }, [onSelectStorm, onClose]);

  return (
    <div className={`w-72 shrink-0 bg-slate-900 border-r border-slate-700/50 flex flex-col h-screen overflow-hidden absolute inset-y-0 left-0 z-30 lg:relative transition-transform duration-300 ease-in-out ${isOpen ? 'translate-x-0' : '-translate-x-full lg:translate-x-0'}`}>
      {/* Header */}
      <div className="px-4 py-4 border-b border-slate-700/50 shrink-0">
        <div className="flex items-center justify-between">
          <h1 className="text-base font-bold text-white tracking-tight">SurgeDPS</h1>
          <div className="flex items-center gap-2">
            <a href="https://stormdps.com" target="_blank" rel="noopener noreferrer"
              className="text-[11px] font-semibold text-cyan-400 hover:text-cyan-300 transition-colors border border-cyan-700 hover:border-cyan-500 rounded px-2 py-0.5">
              ← StormDPS
            </a>
            <button onClick={onClose} className="lg:hidden text-slate-400 hover:text-white transition-colors p-1 rounded" aria-label="Close sidebar">✕</button>
          </div>
        </div>
        <p className="text-[10px] text-slate-500 uppercase tracking-widest mt-0.5">Storm Surge Analysis</p>
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

        {/* Active Storms */}
        <div className="px-4 pt-4 pb-3 border-b border-slate-700/50">
          <h2 className="text-[11px] font-bold text-indigo-400 uppercase tracking-wider mb-2">Active Storms</h2>
          {activeNHC.length === 0 ? (
            <p className="text-xs text-slate-500 leading-relaxed">
              No active tropical cyclones in any basin. During hurricane season (Jun–Nov Atlantic, May–Nov East Pacific),
              active storms will appear here automatically.
            </p>
          ) : (
            <div className="space-y-1">
              {[...activeNHC].sort(byDPS).map(s => (
                <StormRow key={s.storm_id} s={s} activeStormId={activeStormId} activating={activating} onSelect={selectAndClose} />
              ))}
            </div>
          )}
        </div>

        {/* Storm Lookup */}
        <div className="px-4 pt-4 pb-3 border-b border-slate-700/50">
          <h2 className="text-[11px] font-bold text-slate-400 uppercase tracking-wider mb-3">Storm Lookup</h2>
          <input
            type="text"
            placeholder="Search by name, e.g. Katrina, Harvey…"
            value={searchQuery}
            onChange={e => handleSearch(e.target.value)}
            className="w-full px-3 py-2 bg-slate-800 border border-slate-600 rounded-lg text-sm text-white placeholder-slate-500 focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 outline-none mb-2"
          />
          {searchResults !== null && (
            <div className="bg-slate-800 rounded-lg border border-slate-600 max-h-48 overflow-y-auto mt-2">
              {searchLoading ? (
                <p className="text-xs text-slate-500 p-3 text-center">Searching...</p>
              ) : searchResults.length === 0 ? (
                <p className="text-xs text-slate-500 p-3 text-center">No storms found</p>
              ) : (
                [...searchResults].sort(byDPS).map(s => (
                  <StormRow key={s.storm_id} s={s} activeStormId={activeStormId} activating={activating} onSelect={selectAndClose} />
                ))
              )}
            </div>
          )}
        </div>

        {/* Storm Browser */}
        <div className="px-4 pt-4 pb-3">
          <h2 className="text-[11px] font-bold text-slate-400 uppercase tracking-wider mb-1">Storm Browser</h2>
          <p className="text-[10px] text-slate-600 mb-3">Sorted by Damage Potential Score (DPS) — higher = more destructive surge</p>

          <div className="mb-1">
            <button onClick={() => setHistoricOpen(!historicOpen)}
              className="w-full flex items-center justify-between px-3 py-2.5 bg-slate-800/50 hover:bg-slate-800 rounded-lg transition-colors">
              <span className="text-sm font-semibold text-slate-200">Historic Storms</span>
              <div className="flex items-center gap-2">
                <span className="text-xs text-indigo-400 font-medium">{historicStorms.length}</span>
                <span className={`text-slate-500 text-xs transition-transform ${historicOpen ? 'rotate-90' : ''}`}>▸</span>
              </div>
            </button>
            {historicOpen && (
              <div className="mt-1 ml-1 pl-2 border-l border-slate-700/50 space-y-0.5">
                {[...historicStorms].sort(byDPS).map(s => (
                  <StormRow key={s.storm_id} s={s} activeStormId={activeStormId} activating={activating} onSelect={selectAndClose} />
                ))}
              </div>
            )}
          </div>

          {seasons.map(({ year, count }) => {
            const open = expandedYear === year;
            return (
              <div key={year} className="mb-1">
                <button onClick={() => toggleYear(year)}
                  className="w-full flex items-center justify-between px-3 py-2.5 bg-slate-800/50 hover:bg-slate-800 rounded-lg transition-colors">
                  <span className="text-sm font-semibold text-slate-200">{year} Season</span>
                  <div className="flex items-center gap-2">
                    <span className="text-xs text-indigo-400 font-medium">{count}</span>
                    <span className={`text-slate-500 text-xs transition-transform ${open ? 'rotate-90' : ''}`}>▸</span>
                  </div>
                </button>
                {open && (
                  <div className="mt-1 ml-1 pl-2 border-l border-slate-700/50 space-y-0.5">
                    {yearStorms.length === 0 ? (
                      <p className="text-xs text-slate-500 py-2 px-3">Loading...</p>
                    ) : (
                      [...yearStorms].sort(byDPS).map(s => (
                        <StormRow key={s.storm_id} s={s} activeStormId={activeStormId} activating={activating} onSelect={selectAndClose} />
                      ))
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
