// ─────────────────────────────────────────────────────────────────────────────
// DashboardPanel — right-side overlay on the map.
//
// Orchestrates the mode/persona toggles, CAT Deployment Summary,
// Deployment Planner, Resource Staging, Jurisdictions, Beta Layers,
// confidence badges, damage scoreboard, hardest-hit areas, and grid status.
// ─────────────────────────────────────────────────────────────────────────────

import { useState, useEffect } from 'react';
import type { StormInfo, Hotspot, DisplayMode, ConfidenceLevel } from '../types';
import { workloadSummary, shelterPosture } from '../catTeam';
import type { SubPersona } from '../catTeam';
import type { CountyRollup } from '../jurisdictions';
import { formatLossOps, formatCountOps, formatDepthOps } from '../utils/format';
import { CatDeploymentSummary } from './CatDeploymentSummary';
import { DeploymentPlanner } from './DeploymentPlanner';
import { ResourceStagingPanel } from './ResourceStagingPanel';
import { JurisdictionsPanel } from './JurisdictionsPanel';
import { BetaDataLayersPanel } from './BetaDataLayersPanel';

const CAT_COLORS: Record<number, string> = {
  0: '#5eead4', 1: '#facc15', 2: '#fb923c', 3: '#ef4444', 4: '#dc2626', 5: '#7f1d1d',
};

const CONFIDENCE_STYLES: Record<string, { bg: string; text: string; label: string }> = {
  high:        { bg: 'bg-green-100',  text: 'text-green-800',  label: 'High Confidence' },
  medium:      { bg: 'bg-yellow-100', text: 'text-yellow-800', label: 'Medium Confidence' },
  low:         { bg: 'bg-red-100',    text: 'text-red-800',    label: 'Low Confidence' },
  unvalidated: { bg: 'bg-gray-100',   text: 'text-gray-500',   label: 'Unvalidated' },
};

// ── Confidence pip component ──────────────────────────────────────────────────

function Pip({ label, lv, title }: { label: string; lv: 'high' | 'medium' | 'low'; title: string }) {
  const color = lv === 'high' ? 'bg-emerald-500' : lv === 'medium' ? 'bg-amber-400' : 'bg-rose-400';
  const fill  = lv === 'high' ? 5 : lv === 'medium' ? 3 : 2;
  return (
    <div className="flex items-center gap-1.5 text-[10px] text-slate-600" title={title}>
      <span className="font-semibold w-[52px]">{label}</span>
      <div className="flex gap-0.5">
        {[0, 1, 2, 3, 4].map(i => (
          <span key={i} className={`w-1.5 h-2 rounded-sm ${i < fill ? color : 'bg-slate-200'}`} />
        ))}
      </div>
    </div>
  );
}

// ── DashboardPanel ────────────────────────────────────────────────────────────

interface Props {
  storm: StormInfo | null;
  totals: { buildings: number; loss: number; totalDepth: number };
  loadedCells: Set<string>;
  loadingCells: Set<string>;
  confidence: ConfidenceLevel;
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
}

export function DashboardPanel({
  storm,
  totals,
  loadedCells,
  loadingCells,
  confidence,
  eli: _eli,
  validatedDps: _validatedDps,
  onOpenSidebar,
  zoom,
  onClearStorm,
  estimatedPop,
  severityCounts,
  criticalCount,
  criticalBreakdown,
  hotspots,
  onFlyTo,
  mode,
  onModeChange,
  subPersona,
  onSubPersonaChange,
  onGenerateCatReport,
  onGenerateSitRep,
  teamSize,
  windowDays,
  onTeamSizeChange,
  onWindowDaysChange,
  betaLayersEnabled,
  countyRollup,
  countiesGeoJSON,
  totalDisplaced,
  showCounties,
}: Props) {
  // Auto-expand on desktop, collapsed by default on mobile
  const [expanded, setExpanded] = useState(false);
  useEffect(() => {
    const mq = window.matchMedia('(min-width: 1024px)');
    setExpanded(mq.matches);
    const handler = (e: MediaQueryListEvent) => setExpanded(e.matches);
    mq.addEventListener('change', handler);
    return () => mq.removeEventListener('change', handler);
  }, []);

  // Authoritative displaced count from county rollup, falls back to
  // the totalDisplaced from allBuildings when counties aren't loaded yet.
  const rollupDisplaced = countyRollup
    ? countyRollup.reduce((s, r) => s + r.estDisplaced, 0)
    : totalDisplaced;

  if (!storm) return null;

  return (
    <div className="absolute top-4 right-14 bg-white/95 backdrop-blur shadow-2xl rounded-lg w-72 max-w-[calc(100vw-2rem)] border border-gray-100 z-10">

      {/* ── Always-visible compact header ── */}
      <div className="flex items-center gap-2 px-3 py-2.5">
        <button
          onClick={onOpenSidebar}
          className="lg:hidden text-slate-400 hover:text-slate-700 transition-colors p-1 rounded text-base leading-none shrink-0"
          aria-label="Open storm browser"
        >☰</button>

        <div className="flex items-center gap-1.5 flex-1 min-w-0">
          <span className="font-bold text-gray-800 text-sm truncate">{storm.name}</span>
          <span className="text-[10px] font-bold px-1.5 py-0.5 rounded-full text-white shrink-0"
            style={{ backgroundColor: CAT_COLORS[storm.category] }}>CAT {storm.category}</span>
        </div>

        {!expanded && totals.loss > 0 && (
          <span className="text-red-600 font-black text-sm shrink-0">{formatLossOps(totals.loss, mode)}</span>
        )}

        <button
          onClick={onClearStorm}
          className="text-gray-300 hover:text-red-500 transition-colors shrink-0 text-xs px-0.5"
          aria-label="Close storm" title="Close storm"
        >✕</button>

        <button
          onClick={() => setExpanded(e => !e)}
          className="text-gray-400 hover:text-gray-600 transition-colors shrink-0 text-xs px-1"
          aria-label={expanded ? 'Collapse panel' : 'Expand panel'}
        >{expanded ? '▲' : '▼'}</button>
      </div>

      {/* ── Mode toggle ── */}
      {expanded && (
        <div className="px-3 pb-2 -mt-1">
          <div role="tablist" aria-label="Display mode"
            className="inline-flex w-full items-center rounded-md bg-slate-100 p-0.5 text-[10px] font-bold uppercase tracking-wider">
            <button
              role="tab" aria-selected={mode === 'analyst'}
              onClick={() => onModeChange('analyst')}
              className={`flex-1 px-2 py-1 rounded transition-colors ${mode === 'analyst' ? 'bg-white text-slate-800 shadow-sm' : 'text-slate-500 hover:text-slate-700'}`}
              title="Analyst Mode — precise dollar losses and technical detail"
            >Analyst</button>
            <button
              role="tab" aria-selected={mode === 'ops'}
              onClick={() => onModeChange('ops')}
              className={`flex-1 px-2 py-1 rounded transition-colors ${
                mode === 'ops'
                  ? subPersona === 'em' ? 'bg-white text-emerald-700 shadow-sm' : 'bg-white text-orange-700 shadow-sm'
                  : 'text-slate-500 hover:text-slate-700'
              }`}
              title="Ops Mode — deployment-focused with confidence bands and rounded numbers"
            >Ops</button>
          </div>

          {/* Sub-persona selector — Ops only */}
          {mode === 'ops' && (
            <div role="tablist" aria-label="Ops sub-persona"
              className="inline-flex w-full items-center rounded-md bg-slate-50 border border-slate-200 p-0.5 text-[9px] font-bold uppercase tracking-wider mt-1">
              <button
                role="tab" aria-selected={subPersona === 'cat'}
                onClick={() => onSubPersonaChange('cat')}
                className={`flex-1 px-2 py-1 rounded transition-colors ${subPersona === 'cat' ? 'bg-orange-500 text-white shadow-sm' : 'text-slate-500 hover:text-orange-600'}`}
                title="Insurance CAT / CRT lens — adjuster deployment, claims routing, CAT Report"
              >🏢 Insurance CAT</button>
              <button
                role="tab" aria-selected={subPersona === 'em'}
                onClick={() => onSubPersonaChange('em')}
                className={`flex-1 px-2 py-1 rounded transition-colors ${subPersona === 'em' ? 'bg-emerald-600 text-white shadow-sm' : 'text-slate-500 hover:text-emerald-700'}`}
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
            style={{ backgroundColor: `${CAT_COLORS[storm.category]}10`, borderColor: `${CAT_COLORS[storm.category]}40` }}>
            <div className="grid grid-cols-2 gap-x-3 gap-y-0.5 text-xs text-gray-600">
              <span>Wind: <strong className="text-gray-800">{Math.round(storm.max_wind_kt * 1.15078)} mph</strong></span>
              <span>Pressure: <strong className="text-gray-800">{storm.min_pressure_mb} mb</strong></span>
              <span>Year: <strong className="text-gray-800">{storm.year}</strong></span>
            </div>
            {storm.population?.pop_label && (
              <div className="mt-1.5 pt-1.5 border-t border-gray-200/50 flex items-center gap-1.5 text-xs text-gray-600">
                <span className="text-sm">👥</span>
                <span>
                  <strong className="text-gray-800">{storm.population.pop_label}</strong>{' '}
                  in {storm.population.county_name}, {storm.population.state_code}
                </span>
                {storm.population.vintage && <span className="text-[10px] text-gray-400">({storm.population.vintage})</span>}
              </div>
            )}
            <div className="mt-1.5 pt-1.5 border-t border-gray-200/50 text-[10px] text-gray-500">
              <span className="font-semibold">Surge note:</span> Modeled depths reflect SLOSH maximum-of-maximums
              (worst-case tidal alignment). Actual depths may have been lower if landfall did not coincide with local high tide.
            </div>
          </div>

          {/* CAT Deployment Summary (Ops only) */}
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

          {/* Deployment Planner — CAT persona only */}
          {mode === 'ops' && subPersona === 'cat' && (
            <DeploymentPlanner
              hotspots={hotspots}
              teamSize={teamSize}
              windowDays={windowDays}
              onTeamSizeChange={onTeamSizeChange}
              onWindowDaysChange={onWindowDaysChange}
            />
          )}

          {/* Resource Staging — EM persona only */}
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

          {/* Jurisdictions — when Counties overlay is on */}
          {mode === 'ops' && showCounties && countyRollup && countyRollup.length > 0 && (
            <JurisdictionsPanel
              rollup={countyRollup}
              subPersona={subPersona}
              onFlyTo={onFlyTo}
              counties={countiesGeoJSON}
            />
          )}

          {/* Beta data layers — gated by More-menu flag, Ops only */}
          {mode === 'ops' && betaLayersEnabled && (
            <BetaDataLayersPanel storm={storm} hotspots={hotspots} subPersona={subPersona} />
          )}

          {/* Confidence badge */}
          {(() => {
            const cs = CONFIDENCE_STYLES[confidence.level] || CONFIDENCE_STYLES.unvalidated;
            const tip =
              confidence.level === 'high'   ? 'Strong building data coverage in the affected area'
              : confidence.level === 'medium' ? 'Moderate building data — some gaps possible'
              : confidence.level === 'low'    ? 'Limited building data — estimates may be incomplete'
              : 'Model estimate only — building data not yet loaded';

            const buildingsLevel: 'high' | 'medium' | 'low' =
              confidence.level === 'high' ? 'high' : confidence.level === 'medium' ? 'medium' : 'low';
            const popLevel: 'high' | 'medium' | 'low' =
              storm.population?.population != null ? 'high' : storm.population?.pop_label ? 'medium' : 'low';

            return (
              <div className={`${cs.bg} rounded-lg px-3 py-2 mb-3`}>
                <div className="flex items-center justify-between">
                  <span className={`text-xs font-bold ${cs.text}`}>{cs.label}</span>
                  <span className={`text-[10px] ${cs.text}`}>{confidence.count.toLocaleString()} buildings</span>
                </div>
                <p className={`text-[10px] mt-0.5 ${cs.text} opacity-75`}>{tip}</p>
                <div className="mt-1.5 pt-1.5 border-t border-white/60 grid grid-cols-1 gap-0.5">
                  <Pip label="Surge"     lv="high"          title="SLOSH maximum-of-maximums modeling for this event" />
                  <Pip label="Buildings" lv={buildingsLevel} title="Building inventory coverage in the loaded grid cells" />
                  <Pip label="Populatn." lv={popLevel}       title="County-level population data availability for the affected area" />
                </div>
              </div>
            );
          })()}

          {/* Critical Facilities */}
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
                  { key: 'severe',   color: '#7f1d1d', label: 'Severe' },
                  { key: 'major',    color: '#ef4444', label: 'Major' },
                  { key: 'moderate', color: '#fb923c', label: 'Moderate' },
                  { key: 'minor',    color: '#facc15', label: 'Minor' },
                  { key: 'none',     color: '#4ade80', label: 'No Damage' },
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
                const wl = workloadSummary(severityCounts);
                if (wl.inspections_needed === 0) return null;
                const show = wl.uninhabitable > 0 || wl.inspections_needed >= 100;
                if (!show) return null;
                return (
                  <div className="mt-2 bg-red-50 rounded px-2 py-1 border border-red-200">
                    <div className="text-[10px] font-bold text-red-700">
                      {formatCountOps(wl.inspections_needed, mode)} inspections needed
                      {wl.uninhabitable > 0 && <> · {formatCountOps(wl.uninhabitable, mode)} likely uninhabitable</>}
                    </div>
                    <div className="text-[9px] text-red-500 mt-0.5 italic">{wl.headline}</div>
                  </div>
                );
              })()}
            </div>
          )}

          {/* Nuisance Flood Flag */}
          {totals.buildings > 2000 && totals.totalDepth > 0 && (totals.totalDepth / totals.buildings) < 1.5 && (
            <div className="bg-amber-50 rounded-lg px-3 py-2 mb-3 border border-amber-300">
              <div className="text-[10px] text-amber-800 font-bold uppercase tracking-wider">Nuisance Flood Warning</div>
              <div className="text-xs text-amber-700 mt-0.5">
                Avg. depth of {formatDepthOps(totals.totalDepth / totals.buildings, mode)} across{' '}
                {formatCountOps(totals.buildings, mode)} buildings — widespread shallow flooding can cause
                significant aggregate damage even when individual losses appear modest.
              </div>
            </div>
          )}

          {/* Hardest-Hit Areas */}
          {hotspots.length > 0 && (
            <div className="bg-red-50/50 rounded-lg p-2.5 mb-3 border border-red-100">
              <div className="text-[10px] text-red-600 font-bold uppercase tracking-wider mb-1.5">Hardest-Hit Areas</div>
              <div className="space-y-2">
                {hotspots.map((h) => {
                  const isEM = mode === 'ops' && subPersona === 'em';
                  const post = isEM ? shelterPosture(h.maxDepthFt, { windPct: h.windPct, waterPct: h.waterPct }) : null;
                  return (
                    <button
                      key={h.rank}
                      onClick={() => onFlyTo?.(h.lon, h.lat)}
                      className="w-full text-left hover:bg-red-100/50 rounded px-1 py-1 transition-colors"
                    >
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
                          <span className={`text-[9px] font-bold px-1.5 py-0.5 rounded-sm shrink-0 ${post.classes}`} title={post.description}>
                            {post.short}
                          </span>
                        ) : (
                          <span className={`text-[9px] font-bold px-1.5 py-0.5 rounded-sm shrink-0 ${h.routing.classes}`} title={h.routing.description}>
                            {h.routing.short}
                          </span>
                        )}
                      </div>

                      {/* Peril mix bar */}
                      <div className="mt-1 ml-6 flex items-center gap-1.5">
                        <div className="flex-1 h-1.5 rounded-sm overflow-hidden bg-slate-200 flex" title={`${h.waterPct}% water · ${h.windPct}% wind`}>
                          <div className="bg-indigo-500" style={{ width: `${h.waterPct}%` }} />
                          <div className="bg-sky-400"    style={{ width: `${h.windPct}%` }} />
                        </div>
                        <span className="text-[9px] text-slate-500 tabular-nums shrink-0">
                          🌊 {h.waterPct}% · 🌬️ {h.windPct}%
                        </span>
                      </div>

                      {/* Sub-line */}
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
              {totals.buildings > 0
                ? `${totals.buildings.toLocaleString()} buildings`
                : `${loadedCells.size} area${loadedCells.size !== 1 ? 's' : ''}`} analyzed
            </div>
            <div className="text-xs text-blue-500 mt-0.5">
              {loadingCells.size > 0
                ? `Fetching data for ${loadingCells.size} more area${loadingCells.size !== 1 ? 's' : ''}…`
                : zoom >= 13 ? 'Zoom out to see grid borders and expand coverage'
                : 'Click the dashed borders on the map to expand coverage'}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
