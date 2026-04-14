// ─────────────────────────────────────────────────────────────────────────────
// CatDeploymentSummary — glanceable Ops-mode deployment header.
//
// Shown at the top of DashboardPanel in Ops Mode, above the existing
// Total Modeled Loss scoreboard. Purpose: give a CAT / CRT deployment
// lead a glanceable answer to "how big is this, what's the peril
// mix, where do I send people first?" — without making the numbers
// sound more precise than the model warrants.
// ─────────────────────────────────────────────────────────────────────────────

import type { StormInfo, Hotspot, DisplayMode } from '../types';
import {
  workloadSummary,
  aggregatePerilMix,
  perilHeadline,
  timeToClearDays,
  formatTimeToClear,
  shelterPosture,
  worstShelterPosture,
  stagingPlan,
} from '../catTeam';
import type { SubPersona } from '../catTeam';
import { formatLossOps, formatCountOps } from '../utils/format';

interface Props {
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
}

export function CatDeploymentSummary({
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
}: Props) {
  if (mode !== 'ops') return null;
  if (totals.buildings <= 0) return null;
  const isEM = subPersona === 'em';

  const wl = workloadSummary(severityCounts);
  const stormMix = aggregatePerilMix(
    hotspots.map(h => ({
      windPct: h.windPct,
      waterPct: h.waterPct,
      surgePct: h.surgePct,
      rainPct: h.rainPct,
      weight: h.count,
    })),
  );
  const headline = wl.headline;
  const top = hotspots[0];

  const urgencyColor =
    headline === 'Deploy immediately' ? 'bg-red-600'
    : headline === 'Deploy CAT team' ? 'bg-orange-500'
    : headline === 'Deploy field adjusters' ? 'bg-amber-500'
    : headline === 'Standard claims handling' ? 'bg-sky-500'
    : 'bg-slate-400';

  const worstPost = worstShelterPosture(
    hotspots.map(h => ({ maxDepthFt: h.maxDepthFt, windPct: h.windPct, waterPct: h.waterPct })),
  );
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

      {/* Peril mix bar */}
      {stormMix.rainPct > 0 ? (
        <div className="flex items-center gap-2 mb-2" title={`${stormMix.surgePct}% surge · ${stormMix.rainPct}% rain · ${stormMix.windPct}% wind`}>
          <div className="flex-1 h-3 rounded-full overflow-hidden bg-slate-200 flex">
            <div className="bg-indigo-500" style={{ width: `${stormMix.surgePct}%` }} />
            <div className="bg-cyan-400"   style={{ width: `${stormMix.rainPct}%` }} />
            <div className="bg-sky-400"    style={{ width: `${stormMix.windPct}%` }} />
          </div>
          <div className="text-[10px] text-slate-600 tabular-nums shrink-0 font-semibold">
            🌊 {stormMix.surgePct}% · 🌧️ {stormMix.rainPct}% · 🌬️ {stormMix.windPct}%
          </div>
        </div>
      ) : (
        <div className="flex items-center gap-2 mb-2" title={`${stormMix.waterPct}% water · ${stormMix.windPct}% wind`}>
          <div className="flex-1 h-3 rounded-full overflow-hidden bg-slate-200 flex">
            <div className="bg-indigo-500" style={{ width: `${stormMix.waterPct}%` }} />
            <div className="bg-sky-400"    style={{ width: `${stormMix.windPct}%` }} />
          </div>
          <div className="text-[10px] text-slate-600 tabular-nums shrink-0 font-semibold">
            🌊 {stormMix.waterPct}% · 🌬️ {stormMix.windPct}%
          </div>
        </div>
      )}

      {/* Workload translation */}
      {wl.inspections_needed > 0 && (
        <div className="text-[11px] text-slate-700 leading-snug mb-2">
          <span className="font-bold">~{formatCountOps(wl.inspections_needed, mode)}</span> inspections needed
          {wl.uninhabitable > 0 && (
            <> · <span className="font-bold text-red-700">~{formatCountOps(wl.uninhabitable, mode)}</span> likely uninhabitable</>
          )}
        </div>
      )}

      {/* Top priority callout */}
      {top && (
        <div className={`rounded-md bg-white/80 border px-2 py-1.5 mb-2 ${isEM ? 'border-emerald-200' : 'border-orange-200'}`}>
          <div className={`text-[9px] font-bold uppercase tracking-wider ${isEM ? 'text-emerald-700' : 'text-orange-700'}`}>Top Priority</div>
          <div className="text-[11px] text-slate-800">
            <span className="font-bold">#{top.rank}</span> · {formatLossOps(top.loss, mode)} ·{' '}
            <span className="text-slate-500">{formatCountOps(top.count, mode)} bldgs</span>
          </div>
          {isEM ? (
            (() => {
              const post = shelterPosture(top.maxDepthFt, { windPct: top.windPct, waterPct: top.waterPct });
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

      {/* EM-only: worst storm-wide shelter posture */}
      {isEM && (
        <div className="text-[10px] text-slate-700 mb-2 flex items-center gap-1.5">
          <span className="font-semibold">Storm-wide posture:</span>
          <span className={`text-[9px] font-bold px-1.5 py-0.5 rounded-sm ${worstPost.classes}`} title={worstPost.description}>
            {worstPost.icon} {worstPost.label}
          </span>
        </div>
      )}

      {/* EM-only: mutual-aid quick numbers */}
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

      {/* Time to Clear — CAT only */}
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

      {/* Action buttons */}
      <div className="flex gap-1.5">
        {isEM ? (
          <>
            <div className="flex-1 flex">
              <button onClick={() => onGenerateSitRep('html')} title="Download Situation Report (HTML)"
                className="flex-1 text-[10px] font-bold px-2 py-1 rounded-l-md border border-emerald-500 text-white bg-emerald-600 hover:bg-emerald-700 transition-colors">SitRep ↓</button>
              <button onClick={() => onGenerateSitRep('pdf')} title="Save Situation Report as PDF" aria-label="Save SitRep as PDF"
                className="text-[9px] font-bold px-1.5 py-1 rounded-r-md border-t border-r border-b border-emerald-500 text-emerald-700 bg-white hover:bg-emerald-50 transition-colors">PDF</button>
            </div>
            <div className="flex-1 flex">
              <button onClick={() => onGenerateCatReport('html')} title="Download CAT Deployment Report (HTML)"
                className="flex-1 text-[10px] font-bold px-2 py-1 rounded-l-md border border-orange-300 text-orange-700 bg-white hover:bg-orange-50 transition-colors">CAT ↓</button>
              <button onClick={() => onGenerateCatReport('pdf')} title="Save CAT Report as PDF" aria-label="Save CAT Report as PDF"
                className="text-[9px] font-bold px-1.5 py-1 rounded-r-md border-t border-r border-b border-orange-300 text-orange-700 bg-white hover:bg-orange-50 transition-colors">PDF</button>
            </div>
          </>
        ) : (
          <>
            <div className="flex-1 flex">
              <button onClick={() => onGenerateCatReport('html')} title="Download CAT Deployment Report (HTML)"
                className="flex-1 text-[10px] font-bold px-2 py-1 rounded-l-md border border-orange-500 text-white bg-orange-600 hover:bg-orange-700 transition-colors">CAT Report ↓</button>
              <button onClick={() => onGenerateCatReport('pdf')} title="Save CAT Report as PDF" aria-label="Save CAT Report as PDF"
                className="text-[9px] font-bold px-1.5 py-1 rounded-r-md border-t border-r border-b border-orange-500 text-white bg-orange-600 hover:bg-orange-700 transition-colors">PDF</button>
            </div>
            <div className="flex-1 flex">
              <button onClick={() => onGenerateSitRep('html')} title="Download Situation Report (HTML)"
                className="flex-1 text-[10px] font-bold px-2 py-1 rounded-l-md border border-emerald-300 text-emerald-700 bg-white hover:bg-emerald-50 transition-colors">SitRep ↓</button>
              <button onClick={() => onGenerateSitRep('pdf')} title="Save SitRep as PDF" aria-label="Save SitRep as PDF"
                className="text-[9px] font-bold px-1.5 py-1 rounded-r-md border-t border-r border-b border-emerald-300 text-emerald-700 bg-white hover:bg-emerald-50 transition-colors">PDF</button>
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
