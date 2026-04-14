// ─────────────────────────────────────────────────────────────────────────────
// ResourceStagingPanel — EM-only mutual-aid sizing + public advisory draft.
//
// Numbers come from stagingPlan() and the advisory text from
// draftPublicAdvisory(). Nothing here calls an LLM; copy is template-driven
// so an EM can verify every field before release.
// ─────────────────────────────────────────────────────────────────────────────

import { useState } from 'react';
import type { StormInfo, Hotspot } from '../types';
import { stagingPlan } from '../catTeam';
import type { StagingPlan } from '../catTeam';
import { draftPublicAdvisory } from '../catReports';

interface Props {
  storm: StormInfo;
  totals: { buildings: number; loss: number; totalDepth: number };
  hotspots: Hotspot[];
  estimatedPop: number;
  severityCounts: Record<string, number>;
  criticalBreakdown: Array<{ icon: string; label: string; count: number }>;
  rollupDisplaced?: number;
}

export function ResourceStagingPanel({
  storm,
  totals,
  hotspots,
  estimatedPop,
  severityCounts,
  criticalBreakdown,
  rollupDisplaced,
}: Props) {
  const [expanded, setExpanded] = useState(true);
  const [advisoryOpen, setAdvisoryOpen] = useState(false);
  const [copyToast, setCopyToast] = useState(false);

  if (hotspots.length === 0 || totals.buildings <= 0) return null;

  const plan: StagingPlan = stagingPlan(hotspots, estimatedPop, severityCounts, totals.buildings, rollupDisplaced);
  const advisory = draftPublicAdvisory({
    storm: storm as any,
    hotspots: hotspots as any,
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
      <button onClick={() => setExpanded(e => !e)} className="w-full flex items-center gap-2 text-left">
        <span className="text-[10px] font-bold uppercase tracking-wider text-emerald-800">Resource Staging</span>
        <span className="ml-auto text-[9px] font-bold uppercase px-1.5 py-0.5 rounded-sm text-emerald-900 bg-emerald-100">EM</span>
        <span className="text-emerald-600 text-xs">{expanded ? '▲' : '▼'}</span>
      </button>

      {expanded && (
        <>
          {/* Mutual-aid sizing */}
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
            {plan.topStagingArea && <> · Stage {plan.topStagingArea}</>}
          </div>

          {/* Narrative notes */}
          {plan.notes.length > 0 && (
            <ul className="text-[10px] text-slate-600 mb-2 space-y-0.5 list-disc pl-4">
              {plan.notes.map((n, i) => <li key={i}>{n}</li>)}
            </ul>
          )}

          {/* Public advisory toggle */}
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
                {copyToast && <span className="text-[10px] text-emerald-700 font-semibold">✓ Copied</span>}
                <span className="ml-auto text-[9px] text-slate-400 italic">Template only — verify before release</span>
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
