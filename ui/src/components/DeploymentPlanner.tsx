// ─────────────────────────────────────────────────────────────────────────────
// DeploymentPlanner — interactive adjuster-count / window-days slider panel.
//
// CAT persona only. Wires planDeployment() + suggestTeamSize() so a CAT
// lead can interactively size their team against the modeled hotspots.
// ─────────────────────────────────────────────────────────────────────────────

import { useState } from 'react';
import type { Hotspot } from '../types';
import { planDeployment, suggestTeamSize } from '../catTeam';

interface Props {
  hotspots: Hotspot[];
  teamSize: number;
  windowDays: number;
  onTeamSizeChange: (n: number) => void;
  onWindowDaysChange: (n: number) => void;
}

export function DeploymentPlanner({ hotspots, teamSize, windowDays, onTeamSizeChange, onWindowDaysChange }: Props) {
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
      <button onClick={() => setExpanded(e => !e)} className="w-full flex items-center gap-2 text-left">
        <span className="text-[10px] font-bold uppercase tracking-wider text-purple-700">Deployment Planner</span>
        <span className="ml-auto text-[10px] text-purple-600 font-semibold">{plan.coverage_pct}% coverage</span>
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
            <input type="range" min={1} max={80} value={teamSize}
              onChange={e => onTeamSizeChange(Number(e.target.value))}
              className="w-full accent-purple-600" />
          </div>

          {/* Window slider */}
          <div className="mt-1">
            <div className="flex justify-between text-[10px] text-slate-600">
              <span className="font-semibold">Window (days)</span>
              <span className="tabular-nums font-bold text-slate-800">{windowDays}</span>
            </div>
            <input type="range" min={1} max={14} value={windowDays}
              onChange={e => onWindowDaysChange(Number(e.target.value))}
              className="w-full accent-purple-600" />
          </div>

          {/* Coverage bar */}
          <div className="mt-2 mb-1.5">
            <div className="h-2 rounded-full bg-slate-200 overflow-hidden">
              <div className={`h-full ${barColor} transition-all`} style={{ width: `${plan.coverage_pct}%` }} />
            </div>
            <div className="flex justify-between text-[9px] text-slate-500 mt-1 tabular-nums">
              <span>{plan.required_adjuster_days.toFixed(0)} adj-days needed</span>
              <span>{plan.capacity_adjuster_days} capacity</span>
            </div>
          </div>

          {/* Shortfall or full coverage */}
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
                    className={`h-full ${a.status === 'covered' ? 'bg-green-500' : a.status === 'partial' ? 'bg-amber-500' : 'bg-red-400'}`}
                    style={{ width: `${a.coverage_pct}%` }}
                  />
                </div>
                <span className={`text-[9px] font-bold px-1.5 py-0.5 rounded-sm shrink-0 ${statusPill(a.status)}`}>
                  {a.coverage_pct}%
                </span>
              </div>
            ))}
          </div>

          {/* Suggest team size */}
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
