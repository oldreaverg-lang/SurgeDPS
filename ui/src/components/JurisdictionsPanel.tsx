// ─────────────────────────────────────────────────────────────────────────────
// JurisdictionsPanel — per-county rollup when the Counties overlay is on.
//
// Shows EM the slice-by-slice picture for independent resource allocation;
// CAT gets the same panel with loss-first framing.
// ─────────────────────────────────────────────────────────────────────────────

import { useState, useMemo } from 'react';
import type { CountyRollup } from '../jurisdictions';
import type { SubPersona } from '../catTeam';

interface Props {
  rollup: CountyRollup[];
  subPersona: SubPersona;
  onFlyTo?: (lon: number, lat: number) => void;
  counties: any;
}

export function JurisdictionsPanel({ rollup, subPersona, onFlyTo, counties }: Props) {
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
      <button onClick={() => setExpanded(e => !e)} className="w-full flex items-center gap-2 text-left">
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
            {topTwoPct > 60 && (
              <> · <span className="font-semibold text-red-700">{topTwoPct}% concentrated in top 2 counties</span></>
            )}
          </div>

          {/* Per-county rows */}
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
                    <span className="text-xs font-black text-red-700 tabular-nums">${(r.loss / 1e6).toFixed(1)}M</span>
                    <span className="text-[9px] text-slate-400 tabular-nums w-7 text-right">{lossPct}%</span>
                  </div>

                  {/* Row 2: persona-flavored detail */}
                  {isEM ? (
                    <div className="text-[10px] text-slate-600 mt-0.5 flex items-center gap-2">
                      <span>{r.buildings.toLocaleString()} bldgs</span>
                      {r.estDisplaced > 0 && <span>· 🏠 ~{r.estDisplaced.toLocaleString()} displaced</span>}
                      {r.criticalFacilities > 0 && <span>· ⭐ {r.criticalFacilities} critical</span>}
                      {r.maxDepthFt > 0 && <span className="ml-auto text-slate-400">max ~{Math.round(r.maxDepthFt)} ft</span>}
                    </div>
                  ) : (
                    <div className="text-[10px] text-slate-600 mt-0.5 flex items-center gap-2">
                      <span>{r.buildings.toLocaleString()} bldgs</span>
                      {uninhab > 0 && <span className="text-red-700 font-semibold">· {uninhab} uninhabitable</span>}
                      {r.criticalFacilities > 0 && <span>· ⭐ {r.criticalFacilities} critical</span>}
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
              ? 'Allocate rescue, shelter, and mutual-aid requests per-county. Displaced = (severe + major) residential × avg household.'
              : 'Per-county adjuster routing follows the same pattern — drag the planner to see per-jurisdiction coverage.'}
          </div>
        </>
      )}
    </div>
  );
}
