// ─────────────────────────────────────────────────────────────────────────────
// BetaDataLayersPanel — scaffolding for Phase 5 experimental data layers.
//
// Gated behind the betaLayersEnabled flag from the More-menu. All four
// layer stubs (rainfall, shelters, vendor coverage, time-to-access) fetch
// quickly and return placeholder objects — real data ships when each
// backend endpoint lands.
// ─────────────────────────────────────────────────────────────────────────────

import { useState, useEffect } from 'react';
import type { StormInfo, Hotspot } from '../types';
import {
  fetchRainfallOverlay,
  fetchShelterCapacity,
  fetchVendorCoverage,
  fetchTimeToAccess,
} from '../betaLayers';
import type {
  RainfallOverlay,
  ShelterCapacityLayer,
  VendorCoverageLayer,
  TimeToAccessLayer,
} from '../betaLayers';
import type { SubPersona } from '../catTeam';

// ── BetaSection ───────────────────────────────────────────────────────────────

function BetaSection({ title, badge, notes }: { title: string; badge: string; notes: string }) {
  return (
    <div className="rounded-md border border-dashed border-purple-300 bg-white/70 px-2 py-1.5 mb-1.5">
      <div className="flex items-center gap-1.5 mb-0.5">
        <span className="text-[10px] font-bold text-purple-900">{title}</span>
        <span className="ml-auto text-[8px] font-bold uppercase tracking-wider px-1 py-0.5 rounded-sm bg-purple-100 text-purple-800 border border-purple-200">
          {badge}
        </span>
      </div>
      <div className="text-[9px] text-slate-600 italic leading-snug">{notes}</div>
      <div className="text-[8px] text-purple-500 mt-0.5 uppercase tracking-wider font-bold">Data layer pending</div>
    </div>
  );
}

// ── BetaDataLayersPanel ───────────────────────────────────────────────────────

interface Props {
  storm: StormInfo;
  hotspots: Hotspot[];
  subPersona: SubPersona;
}

export function BetaDataLayersPanel({ storm, hotspots, subPersona }: Props) {
  const [expanded, setExpanded] = useState(false);
  const [rainfall, setRainfall] = useState<RainfallOverlay | null>(null);
  const [shelters, setShelters] = useState<ShelterCapacityLayer | null>(null);
  const [vendors, setVendors] = useState<VendorCoverageLayer | null>(null);
  const [access, setAccess] = useState<TimeToAccessLayer | null>(null);

  const stormId = (storm as any)?.id || (storm as any)?.storm_id || storm.name || 'unknown';

  useEffect(() => {
    let cancelled = false;
    (async () => {
      const [r, s, v, a] = await Promise.all([
        fetchRainfallOverlay(stormId),
        subPersona === 'em'
          ? fetchShelterCapacity(stormId, { lat: (storm as any).landfall_lat ?? 0, lon: (storm as any).landfall_lon ?? 0 })
          : Promise.resolve(null),
        subPersona === 'cat' ? fetchVendorCoverage(stormId) : Promise.resolve(null),
        subPersona === 'em'
          ? fetchTimeToAccess(stormId, hotspots.map(h => ({ rank: h.rank, lat: h.lat, lon: h.lon })))
          : Promise.resolve(null),
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
      <button onClick={() => setExpanded(e => !e)} className="w-full flex items-center gap-2 text-left">
        <span className="text-[10px] font-bold uppercase tracking-wider text-purple-800">🧪 Beta data layers</span>
        <span className="ml-auto text-[9px] font-bold uppercase px-1.5 py-0.5 rounded-sm text-purple-900 bg-purple-100">Preview</span>
        <span className="text-purple-600 text-xs">{expanded ? '▲' : '▼'}</span>
      </button>

      {expanded && (
        <div className="mt-2">
          {rainfall && (
            <BetaSection title="Rainfall overlay (B7)" badge="both" notes={rainfall.notes} />
          )}
          {subPersona === 'em' && shelters && (
            <BetaSection title="Shelter capacity (E5)" badge="EM" notes={shelters.notes} />
          )}
          {subPersona === 'cat' && vendors && (
            <BetaSection title="Vendor coverage (C6)" badge="CAT" notes={vendors.notes} />
          )}
          {subPersona === 'em' && access && (
            <BetaSection title="Time-to-access (E6)" badge="EM" notes={access.notes} />
          )}
          <div className="text-[9px] text-purple-500 mt-1 italic leading-snug">
            These layers are scaffolding only — real data ships as each backend endpoint lands.
            See PHASE5_DATA_CONTRACTS.md.
          </div>
        </div>
      )}
    </div>
  );
}
