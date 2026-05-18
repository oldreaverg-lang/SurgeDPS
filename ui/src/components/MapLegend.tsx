// ─────────────────────────────────────────────────────────────────────────────
// MapLegend — collapsible key for the active map layers. Sits at bottom-right
// of the map. Each row is rendered conditionally based on which layers are
// currently visible, so the user only sees keys for the data on screen.
//
// Color stops mirror the layer definitions in src/layers/* — kept in sync
// manually since MapLibre paint expressions aren't easily introspectable.
// ─────────────────────────────────────────────────────────────────────────────

import { useState } from 'react';

type Props = {
  /** Set true while the surge depth raster is rendered. */
  showSurge: boolean;
  /** Set true while the rainfall accumulation raster is rendered. */
  showRainfall: boolean;
  /** True when mapView === 'damage' — damage-keyed bubbles + dots. */
  showDamage: boolean;
  /** True when mapView === 'population' — pop-keyed bubbles. */
  showPopulation: boolean;
  /** FEMA NFHL flood zones visible. */
  showFEMAZones: boolean;
  /** USGS stream gauges visible. */
  showGauges: boolean;
  /** Red Cross / open shelters visible. */
  showShelters: boolean;
};

// ── Small primitives ──────────────────────────────────────────────────────────

function Row({ children, title }: { children: React.ReactNode; title?: string }) {
  return (
    <div className="flex items-center gap-2 py-0.5" title={title}>{children}</div>
  );
}

function Swatch({ color, shape = 'square' }: { color: string; shape?: 'square' | 'circle' | 'line' }) {
  const cls = shape === 'circle' ? 'rounded-full' : shape === 'line' ? 'h-0.5 rounded-full' : 'rounded-sm';
  return (
    <span
      aria-hidden
      className={`inline-block ${cls} shrink-0`}
      style={{
        width: shape === 'line' ? 14 : 10,
        height: shape === 'line' ? 2 : 10,
        backgroundColor: color,
      }}
    />
  );
}

function Label({ children }: { children: React.ReactNode }) {
  return <span className="text-[10px] text-slate-700 leading-tight">{children}</span>;
}

// ── Surge depth gradient ──────────────────────────────────────────────────────
// Mirrors the stops in src/layers/flood.ts (depth in metres → color).
// 1 m ≈ 3.28 ft, so showing imperial endpoints helps EMs reading the map.

function SurgeRow() {
  return (
    <div className="flex items-center gap-2 py-0.5" title="Surge inundation depth — yellow ≈ shallow, dark red ≈ 10+ ft">
      <div
        className="h-2 w-20 rounded-sm shrink-0 border border-slate-200"
        style={{
          background:
            'linear-gradient(to right, #ffffb2 0%, #fecc5c 18%, #fd8d3c 40%, #f03b20 65%, #bd0026 100%)',
        }}
      />
      <Label>
        <span className="font-semibold">Surge</span>
        <span className="text-slate-400 ml-1 tabular-nums">0.5 → 10+ ft</span>
      </Label>
    </div>
  );
}

// ── Damage bubble categories ──────────────────────────────────────────────────

function DamageRow() {
  return (
    <div className="py-0.5" title="Building damage category from HAZUS depth-damage curves">
      <div className="text-[10px] text-slate-500 mb-0.5">Damage bubbles · larger = more $ loss</div>
      <div className="flex items-center gap-1.5 flex-wrap">
        <span className="inline-flex items-center gap-1"><Swatch color="#7f1d1d" shape="circle" /><Label>severe</Label></span>
        <span className="inline-flex items-center gap-1"><Swatch color="#dc2626" shape="circle" /><Label>major</Label></span>
        <span className="inline-flex items-center gap-1"><Swatch color="#f97316" shape="circle" /><Label>mod.</Label></span>
        <span className="inline-flex items-center gap-1"><Swatch color="#facc15" shape="circle" /><Label>minor</Label></span>
      </div>
    </div>
  );
}

// ── Population bubble row ─────────────────────────────────────────────────────

function PopulationRow() {
  return (
    <Row title="Estimated displaced residents — larger circle = more displaced people">
      <Swatch color="#6366f1" shape="circle" />
      <Label>
        <span className="font-semibold">Pop displaced</span>
        <span className="text-slate-400 ml-1">larger = more</span>
      </Label>
    </Row>
  );
}

// ── FEMA flood zones ──────────────────────────────────────────────────────────

function FEMARow() {
  return (
    <div className="py-0.5" title="FEMA National Flood Hazard Layer — Special Flood Hazard Areas">
      <div className="text-[10px] text-slate-500 mb-0.5">FEMA zones</div>
      <div className="flex items-center gap-2 flex-wrap">
        <span className="inline-flex items-center gap-1"><Swatch color="#dc2626" /><Label>V / VE</Label></span>
        <span className="inline-flex items-center gap-1"><Swatch color="#f97316" /><Label>A / AE</Label></span>
        <span className="inline-flex items-center gap-1"><Swatch color="#facc15" /><Label>X</Label></span>
      </div>
    </div>
  );
}

// ── Gauges (AHPS flood category) ─────────────────────────────────────────────

function GaugeRow() {
  return (
    <div className="py-0.5" title="USGS / NWS stream gauges colored by NWS AHPS flood category">
      <div className="text-[10px] text-slate-500 mb-0.5">Gauges (AHPS)</div>
      <div className="flex items-center gap-2 flex-wrap">
        <span className="inline-flex items-center gap-1"><Swatch color="#7f1d1d" shape="circle" /><Label>major</Label></span>
        <span className="inline-flex items-center gap-1"><Swatch color="#ef4444" shape="circle" /><Label>mod.</Label></span>
        <span className="inline-flex items-center gap-1"><Swatch color="#fb923c" shape="circle" /><Label>minor</Label></span>
        <span className="inline-flex items-center gap-1"><Swatch color="#facc15" shape="circle" /><Label>action</Label></span>
      </div>
    </div>
  );
}

// ── Shelters (capacity / fullness) ───────────────────────────────────────────

function ShelterRow() {
  return (
    <div className="py-0.5" title="Open shelters colored by current fullness">
      <div className="text-[10px] text-slate-500 mb-0.5">Shelters</div>
      <div className="flex items-center gap-2 flex-wrap">
        <span className="inline-flex items-center gap-1"><Swatch color="#16a34a" shape="circle" /><Label>open</Label></span>
        <span className="inline-flex items-center gap-1"><Swatch color="#f59e0b" shape="circle" /><Label>filling</Label></span>
        <span className="inline-flex items-center gap-1"><Swatch color="#dc2626" shape="circle" /><Label>full</Label></span>
      </div>
    </div>
  );
}

// ── Component ────────────────────────────────────────────────────────────────

export function MapLegend(props: Props) {
  const [open, setOpen] = useState(true);

  const { showSurge, showRainfall, showDamage, showPopulation, showFEMAZones, showGauges, showShelters } = props;

  const anyVisible =
    showSurge || showRainfall || showDamage || showPopulation || showFEMAZones || showGauges || showShelters;
  if (!anyVisible) return null;

  if (!open) {
    return (
      <button
        type="button"
        onClick={() => setOpen(true)}
        className="absolute bottom-4 right-4 z-20 bg-white/90 backdrop-blur shadow-lg rounded-lg border border-gray-200 px-2.5 py-1.5 text-[11px] font-semibold text-gray-700 hover:bg-gray-50 inline-flex items-center gap-1.5"
        title="Show map legend"
      >
        <span>🗝️</span><span>Legend</span>
      </button>
    );
  }

  return (
    <div className="absolute bottom-4 right-4 z-20 bg-white/95 backdrop-blur shadow-lg rounded-lg border border-gray-200 px-2.5 py-1.5 w-[200px] max-w-[40vw]">
      <div className="flex items-center justify-between mb-1">
        <span className="text-[10px] font-bold uppercase tracking-wider text-slate-600">Legend</span>
        <button
          type="button"
          onClick={() => setOpen(false)}
          className="text-slate-400 hover:text-slate-700 text-[10px] leading-none px-1"
          title="Collapse legend"
          aria-label="Collapse legend"
        >–</button>
      </div>
      <div className="space-y-0.5">
        {showSurge && <SurgeRow />}
        {showRainfall && (
          <Row title="MRMS observed rainfall accumulation since storm formation">
            <div
              className="h-2 w-20 rounded-sm shrink-0 border border-slate-200"
              style={{ background: 'linear-gradient(to right, #c7d2fe 0%, #6366f1 50%, #4338ca 100%)' }}
            />
            <Label>
              <span className="font-semibold">Rainfall</span>
              <span className="text-slate-400 ml-1 tabular-nums">light → heavy</span>
            </Label>
          </Row>
        )}
        {showDamage && <DamageRow />}
        {showPopulation && <PopulationRow />}
        {showFEMAZones && <FEMARow />}
        {showGauges && <GaugeRow />}
        {showShelters && <ShelterRow />}
      </div>
    </div>
  );
}
