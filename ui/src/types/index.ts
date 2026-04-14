// ─────────────────────────────────────────────────────────────────────────────
// Shared types — imported by components, hooks, and utilities.
// Centralising here prevents circular imports and makes the data model
// visible at a glance.
// ─────────────────────────────────────────────────────────────────────────────

export interface StormInfo {
  storm_id: string;
  name: string;
  year: number;
  category: number;
  status: string;
  landfall_lon: number;
  landfall_lat: number;
  max_wind_kt: number;
  min_pressure_mb: number;
  grid_origin_lon: number;
  grid_origin_lat: number;
  rmax_nm?: number;
  dps_score: number;
  confidence?: string;
  building_count?: number;
  population?: {
    county_name?: string;
    state_code?: string;
    population?: number;
    pop_label?: string;
    vintage?: number;
  };
}

export interface Season {
  year: number;
  count: number;
}

export interface LayerDef {
  id: string;
  type: 'fill' | 'line' | 'circle' | 'symbol' | 'raster';
  paint?: Record<string, any>;
  layout?: Record<string, any>;
  filter?: any[];
  minzoom?: number;
  maxzoom?: number;
}

export type DisplayMode = 'analyst' | 'ops';

export type PerilKey = 'surge' | 'rainfall' | 'cumulative';

/** Row schema: [s_ft, r_ft, c_ft, s_state, r_state, c_state, s_loss, r_loss, c_loss] */
export type TickRow = [number, number, number, string, string, string, number, number, number];

export interface TicksBundle {
  schema_version: string;
  tick_hours: number[];
  duration_h: number;
  peril_fields: string[];
  buildings: { id: string; lat: number; lon: number; ticks: TickRow[] }[];
}

/** Simplified hotspot from useImpactAggregates — used for map bubbles */
export interface HotspotBasic {
  lon: number;
  lat: number;
  count: number;
  totalLoss: number;
  avgLoss: number;
  maxDepthFt: number;
  label: string;
  windPct: number;
  waterPct: number;
  surgePct: number;
  rainPct: number;
}

/**
 * Enriched hotspot computed in App — includes adjuster routing, shelter
 * posture, severity breakdown, and deployment-planning fields.
 *
 * Structurally compatible with ReportHotspot from catReports.ts so the
 * same array can be passed to report generators without casting.
 */
export interface Hotspot {
  rank: number;
  loss: number;
  count: number;
  lat: number;
  lon: number;
  avgLoss: number;
  maxDepthFt: number;
  windPct: number;
  waterPct: number;
  surgePct: number;
  rainPct: number;
  severity: { severe: number; major: number; moderate: number; minor: number; none: number };
  /** Populated by recommendAdjusters() from catTeam.ts */
  recommend: { adjuster_days: number; adjusters: number; days: number; label: string };
  /** Populated by routingHint() from catTeam.ts */
  routing: { hint: string; label: string; short: string; description: string; classes: string };
}

export interface ImpactTotals {
  buildings: number;
  loss: number;
  totalDepth: number;
}

export interface ConfidenceLevel {
  level: string;
  count: number;
}

export interface EliState {
  value: number;
  tier: string;
}

export interface ValidatedDpsState {
  value: number;
  adj: number;
  reason: string;
}

export interface ShelterFeature {
  id: string;
  name: string;
  lat: number;
  lon: number;
  capacity: number;
  occupancy: number | null;
  operator: string;
  isAccessible: boolean;
  isPetFriendly: boolean;
}

export interface SheltersData {
  features: ShelterFeature[];
  totalCapacity: number;
  totalOccupancy: number | null;
}

export interface RainfallStats {
  maxIn: number | null;
  avgIn: number | null;
  product: string | null;
  validTime: string | null;
  notes: string;
  tileUrl: string | null;
}

export interface QpfStats {
  maxIn: number | null;
  caveat: string | null;
  source: string | null;
  tileUrl: string | null;
  notes: string;
}

export interface CompoundStats {
  maxFt: number | null;
  avgFt: number | null;
  cellCount: number;
  notes: string;
  tileUrl: string | null;
}

export interface GaugesSummary {
  major: number;
  moderate: number;
  minor: number;
  count: number;
}

export interface LoadProgress {
  step: string;
  step_num: number;
  total_steps: number;
  elapsed: number;
}

export interface SimMarker {
  lng: number;
  lat: number;
}
