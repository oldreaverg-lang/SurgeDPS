// ─────────────────────────────────────────────────────────────────────────────
// Overlay layer definitions: FEMA zones, stream gauges, shelters,
// critical facilities, grid cells, and forecast cone.
// ─────────────────────────────────────────────────────────────────────────────

import type { LayerDef } from '../types';

// ── Shared color expressions ──────────────────────────────────────────────────

const _GAUGE_COLOR = ['match', ['get', 'category'],
  'major', '#7f1d1d', 'moderate', '#ef4444',
  'minor', '#fb923c', 'action', '#facc15',
  '#94a3b8',
] as any;

const _SHELTER_FILL_COLOR = ['case',
  ['<', ['get', 'fullness'], 0], '#94a3b8',
  ['<', ['get', 'fullness'], 0.6], '#16a34a',
  ['<', ['get', 'fullness'], 0.9], '#f59e0b',
  '#dc2626',
] as any;

// ── FEMA NFHL flood zones ─────────────────────────────────────────────────────

export const FEMA_LAYERS: LayerDef[] = [
  {
    id: 'fema-zones-fill',
    type: 'fill',
    paint: {
      'fill-color': ['match', ['get', 'FLD_ZONE'],
        ['VE', 'V'], '#dc2626',
        ['AE', 'AO', 'AH', 'A'], '#f97316',
        ['X'], '#facc15',
        '#94a3b8'],
      'fill-opacity': 0.30,
    },
  },
  {
    id: 'fema-zones-line',
    type: 'line',
    paint: {
      'line-color': ['match', ['get', 'FLD_ZONE'],
        ['VE', 'V'], '#ef4444',
        ['AE', 'AO', 'AH', 'A'], '#fb923c',
        ['X'], '#fde047',
        '#cbd5e1'],
      'line-width': 1,
      'line-opacity': 0.7,
    },
  },
  {
    id: 'fema-zones-labels',
    type: 'symbol',
    minzoom: 10,
    layout: {
      'text-field': ['get', 'FLD_ZONE'],
      'text-font': ['Open Sans Bold', 'Arial Unicode MS Bold'],
      'text-size': 10,
      'text-anchor': 'center',
    },
    paint: { 'text-color': '#fff', 'text-halo-color': '#000', 'text-halo-width': 1 },
  },
];

// ── AHPS stream gauges ────────────────────────────────────────────────────────

export const GAUGE_LAYERS: LayerDef[] = [
  {
    id: 'stream-gauges-halo',
    type: 'circle',
    paint: {
      'circle-radius': ['interpolate', ['linear'], ['zoom'], 6, 8, 10, 14, 14, 20],
      'circle-color': _GAUGE_COLOR,
      'circle-opacity': 0.25,
      'circle-stroke-width': 0,
    },
  },
  {
    id: 'stream-gauges-dot',
    type: 'circle',
    paint: {
      'circle-radius': ['interpolate', ['linear'], ['zoom'], 6, 3, 10, 5, 14, 7],
      'circle-color': _GAUGE_COLOR,
      'circle-stroke-color': '#fff',
      'circle-stroke-width': 1.5,
    },
  },
  {
    id: 'stream-gauges-label',
    type: 'symbol',
    minzoom: 9,
    layout: {
      'text-field': ['coalesce', ['get', 'name'], ['get', 'nws_lid'], ''],
      'text-font': ['Open Sans Bold', 'Arial Unicode MS Bold'],
      'text-size': 10,
      'text-anchor': 'top',
      'text-offset': [0, 0.8],
      'text-allow-overlap': false,
    },
    paint: { 'text-color': '#0f172a', 'text-halo-color': '#fff', 'text-halo-width': 1.5 },
  },
];

// ── Emergency shelters ────────────────────────────────────────────────────────

export const SHELTER_LAYERS: LayerDef[] = [
  {
    // Halo radius scales with capacity; color encodes fullness.
    id: 'shelter-markers-halo',
    type: 'circle',
    paint: {
      'circle-radius': ['interpolate', ['linear'], ['get', 'capacity'], 50, 8, 500, 18, 2000, 28],
      'circle-color': _SHELTER_FILL_COLOR,
      'circle-opacity': 0.25,
    },
  },
  {
    id: 'shelter-markers-dot',
    type: 'circle',
    paint: {
      'circle-radius': ['interpolate', ['linear'], ['get', 'capacity'], 50, 4, 500, 8, 2000, 12],
      'circle-color': ['case',
        ['<', ['get', 'fullness'], 0], '#f1f5f9',
        ['<', ['get', 'fullness'], 0.6], '#16a34a',
        ['<', ['get', 'fullness'], 0.9], '#f59e0b',
        '#dc2626'],
      'circle-stroke-color': '#0f172a',
      'circle-stroke-width': 1.5,
    },
  },
  {
    id: 'shelter-markers-label',
    type: 'symbol',
    minzoom: 9,
    layout: {
      'text-field': ['get', 'name'],
      'text-font': ['Open Sans Bold', 'Arial Unicode MS Bold'],
      'text-size': 10,
      'text-anchor': 'top',
      'text-offset': [0, 0.9],
      'text-allow-overlap': false,
    },
    paint: { 'text-color': '#0f172a', 'text-halo-color': '#fff', 'text-halo-width': 1.5 },
  },
];

// ── Critical facilities ───────────────────────────────────────────────────────

export const CRITICAL_FACILITY_LAYERS: LayerDef[] = [
  {
    // Hospitals=red, gov=gold, schools=blue, nursing=teal, churches=purple.
    id: 'critical-icon-halos',
    type: 'circle',
    minzoom: 15,
    paint: {
      'circle-radius': ['interpolate', ['linear'], ['zoom'], 15, 10, 17, 14, 19, 20],
      'circle-color': ['match', ['get', 'critical_icon'],
        '➕', '#dc2626', '⭐', '#f59e0b', '🏫', '#2563eb',
        '🛏️', '#0d9488', '⛪', '#7c3aed', '#475569'],
      'circle-stroke-color': '#ffffff',
      'circle-stroke-width': 2,
      'circle-opacity': 0.95,
    },
  },
  {
    id: 'critical-icons',
    type: 'symbol',
    minzoom: 15,
    layout: {
      'text-field': ['get', 'critical_icon'],
      'text-size': ['interpolate', ['linear'], ['zoom'], 15, 12, 17, 18, 19, 24],
      'text-allow-overlap': true,
      'text-ignore-placement': true,
      'symbol-sort-key': ['case',
        ['==', ['get', 'critical_icon'], '➕'], 0,
        ['==', ['get', 'critical_icon'], '⭐'], 1,
        ['==', ['get', 'critical_icon'], '🏫'], 2,
        ['==', ['get', 'critical_icon'], '🛏️'], 3,
        ['==', ['get', 'critical_icon'], '⛪'], 4,
        5],
    },
    paint: {
      'text-color': '#ffffff',
      'text-halo-color': 'rgba(0,0,0,0.5)',
      'text-halo-width': 0.5,
    },
  },
];

// ── Grid status cells ─────────────────────────────────────────────────────────

export const GRID_LAYERS: LayerDef[] = [
  { id: 'grid-loaded-border',    type: 'line',   filter: ['==', ['get', 'status'], 'loaded'],
    paint: { 'line-color': '#4ade80', 'line-width': 2, 'line-opacity': 0.6, 'line-dasharray': [4, 2] } },
  { id: 'grid-available-fill',   type: 'fill',   filter: ['==', ['get', 'status'], 'available'],
    paint: { 'fill-color': '#6366f1', 'fill-opacity': 0.05 } },
  { id: 'grid-available-border', type: 'line',   filter: ['==', ['get', 'status'], 'available'],
    paint: { 'line-color': '#a5b4fc', 'line-width': 1.5, 'line-opacity': 0.6, 'line-dasharray': [6, 3] } },
  { id: 'grid-available-label',  type: 'symbol', filter: ['==', ['get', 'status'], 'available'],
    layout: { 'text-field': '+ Click to load', 'text-size': 13, 'text-font': ['Open Sans Semibold'] },
    paint: { 'text-color': '#c7d2fe', 'text-opacity': 0.85, 'text-halo-color': '#000', 'text-halo-width': 1.2 } },
  { id: 'grid-loading-fill',     type: 'fill',   filter: ['==', ['get', 'status'], 'loading'],
    paint: { 'fill-color': '#facc15', 'fill-opacity': 0.1 } },
  { id: 'grid-loading-border',   type: 'line',   filter: ['==', ['get', 'status'], 'loading'],
    paint: { 'line-color': '#facc15', 'line-width': 2.5, 'line-opacity': 0.9 } },
  { id: 'grid-loading-label',    type: 'symbol', filter: ['==', ['get', 'status'], 'loading'],
    layout: { 'text-field': 'Loading...', 'text-size': 13, 'text-font': ['Open Sans Regular'] },
    paint: { 'text-color': '#facc15', 'text-opacity': 0.9, 'text-halo-color': '#000', 'text-halo-width': 1 } },
  { id: 'grid-ready-fill',       type: 'fill',   filter: ['==', ['get', 'status'], 'ready'],
    paint: { 'fill-color': '#4ade80', 'fill-opacity': 0.06 } },
  { id: 'grid-ready-border',     type: 'line',   filter: ['==', ['get', 'status'], 'ready'],
    paint: { 'line-color': '#4ade80', 'line-width': 2, 'line-opacity': 0.7 } },
  { id: 'grid-ready-label',      type: 'symbol', filter: ['==', ['get', 'status'], 'ready'],
    layout: { 'text-field': 'Cached ✓', 'text-size': 12, 'text-font': ['Open Sans Regular'] },
    paint: { 'text-color': '#4ade80', 'text-opacity': 0.8, 'text-halo-color': '#000', 'text-halo-width': 1 } },
];

// ── NHC forecast cone ─────────────────────────────────────────────────────────

export const FORECAST_CONE_LAYERS: LayerDef[] = [
  { id: 'cone-fill',   type: 'fill', paint: { 'fill-color': '#ffffff', 'fill-opacity': 0.12 } },
  { id: 'cone-border', type: 'line', paint: { 'line-color': '#ffffff', 'line-width': 2, 'line-opacity': 0.5, 'line-dasharray': [4, 3] } },
];
