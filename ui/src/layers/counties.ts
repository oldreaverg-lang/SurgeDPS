// ─────────────────────────────────────────────────────────────────────────────
// County boundary layer definitions.
// ─────────────────────────────────────────────────────────────────────────────

import type { LayerDef } from '../types';

export const COUNTY_LAYERS: LayerDef[] = [
  {
    id: 'county-fill',
    type: 'fill',
    paint: {
      'fill-color': [
        'match', ['get', 'colorIdx'],
        0, '#93c5fd', 1, '#a5b4fc', 2, '#c4b5fd', 3, '#d8b4fe',
        4, '#f0abfc', 5, '#7dd3fc', 6, '#67e8f9', 7, '#5eead4',
        '#cbd5e1',
      ],
      'fill-opacity': 0.32,
    },
  },
  {
    id: 'county-line',
    type: 'line',
    paint: { 'line-color': '#ffffff', 'line-width': 1, 'line-opacity': 0.7 },
  },
  {
    // Append " County" / " Parish" (Louisiana FIPS 22).
    id: 'county-labels',
    type: 'symbol',
    minzoom: 6,
    layout: {
      'text-field': ['concat', ['get', 'NAME'],
        ['case', ['==', ['get', 'STATE'], '22'], ' Parish', ' County']],
      'text-font': ['Open Sans Semibold', 'Arial Unicode MS Regular'],
      'text-size': ['interpolate', ['linear'], ['zoom'], 6, 11, 10, 15, 14, 18],
      'text-anchor': 'center',
      'text-max-width': 10,
      'text-letter-spacing': 0.02,
      'text-transform': 'uppercase',
    },
    paint: {
      'text-color': '#ffffff',
      'text-halo-color': '#0f172a',
      'text-halo-width': 2,
      'text-halo-blur': 0.5,
    },
  },
];
