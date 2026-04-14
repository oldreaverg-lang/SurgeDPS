// ─────────────────────────────────────────────────────────────────────────────
// Flood-depth layer definitions.
// ─────────────────────────────────────────────────────────────────────────────

export const floodLayerStyle = {
  id: 'flood-depth-layer',
  type: 'fill',
  paint: {
    'fill-color': [
      'interpolate', ['linear'], ['get', 'depth'],
      0.05, '#ffffb2',
      0.3,  '#fecc5c',
      0.9,  '#fd8d3c',
      1.8,  '#f03b20',
      3.0,  '#bd0026',
    ],
    'fill-opacity': [
      'interpolate', ['linear'], ['zoom'],
      10, 0.35,
      13, 0.3,
      15, 0.15,
      17, 0.08,
    ],
  },
};
