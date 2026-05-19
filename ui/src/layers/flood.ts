// ─────────────────────────────────────────────────────────────────────────────
// Flood-depth layer definitions.
// ─────────────────────────────────────────────────────────────────────────────

// Color ramp for surge depth (depth is in metres).
//
// History: the v1 ramp ended at 3 m (~10 ft) with a flat dark red. For
// Katrina-class events with 20–28 ft inundation, every cell above 10 ft
// saturated to the same color and the map lost all nuance in exactly the
// areas an adjuster cares about most. v2 keeps the original YlOrRd low end
// (intuitive "redder = worse") and adds three darker stops between 15 ft
// and 30 ft so catastrophic depths still read distinctly.
//
// Stops below are metres → hex; comment alongside is the imperial value
// the MapLegend caption pairs with.
export const floodLayerStyle = {
  id: 'flood-depth-layer',
  type: 'fill',
  paint: {
    'fill-color': [
      'interpolate', ['linear'], ['get', 'depth'],
      0.05, '#ffeda0',  //  0.2 ft — pale yellow
      0.3,  '#feb24c',  //  1 ft
      0.9,  '#fd8d3c',  //  3 ft
      1.8,  '#f03b20',  //  6 ft
      3.0,  '#bd0026',  // 10 ft
      4.6,  '#800026',  // 15 ft — very dark red
      6.1,  '#4a0026',  // 20 ft — wine
      9.1,  '#1a0011',  // 30 ft — near black
    ],
    'fill-opacity': [
      'interpolate', ['linear'], ['zoom'],
      // At state-wide zoom (6-8) the impact polygon previously blanketed
      // the entire visible map in saturated red, hiding the geography
      // behind it. Fading the raster down to 0.18 at zoom 6 lets users
      // see coastline and parish lines while the storm footprint still
      // reads clearly. Mid zooms (10-13) stay the same — that's where
      // analysts spend most of their time.
      6,  0.18,
      8,  0.28,
      10, 0.35,
      13, 0.30,
      15, 0.15,
      17, 0.08,
    ],
  },
};
