// ─────────────────────────────────────────────────────────────────────────────
// Basemap style definitions.
// ─────────────────────────────────────────────────────────────────────────────

export const BASEMAPS: Record<string, any> = {
  dark: 'https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json',
  satellite: {
    version: 8,
    name: 'Satellite',
    sources: {
      'esri-sat': {
        type: 'raster',
        tiles: ['https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}'],
        tileSize: 256,
        attribution: 'Esri World Imagery',
      },
    },
    layers: [{ id: 'esri-sat', type: 'raster', source: 'esri-sat' }],
  },
  street: {
    version: 8,
    name: 'Street',
    sources: {
      'osm': {
        type: 'raster',
        tiles: ['https://tile.openstreetmap.org/{z}/{x}/{y}.png'],
        tileSize: 256,
        attribution: '© OpenStreetMap',
      },
    },
    layers: [{ id: 'osm', type: 'raster', source: 'osm' }],
  },
};

export const BASEMAP_LABELS: Record<string, string> = {
  dark: 'Dark',
  satellite: 'Satellite',
  street: 'Street',
};
