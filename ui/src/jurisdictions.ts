// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// jurisdictions.ts — per-county rollup of damage + exposure
//
// When the Counties overlay is on, the Emergency Manager needs to
// see the map chopped up by jurisdiction. Each row in the rollup
// is a county — total buildings exposed, damaged buildings by
// severity, estimated loss, and estimated displaced residents.
// The EM uses this to allocate rescue teams and shelter capacity
// across independently-managed jurisdictions.
//
// Dependency-free: does point-in-polygon in JS. Building counts
// in SurgeDPS are typically <50k per loaded cell, and a viewport
// has <20 counties, so the O(B·C·V) loop is fine in practice.
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

type Ring = [number, number][];

// Ray-casting point-in-polygon. Works on a single ring.
function pointInRing(lon: number, lat: number, ring: Ring): boolean {
  let inside = false;
  for (let i = 0, j = ring.length - 1; i < ring.length; j = i++) {
    const xi = ring[i][0], yi = ring[i][1];
    const xj = ring[j][0], yj = ring[j][1];
    const intersect = ((yi > lat) !== (yj > lat))
      && (lon < (xj - xi) * (lat - yi) / (yj - yi + 1e-12) + xi);
    if (intersect) inside = !inside;
  }
  return inside;
}

// GeoJSON geometry can be Polygon or MultiPolygon. We accept either.
// A point is "inside" if it's in any outer ring AND not in a hole.
function pointInGeometry(lon: number, lat: number, geom: any): boolean {
  if (!geom) return false;
  if (geom.type === 'Polygon') {
    const rings: Ring[] = geom.coordinates;
    if (!rings.length) return false;
    if (!pointInRing(lon, lat, rings[0])) return false;
    // Holes
    for (let i = 1; i < rings.length; i++) {
      if (pointInRing(lon, lat, rings[i])) return false;
    }
    return true;
  }
  if (geom.type === 'MultiPolygon') {
    for (const poly of geom.coordinates as Ring[][]) {
      if (!poly.length) continue;
      if (!pointInRing(lon, lat, poly[0])) continue;
      let inHole = false;
      for (let i = 1; i < poly.length; i++) {
        if (pointInRing(lon, lat, poly[i])) { inHole = true; break; }
      }
      if (!inHole) return true;
    }
    return false;
  }
  return false;
}

// Axis-aligned bounding box of a GeoJSON geometry. Used to skip
// obviously-outside buildings without running the full ray cast.
function bboxOf(geom: any): [number, number, number, number] {
  let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
  const scan = (rings: Ring[][]) => {
    for (const poly of rings) {
      for (const ring of poly) {
        for (const [x, y] of ring) {
          if (x < minX) minX = x;
          if (y < minY) minY = y;
          if (x > maxX) maxX = x;
          if (y > maxY) maxY = y;
        }
      }
    }
  };
  if (geom?.type === 'Polygon') scan([geom.coordinates]);
  else if (geom?.type === 'MultiPolygon') scan(geom.coordinates);
  return [minX, minY, maxX, maxY];
}

export interface CountyRollup {
  geoid: string;
  name: string;
  state: string;
  buildings: number;
  loss: number;
  severe: number;
  major: number;
  moderate: number;
  minor: number;
  criticalFacilities: number;
  estDisplaced: number;    // residential major+severe × 2.5 avg household
  maxDepthFt: number;
  centerLon: number;       // bbox midpoint — "where to put the label"
  centerLat: number;
}

const CRITICAL_OCCTYPES = new Set(['GOV1','GOV2','EDU1','EDU2','MED1','MED2','COM8','COM9','COM10']);
const AVG_HOUSEHOLD = 2.5;

/**
 * Given a FeatureCollection of building points and a FeatureCollection of
 * county polygons, produce one rollup row per county that contains at
 * least one building. Sorted by estimated loss desc.
 */
export function rollupByCounty(
  buildings: any,
  counties: any,
): CountyRollup[] {
  if (!buildings?.features?.length || !counties?.features?.length) return [];

  // Pre-compute county bboxes once
  const countyData = counties.features.map((f: any) => ({
    props: f.properties || {},
    geom: f.geometry,
    bbox: bboxOf(f.geometry),
    rollup: {
      geoid: f.properties?.GEOID || f.properties?.NAME || '?',
      name: f.properties?.NAME || 'Unknown',
      state: f.properties?.STUSAB || '',
      buildings: 0,
      loss: 0,
      severe: 0,
      major: 0,
      moderate: 0,
      minor: 0,
      criticalFacilities: 0,
      estDisplaced: 0,
      maxDepthFt: 0,
      centerLon: 0,
      centerLat: 0,
    } as CountyRollup,
  }));
  // Fill bbox midpoint as center
  for (const c of countyData) {
    const [minX, minY, maxX, maxY] = c.bbox;
    c.rollup.centerLon = (minX + maxX) / 2;
    c.rollup.centerLat = (minY + maxY) / 2;
  }

  for (const b of buildings.features) {
    const coords = b.geometry?.coordinates;
    if (!coords) continue;
    const [lon, lat] = coords;
    const p = b.properties || {};

    for (const c of countyData) {
      const [minX, minY, maxX, maxY] = c.bbox;
      if (lon < minX || lon > maxX || lat < minY || lat > maxY) continue;
      if (!pointInGeometry(lon, lat, c.geom)) continue;

      const r = c.rollup;
      r.buildings += 1;
      r.loss += p.estimated_loss_usd || 0;
      if (p.depth_ft && p.depth_ft > r.maxDepthFt) r.maxDepthFt = p.depth_ft;

      const cat = p.damage_category;
      if (cat === 'severe') r.severe += 1;
      else if (cat === 'major') r.major += 1;
      else if (cat === 'moderate') r.moderate += 1;
      else if (cat === 'minor') r.minor += 1;

      // Critical facility check (GOV/EDU/MED + heavy commercial)
      const occ = (p.occtype || '').toUpperCase().split('-')[0];
      if (CRITICAL_OCCTYPES.has(occ)) r.criticalFacilities += 1;

      // A building belongs to exactly one county — stop checking
      break;
    }
  }

  // Finalize: displaced persons for each county
  const rows: CountyRollup[] = [];
  for (const c of countyData) {
    if (c.rollup.buildings === 0) continue;
    // Displaced = residential (~70% of footprint in most coastal counties)
    // major+severe × avg household. Conservative — EM can verify.
    c.rollup.estDisplaced = Math.round((c.rollup.severe + c.rollup.major) * 0.7 * AVG_HOUSEHOLD);
    rows.push(c.rollup);
  }

  rows.sort((a, b) => b.loss - a.loss);
  return rows;
}

/**
 * Convert a county rollup array into a Point FeatureCollection suitable for
 * rendering as an aggregated bubble layer at low zoom. Each feature sits at
 * the county's bbox center and carries aggregate properties for paint exprs.
 *
 * The `worstCategory` field is the worst severity present in the county and
 * drives bubble color so the EM can see "red counties" at a glance.
 */
export function rollupToCentroidGeoJSON(rows: CountyRollup[]): any {
  const features = rows.map(r => {
    let worst: string = 'none';
    if (r.severe > 0) worst = 'severe';
    else if (r.major > 0) worst = 'major';
    else if (r.moderate > 0) worst = 'moderate';
    else if (r.minor > 0) worst = 'minor';
    return {
      type: 'Feature' as const,
      geometry: { type: 'Point' as const, coordinates: [r.centerLon, r.centerLat] },
      properties: {
        geoid: r.geoid,
        name: r.name,
        state: r.state,
        buildings: r.buildings,
        loss: r.loss,
        severe: r.severe,
        major: r.major,
        moderate: r.moderate,
        minor: r.minor,
        criticalFacilities: r.criticalFacilities,
        estDisplaced: r.estDisplaced,
        worstCategory: worst,
        label: `${r.name}  ${r.buildings.toLocaleString()}`,
      },
    };
  });
  return { type: 'FeatureCollection', features };
}
