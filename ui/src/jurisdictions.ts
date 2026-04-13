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

// counties-coastal.json has STATE as a 2-digit FIPS string (e.g. "48" for Texas).
// Map to the two-letter postal abbreviation for human-readable sidebar labels.
const FIPS_TO_ABBREV: Record<string, string> = {
  '01':'AL','02':'AK','04':'AZ','05':'AR','06':'CA','08':'CO','09':'CT',
  '10':'DE','11':'DC','12':'FL','13':'GA','15':'HI','16':'ID','17':'IL',
  '18':'IN','19':'IA','20':'KS','21':'KY','22':'LA','23':'ME','24':'MD',
  '25':'MA','26':'MI','27':'MN','28':'MS','29':'MO','30':'MT','31':'NE',
  '32':'NV','33':'NH','34':'NJ','35':'NM','36':'NY','37':'NC','38':'ND',
  '39':'OH','40':'OK','41':'OR','42':'PA','44':'RI','45':'SC','46':'SD',
  '47':'TN','48':'TX','49':'UT','50':'VT','51':'VA','53':'WA','54':'WV',
  '55':'WI','56':'WY','72':'PR',
};

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

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// City entry shape — matches cities-coastal.json records
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
export interface CityEntry {
  name: string;
  state: string;
  county_geoid: string;
  lat: number;
  lon: number;
  pop: number;
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// City-level rollup — one row per city (or "Unincorporated"
// area) within the loaded cells.
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
export interface CityRollup {
  key: string;          // unique bucket key
  name: string;         // city name, or "Unincorporated"
  state: string;
  countyGeoid: string;
  countyName: string;   // parent county name
  buildings: number;
  loss: number;
  severe: number;
  major: number;
  moderate: number;
  minor: number;
  criticalFacilities: number;
  estDisplaced: number;
  maxDepthFt: number;
  centerLon: number;
  centerLat: number;
  pop: number;
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
// Census 2020 ACS 5-year US avg persons/household.
const AVG_HOUSEHOLD = 2.53;
// Share of residentially-damaged households that actually vacate. Empirical:
// FEMA TSA check-ins / (major+severe residential) from Harvey + Ian runs out
// to roughly 0.5–0.7. We use 0.7 as an upper-bound planning figure, applied
// consistently to both the county and city rollups so their totals reconcile.
const DISPLACEMENT_HAIRCUT = 0.7;

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
      state: FIPS_TO_ABBREV[f.properties?.STATE || ''] || f.properties?.STATE || '',
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

  // Drop counties whose bbox never finite-ized (invalid or missing geometry).
  // Without this guard the bbox prefilter `lon < +Inf` is always true and the
  // ray-cast fallback returns false → the county silently reports zero
  // buildings even though the data passed through.
  const validCounties = countyData.filter((c: any) => {
    const ok = Number.isFinite(c.bbox[0]) && Number.isFinite(c.bbox[2]);
    if (!ok) console.warn('[jurisdictions] dropping county with invalid geometry:', c.rollup.name);
    return ok;
  });

  for (const b of buildings.features) {
    const coords = b.geometry?.coordinates;
    if (!coords) continue;
    const [lon, lat] = coords;
    const p = b.properties || {};

    for (const c of validCounties) {
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
  for (const c of validCounties) {
    if (c.rollup.buildings === 0) continue;
    // Displaced = residential (~70% of footprint in most coastal counties)
    // major+severe × avg household. Conservative — EM can verify.
    c.rollup.estDisplaced = Math.round((c.rollup.severe + c.rollup.major) * DISPLACEMENT_HAIRCUT * AVG_HOUSEHOLD);
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
    const safeName = r.name || 'Unknown';
    return {
      type: 'Feature' as const,
      geometry: { type: 'Point' as const, coordinates: [r.centerLon, r.centerLat] },
      properties: {
        geoid: r.geoid,
        name: safeName,
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
        label: `${safeName}  ${r.buildings.toLocaleString()}`,
      },
    };
  });
  return { type: 'FeatureCollection', features };
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// rollupByCity
//
// Groups buildings by the nearest city centroid within MAX_DIST_DEG
// (≈12 km at Gulf-Coast latitudes). Buildings beyond that radius are
// bucketed as "Unincorporated" and grouped by parent county GEOID so
// rural clusters stay geographically distinct.
//
// Performance: uses a 0.5° lat/lon grid index so each building only
// checks the ~20-30 city candidates in its local grid cell rather than
// all 4,000+ cities in the dataset — O(B × ~25) instead of O(B × C).
//
// countyNameMap: GEOID → county name, built from the county rollup or
// the counties GeoJSON properties so "Unincorporated" rows can carry a
// recognisable parent name.
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
const MAX_DIST_DEG = 0.12;   // ≈12 km — keeps neighbouring cities distinct
const MAX_DIST_SQ  = MAX_DIST_DEG * MAX_DIST_DEG;
const GRID_SIZE    = 0.5;    // grid cell width/height in degrees

export function rollupByCity(
  buildings: any,
  cities: CityEntry[],
  countyNameMap: Record<string, string>,  // geoid → county name
): CityRollup[] {
  if (!buildings?.features?.length || !cities?.length) return [];

  // ── Build spatial grid index ──────────────────────────────────────
  const grid: Record<string, CityEntry[]> = {};
  for (const city of cities) {
    const gx = Math.floor(city.lon / GRID_SIZE);
    const gy = Math.floor(city.lat / GRID_SIZE);
    const k  = `${gx},${gy}`;
    if (!grid[k]) grid[k] = [];
    grid[k].push(city);
  }

  // ── Accumulate per-bucket rollup ──────────────────────────────────
  const buckets: Record<string, {
    rollup: CityRollup;
    sumLon: number;
    sumLat: number;
    count: number;
    resMajorSevere: number;
  }> = {};

  for (const b of buildings.features) {
    const coords = b.geometry?.coordinates;
    if (!coords) continue;
    const [lon, lat] = coords;
    const p = b.properties || {};

    // Find nearest city using grid index
    const gx = Math.floor(lon / GRID_SIZE);
    const gy = Math.floor(lat / GRID_SIZE);
    let bestCity: CityEntry | null = null;
    let bestDSq = MAX_DIST_SQ;

    for (let dx = -1; dx <= 1; dx++) {
      for (let dy = -1; dy <= 1; dy++) {
        const candidates = grid[`${gx + dx},${gy + dy}`];
        if (!candidates) continue;
        for (const city of candidates) {
          const dlon = lon - city.lon;
          const dlat = lat - city.lat;
          const dsq  = dlon * dlon + dlat * dlat;
          if (dsq < bestDSq) { bestDSq = dsq; bestCity = city; }
        }
      }
    }

    // Derive bucket key + metadata
    let key: string, name: string, state: string, countyGeoid: string, countyName: string;
    if (bestCity) {
      key         = `${bestCity.name}|${bestCity.state}`;
      name        = bestCity.name;
      state       = bestCity.state;
      countyGeoid = bestCity.county_geoid;
      countyName  = countyNameMap[bestCity.county_geoid] || '';
    } else {
      // Unincorporated: bucket at 0.2° resolution so adjacent rural areas
      // don't all collapse into one giant "Unincorporated" cluster.
      const gLon = Math.round(lon * 5) / 5;
      const gLat = Math.round(lat * 5) / 5;
      key         = `unincorp|${gLat}|${gLon}`;
      name        = 'Unincorporated';
      state       = '';
      countyGeoid = '';
      countyName  = '';
    }

    if (!buckets[key]) {
      buckets[key] = {
        rollup: {
          key, name, state, countyGeoid, countyName,
          buildings: 0, loss: 0,
          severe: 0, major: 0, moderate: 0, minor: 0,
          criticalFacilities: 0, estDisplaced: 0, maxDepthFt: 0,
          centerLon: bestCity?.lon ?? lon,
          centerLat: bestCity?.lat ?? lat,
          pop: bestCity?.pop ?? 0,
        },
        sumLon: 0, sumLat: 0, count: 0,
        resMajorSevere: 0,
      };
    }

    const { rollup } = buckets[key];
    rollup.buildings += 1;
    rollup.loss      += p.estimated_loss_usd || 0;

    const cat = p.damage_category;
    if      (cat === 'severe')   rollup.severe   += 1;
    else if (cat === 'major')    rollup.major    += 1;
    else if (cat === 'moderate') rollup.moderate += 1;
    else if (cat === 'minor')    rollup.minor    += 1;

    if (p.depth_ft && p.depth_ft > rollup.maxDepthFt) rollup.maxDepthFt = p.depth_ft;

    const occ = (p.occtype || '').toUpperCase().split('-')[0];
    if (CRITICAL_OCCTYPES.has(occ)) rollup.criticalFacilities += 1;

    const isRes = (p.building_type || '').startsWith('RES');
    if (isRes && (cat === 'major' || cat === 'severe')) buckets[key].resMajorSevere += 1;

    // For unincorporated buckets, drift centroid toward actual building cluster
    if (!bestCity) {
      buckets[key].sumLon += lon;
      buckets[key].sumLat += lat;
      buckets[key].count  += 1;
    }
  }

  // Finalize unincorporated centroids + displacement
  const rows: CityRollup[] = [];
  for (const { rollup, sumLon, sumLat, count, resMajorSevere } of Object.values(buckets)) {
    if (rollup.buildings === 0) continue;
    if (rollup.name === 'Unincorporated' && count > 0) {
      rollup.centerLon = sumLon / count;
      rollup.centerLat = sumLat / count;
    }
    // Same formula as rollupByCounty: residential major+severe × haircut × hh.
    rollup.estDisplaced = Math.round(resMajorSevere * DISPLACEMENT_HAIRCUT * AVG_HOUSEHOLD);
    rows.push(rollup);
  }

  rows.sort((a, b) => b.loss - a.loss);
  return rows;
}

/**
 * Convert a city rollup array into a Point FeatureCollection for the
 * city-aggregate bubble layer (rendered between zoom 8 and 11).
 */
export function cityRollupToCentroidGeoJSON(rows: CityRollup[]): any {
  const features = rows.map(r => {
    let worst = 'none';
    if (r.severe > 0) worst = 'severe';
    else if (r.major > 0) worst = 'major';
    else if (r.moderate > 0) worst = 'moderate';
    else if (r.minor > 0) worst = 'minor';
    const displayName = r.name === 'Unincorporated' && r.countyName
      ? `Unincorp. ${r.countyName}`
      : r.name;
    return {
      type: 'Feature' as const,
      geometry: { type: 'Point' as const, coordinates: [r.centerLon, r.centerLat] },
      properties: {
        key: r.key,
        name: displayName,
        state: r.state,
        countyGeoid: r.countyGeoid,
        countyName: r.countyName,
        buildings: r.buildings,
        loss: r.loss,
        severe: r.severe,
        major: r.major,
        moderate: r.moderate,
        minor: r.minor,
        criticalFacilities: r.criticalFacilities,
        estDisplaced: r.estDisplaced,
        pop: r.pop,
        worstCategory: worst,
        label: `${displayName}  ${r.buildings.toLocaleString()}`,
      },
    };
  });
  return { type: 'FeatureCollection', features };
}
