"""
Real Building Footprint Fetcher

Queries OpenStreetMap via the Overpass API to retrieve actual building
footprints within a bounding box, then maps OSM building tags to FEMA
HAZUS occupancy codes for the damage model.

Outputs GeoJSON FeatureCollections in the exact format expected by
estimate_damage_from_raster() and the React/MapLibre frontend.
"""

from __future__ import annotations

import json
import logging
import math
import os
import time
from typing import Dict, List, Optional, Tuple

import requests

logger = logging.getLogger(__name__)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# OSM Tag → HAZUS Occupancy Code Mapping
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# Maps the OSM `building=*` tag value to a HAZUS code.
# The damage model recognises: RES1-1SNB, RES1-2SNB, RES1-1SWB,
# RES1-2SWB, RES1-SL, COM, IND
OSM_TO_HAZUS: Dict[str, str] = {
    # ── Residential ──────────────────────────────────────
    "yes":          "RES1-1SNB",   # generic → default 1-story res
    "residential":  "RES1-1SNB",
    "house":        "RES1-1SNB",
    "detached":     "RES1-1SNB",
    "semidetached_house": "RES1-1SNB",
    "terrace":      "RES1-1SNB",
    "bungalow":     "RES1-1SNB",
    "cabin":        "RES1-1SNB",
    "static_caravan": "RES1-1SNB",
    "houseboat":    "RES1-1SNB",
    "farm":         "RES1-1SNB",
    "apartments":   "RES1-2SNB",   # multi-story → 2-story proxy
    "dormitory":    "RES1-2SNB",
    "hotel":        "COM",
    # ── Commercial ───────────────────────────────────────
    "commercial":   "COM",
    "retail":       "COM",
    "office":       "COM",
    "supermarket":  "COM",
    "kiosk":        "COM",
    "store":        "COM",
    "shop":         "COM",
    "restaurant":   "COM",
    "civic":        "COM",
    "government":   "COM",
    "hospital":     "COM",
    "school":       "COM",
    "university":   "COM",
    "church":       "COM",
    "cathedral":    "COM",
    "chapel":       "COM",
    "mosque":       "COM",
    "synagogue":    "COM",
    "temple":       "COM",
    "public":       "COM",
    "fire_station": "COM",
    "train_station": "COM",
    "transportation": "COM",
    # ── Industrial ───────────────────────────────────────
    "industrial":   "IND",
    "warehouse":    "IND",
    "manufacture":  "IND",
    "factory":      "IND",
    # ── Other (mapped to nearest HAZUS proxy) ────────────
    "garage":       "RES1-1SNB",
    "garages":      "RES1-1SNB",
    "shed":         "RES1-1SNB",
    "roof":         "RES1-1SNB",
    "construction": "RES1-1SNB",
    "ruins":        "RES1-1SNB",
}


def _classify_building(tags: Dict[str, str]) -> str:
    """
    Determine the HAZUS code from an OSM element's tag dictionary.

    Uses the `building` tag first, then refines with `building:levels`
    to distinguish 1-story vs 2-story residential types.
    """
    btype = tags.get("building", "yes").lower().strip()
    hazus = OSM_TO_HAZUS.get(btype, "RES1-1SNB")

    # Refine residential codes with level data when available
    if hazus.startswith("RES1"):
        levels_str = tags.get("building:levels", "")
        if levels_str:
            try:
                levels = int(float(levels_str))
                if levels >= 2:
                    # Upgrade to 2-story variant, keeping basement flag
                    hazus = hazus.replace("1S", "2S")
            except ValueError:
                pass

    return hazus


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Overpass Query Builder
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

OVERPASS_ENDPOINTS = [
    "https://lz4.overpass-api.de/api/interpreter",
    "https://z.overpass-api.de/api/interpreter",
    "https://overpass-api.de/api/interpreter",
]

# Timeout for the Overpass server-side query (seconds)
OVERPASS_TIMEOUT = 25


def _build_query(bbox: Tuple[float, float, float, float]) -> str:
    """
    Build an Overpass QL query for all buildings inside a bbox.

    bbox: (lat_min, lon_min, lat_max, lon_max)   — Overpass convention
    Uses `out center geom` so we get both the centroid and polygon vertices.
    The polygon vertices let us compute actual building footprint area.
    """
    south, west, north, east = bbox
    return f"""
[out:json][timeout:{OVERPASS_TIMEOUT}];
(
  way["building"]({south},{west},{north},{east});
  relation["building"]({south},{west},{north},{east});
);
out center geom;
""".strip()


def _polygon_area_sqft(geometry: List[Dict]) -> Optional[float]:
    """
    Compute polygon footprint area from Overpass geometry (list of {lat, lon}
    dicts) using the Shoelace formula with a local degree-to-metres conversion.

    Returns area in square feet, clamped to [200, 50 000], or None if the
    geometry is too small to be a real building.
    """
    if not geometry or len(geometry) < 3:
        return None

    lat_mean = sum(p["lat"] for p in geometry) / len(geometry)
    m_per_deg_lat = 111_320.0
    m_per_deg_lon = 111_320.0 * math.cos(math.radians(lat_mean))

    # Shoelace / surveyor's formula
    n = len(geometry)
    area_deg2 = 0.0
    for i in range(n):
        j = (i + 1) % n
        area_deg2 += geometry[i]["lon"] * geometry[j]["lat"]
        area_deg2 -= geometry[j]["lon"] * geometry[i]["lat"]
    area_m2 = abs(area_deg2) / 2.0 * m_per_deg_lat * m_per_deg_lon
    area_sqft = area_m2 * 10.7639

    # Guard against degenerate polygons (sheds < 200 sqft) and spurious giants
    if area_sqft < 200:
        return None
    return min(area_sqft, 50_000.0)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Public Interface
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def fetch_buildings(
    lon_min: float,
    lat_min: float,
    lon_max: float,
    lat_max: float,
    output_path: str,
    cache: bool = True,
) -> str:
    """
    Fetch building inventory for a bounding box.

    Primary source: FEMA National Structure Inventory (NSI) — actual tabulated
    replacement values, foundation heights, and footprint areas per building.

    Fallback: OpenStreetMap via Overpass API — building type from tags plus
    polygon-derived footprint area; no per-building value data.

    The returned GeoJSON is consumed by estimate_damage_from_raster().

    Args:
        lon_min, lat_min, lon_max, lat_max: WGS-84 bounding box
        output_path: Where to write the GeoJSON result
        cache: If True, skip network calls when output_path already exists
               and was written by the same source (NSI preferred)

    Returns:
        Path to the written GeoJSON file
    """
    # ── Try NSI first ────────────────────────────────────────────
    from .nsi_fetcher import fetch_buildings_nsi
    nsi_result = fetch_buildings_nsi(lon_min, lat_min, lon_max, lat_max,
                                     output_path, cache=cache)
    if nsi_result:
        return nsi_result

    print("  [buildings] NSI unavailable, falling back to OpenStreetMap")
    # ── OSM fallback below ────────────────────────────────────────
    if cache and os.path.exists(output_path):
        with open(output_path) as f:
            data = json.load(f)
        n = len(data.get("features", []))
        logger.info(f"Using cached buildings ({n} features): {output_path}")
        print(f"  [cache hit] {n} buildings loaded from {output_path}")
        return output_path

    # Overpass uses (south, west, north, east) ordering
    bbox = (lat_min, lon_min, lat_max, lon_max)
    query = _build_query(bbox)

    print(f"  Querying Overpass API for buildings in "
          f"[{lon_min:.2f},{lat_min:.2f} → {lon_max:.2f},{lat_max:.2f}] ...")

    raw = None
    last_err = None
    for endpoint in OVERPASS_ENDPOINTS:
        try:
            print(f"  Trying endpoint: {endpoint}")
            resp = requests.post(
                endpoint,
                data={"data": query},
                timeout=OVERPASS_TIMEOUT + 30,
                headers={"User-Agent": "SurgeDPS/1.0 (flood-damage-model)"},
            )
            resp.raise_for_status()
            raw = resp.json()
            break
        except (requests.RequestException, ValueError) as e:
            last_err = e
            print(f"  Endpoint failed ({e.__class__.__name__}), trying next...")
            continue

    if raw is None:
        raise RuntimeError(
            f"All Overpass endpoints failed. Last error: {last_err}"
        )

    elements = raw.get("elements", [])
    print(f"  Overpass returned {len(elements)} raw elements")

    # Convert to GeoJSON FeatureCollection
    features = []
    skipped = 0
    for elem in elements:
        # `out center` puts centroid in elem["center"] for ways/relations
        if "center" in elem:
            lon = elem["center"]["lon"]
            lat = elem["center"]["lat"]
        elif elem.get("type") == "node":
            lon = elem.get("lon", 0)
            lat = elem.get("lat", 0)
        else:
            skipped += 1
            continue

        tags = elem.get("tags", {})
        hazus_code = _classify_building(tags)

        # Compute actual footprint area from polygon vertices when available.
        # `out center geom` provides geometry for ways; relations may not.
        geom_nodes = elem.get("geometry")  # list of {lat, lon} or None
        area_sqft = _polygon_area_sqft(geom_nodes) if geom_nodes else None

        props: Dict = {
            "id": f"osm_{elem.get('id', len(features))}",
            "type": hazus_code,
            # Preserve useful OSM metadata for tooltip enrichment
            "osm_name": tags.get("name", ""),
            "osm_levels": tags.get("building:levels", ""),
            "osm_building": tags.get("building", "yes"),
        }
        if area_sqft is not None:
            props["area_sqft"] = round(area_sqft, 1)

        features.append({
            "type": "Feature",
            "properties": props,
            "geometry": {
                "type": "Point",
                "coordinates": [lon, lat],
            },
        })

    geojson = {"type": "FeatureCollection", "features": features}

    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(geojson, f)

    if skipped:
        logger.info(f"Skipped {skipped} elements with no centroid data")

    # Quick stats
    type_counts: Dict[str, int] = {}
    for feat in features:
        t = feat["properties"]["type"]
        type_counts[t] = type_counts.get(t, 0) + 1

    print(f"  Wrote {len(features)} buildings to {output_path}")
    print(f"  HAZUS breakdown: {type_counts}")

    return output_path
