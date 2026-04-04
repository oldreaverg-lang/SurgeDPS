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
import os
import time
from typing import Dict, Optional, Tuple

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
OVERPASS_TIMEOUT = 300


def _build_query(bbox: Tuple[float, float, float, float]) -> str:
    """
    Build an Overpass QL query for all buildings inside a bbox.

    bbox: (lat_min, lon_min, lat_max, lon_max)   — Overpass convention
    Returns centroids (`out center`) so we get one point per building,
    matching the pipeline's point-based damage model.
    """
    south, west, north, east = bbox
    return f"""
[out:json][timeout:{OVERPASS_TIMEOUT}];
(
  way["building"]({south},{west},{north},{east});
  relation["building"]({south},{west},{north},{east});
);
out center;
""".strip()


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
    Fetch real building footprints from OpenStreetMap for a bounding box.

    Downloads all buildings via the Overpass API, classifies each one
    into a HAZUS occupancy code, and writes a GeoJSON FeatureCollection
    of Point features (centroids) with `id` and `type` properties —
    the exact schema consumed by the damage pipeline.

    Args:
        lon_min, lat_min, lon_max, lat_max: WGS-84 bounding box
        output_path: Where to write the GeoJSON result
        cache: If True, skip the API call when output_path already exists

    Returns:
        Path to the written GeoJSON file
    """
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

        features.append({
            "type": "Feature",
            "properties": {
                "id": f"osm_{elem.get('id', len(features))}",
                "type": hazus_code,
                # Preserve useful OSM metadata for tooltip enrichment
                "osm_name": tags.get("name", ""),
                "osm_levels": tags.get("building:levels", ""),
                "osm_building": tags.get("building", "yes"),
            },
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
