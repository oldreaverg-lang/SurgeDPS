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
# Microsoft Building Footprints (international fallback)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# Microsoft's Global ML Building Footprints dataset — free, worldwide
# coverage derived from satellite imagery.  Available as GeoJSON per
# geographic tile via the Planetary Computer STAC API.
#
# Useful for Caribbean / Central America hurricanes where NSI does
# not exist and OSM building coverage is sparse.
#
# Source: https://planetarycomputer.microsoft.com/dataset/ms-buildings

_MSFT_BUILDINGS_ENDPOINT = (
    "https://planetarycomputer.microsoft.com/api/stac/v1/search"
)
_MSFT_TIMEOUT = 20


def _fetch_microsoft_buildings(
    lon_min: float,
    lat_min: float,
    lon_max: float,
    lat_max: float,
    output_path: str,
) -> Optional[str]:
    """
    Fetch building footprints from Microsoft's Global ML Building Footprints
    via the Planetary Computer STAC API.

    Returns path to GeoJSON file, or None if unavailable.

    The footprints are ML-derived from satellite imagery so they contain only
    geometry (polygon) — no occupancy type, valuation, or height info.
    All buildings are tagged as RES1-1SNB (residential default) with
    source="MSFT".
    """
    # Check if the bbox is outside the US (NSI would have covered US)
    # Simple heuristic: skip if entirely within CONUS bbox
    if (lon_min > -125 and lon_max < -66 and lat_min > 24 and lat_max < 50):
        return None  # US mainland — let OSM handle it instead

    bbox_geojson = {
        "type": "Polygon",
        "coordinates": [[
            [lon_min, lat_min], [lon_max, lat_min],
            [lon_max, lat_max], [lon_min, lat_max],
            [lon_min, lat_min],
        ]],
    }

    try:
        resp = requests.post(
            _MSFT_BUILDINGS_ENDPOINT,
            json={
                "collections": ["ms-buildings"],
                "intersects": bbox_geojson,
                "limit": 1,
            },
            timeout=_MSFT_TIMEOUT,
            headers={"User-Agent": "SurgeDPS/1.0 (flood-damage-model)"},
        )
        resp.raise_for_status()
        stac_result = resp.json()
    except Exception as exc:
        logger.info("[MSFT] STAC query failed: %s", exc)
        return None

    items = stac_result.get("features", [])
    if not items:
        logger.info("[MSFT] No building footprint tiles found for this bbox")
        return None

    # Get the GeoJSON asset URL from the first matching tile
    asset_url = None
    for item in items:
        assets = item.get("assets", {})
        for key in ("data", "default"):
            if key in assets:
                asset_url = assets[key].get("href")
                break
        if asset_url:
            break

    if not asset_url:
        logger.info("[MSFT] No downloadable GeoJSON asset in STAC result")
        return None

    # Download and filter to our bbox
    try:
        dl_resp = requests.get(asset_url, timeout=60,
                                headers={"User-Agent": "SurgeDPS/1.0"})
        dl_resp.raise_for_status()
        msft_data = dl_resp.json()
    except Exception as exc:
        logger.info("[MSFT] Asset download failed: %s", exc)
        return None

    features = []
    for feat in msft_data.get("features", []):
        geom = feat.get("geometry", {})
        # Get centroid from polygon
        coords = geom.get("coordinates", [])
        if not coords or not coords[0]:
            continue
        ring = coords[0]
        avg_lon = sum(p[0] for p in ring) / len(ring)
        avg_lat = sum(p[1] for p in ring) / len(ring)

        # Filter to our exact bbox
        if not (lon_min <= avg_lon <= lon_max and lat_min <= avg_lat <= lat_max):
            continue

        # Compute area from polygon
        area_sqft = _polygon_area_sqft(
            [{"lat": p[1], "lon": p[0]} for p in ring]
        )

        # Data quality: MSFT baseline 0.1 (ML footprint, no attributes)
        _dq = 0.1
        if area_sqft: _dq += 0.1  # have area from polygon

        props: Dict = {
            "id": f"msft_{len(features)}",
            "type": "RES1-1SNB",   # conservative default — no type info
            "source": "MSFT",
            "data_quality": round(min(_dq, 1.0), 2),
        }
        if area_sqft:
            props["area_sqft"] = round(area_sqft, 1)

        features.append({
            "type": "Feature",
            "properties": props,
            "geometry": {"type": "Point", "coordinates": [avg_lon, avg_lat]},
        })

    if not features:
        logger.info("[MSFT] No buildings within bbox after filtering")
        return None

    geojson = {"type": "FeatureCollection", "features": features}
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    # Atomic write so a partial .json from a killed process can't survive.
    import threading as _th_bld
    _tmp = f"{output_path}.tmp.{os.getpid()}.{_th_bld.get_ident()}"
    try:
        with open(_tmp, "w") as f:
            json.dump(geojson, f)
        os.replace(_tmp, output_path)
    except Exception:
        try:
            if os.path.exists(_tmp):
                os.remove(_tmp)
        except OSError:
            pass
        raise

    logger.info("[MSFT] Wrote %d building footprints to %s", len(features), output_path)
    return output_path


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
    # ── Try NSI first (US only — best quality) ──────────────────
    from .nsi_fetcher import fetch_buildings_nsi
    nsi_result = fetch_buildings_nsi(lon_min, lat_min, lon_max, lat_max,
                                     output_path, cache=cache)
    if nsi_result:
        return nsi_result

    print("  [buildings] NSI unavailable, falling back to OpenStreetMap")

    # ── Try Microsoft Building Footprints (international coverage) ──
    try:
        msft_result = _fetch_microsoft_buildings(lon_min, lat_min, lon_max, lat_max, output_path)
        if msft_result:
            return msft_result
    except Exception as exc:
        logger.warning("[MSFT] Building footprints fetch failed: %s", exc)

    # ── OSM fallback below ────────────────────────────────────────
    if cache and os.path.exists(output_path):
        try:
            with open(output_path) as f:
                data = json.load(f)
            n = len(data.get("features", []))
            logger.info(f"Using cached buildings ({n} features): {output_path}")
            print(f"  [cache hit] {n} buildings loaded from {output_path}")
            return output_path
        except (json.JSONDecodeError, OSError) as exc:
            # Corrupt cache (partial write from a killed previous run) —
            # refetch instead of propagating "Unterminated string" up.
            logger.warning("Cached buildings file %s unreadable (%s); refetching",
                           output_path, exc)
            try:
                os.remove(output_path)
            except OSError:
                pass

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

        # Data quality: OSM baseline 0.2 (geometry only, no valuations)
        _dq = 0.2
        if area_sqft is not None: _dq += 0.1   # have real footprint area
        if tags.get("building:levels"): _dq += 0.1  # have story count
        if tags.get("building", "yes") != "yes": _dq += 0.05  # specific type

        props: Dict = {
            "id": f"osm_{elem.get('id', len(features))}",
            "type": hazus_code,
            "source": "OSM",
            "data_quality": round(min(_dq, 1.0), 2),
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
    # Atomic write — partial writes would surface later as "Unterminated
    # string" JSONDecodeError from warm_cell.
    import threading as _th_bld
    _tmp = f"{output_path}.tmp.{os.getpid()}.{_th_bld.get_ident()}"
    try:
        with open(_tmp, "w") as f:
            json.dump(geojson, f)
        os.replace(_tmp, output_path)
    except Exception:
        try:
            if os.path.exists(_tmp):
                os.remove(_tmp)
        except OSError:
            pass
        raise

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
