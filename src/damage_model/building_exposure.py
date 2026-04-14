"""
Building Exposure Layer

Provides building footprint data for damage estimation by:
  1. Loading from a pre-downloaded GeoJSON/GeoPackage of building footprints
     (Microsoft Open Buildings, OpenStreetMap, or county parcel data)
  2. Generating synthetic building points for development/demo

The output is a GeoJSON FeatureCollection of Point geometries (centroids)
that can be intersected with the flood depth raster.

Data sources (in priority order):
  - Microsoft Open Buildings (US): ~130M footprints, free download
    https://github.com/microsoft/USBuildingFootprints
  - OpenStreetMap buildings export
  - County parcel centroids from local GIS
  - Synthetic generation (dev fallback)
"""

from __future__ import annotations

import json
import logging
import math
import os
import random
from dataclasses import dataclass
from typing import List, Optional, Tuple

logger = logging.getLogger(__name__)


@dataclass
class BuildingInventory:
    """Result of building exposure extraction."""

    geojson_path: str
    building_count: int
    bounds: Tuple[float, float, float, float]  # west, south, east, north
    source: str  # "ms_open_buildings", "osm", "synthetic"


def load_buildings_for_extent(
    storm_geometry: dict,
    data_path: str = "",
    output_path: str = "",
    max_buildings: int = 50000,
) -> BuildingInventory:
    """
    Load or generate building footprints within the storm extent.

    Args:
        storm_geometry: GeoJSON Polygon geometry of the storm area
        data_path: Path to pre-downloaded buildings file (.gpkg, .geojson)
        output_path: Path to write the clipped buildings GeoJSON
        max_buildings: Maximum buildings to include (performance limit)

    Returns:
        BuildingInventory with path and count
    """
    coords = storm_geometry.get("coordinates", [[]])
    flat = coords[0] if coords else []
    if not flat:
        logger.warning("No storm geometry — cannot load buildings")
        return BuildingInventory(
            geojson_path="", building_count=0,
            bounds=(0, 0, 0, 0), source="none",
        )

    lons = [c[0] for c in flat]
    lats = [c[1] for c in flat]
    bounds = (min(lons), min(lats), max(lons), max(lats))

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # Try loading from real data
    if data_path and os.path.exists(data_path):
        return _load_from_file(data_path, bounds, output_path, max_buildings)

    # Fallback: generate synthetic buildings
    return _generate_synthetic_buildings(bounds, output_path, max_buildings)


def _load_from_file(
    data_path: str,
    bounds: Tuple[float, float, float, float],
    output_path: str,
    max_buildings: int,
) -> BuildingInventory:
    """Load buildings from GeoPackage or GeoJSON, clipped to bounds."""
    try:
        import fiona
        from shapely.geometry import shape, box, mapping

        west, south, east, north = bounds
        clip_box = box(west, south, east, north)
        features = []

        with fiona.open(data_path) as src:
            for feat in src.filter(bbox=bounds):
                if len(features) >= max_buildings:
                    break

                geom = shape(feat["geometry"])
                centroid = geom.centroid

                if not clip_box.contains(centroid):
                    continue

                props = feat.get("properties", {})
                features.append({
                    "type": "Feature",
                    "properties": {
                        "building_id": props.get("id", props.get("OBJECTID", len(features))),
                        "area_sqft": _estimate_area_sqft(geom),
                        "building_type": _classify_building(props),
                        "source": "file",
                    },
                    "geometry": {
                        "type": "Point",
                        "coordinates": [centroid.x, centroid.y],
                    },
                })

        geojson = {"type": "FeatureCollection", "features": features}
        # Atomic write to avoid partial GeoJSON blocking subsequent reads.
        import threading as _th_be
        _tmp = f"{output_path}.tmp.{os.getpid()}.{_th_be.get_ident()}"
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

        logger.info(f"Loaded {len(features)} buildings from {data_path}")
        return BuildingInventory(
            geojson_path=output_path,
            building_count=len(features),
            bounds=bounds,
            source="file",
        )

    except Exception as e:
        logger.warning(f"Failed to load buildings from {data_path}: {e}")
        return _generate_synthetic_buildings(bounds, output_path, max_buildings)


def _estimate_area_sqft(geom) -> float:
    """Estimate building footprint area in square feet from geometry."""
    try:
        # geom.area is in square degrees — convert roughly
        # 1 degree lat ~= 111km, so 1 sq degree ~= 1.23e10 sq meters
        area_sq_deg = geom.area
        area_sq_m = area_sq_deg * 1.23e10  # Very rough
        area_sqft = area_sq_m * 10.764
        return min(max(area_sqft, 500), 50000)  # Clamp to reasonable range
    except Exception:
        return 1400.0  # Default residential


def _classify_building(props: dict) -> str:
    """Classify building type from feature properties."""
    # OSM
    building = props.get("building", "").lower()
    if building in ("commercial", "retail", "office"):
        return "COM"
    elif building in ("industrial", "warehouse"):
        return "IND"

    # MS Open Buildings — mostly residential
    return "RES1-1SNB"


def _generate_synthetic_buildings(
    bounds: Tuple[float, float, float, float],
    output_path: str,
    max_buildings: int,
) -> BuildingInventory:
    """
    Generate synthetic building points for development and demo.

    Places buildings in a semi-realistic pattern:
      - Higher density near the coast (lower latitude in Gulf region)
      - Clustered along a grid with jitter to simulate neighborhoods
      - Random building types weighted toward residential
    """
    west, south, east, north = bounds

    # Estimate density: ~100 buildings per 0.01° x 0.01° (~1km²) in developed areas
    # Use lower density for storm-scale areas (many are rural/water)
    density_per_sq_deg = 800  # ~8 buildings per sq km

    area_sq_deg = (east - west) * (north - south)
    target_count = min(int(area_sq_deg * density_per_sq_deg), max_buildings)

    # Use deterministic seed for reproducibility
    rng = random.Random(hash((west, south, east, north)) & 0xFFFFFFFF)

    building_types = [
        ("RES1-1SNB", 0.55),
        ("RES1-2SNB", 0.20),
        ("RES1-1SWB", 0.05),
        ("RES1-2SWB", 0.05),
        ("COM", 0.10),
        ("IND", 0.05),
    ]

    features = []
    for i in range(target_count):
        # Weighted toward coast (lower latitudes in Gulf region)
        lat = south + rng.random() ** 0.7 * (north - south)
        lon = west + rng.random() * (east - west)

        # Add neighborhood clustering
        if rng.random() < 0.6:
            # Snap to ~0.005° grid with jitter (neighborhood blocks)
            lon = round(lon / 0.005) * 0.005 + rng.gauss(0, 0.001)
            lat = round(lat / 0.005) * 0.005 + rng.gauss(0, 0.001)

        # Pick building type
        r = rng.random()
        cumulative = 0
        btype = "RES1-1SNB"
        for bt, weight in building_types:
            cumulative += weight
            if r < cumulative:
                btype = bt
                break

        features.append({
            "type": "Feature",
            "properties": {
                "building_id": f"SYN-{i:06d}",
                "area_sqft": int(rng.gauss(
                    {"RES1-1SNB": 1400, "RES1-2SNB": 2200, "COM": 5000, "IND": 10000}.get(btype, 1400),
                    300,
                )),
                "building_type": btype,
                "source": "synthetic",
            },
            "geometry": {
                "type": "Point",
                "coordinates": [round(lon, 6), round(lat, 6)],
            },
        })

    geojson = {"type": "FeatureCollection", "features": features}
    import threading as _th_be2
    _tmp = f"{output_path}.tmp.{os.getpid()}.{_th_be2.get_ident()}"
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

    logger.info(f"Generated {len(features)} synthetic buildings in {bounds}")
    return BuildingInventory(
        geojson_path=output_path,
        building_count=len(features),
        bounds=bounds,
        source="synthetic",
    )
