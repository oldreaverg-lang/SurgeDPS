"""
PMTiles Builder

Converts flood model outputs into PMTiles archives for efficient
single-file tile serving from S3 via HTTP range requests.

PMTiles eliminates the need for a tile server — the browser fetches
individual tiles by reading byte ranges from a single archive file.

Two output tracks:
  1. Vector PMTiles — flood depth polygons for overlay rendering
  2. Raster PMTiles — depth-encoded raster tiles for pixel queries

The vector track is preferred for MapLibre GL JS rendering,
while the raster track is used for point-query (click-to-depth).
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import numpy as np

logger = logging.getLogger(__name__)


@dataclass
class PMTilesResult:
    """Result of PMTiles generation."""

    path: str
    size_bytes: int
    format: str                # "vector" or "raster"
    min_zoom: int
    max_zoom: int
    bounds: Tuple[float, float, float, float]
    s3_key: Optional[str] = None


@dataclass
class TileGenResult:
    """Complete tile generation output for one flood layer."""

    layer_name: str            # "surge", "rainfall", "compound", "overlap"
    cog: Optional[str] = None  # COG path for raster queries
    pmtiles_free: Optional[str] = None   # Low-res PMTiles (zoom 8-12)
    pmtiles_premium: Optional[str] = None  # High-res PMTiles (zoom 13-16)
    metadata: Dict = field(default_factory=dict)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Raster to Vector Conversion
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def raster_to_geojson(
    raster_path: str,
    output_path: str,
    depth_property: str = "depth",
    min_depth: float = 0.05,
    simplify_tolerance: float = 0.0001,
) -> str:
    """
    Convert a flood depth raster to GeoJSON polygons.

    Each contiguous flooded region becomes a polygon feature
    with the average depth as a property.

    Uses rasterio.features.shapes for vectorization.
    """
    import rasterio
    from rasterio.features import shapes
    from shapely.geometry import shape, mapping
    from shapely.ops import unary_union

    logger.info(f"Vectorizing {raster_path} -> {output_path}")

    with rasterio.open(raster_path) as src:
        data = src.read(1)
        nodata = src.nodata or -9999
        transform = src.transform

    # Classify depth into bins for cleaner polygons
    bins = [0.3, 0.9, 1.8, 3.0, 5.0, 10.0]  # meters
    classified = np.zeros_like(data, dtype=np.int16)
    valid = (data != nodata) & (data > min_depth)
    for i, threshold in enumerate(bins):
        classified[valid & (data > threshold)] = i + 1

    # No flooding → skip
    if not np.any(classified > 0):
        # Write empty GeoJSON
        with open(output_path, "w") as f:
            json.dump({"type": "FeatureCollection", "features": []}, f)
        return output_path

    # Vectorize — collect all shapes per depth class, then union them
    depth_ranges = [
        (0.3, 0.3), (0.3, 0.9), (0.9, 1.8),
        (1.8, 3.0), (3.0, 5.0), (5.0, 10.0), (10.0, 20.0)
    ]
    class_polys: dict = {}
    for geom, value in shapes(
        classified, mask=(classified > 0), transform=transform
    ):
        if value == 0:
            continue
        cls = int(value)
        class_polys.setdefault(cls, []).append(shape(geom))

    features = []
    for cls, polys in class_polys.items():
        # Merge all pixels of the same depth class into one smooth polygon
        merged = unary_union(polys)
        merged = merged.simplify(0.002, preserve_topology=True)
        if merged.is_empty or not merged.is_valid:
            continue

        if cls < len(depth_ranges):
            lo, hi = depth_ranges[cls]
            avg_depth = (lo + hi) / 2
        else:
            avg_depth = 12.0

        depth_ft = avg_depth * 3.28084

        features.append({
            "type": "Feature",
            "properties": {
                depth_property: round(avg_depth, 2),
                "depth_ft": round(depth_ft, 1),
                "class": cls,
            },
            "geometry": mapping(merged),
        })

    geojson = {
        "type": "FeatureCollection",
        "features": features,
    }

    with open(output_path, "w") as f:
        json.dump(geojson, f)

    logger.info(
        f"Vectorized: {len(features)} polygons, "
        f"{os.path.getsize(output_path) / 1024:.0f} KB"
    )
    return output_path


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# PMTiles Generation
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def build_vector_pmtiles(
    geojson_path: str,
    output_path: str,
    min_zoom: int = 8,
    max_zoom: int = 12,
    layer_name: str = "flood",
) -> PMTilesResult:
    """
    Build a vector PMTiles archive from GeoJSON using tippecanoe.

    tippecanoe must be installed and on PATH.

    Args:
        geojson_path: Input GeoJSON file
        output_path: Output .pmtiles file
        min_zoom: Minimum zoom level
        max_zoom: Maximum zoom level
        layer_name: Name of the vector tile layer

    Returns:
        PMTilesResult with archive metadata
    """
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # Build tippecanoe command
    cmd = [
        "tippecanoe",
        "-o", output_path,
        f"--minimum-zoom={min_zoom}",
        f"--maximum-zoom={max_zoom}",
        f"--layer={layer_name}",
        "--no-tile-size-limit",
        "--simplification=10",
        "--detect-shared-borders",
        "--coalesce-densest-as-needed",
        "--force",
        geojson_path,
    ]

    logger.info(f"Running tippecanoe: {' '.join(cmd)}")

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=300,
        )
        if result.returncode != 0:
            logger.error(f"tippecanoe failed: {result.stderr}")
            raise RuntimeError(f"tippecanoe failed: {result.stderr}")
    except FileNotFoundError:
        logger.warning(
            "tippecanoe not found — falling back to Python PMTiles builder"
        )
        return _build_pmtiles_python(
            geojson_path, output_path, min_zoom, max_zoom, layer_name
        )

    size = os.path.getsize(output_path)
    logger.info(
        f"PMTiles created: {output_path} ({size / 1024:.0f} KB, "
        f"z{min_zoom}-{max_zoom})"
    )

    return PMTilesResult(
        path=output_path,
        size_bytes=size,
        format="vector",
        min_zoom=min_zoom,
        max_zoom=max_zoom,
        bounds=_get_geojson_bounds(geojson_path),
    )


def _build_pmtiles_python(
    geojson_path: str,
    output_path: str,
    min_zoom: int,
    max_zoom: int,
    layer_name: str,
) -> PMTilesResult:
    """
    Fallback: generate a minimal PMTiles-compatible file using Python.

    This is a simplified alternative when tippecanoe is not available.
    For production, tippecanoe is strongly recommended.

    In this fallback, we write the GeoJSON as-is with a .geojson
    extension (the frontend can load GeoJSON directly as a source).
    """
    import shutil

    # For now, just copy the GeoJSON — the frontend can load it directly
    fallback_path = output_path.replace(".pmtiles", ".geojson")
    shutil.copy2(geojson_path, fallback_path)

    size = os.path.getsize(fallback_path)
    logger.info(
        f"Fallback: copied GeoJSON as {fallback_path} ({size / 1024:.0f} KB)"
    )

    return PMTilesResult(
        path=fallback_path,
        size_bytes=size,
        format="geojson_fallback",
        min_zoom=min_zoom,
        max_zoom=max_zoom,
        bounds=_get_geojson_bounds(geojson_path),
    )


def _get_geojson_bounds(
    path: str,
) -> Tuple[float, float, float, float]:
    """Extract bounding box from a GeoJSON file."""
    try:
        with open(path) as f:
            data = json.load(f)

        coords = []
        for feat in data.get("features", []):
            geom = feat.get("geometry", {})
            _extract_coords(geom, coords)

        if not coords:
            return (-180, -90, 180, 90)

        lons = [c[0] for c in coords]
        lats = [c[1] for c in coords]
        return (min(lons), min(lats), max(lons), max(lats))

    except Exception:
        return (-180, -90, 180, 90)


def _extract_coords(geom: dict, coords: list):
    """Recursively extract coordinates from a GeoJSON geometry."""
    geom_type = geom.get("type", "")
    raw_coords = geom.get("coordinates", [])

    if geom_type == "Point":
        coords.append(raw_coords)
    elif geom_type in ("LineString", "MultiPoint"):
        coords.extend(raw_coords)
    elif geom_type in ("Polygon", "MultiLineString"):
        for ring in raw_coords:
            coords.extend(ring)
    elif geom_type == "MultiPolygon":
        for polygon in raw_coords:
            for ring in polygon:
                coords.extend(ring)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Full Tile Generation Pipeline
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def generate_tiles_for_layer(
    depth_raster_path: str,
    output_dir: str,
    layer_name: str,
    storm_id: str,
    advisory_num: str,
    timestep: int = 0,
) -> TileGenResult:
    """
    Generate all tile products for a single flood layer.

    Produces:
      - COG for raster point queries
      - Free-tier PMTiles (low-res, zoom 8-12)
      - Premium-tier PMTiles (high-res, zoom 13-16)
    """
    from .cog_builder import build_cog, build_classified_cog

    ts_str = f"t{timestep:03d}" if timestep > 0 else "t000"
    layer_dir = os.path.join(output_dir, layer_name, ts_str)
    os.makedirs(layer_dir, exist_ok=True)

    result = TileGenResult(layer_name=layer_name)

    # 1. Build COG (full resolution, for point queries)
    cog_path = os.path.join(layer_dir, f"{layer_name}_depth.tif")
    try:
        cog_result = build_cog(depth_raster_path, cog_path)
        result.cog = cog_result.path
    except Exception as e:
        logger.error(f"COG build failed for {layer_name}: {e}")

    # 2. Vectorize for PMTiles
    geojson_path = os.path.join(layer_dir, f"{layer_name}.geojson")
    try:
        raster_to_geojson(depth_raster_path, geojson_path)
    except Exception as e:
        logger.error(f"Vectorization failed for {layer_name}: {e}")
        return result

    # 3. Free-tier PMTiles (zoom 8-12, simplified)
    free_path = os.path.join(layer_dir, "free", f"{layer_name}.pmtiles")
    try:
        free_result = build_vector_pmtiles(
            geojson_path, free_path,
            min_zoom=8, max_zoom=12, layer_name=layer_name,
        )
        result.pmtiles_free = free_result.path
    except Exception as e:
        logger.error(f"Free PMTiles failed for {layer_name}: {e}")

    # 4. Premium-tier PMTiles (zoom 13-16, detailed)
    premium_path = os.path.join(
        layer_dir, "premium", f"{layer_name}.pmtiles"
    )
    try:
        premium_result = build_vector_pmtiles(
            geojson_path, premium_path,
            min_zoom=13, max_zoom=16, layer_name=layer_name,
        )
        result.pmtiles_premium = premium_result.path
    except Exception as e:
        logger.error(f"Premium PMTiles failed for {layer_name}: {e}")

    # 5. Metadata
    result.metadata = {
        "layer": layer_name,
        "storm_id": storm_id,
        "advisory": advisory_num,
        "timestep": timestep,
        "cog": result.cog,
        "pmtiles_free": result.pmtiles_free,
        "pmtiles_premium": result.pmtiles_premium,
    }

    return result
