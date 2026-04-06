"""
Vector Overlay Tile Builder

Generates vector tile layers (.pmtiles) for non-raster map overlays:
  - Storm forecast cone (polygon)
  - Storm track line (line)
  - NHDPlus river reaches (line)

These overlay layers are served alongside the depth-based flood tiles
and rendered as separate MapLibre GL JS sources/layers.

Each overlay is first written as GeoJSON, then run through tippecanoe
(or the Python fallback) to produce PMTiles for HTTP range-request
streaming from S3/CloudFront.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from common.saffir_simpson import wind_to_category as _wind_to_cat_int

logger = logging.getLogger(__name__)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Data Classes
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


@dataclass
class OverlayResult:
    """Result of building a single vector overlay layer."""

    layer_name: str
    geojson_path: str
    pmtiles_path: Optional[str] = None
    feature_count: int = 0
    size_bytes: int = 0


@dataclass
class VectorOverlaysResult:
    """Combined result of all vector overlay layers."""

    layers: Dict[str, OverlayResult] = field(default_factory=dict)

    @property
    def layer_names(self) -> List[str]:
        return list(self.layers.keys())


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Storm Cone Overlay
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def build_cone_geojson(
    storm_geometry: dict,
    storm_id: str,
    advisory_num: str,
    storm_name: str = "",
    output_path: str = "",
) -> str:
    """
    Convert the buffered storm cone geometry into a styled GeoJSON
    FeatureCollection for vector tile generation.

    Args:
        storm_geometry: GeoJSON Polygon geometry (already buffered by orchestrator)
        storm_id: Storm identifier (e.g. "AL142024")
        advisory_num: Advisory number (e.g. "003")
        storm_name: Human-readable storm name
        output_path: Where to write the GeoJSON file

    Returns:
        Path to the written GeoJSON file
    """
    features = []

    # Main cone polygon
    if storm_geometry and storm_geometry.get("coordinates"):
        features.append({
            "type": "Feature",
            "properties": {
                "layer": "cone",
                "storm_id": storm_id,
                "advisory": advisory_num,
                "name": storm_name,
                "type": "forecast_cone",
            },
            "geometry": storm_geometry,
        })

    geojson = {
        "type": "FeatureCollection",
        "features": features,
    }

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(geojson, f)

    logger.info(f"Cone GeoJSON: {len(features)} features -> {output_path}")
    return output_path


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Storm Track Line Overlay
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def extract_track_line(shapefile_dir: str) -> Optional[dict]:
    """
    Read the forecast track line from an NHC shapefile directory.

    NHC 5-day archives contain both polygon (cone) and line (track)
    shapefiles. The track line files typically have "lin" in the name.

    Returns:
        GeoJSON geometry dict (LineString or MultiLineString), or None
    """
    try:
        import fiona
    except ImportError:
        logger.warning("fiona not installed — cannot extract track line")
        return None

    if not shapefile_dir or not os.path.isdir(shapefile_dir):
        return None

    # Find track line shapefile: look for files with "lin" in name
    shp_files = [
        f for f in os.listdir(shapefile_dir)
        if f.lower().endswith(".shp") and "lin" in f.lower()
    ]

    if not shp_files:
        # No dedicated line file — try to find any non-polygon shapefile
        shp_files = [
            f for f in os.listdir(shapefile_dir)
            if f.lower().endswith(".shp") and "pgn" not in f.lower()
        ]

    if not shp_files:
        logger.info("No track line shapefile found")
        return None

    shp_path = os.path.join(shapefile_dir, shp_files[0])
    logger.info(f"Reading track line from: {shp_path}")

    try:
        with fiona.open(shp_path) as src:
            features = list(src)
            if not features:
                return None

            # Find line geometry features
            line_features = [
                f for f in features
                if f["geometry"]["type"] in ("LineString", "MultiLineString")
            ]

            if line_features:
                return line_features[0]["geometry"]

            # If no line features, try point features and build a line
            point_features = [
                f for f in features
                if f["geometry"]["type"] == "Point"
            ]

            if len(point_features) >= 2:
                coords = [f["geometry"]["coordinates"] for f in point_features]
                return {"type": "LineString", "coordinates": coords}

    except Exception as e:
        logger.warning(f"Failed to extract track line: {e}")

    return None


def extract_track_points(shapefile_dir: str) -> List[dict]:
    """
    Extract forecast track points from NHC shapefile.

    Each point includes forecast hour, wind speed, and category
    as properties for rendering sized/colored markers on the map.

    Returns:
        List of GeoJSON Feature dicts (Point geometry)
    """
    try:
        import fiona
    except ImportError:
        return []

    if not shapefile_dir or not os.path.isdir(shapefile_dir):
        return []

    # Find point shapefile (usually the "pts" or non-pgn, non-lin file)
    shp_files = [
        f for f in os.listdir(shapefile_dir)
        if f.lower().endswith(".shp") and "pts" in f.lower()
    ]
    if not shp_files:
        shp_files = [
            f for f in os.listdir(shapefile_dir)
            if f.lower().endswith(".shp")
            and "pgn" not in f.lower()
            and "lin" not in f.lower()
        ]

    if not shp_files:
        return []

    shp_path = os.path.join(shapefile_dir, shp_files[0])

    try:
        with fiona.open(shp_path) as src:
            features = []
            for feat in src:
                if feat["geometry"]["type"] != "Point":
                    continue

                props = feat.get("properties", {})
                # NHC shapefiles use MAXWIND, TAU (forecast hour), STORMTYPE
                wind_kt = props.get("MAXWIND", props.get("maxwind", 0))
                tau = props.get("TAU", props.get("tau", 0))
                storm_type = props.get("STORMTYPE", props.get("stormtype", ""))

                features.append({
                    "type": "Feature",
                    "properties": {
                        "layer": "track_point",
                        "wind_kt": int(wind_kt) if wind_kt else 0,
                        "wind_mph": int(wind_kt * 1.151) if wind_kt else 0,
                        "forecast_hour": int(tau) if tau else 0,
                        "storm_type": str(storm_type),
                        "category": _wind_to_category(int(wind_kt) if wind_kt else 0),
                    },
                    "geometry": feat["geometry"],
                })

            return features

    except Exception as e:
        logger.warning(f"Failed to extract track points: {e}")
        return []


def _wind_to_category(wind_kt: int) -> str:
    """Convert max sustained wind (knots) to Saffir-Simpson category string.

    Uses the canonical wind_to_category() from common.saffir_simpson and
    wraps the int result into the string label expected by the tile layer.
    """
    cat = _wind_to_cat_int(wind_kt)
    if cat >= 1:
        return f"CAT{cat}"
    return "TS" if wind_kt >= 34 else "TD"


def build_track_geojson(
    shapefile_dir: str,
    storm_id: str,
    advisory_num: str,
    storm_name: str = "",
    output_path: str = "",
) -> str:
    """
    Build a GeoJSON FeatureCollection containing the storm track
    line and forecast position points.

    Args:
        shapefile_dir: Directory containing NHC shapefiles
        storm_id: Storm identifier
        advisory_num: Advisory number
        storm_name: Human-readable storm name
        output_path: Where to write the GeoJSON file

    Returns:
        Path to the written GeoJSON file
    """
    features = []

    # Extract track line
    track_geom = extract_track_line(shapefile_dir)
    if track_geom:
        features.append({
            "type": "Feature",
            "properties": {
                "layer": "track_line",
                "storm_id": storm_id,
                "advisory": advisory_num,
                "name": storm_name,
                "type": "forecast_track",
            },
            "geometry": track_geom,
        })

    # Extract forecast position points
    track_points = extract_track_points(shapefile_dir)
    features.extend(track_points)

    geojson = {
        "type": "FeatureCollection",
        "features": features,
    }

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(geojson, f)

    logger.info(f"Track GeoJSON: {len(features)} features -> {output_path}")
    return output_path


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# NHDPlus Reaches Overlay
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def build_reaches_geojson(
    storm_geometry: dict,
    nhdplus_path: str,
    output_path: str,
    reach_ids: Optional[List[int]] = None,
) -> str:
    """
    Clip NHDPlus flowlines to the storm extent and write as GeoJSON.

    If a pre-built NHDPlus GeoPackage or FlatGeobuf exists, clip it
    spatially. Otherwise, generate a synthetic river network for
    development/demo purposes.

    Args:
        storm_geometry: GeoJSON Polygon geometry for spatial clip
        nhdplus_path: Path to NHDPlus data file (.gpkg or .fgb)
        output_path: Where to write the GeoJSON file
        reach_ids: Optional list of specific COMID reach IDs to include

    Returns:
        Path to the written GeoJSON file
    """
    features = []

    if nhdplus_path and os.path.exists(nhdplus_path):
        features = _clip_nhdplus_from_file(
            nhdplus_path, storm_geometry, reach_ids
        )
    else:
        # Generate synthetic reaches for dev/demo
        features = _generate_synthetic_reaches(storm_geometry)

    geojson = {
        "type": "FeatureCollection",
        "features": features,
    }

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(geojson, f)

    logger.info(f"Reaches GeoJSON: {len(features)} features -> {output_path}")
    return output_path


def _clip_nhdplus_from_file(
    nhdplus_path: str,
    storm_geometry: dict,
    reach_ids: Optional[List[int]] = None,
) -> List[dict]:
    """
    Read NHDPlus flowlines from a GeoPackage/FlatGeobuf and clip
    to the storm bounding box.
    """
    try:
        import fiona
        from shapely.geometry import shape, mapping, box
    except ImportError:
        logger.warning("fiona/shapely not available for NHDPlus clipping")
        return _generate_synthetic_reaches(storm_geometry)

    # Extract bbox from storm geometry
    storm_shape = shape(storm_geometry)
    bbox = storm_shape.bounds  # (minx, miny, maxx, maxy)

    reach_id_set = set(reach_ids) if reach_ids else None
    features = []

    try:
        with fiona.open(nhdplus_path) as src:
            for feat in src.filter(bbox=bbox):
                props = feat.get("properties", {})
                comid = props.get("COMID", props.get("comid", 0))

                # Filter to specific reaches if provided
                if reach_id_set and int(comid) not in reach_id_set:
                    continue

                geom = shape(feat["geometry"])
                if not geom.intersects(storm_shape):
                    continue

                # Clip geometry to storm extent
                clipped = geom.intersection(storm_shape)
                if clipped.is_empty:
                    continue

                # Extract stream order for styling
                stream_order = props.get(
                    "StreamOrde", props.get("streamorde", 1)
                )

                features.append({
                    "type": "Feature",
                    "properties": {
                        "layer": "reach",
                        "comid": int(comid),
                        "stream_order": int(stream_order) if stream_order else 1,
                        "name": props.get("GNIS_NAME", props.get("gnis_name", "")),
                    },
                    "geometry": mapping(clipped),
                })

        logger.info(f"Clipped {len(features)} NHDPlus reaches from {nhdplus_path}")

    except Exception as e:
        logger.warning(f"NHDPlus file read failed: {e}")
        features = _generate_synthetic_reaches(storm_geometry)

    return features


def _generate_synthetic_reaches(storm_geometry: dict) -> List[dict]:
    """
    Generate a synthetic river network within the storm extent
    for development and demo use when NHDPlus data isn't available.

    Creates a grid of N-S and E-W lines with meandering offsets
    to simulate a basic drainage network.
    """
    import math

    coords = storm_geometry.get("coordinates", [[]])
    flat = coords[0] if coords else []
    if not flat:
        return []

    lons = [c[0] for c in flat]
    lats = [c[1] for c in flat]
    west, south = min(lons), min(lats)
    east, north = max(lons), max(lats)

    features = []
    comid = 90000
    step = 0.15  # ~17km grid spacing

    # Generate N-S flowing reaches (main channels)
    lon = west + step / 2
    while lon < east:
        points = []
        lat = north
        while lat > south:
            # Add slight meander
            offset = 0.02 * math.sin(lat * 50)
            points.append([lon + offset, lat])
            lat -= 0.02
        if len(points) >= 2:
            features.append({
                "type": "Feature",
                "properties": {
                    "layer": "reach",
                    "comid": comid,
                    "stream_order": 3,
                    "name": "",
                    "synthetic": True,
                },
                "geometry": {"type": "LineString", "coordinates": points},
            })
            comid += 1
        lon += step

    # Generate E-W tributary reaches
    lat = south + step / 2
    while lat < north:
        points = []
        x = west
        while x < east:
            offset = 0.01 * math.sin(x * 80)
            points.append([x, lat + offset])
            x += 0.02
        if len(points) >= 2:
            features.append({
                "type": "Feature",
                "properties": {
                    "layer": "reach",
                    "comid": comid,
                    "stream_order": 1,
                    "name": "",
                    "synthetic": True,
                },
                "geometry": {"type": "LineString", "coordinates": points},
            })
            comid += 1
        lat += step

    logger.info(f"Generated {len(features)} synthetic reaches")
    return features


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Combined Overlay Builder
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def build_vector_overlays(
    output_dir: str,
    storm_id: str,
    advisory_num: str,
    storm_name: str = "",
    storm_geometry: Optional[dict] = None,
    cone_shapefile_dir: str = "",
    nhdplus_path: str = "",
    reach_ids: Optional[List[int]] = None,
    tide_gauge_geojson: str = "",
) -> VectorOverlaysResult:
    """
    Build all vector overlay layers for a storm advisory.

    This is the main entry point called from the TILEGEN pipeline step.
    Each layer is written as GeoJSON, then converted to PMTiles.

    Args:
        output_dir: Base directory for output files
        storm_id: Storm identifier
        advisory_num: Advisory number
        storm_name: Human-readable storm name
        storm_geometry: Buffered storm extent polygon (GeoJSON geometry)
        cone_shapefile_dir: Path to downloaded NHC shapefiles
        nhdplus_path: Path to NHDPlus flowline data
        reach_ids: NHDPlus COMID reach IDs from the INGEST stage

    Returns:
        VectorOverlaysResult with paths and metadata for each layer
    """
    from .pmtiles_builder import build_vector_pmtiles

    overlay_dir = os.path.join(output_dir, "overlays")
    os.makedirs(overlay_dir, exist_ok=True)

    result = VectorOverlaysResult()

    # ── 1. Storm Cone ─────────────────────────────────────────
    if storm_geometry:
        cone_geojson = os.path.join(overlay_dir, "storm_cone.geojson")
        build_cone_geojson(
            storm_geometry, storm_id, advisory_num,
            storm_name, cone_geojson,
        )

        cone_pmtiles = os.path.join(overlay_dir, "storm_cone.pmtiles")
        try:
            pm_result = build_vector_pmtiles(
                cone_geojson, cone_pmtiles,
                min_zoom=3, max_zoom=12,
                layer_name="storm_cone",
            )
            result.layers["storm_cone"] = OverlayResult(
                layer_name="storm_cone",
                geojson_path=cone_geojson,
                pmtiles_path=pm_result.path,
                feature_count=1,
                size_bytes=pm_result.size_bytes,
            )
        except Exception as e:
            logger.error(f"Cone PMTiles failed: {e}")
            result.layers["storm_cone"] = OverlayResult(
                layer_name="storm_cone",
                geojson_path=cone_geojson,
                feature_count=1,
            )

    # ── 2. Storm Track ────────────────────────────────────────
    if cone_shapefile_dir and os.path.isdir(cone_shapefile_dir):
        track_geojson = os.path.join(overlay_dir, "storm_track.geojson")
        build_track_geojson(
            cone_shapefile_dir, storm_id, advisory_num,
            storm_name, track_geojson,
        )

        # Check if we got any features
        with open(track_geojson) as f:
            track_data = json.load(f)
        feat_count = len(track_data.get("features", []))

        if feat_count > 0:
            track_pmtiles = os.path.join(overlay_dir, "storm_track.pmtiles")
            try:
                pm_result = build_vector_pmtiles(
                    track_geojson, track_pmtiles,
                    min_zoom=3, max_zoom=12,
                    layer_name="storm_track",
                )
                result.layers["storm_track"] = OverlayResult(
                    layer_name="storm_track",
                    geojson_path=track_geojson,
                    pmtiles_path=pm_result.path,
                    feature_count=feat_count,
                    size_bytes=pm_result.size_bytes,
                )
            except Exception as e:
                logger.error(f"Track PMTiles failed: {e}")
                result.layers["storm_track"] = OverlayResult(
                    layer_name="storm_track",
                    geojson_path=track_geojson,
                    feature_count=feat_count,
                )

    # ── 3. NHDPlus Reaches ────────────────────────────────────
    if storm_geometry:
        reaches_geojson = os.path.join(overlay_dir, "reaches.geojson")
        build_reaches_geojson(
            storm_geometry, nhdplus_path,
            reaches_geojson, reach_ids,
        )

        with open(reaches_geojson) as f:
            reaches_data = json.load(f)
        feat_count = len(reaches_data.get("features", []))

        if feat_count > 0:
            reaches_pmtiles = os.path.join(overlay_dir, "reaches.pmtiles")
            try:
                pm_result = build_vector_pmtiles(
                    reaches_geojson, reaches_pmtiles,
                    min_zoom=6, max_zoom=14,
                    layer_name="reaches",
                )
                result.layers["reaches"] = OverlayResult(
                    layer_name="reaches",
                    geojson_path=reaches_geojson,
                    pmtiles_path=pm_result.path,
                    feature_count=feat_count,
                    size_bytes=pm_result.size_bytes,
                )
            except Exception as e:
                logger.error(f"Reaches PMTiles failed: {e}")
                result.layers["reaches"] = OverlayResult(
                    layer_name="reaches",
                    geojson_path=reaches_geojson,
                    feature_count=feat_count,
                )

    # ── 4. Tide Gauges ───────────────────────────────────────
    if tide_gauge_geojson and os.path.exists(tide_gauge_geojson):
        import shutil

        # Copy the pre-built GeoJSON into the overlay output directory
        dest_geojson = os.path.join(overlay_dir, "tide_gauges.geojson")
        shutil.copy2(tide_gauge_geojson, dest_geojson)

        with open(dest_geojson) as f:
            gauge_data = json.load(f)
        feat_count = len(gauge_data.get("features", []))

        if feat_count > 0:
            gauge_pmtiles = os.path.join(overlay_dir, "tide_gauges.pmtiles")
            try:
                pm_result = build_vector_pmtiles(
                    dest_geojson, gauge_pmtiles,
                    min_zoom=3, max_zoom=14,
                    layer_name="tide_gauges",
                )
                result.layers["tide_gauges"] = OverlayResult(
                    layer_name="tide_gauges",
                    geojson_path=dest_geojson,
                    pmtiles_path=pm_result.path,
                    feature_count=feat_count,
                    size_bytes=pm_result.size_bytes,
                )
            except Exception as e:
                logger.error(f"Tide gauge PMTiles failed: {e}")
                result.layers["tide_gauges"] = OverlayResult(
                    layer_name="tide_gauges",
                    geojson_path=dest_geojson,
                    feature_count=feat_count,
                )

    logger.info(
        f"Vector overlays complete: {len(result.layers)} layers "
        f"({', '.join(result.layer_names)})"
    )
    return result
