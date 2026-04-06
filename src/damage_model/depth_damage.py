"""
FEMA Depth-Damage Functions

Implements the FIA (Federal Insurance Administration) credibility-weighted
depth-damage curves used by FEMA's HAZUS flood model.

These functions map flood depth (feet above first finished floor) to a
damage percentage for both building structure and contents.

Source: FEMA HAZUS Flood Model Technical Manual (Chapter 5: Direct Physical
Damage — General Building Stock). Curves derived from FIA/NFIP claims data,
interpolated to 1-foot increments from -4 to +24 feet.

Building categories follow HAZUS occupancy codes:
  RES1-1SNB: 1-story, no basement
  RES1-2SNB: 2-story, no basement
  RES1-1SWB: 1-story, with basement
  RES1-2SWB: 2-story, with basement
  RES1-SL:   Split-level

Each curve has structure and contents variants.
"""

from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import numpy as np

logger = logging.getLogger(__name__)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# FIA Depth-Damage Tables
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# Depths in feet relative to first finished floor.
# Negative = water below floor level; 0 = at floor; positive = above.
# HAZUS uses 29 points from -4 to +24 feet at 1-foot intervals.
# We use the key inflection points for interpolation.

DEPTHS_FT = [-4, -3, -2, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8,
             10, 12, 14, 16, 18, 20, 22, 24]

# Structure damage (% of replacement cost)
# Source: FIA credibility-weighted damage functions, HAZUS Tech Manual Ch.5
STRUCTURE_DAMAGE: Dict[str, List[float]] = {
    # 1-story, no basement (most common coastal residential)
    "RES1-1SNB": [
        0, 0, 0, 0, 10, 18, 25, 31, 37, 42, 46, 50, 53,
        58, 62, 65, 67, 69, 70, 71, 72,
    ],
    # 2-story, no basement
    "RES1-2SNB": [
        0, 0, 0, 0, 8, 14, 20, 25, 29, 33, 36, 39, 42,
        47, 52, 56, 59, 62, 64, 66, 68,
    ],
    # 1-story, with basement
    "RES1-1SWB": [
        2, 5, 8, 12, 16, 23, 29, 35, 40, 45, 49, 52, 55,
        60, 64, 67, 69, 71, 72, 73, 74,
    ],
    # 2-story, with basement
    "RES1-2SWB": [
        2, 4, 7, 10, 13, 18, 24, 29, 33, 37, 40, 43, 46,
        51, 55, 59, 62, 64, 66, 68, 70,
    ],
    # Split-level
    "RES1-SL": [
        1, 3, 5, 8, 12, 17, 22, 27, 32, 36, 40, 43, 46,
        51, 55, 59, 62, 64, 66, 68, 70,
    ],
    # Commercial (simplified: average of COM1-COM10)
    "COM": [
        0, 0, 0, 0, 5, 10, 15, 20, 25, 30, 35, 39, 43,
        50, 55, 59, 62, 65, 67, 69, 70,
    ],
    # Industrial
    "IND": [
        0, 0, 0, 0, 4, 8, 13, 18, 23, 28, 32, 36, 40,
        47, 52, 56, 60, 63, 65, 67, 69,
    ],
}

# Contents damage (% of contents value)
# Contents value defaults to 50% of building replacement cost (HAZUS default)
CONTENTS_DAMAGE: Dict[str, List[float]] = {
    "RES1-1SNB": [
        0, 0, 1, 5, 12, 22, 32, 40, 47, 53, 58, 62, 65,
        70, 74, 77, 79, 81, 82, 83, 84,
    ],
    "RES1-2SNB": [
        0, 0, 1, 4, 10, 18, 26, 33, 39, 44, 49, 53, 56,
        62, 67, 71, 74, 76, 78, 80, 82,
    ],
    "RES1-1SWB": [
        5, 8, 12, 16, 20, 28, 36, 43, 49, 55, 60, 64, 67,
        72, 76, 79, 81, 83, 84, 85, 86,
    ],
    "RES1-2SWB": [
        4, 6, 10, 14, 17, 24, 31, 37, 43, 48, 52, 56, 59,
        65, 70, 74, 77, 79, 81, 83, 84,
    ],
    "RES1-SL": [
        3, 5, 8, 12, 15, 21, 28, 35, 41, 46, 50, 54, 57,
        63, 68, 72, 75, 78, 80, 82, 83,
    ],
    "COM": [
        0, 0, 0, 1, 8, 16, 24, 32, 39, 46, 52, 57, 61,
        68, 73, 77, 80, 82, 84, 86, 87,
    ],
    "IND": [
        0, 0, 0, 1, 6, 13, 20, 27, 34, 40, 46, 51, 55,
        63, 69, 73, 77, 80, 82, 84, 85,
    ],
}

# Default building type when not specified
DEFAULT_BUILDING_TYPE = "RES1-1SNB"

# Default replacement cost per sq ft (USD, 2024 Gulf Coast average)
# Source: RSMeans residential cost data, adjusted for Gulf Coast region
DEFAULT_COST_PER_SQFT: Dict[str, float] = {
    "RES1-1SNB": 150.0,
    "RES1-2SNB": 145.0,
    "RES1-1SWB": 160.0,
    "RES1-2SWB": 155.0,
    "RES1-SL": 155.0,
    "COM": 175.0,
    "IND": 120.0,
}

# Default building sizes (sq ft) for damage estimation when area is unknown
DEFAULT_SQFT: Dict[str, float] = {
    "RES1-1SNB": 1400.0,
    "RES1-2SNB": 2200.0,
    "RES1-1SWB": 1600.0,
    "RES1-2SWB": 2400.0,
    "RES1-SL": 1800.0,
    "COM": 5000.0,
    "IND": 10000.0,
}

# HAZUS default: contents value = 50% of building replacement value
CONTENTS_TO_STRUCTURE_RATIO = 0.50

# First finished floor height above grade (feet)
# Slab-on-grade (no basement) = ~1 foot; with basement = ~1 foot;
# elevated (V-zone) = varies. This shifts the depth relative to ground.
DEFAULT_FFH_FT: Dict[str, float] = {
    "RES1-1SNB": 1.0,
    "RES1-2SNB": 1.0,
    "RES1-1SWB": 1.0,  # Basement floor is below, first floor at grade+1
    "RES1-2SWB": 1.0,
    "RES1-SL": 0.5,
    "COM": 0.0,
    "IND": 0.0,
}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Interpolation Engine
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def get_damage_pct(
    depth_ft: float,
    building_type: str = DEFAULT_BUILDING_TYPE,
    component: str = "structure",
) -> float:
    """
    Look up the damage percentage for a given flood depth and building type.

    Uses linear interpolation between the tabulated depth-damage curve points.

    Args:
        depth_ft: Flood depth in feet relative to first finished floor.
                  Negative values = water below floor level.
        building_type: HAZUS occupancy code (e.g., "RES1-1SNB")
        component: "structure" or "contents"

    Returns:
        Damage as a percentage (0-100) of replacement value
    """
    table = STRUCTURE_DAMAGE if component == "structure" else CONTENTS_DAMAGE
    curve = table.get(building_type, table[DEFAULT_BUILDING_TYPE])

    # Clamp to curve bounds
    if depth_ft <= DEPTHS_FT[0]:
        return float(curve[0])
    if depth_ft >= DEPTHS_FT[-1]:
        return float(curve[-1])

    # Linear interpolation
    for i in range(len(DEPTHS_FT) - 1):
        if DEPTHS_FT[i] <= depth_ft <= DEPTHS_FT[i + 1]:
            frac = (depth_ft - DEPTHS_FT[i]) / (DEPTHS_FT[i + 1] - DEPTHS_FT[i])
            return curve[i] + frac * (curve[i + 1] - curve[i])

    return 0.0


def get_total_damage_pct(
    depth_ft: float,
    building_type: str = DEFAULT_BUILDING_TYPE,
) -> float:
    """
    Combined structure + contents damage as percentage of total value.

    Total value = structure replacement + contents (50% of structure).
    Combined damage = (struct_dmg * struct_val + content_dmg * content_val) / total_val
    """
    struct_pct = get_damage_pct(depth_ft, building_type, "structure")
    content_pct = get_damage_pct(depth_ft, building_type, "contents")

    # Weighted average: structure is 1/(1+0.5) = 66.7%, contents is 0.5/(1+0.5) = 33.3%
    r = CONTENTS_TO_STRUCTURE_RATIO
    return (struct_pct + r * content_pct) / (1 + r)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Damage Estimation
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


@dataclass
class BuildingDamage:
    """Estimated damage for a single building."""

    building_id: str
    lon: float
    lat: float
    depth_m: float
    depth_ft: float
    building_type: str
    structure_damage_pct: float
    contents_damage_pct: float
    total_damage_pct: float
    estimated_loss_usd: float
    replacement_value_usd: float


@dataclass
class DamageEstimate:
    """Aggregated damage estimate for a storm area."""

    buildings_assessed: int
    buildings_damaged: int
    total_loss_usd: float
    total_replacement_usd: float
    avg_damage_pct: float
    max_damage_pct: float
    damage_by_category: Dict[str, int]  # e.g., {"minor": 50, "major": 20, ...}
    buildings: List[BuildingDamage]

    def damage_category(self, pct: float) -> str:
        """Classify damage percentage into FEMA categories."""
        return _damage_category(pct)


def _damage_category(pct: float) -> str:
    """Classify damage percentage into FEMA categories (standalone version)."""
    if pct <= 0:
        return "none"
    elif pct < 10:
        return "minor"
    elif pct < 30:
        return "moderate"
    elif pct < 50:
        return "major"
    else:
        return "severe"


def _cost_multiplier(building_id: str) -> float:
    """
    Return a deterministic per-building replacement-cost multiplier in [0.60, 1.40].

    Seeded by the building ID so the same building always gets the same value,
    but neighbouring buildings with different IDs get meaningfully different
    estimates — reflecting real-world variation in age, condition, finish level,
    and local market value that OSM tags don't capture.
    """
    if not building_id:
        return 1.0
    digest = int(hashlib.md5(building_id.encode()).hexdigest()[:8], 16)
    return 0.60 + (digest / 0xFFFF_FFFF) * 0.80   # uniform in [0.60, 1.40]


def estimate_building_damage(
    depth_m: float,
    lon: float = 0.0,
    lat: float = 0.0,
    building_type: str = DEFAULT_BUILDING_TYPE,
    building_id: str = "",
    sqft: Optional[float] = None,
    first_floor_ht_ft: Optional[float] = None,
    val_struct: Optional[float] = None,
    val_cont: Optional[float] = None,
) -> BuildingDamage:
    """
    Estimate flood damage for a single building.

    When val_struct / val_cont are provided (sourced from FEMA NSI), they are
    used directly as the replacement values so the damage calculation is:
        loss = struct_pct% × val_struct + content_pct% × val_cont

    Without NSI data the model falls back to:
        replacement = (sqft or type_default) × cost_per_sqft × id_multiplier

    Args:
        depth_m: Flood depth at the building location (meters, above ground)
        lon, lat: Building coordinates
        building_type: HAZUS occupancy code
        building_id: Unique building identifier
        sqft: Building area in square feet (defaults per type if not provided)
        first_floor_ht_ft: First-floor elevation above grade (ft); from NSI
                           found_ht field or falls back to type default
        val_struct: Structure replacement value in USD (from FEMA NSI)
        val_cont:   Contents replacement value in USD (from FEMA NSI)

    Returns:
        BuildingDamage with loss estimates
    """
    depth_ft = depth_m * 3.28084

    # Adjust for first finished floor height
    # NSI found_ht is the elevation above grade — use it when available
    ffh = (first_floor_ht_ft if first_floor_ht_ft is not None
           else DEFAULT_FFH_FT.get(building_type, 1.0))
    depth_above_floor_ft = depth_ft - ffh

    # Look up damage percentages from HAZUS depth-damage curves
    struct_pct = get_damage_pct(depth_above_floor_ft, building_type, "structure")
    content_pct = get_damage_pct(depth_above_floor_ft, building_type, "contents")
    total_pct = get_total_damage_pct(depth_above_floor_ft, building_type)

    # ── Replacement value ───────────────────────────────────────────
    if val_struct is not None:
        # NSI path: use actual tabulated replacement costs
        struct_value = float(val_struct)
        content_value = float(val_cont) if val_cont is not None else struct_value * CONTENTS_TO_STRUCTURE_RATIO
    else:
        # Fallback path: sqft × $/sqft with per-building cost variation
        area = sqft or DEFAULT_SQFT.get(building_type, 1400)
        base_cost = DEFAULT_COST_PER_SQFT.get(building_type, 150)
        cost_per_sqft = base_cost * _cost_multiplier(building_id)
        struct_value = area * cost_per_sqft
        content_value = struct_value * CONTENTS_TO_STRUCTURE_RATIO

    replacement = struct_value + content_value
    loss = (struct_pct / 100 * struct_value) + (content_pct / 100 * content_value)

    return BuildingDamage(
        building_id=building_id,
        lon=lon,
        lat=lat,
        depth_m=depth_m,
        depth_ft=depth_ft,
        building_type=building_type,
        structure_damage_pct=round(struct_pct, 1),
        contents_damage_pct=round(content_pct, 1),
        total_damage_pct=round(total_pct, 1),
        estimated_loss_usd=round(loss, 0),
        replacement_value_usd=round(replacement, 0),
    )


def estimate_damage_from_raster(
    depth_raster_path: str,
    buildings_geojson_path: str,
    output_path: str = "",
    building_type: str = DEFAULT_BUILDING_TYPE,
) -> DamageEstimate:
    """
    Estimate damage for all buildings by sampling flood depth at each location.

    Args:
        depth_raster_path: Path to flood depth GeoTIFF (meters)
        buildings_geojson_path: GeoJSON with building point/polygon features
        output_path: Optional path to write damage results as GeoJSON
        building_type: Default building type (used when feature has no type)

    Returns:
        DamageEstimate with per-building and aggregated loss data
    """
    import json
    import rasterio

    logger.info(
        f"Estimating damage: raster={depth_raster_path}, "
        f"buildings={buildings_geojson_path}"
    )

    # Load buildings
    with open(buildings_geojson_path) as f:
        buildings_data = json.load(f)

    features = buildings_data.get("features", [])
    if not features:
        logger.warning("No buildings in input GeoJSON")
        return DamageEstimate(
            buildings_assessed=0, buildings_damaged=0,
            total_loss_usd=0, total_replacement_usd=0,
            avg_damage_pct=0, max_damage_pct=0,
            damage_by_category={}, buildings=[],
        )

    # Open depth raster
    with rasterio.open(depth_raster_path) as src:
        depth_band = src.read(1)
        nodata = src.nodata or -9999
        transform = src.transform

        buildings = []
        for i, feat in enumerate(features):
            geom = feat.get("geometry", {})
            props = feat.get("properties", {})

            # Get building centroid
            lon, lat = _get_centroid(geom)
            if lon == 0 and lat == 0:
                continue

            # Sample depth at building location
            try:
                row, col = rasterio.transform.rowcol(transform, lon, lat)
                if 0 <= row < depth_band.shape[0] and 0 <= col < depth_band.shape[1]:
                    depth_m = float(depth_band[row, col])
                else:
                    depth_m = 0.0
            except Exception:
                depth_m = 0.0

            if depth_m == nodata or depth_m <= 0.0:
                depth_m = 0.0

            # Get building type from properties or use default
            btype = props.get("building_type", props.get("type", building_type))
            if btype not in STRUCTURE_DAMAGE:
                btype = building_type

            bid = props.get("id", props.get("building_id", str(i)))
            sqft      = props.get("area_sqft")
            val_struct = props.get("val_struct")   # NSI: actual structure value
            val_cont   = props.get("val_cont")     # NSI: actual contents value
            found_ht   = props.get("found_ht")     # NSI: first-floor elevation

            damage = estimate_building_damage(
                depth_m=depth_m, lon=lon, lat=lat,
                building_type=btype, building_id=str(bid),
                sqft=float(sqft) if sqft else None,
                first_floor_ht_ft=float(found_ht) if found_ht is not None else None,
                val_struct=float(val_struct) if val_struct is not None else None,
                val_cont=float(val_cont) if val_cont is not None else None,
            )
            buildings.append(damage)

    # Aggregate
    damaged = [b for b in buildings if b.total_damage_pct > 0]
    total_loss = sum(b.estimated_loss_usd for b in buildings)
    total_replacement = sum(b.replacement_value_usd for b in buildings)
    max_pct = max((b.total_damage_pct for b in buildings), default=0)
    avg_pct = (
        sum(b.total_damage_pct for b in damaged) / len(damaged)
        if damaged else 0
    )

    # Categorize
    categories = {"none": 0, "minor": 0, "moderate": 0, "major": 0, "severe": 0}
    est = DamageEstimate(
        buildings_assessed=len(buildings),
        buildings_damaged=len(damaged),
        total_loss_usd=total_loss,
        total_replacement_usd=total_replacement,
        avg_damage_pct=round(avg_pct, 1),
        max_damage_pct=round(max_pct, 1),
        damage_by_category=categories,
        buildings=buildings,
    )
    for b in buildings:
        cat = est.damage_category(b.total_damage_pct)
        categories[cat] = categories.get(cat, 0) + 1

    logger.info(
        f"Damage estimate: {est.buildings_assessed} assessed, "
        f"{est.buildings_damaged} damaged, "
        f"${est.total_loss_usd:,.0f} total loss"
    )

    # Write output GeoJSON
    if output_path:
        _write_damage_geojson(buildings, output_path)

    return est


def _get_centroid(geom: dict) -> Tuple[float, float]:
    """Extract centroid coordinates from a GeoJSON geometry."""
    gtype = geom.get("type", "")
    coords = geom.get("coordinates", [])

    if gtype == "Point":
        return (coords[0], coords[1]) if len(coords) >= 2 else (0, 0)

    elif gtype == "Polygon":
        ring = coords[0] if coords else []
        if not ring:
            return (0, 0)
        lons = [c[0] for c in ring]
        lats = [c[1] for c in ring]
        return (sum(lons) / len(lons), sum(lats) / len(lats))

    elif gtype == "MultiPolygon":
        # Use first polygon's centroid
        if coords and coords[0]:
            ring = coords[0][0]
            lons = [c[0] for c in ring]
            lats = [c[1] for c in ring]
            return (sum(lons) / len(lons), sum(lats) / len(lats))

    return (0, 0)


def _write_damage_geojson(
    buildings: List[BuildingDamage],
    output_path: str,
) -> str:
    """Write building damage results as a GeoJSON FeatureCollection."""
    import json
    import os

    features = []
    for b in buildings:
        features.append({
            "type": "Feature",
            "properties": {
                "layer": "damage",
                "building_id": b.building_id,
                "depth_m": round(b.depth_m, 2),
                "depth_ft": round(b.depth_ft, 1),
                "building_type": b.building_type,
                "structure_damage_pct": b.structure_damage_pct,
                "contents_damage_pct": b.contents_damage_pct,
                "total_damage_pct": b.total_damage_pct,
                "estimated_loss_usd": b.estimated_loss_usd,
                "damage_category": _damage_category(b.total_damage_pct),
            },
            "geometry": {
                "type": "Point",
                "coordinates": [b.lon, b.lat],
            },
        })

    geojson = {"type": "FeatureCollection", "features": features}

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(geojson, f)

    logger.info(f"Damage GeoJSON: {len(features)} buildings -> {output_path}")
    return output_path
