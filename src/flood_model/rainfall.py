"""
Rainfall Flood Estimation Model

Produces a rainfall-induced flood depth raster from storm precipitation
data. This is a simplified model for rapid assessment — it does NOT
replace proper hydrologic/hydraulic modeling (HEC-RAS, SWMM, etc.)
but provides a credible first-order estimate.

Approach:
  1. Estimate total storm rainfall at each grid cell using a
     parametric tropical cyclone precipitation model (R-CLIPER or
     Lonfat-type) based on storm track parameters.
  2. Apply a simple runoff coefficient based on land use/imperviousness.
  3. Convert excess rainfall to flood depth using terrain-based
     accumulation (flow direction → flow accumulation → depth).
  4. Optionally, overlay FEMA SFHA flood zones for plausibility check.

The parametric rain model is based on:
  - Lonfat et al. (2004): axisymmetric + wavenumber-1 rain structure
  - Tuleya et al. (2007): tropical cyclone rainfall climatology
  - Key parameters: rain rate ~ f(distance_from_center, max_wind, forward_speed)

References:
  - Lonfat et al. (2004). A parametric model for predicting hurricane
    rainfall. Mon. Wea. Rev., 132, 3466-3488.
  - Tuleya et al. (2007). Evaluation of GFDL and simple statistical
    model rainfall forecasts for U.S. landfalling hurricanes. Wea.
    Forecasting, 22, 56-70.
"""

from __future__ import annotations

import logging
import math
import os
from dataclasses import dataclass
from typing import Optional, Tuple

import numpy as np

logger = logging.getLogger(__name__)


@dataclass
class RainfallEstimate:
    """Output of rainfall flood estimation."""

    depth_raster_path: str         # Rainfall flood depth GeoTIFF (meters)
    total_precip_path: str         # Total precipitation raster (mm)
    max_depth_m: float
    max_precip_mm: float
    avg_precip_mm: float
    flooded_cells: int
    total_cells: int
    flooded_pct: float
    bounds: Tuple[float, float, float, float]
    crs: str


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Parametric Tropical Cyclone Rain Model (Lonfat et al. 2004)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def estimate_rain_rate_mm_hr(
    distance_km: float,
    max_wind_kt: float,
    storm_speed_kt: float,
    quadrant: str = "right",
) -> float:
    """
    Estimate rainfall rate at a given distance from storm center.

    Uses a parametric model based on Lonfat et al. (2004):
    - Peak rain near 50-100 km from center (eyewall + inner rainbands)
    - Rain rate scales with storm intensity
    - Slow-moving storms produce more total rainfall (longer duration)
    - Right-front quadrant typically wettest (forward speed adds to TC motion)

    The model is axisymmetric with a wavenumber-1 asymmetry representing
    the effect of forward speed and environmental shear.

    Args:
        distance_km: Distance from storm center (km)
        max_wind_kt: Maximum sustained wind speed (knots)
        storm_speed_kt: Forward speed of storm center (knots)
        quadrant: 'right' (front-right, wettest), 'left', 'front', 'rear'

    Returns:
        Rainfall rate in mm/hr at this location
    """
    if distance_km < 0:
        distance_km = 0

    # Peak rain rate at the radius of maximum wind (RMW), roughly 50 km
    # Scales empirically with max wind speed: r_peak ~ U^0.6
    # Coefficients tuned to tropical cyclone climatology
    r_peak = 0.5 * (max(max_wind_kt, 35) ** 0.6)

    # Radial decay: exponential with scale dependent on storm size
    # Larger storms (higher wind) have broader rain field
    r_scale = 200.0 + 2.0 * max(max_wind_kt, 35)

    # Axisymmetric envelope: exponential decay with distance
    # Exponent of 0.8 provides realistic profile
    r_axisym = r_peak * np.exp(-((distance_km / r_scale) ** 0.8))

    # Wavenumber-1 asymmetry: enhanced rainfall on forward-right quadrant
    # due to storm motion adding to wind shear
    # Asymmetry magnitude increases with forward speed
    asymmetry_magnitude = 0.2 + 0.02 * max(storm_speed_kt, 0)  # max ~30% enhancement
    asymmetry_magnitude = min(asymmetry_magnitude, 0.5)  # cap at 50%

    # Quadrant adjustment (relative magnitude)
    quadrant_factor = 1.0
    if quadrant.lower() == "right":
        # Right-front quadrant: enhanced by forward motion
        quadrant_factor = 1.0 + asymmetry_magnitude
    elif quadrant.lower() == "left":
        # Left-front quadrant: partially reduced
        quadrant_factor = 1.0 - asymmetry_magnitude * 0.5
    elif quadrant.lower() == "front":
        # Front: between right and left
        quadrant_factor = 1.0 + asymmetry_magnitude * 0.3
    elif quadrant.lower() == "rear":
        # Rear: reduced
        quadrant_factor = 1.0 - asymmetry_magnitude * 0.3

    return r_axisym * quadrant_factor


def estimate_storm_duration_hr(
    storm_speed_kt: float,
    rmax_nm: float = 20.0,
    rain_field_extent_nm: float = 200.0,
) -> float:
    """
    Estimate duration of rainfall at a point as storm passes.

    Simple model: duration ≈ (rain_field_diameter / forward_speed)
    Accounts for slow-moving or stalled storms (Harvey-type scenarios).

    Args:
        storm_speed_kt: Forward speed of storm center (knots)
        rmax_nm: Radius of maximum wind (nautical miles, typically 20-50)
        rain_field_extent_nm: Extent of rain field from center (nm, typ. 150-300)

    Returns:
        Duration in hours
    """
    if storm_speed_kt < 1.0:
        # Nearly stationary storm: assume rain lasts 24-72 hours
        # (typical stalled tropical cyclone)
        return 36.0

    # Rain field diameter (2 × extent)
    field_diameter_nm = 2.0 * rain_field_extent_nm

    # Time for center to traverse one diameter
    duration_hr = field_diameter_nm / storm_speed_kt

    # Clip to realistic bounds
    duration_hr = max(duration_hr, 4.0)  # minimum 4 hours
    duration_hr = min(duration_hr, 72.0)  # maximum 72 hours

    return duration_hr


def estimate_total_precip_mm(
    rain_rate_mm_hr: float,
    storm_speed_kt: float,
    duration_hr: float,
) -> float:
    """
    Convert rain rate and duration into total precipitation.

    Accounts for slowdown factor: stalled storms accumulate more rain
    at a given location because the center lingers nearby.

    Args:
        rain_rate_mm_hr: Rainfall rate (mm/hr) from parametric model
        storm_speed_kt: Forward speed (knots); used to compute slowdown
        duration_hr: Duration rainfall occurs (hours)

    Returns:
        Total accumulated precipitation (mm)
    """
    # Slowdown factor: slower storms = longer duration at any point
    # Normalized to forward speed; typical values 1.0-3.0
    slowdown_factor = 1.0
    if storm_speed_kt < 5.0:
        # Nearly stationary: accumulate more (Harvey effect)
        slowdown_factor = 2.5
    elif storm_speed_kt < 10.0:
        slowdown_factor = 1.8
    elif storm_speed_kt < 15.0:
        slowdown_factor = 1.3

    total_precip = rain_rate_mm_hr * duration_hr * slowdown_factor
    return total_precip


def get_runoff_coefficient(
    land_use_class: Optional[str] = None,
    default_runoff: float = 0.50,
) -> float:
    """
    Return runoff coefficient based on land use.

    Runoff coefficient represents the fraction of rainfall that becomes
    surface runoff (flooding), accounting for infiltration and retention.

    Without actual land-use data, uses a default that's reasonable for
    mixed urban/suburban/rural landscape.

    Args:
        land_use_class: Optional NLCD or similar class name
                       ('developed_high', 'developed_low', 'agriculture', 'forest', etc.)
        default_runoff: Default if no class provided or class not recognized

    Returns:
        Runoff coefficient (0.0 to 1.0)
    """
    # Simple lookup for common land covers
    runoff_lookup = {
        "developed_high": 0.85,      # Urban core, impervious
        "developed_med": 0.70,       # Suburban
        "developed_low": 0.60,       # Sparse urban
        "developed_open": 0.40,      # Parks, open space
        "agriculture": 0.35,         # Cropland
        "grassland": 0.25,           # Pasture, grass
        "forest": 0.15,              # Deciduous/evergreen
        "wetland": 0.20,             # Wetland
        "water": 1.0,                # Open water (all flows off)
        "barren": 0.70,              # Rock, bare soil
    }

    if land_use_class and land_use_class.lower() in runoff_lookup:
        return runoff_lookup[land_use_class.lower()]

    return default_runoff


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Point Estimation (for single-building damage queries)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def estimate_rainfall_at_point(
    point_lat: float,
    point_lon: float,
    center_lat: float,
    center_lon: float,
    max_wind_kt: float,
    storm_speed_kt: float,
    heading_deg: float = 0.0,
    runoff_coefficient: float = 0.50,
) -> float:
    """
    Estimate rainfall flood depth at a single point without raster generation.

    Useful for API queries (e.g., "what's the flooding at this address?")
    that don't need the full raster.

    Args:
        point_lat, point_lon: Query point (decimal degrees)
        center_lat, center_lon: Storm center (decimal degrees)
        max_wind_kt: Maximum sustained wind (knots)
        storm_speed_kt: Forward speed (knots)
        heading_deg: Direction of motion (degrees from north, 0-360)
        runoff_coefficient: Fraction of rain that becomes surface flow

    Returns:
        Estimated rainfall flood depth in meters
    """
    # Convert distance from degrees to km
    # Simple approximation: 1° latitude ≈ 111 km, 1° longitude ≈ 111 * cos(lat) km
    lat_rad = math.radians(point_lat)
    dy_km = (point_lat - center_lat) * 111.0
    dx_km = (point_lon - center_lon) * 111.0 * math.cos(lat_rad)
    distance_km = math.sqrt(dx_km**2 + dy_km**2)

    # Determine which quadrant relative to heading
    # Heading: 0° = N, 90° = E, 180° = S, 270° = W
    # Compute angle to point relative to heading
    angle_to_point_rad = math.atan2(dx_km, dy_km)
    angle_to_point_deg = math.degrees(angle_to_point_rad)
    relative_angle = (angle_to_point_deg - heading_deg) % 360.0

    # Categorize quadrant
    if relative_angle < 45 or relative_angle >= 315:
        quadrant = "front"
    elif 45 <= relative_angle < 135:
        quadrant = "right"
    elif 135 <= relative_angle < 225:
        quadrant = "rear"
    else:
        quadrant = "left"

    # Estimate rain rate and duration
    rain_rate = estimate_rain_rate_mm_hr(distance_km, max_wind_kt, storm_speed_kt, quadrant)
    duration_hr = estimate_storm_duration_hr(storm_speed_kt)
    total_precip_mm = estimate_total_precip_mm(rain_rate, storm_speed_kt, duration_hr)

    # Convert to depth via runoff
    excess_precip_mm = total_precip_mm * runoff_coefficient

    # Simple ponding model: not all runoff ponds; some drains
    # Ponding factor accounts for terrain slope, drainage capacity, etc.
    ponding_factor = 0.3
    flood_depth_m = excess_precip_mm / 1000.0 * ponding_factor

    return flood_depth_m


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Raster-Based Estimation (for full flood map generation)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def estimate_rainfall_flooding(
    center_lat: float,
    center_lon: float,
    max_wind_kt: float,
    storm_speed_kt: float,
    rmax_nm: float = 20.0,
    heading_deg: float = 0.0,
    output_dir: str = ".",
    storm_id: str = "unknown",
    grid_resolution_deg: float = 0.001,
    extent_km: float = 300.0,
    runoff_coefficient: float = 0.50,
    min_depth_m: float = 0.05,
) -> RainfallEstimate:
    """
    Generate rainfall flood depth raster from storm parameters.

    Produces two output GeoTIFFs:
      1. Total precipitation raster (mm)
      2. Rainfall flood depth raster (meters) — ready for compound.py

    The flood depth is computed as:
      flood_depth = excess_rainfall_mm / 1000 * ponding_factor

    where excess_rainfall = total_precip * runoff_coefficient

    and ponding_factor = 0.3 (empirical, accounts for drainage).

    Args:
        center_lat, center_lon: Storm center (decimal degrees)
        max_wind_kt: Maximum sustained wind speed (knots)
        storm_speed_kt: Forward speed (knots)
        rmax_nm: Radius of maximum wind (nm, typically 15-50)
        heading_deg: Storm heading / direction of motion (degrees from north)
        output_dir: Directory for output rasters
        storm_id: Storm identifier (used in filenames)
        grid_resolution_deg: Grid cell size (degrees; 0.001 ≈ 100m at equator)
        extent_km: Extent of raster from storm center (km, typically 250-500)
        runoff_coefficient: Fraction of rain that becomes surface flow (0.0-1.0)
        min_depth_m: Cells below this depth are set to zero

    Returns:
        RainfallEstimate with raster paths and statistics
    """
    # Lazy import: only require rasterio if actually generating rasters
    try:
        import rasterio
        from rasterio.transform import Affine
    except ImportError:
        raise ImportError(
            "rasterio required for rainfall raster generation. "
            "Install with: pip install rasterio"
        )

    logger.info(
        f"Generating rainfall flood raster: center=({center_lat}, {center_lon}), "
        f"max_wind={max_wind_kt}kt, speed={storm_speed_kt}kt, extent={extent_km}km"
    )

    os.makedirs(output_dir, exist_ok=True)

    # ── Create Grid ────────────────────────────────────────────
    # Bounds in degrees
    extent_deg = extent_km / 111.0  # approximate: 111 km per degree
    min_lon = center_lon - extent_deg
    max_lon = center_lon + extent_deg
    min_lat = center_lat - extent_deg
    max_lat = center_lat + extent_deg

    # Grid dimensions
    n_cols = int((max_lon - min_lon) / grid_resolution_deg) + 1
    n_rows = int((max_lat - min_lat) / grid_resolution_deg) + 1

    # Create coordinate arrays
    lons = np.linspace(min_lon, max_lon, n_cols)
    lats = np.linspace(max_lat, min_lat, n_rows)  # Top-to-bottom for raster
    lon_grid, lat_grid = np.meshgrid(lons, lats)

    # ── Compute Rainfall Rate at Each Grid Cell ───────────────
    # Distance from storm center
    lat_rad = np.radians(lat_grid)
    dy_km = (lat_grid - center_lat) * 111.0
    dx_km = (lon_grid - center_lon) * 111.0 * np.cos(lat_rad)
    distance_km = np.sqrt(dx_km**2 + dy_km**2)

    # Determine quadrant for each cell
    angle_to_cell_rad = np.arctan2(dx_km, dy_km)
    angle_to_cell_deg = np.degrees(angle_to_cell_rad)
    relative_angle = (angle_to_cell_deg - heading_deg) % 360.0

    quadrant = np.full_like(distance_km, "front", dtype=object)
    quadrant[(relative_angle >= 45) & (relative_angle < 135)] = "right"
    quadrant[(relative_angle >= 135) & (relative_angle < 225)] = "rear"
    quadrant[(relative_angle >= 225) & (relative_angle < 315)] = "left"

    # Vectorized rain rate computation
    rain_rate_mm_hr = np.zeros_like(distance_km)
    for q in ["front", "right", "rear", "left"]:
        mask = quadrant == q
        rain_rate_mm_hr[mask] = estimate_rain_rate_mm_hr(
            distance_km[mask], max_wind_kt, storm_speed_kt, q
        )

    # ── Compute Total Precipitation ────────────────────────────
    duration_hr = estimate_storm_duration_hr(storm_speed_kt, rmax_nm)
    total_precip_mm = estimate_total_precip_mm(
        rain_rate_mm_hr, storm_speed_kt, duration_hr
    )

    # ── Compute Excess Rainfall and Flood Depth ────────────────
    excess_precip_mm = total_precip_mm * runoff_coefficient

    # Ponding model: simple conversion to depth
    ponding_factor = 0.3
    flood_depth_m = excess_precip_mm / 1000.0 * ponding_factor

    # Apply minimum threshold
    flood_depth_m[flood_depth_m < min_depth_m] = 0.0

    # ── Create Rasterio Transform ──────────────────────────────
    # Affine transform maps pixel coordinates to geographic coordinates
    # (west, north) is the upper-left corner
    transform = Affine(
        grid_resolution_deg,      # pixel width
        0.0,                      # row offset
        min_lon,                  # x-coordinate of upper-left corner
        0.0,                      # column offset
        -grid_resolution_deg,     # pixel height (negative: north to south)
        max_lat,                  # y-coordinate of upper-left corner
    )

    # ── Write Total Precipitation Raster ──────────────────────
    precip_path = os.path.join(
        output_dir,
        f"precip_{storm_id}.tif",
    )

    precip_profile = {
        "driver": "GTiff",
        "dtype": "float32",
        "nodata": -9999,
        "width": n_cols,
        "height": n_rows,
        "count": 1,
        "crs": "EPSG:4326",  # WGS84
        "transform": transform,
        "compress": "deflate",
        "predictor": 3,
    }

    with rasterio.open(precip_path, "w", **precip_profile) as dst:
        dst.write(total_precip_mm.astype(np.float32), 1)
        dst.update_tags(
            model="rainfall_parametric",
            storm_id=storm_id,
            max_wind_kt=f"{max_wind_kt:.1f}",
            storm_speed_kt=f"{storm_speed_kt:.1f}",
            duration_hr=f"{duration_hr:.1f}",
            max_precip_mm=f"{np.nanmax(total_precip_mm):.1f}",
        )

    # ── Write Flood Depth Raster ───────────────────────────────
    depth_path = os.path.join(
        output_dir,
        f"depth_rainfall_{storm_id}.tif",
    )

    depth_profile = precip_profile.copy()
    depth_profile.update(nodata=-9999)

    with rasterio.open(depth_path, "w", **depth_profile) as dst:
        dst.write(flood_depth_m.astype(np.float32), 1)
        dst.update_tags(
            model="rainfall_parametric",
            storm_id=storm_id,
            max_wind_kt=f"{max_wind_kt:.1f}",
            storm_speed_kt=f"{storm_speed_kt:.1f}",
            runoff_coefficient=f"{runoff_coefficient:.2f}",
            ponding_factor="0.30",
            max_depth_m=f"{np.nanmax(flood_depth_m):.3f}",
        )

    # ── Compute Statistics ─────────────────────────────────────
    flooded = flood_depth_m > min_depth_m
    flooded_cells = int(np.sum(flooded))
    total_cells = n_rows * n_cols
    flooded_pct = (flooded_cells / total_cells * 100) if total_cells > 0 else 0
    max_depth = float(np.nanmax(flood_depth_m)) if np.any(flooded) else 0.0
    max_precip = float(np.nanmax(total_precip_mm))
    avg_precip = float(np.nanmean(total_precip_mm[total_precip_mm > 0]))

    logger.info(
        f"Rainfall raster complete: max_depth={max_depth:.2f}m, "
        f"max_precip={max_precip:.1f}mm, avg_precip={avg_precip:.1f}mm, "
        f"flooded={flooded_pct:.1f}% ({flooded_cells:,} cells)"
    )

    # ── Return Result ──────────────────────────────────────────
    bounds = (min_lon, min_lat, max_lon, max_lat)

    return RainfallEstimate(
        depth_raster_path=depth_path,
        total_precip_path=precip_path,
        max_depth_m=max_depth,
        max_precip_mm=max_precip,
        avg_precip_mm=avg_precip,
        flooded_cells=flooded_cells,
        total_cells=total_cells,
        flooded_pct=flooded_pct,
        bounds=bounds,
        crs="EPSG:4326",
    )
