"""
Bathtub Storm Surge Model (Tier 1)

The simplest and fastest flood model. Computes flood depth by
subtracting ground elevation from the surge water surface height:

    flood_depth = max(0, surge_height - elevation)

Strengths:
  - Extremely fast (seconds for a county-sized area)
  - No parameters to calibrate
  - Runs easily on Lambda

Limitations:
  - No flow connectivity: floods disconnected low-lying areas
  - No momentum or timing
  - Overpredicts inland flooding
  - No rainfall component

Use case: Free-tier surge maps, initial rapid assessment.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Optional, Tuple

import numpy as np

logger = logging.getLogger(__name__)


@dataclass
class BathtubResult:
    """Output of the bathtub model."""

    depth_path: str          # Path to flood depth GeoTIFF
    max_depth_m: float       # Maximum flood depth (meters)
    flooded_cells: int       # Number of cells with depth > 0
    total_cells: int         # Total cells in the raster
    flooded_pct: float       # Percentage of area flooded
    bounds: Tuple[float, float, float, float]
    crs: str
    resolution: float
    s3_key: Optional[str] = None


def run_bathtub_model(
    dem_path: str,
    surge_path: str,
    output_dir: str,
    storm_id: str = "unknown",
    advisory_num: str = "000",
    min_depth_m: float = 0.05,
) -> BathtubResult:
    """
    Run the bathtub surge model.

    Args:
        dem_path: Path to the clipped DEM GeoTIFF (elevation in meters)
        surge_path: Path to the surge height raster (meters above datum)
        output_dir: Directory for output flood depth raster
        storm_id: For naming the output
        advisory_num: For naming the output
        min_depth_m: Minimum depth to count as flooded (noise filter)

    Returns:
        BathtubResult with flood depth raster path and statistics
    """
    import rasterio
    from rasterio.warp import reproject, Resampling

    logger.info(f"Running bathtub model: DEM={dem_path}, Surge={surge_path}")

    os.makedirs(output_dir, exist_ok=True)

    # Read DEM
    with rasterio.open(dem_path) as dem_src:
        dem_data = dem_src.read(1)
        dem_profile = dem_src.profile.copy()
        dem_nodata = dem_src.nodata or -9999
        dem_transform = dem_src.transform
        dem_crs = dem_src.crs
        dem_bounds = dem_src.bounds

    # Read surge raster — may need reprojection/resampling to match DEM
    with rasterio.open(surge_path) as surge_src:
        if (
            surge_src.crs != dem_crs
            or surge_src.transform != dem_transform
            or surge_src.shape != dem_data.shape
        ):
            # Reproject/resample surge to match DEM grid
            logger.info("Reprojecting surge raster to match DEM grid")
            surge_data = np.empty_like(dem_data)
            reproject(
                source=rasterio.band(surge_src, 1),
                destination=surge_data,
                src_transform=surge_src.transform,
                src_crs=surge_src.crs,
                dst_transform=dem_transform,
                dst_crs=dem_crs,
                resampling=Resampling.bilinear,
                dst_nodata=-9999,
            )
        else:
            surge_data = surge_src.read(1)
        surge_nodata = surge_src.nodata or -9999

    # ── Core Bathtub Calculation ───────────────────────────────
    # flood_depth = surge_height - elevation (where positive)

    # Mask out NoData cells
    valid_mask = (dem_data != dem_nodata) & (surge_data != surge_nodata)

    flood_depth = np.full_like(dem_data, -9999, dtype=np.float32)
    flood_depth[valid_mask] = surge_data[valid_mask] - dem_data[valid_mask]

    # Zero out negative depths (above surge level = not flooded)
    flood_depth[valid_mask & (flood_depth < min_depth_m)] = 0

    # ── Statistics ─────────────────────────────────────────────
    flooded_mask = valid_mask & (flood_depth > min_depth_m)
    flooded_cells = int(np.sum(flooded_mask))
    total_cells = int(np.sum(valid_mask))
    flooded_pct = (
        (flooded_cells / total_cells * 100) if total_cells > 0 else 0
    )
    max_depth = (
        float(np.nanmax(flood_depth[flooded_mask]))
        if flooded_cells > 0
        else 0.0
    )

    logger.info(
        f"Bathtub result: max_depth={max_depth:.2f}m, "
        f"flooded={flooded_pct:.1f}% ({flooded_cells:,} cells)"
    )

    # ── Write Output Raster ────────────────────────────────────
    output_path = os.path.join(
        output_dir, f"depth_surge_{storm_id}_{advisory_num}.tif"
    )

    out_profile = dem_profile.copy()
    out_profile.update(
        dtype="float32",
        nodata=-9999,
        compress="deflate",
        predictor=3,  # floating point predictor
    )

    with rasterio.open(output_path, "w", **out_profile) as dst:
        dst.write(flood_depth.astype(np.float32), 1)
        dst.update_tags(
            model="bathtub",
            storm_id=storm_id,
            advisory=advisory_num,
            max_depth_m=f"{max_depth:.3f}",
            flooded_pct=f"{flooded_pct:.2f}",
        )

    return BathtubResult(
        depth_path=output_path,
        max_depth_m=max_depth,
        flooded_cells=flooded_cells,
        total_cells=total_cells,
        flooded_pct=flooded_pct,
        bounds=(
            dem_bounds.left, dem_bounds.bottom,
            dem_bounds.right, dem_bounds.top,
        ),
        crs=str(dem_crs),
        resolution=dem_transform.a,
    )
