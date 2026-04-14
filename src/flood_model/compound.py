"""
Compound Flooding Model

Merges storm surge and rainfall/riverine flood depth rasters
into a single compound flood depth raster.

In reality, surge and rainfall flooding interact nonlinearly:
  - Surge blocks drainage outlets, backing up rainfall runoff
  - Rainfall adds volume on top of surge
  - Combined flood depths can exceed the simple max of either

Tier 2 Approximation (this module):
  - In non-overlap zones: max(surge, rainfall)
  - In overlap zones: surge + (rainfall * interaction_factor)
  - interaction_factor accounts for the additive effect

Tier 3 (HEC-RAS): Solves the full 2D shallow water equations
with surge as a boundary condition and rainfall as a distributed
source, which captures the interaction physics directly.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Optional, Tuple

import numpy as np

from flood_model.raster_utils import write_raster

logger = logging.getLogger(__name__)


@dataclass
class CompoundResult:
    """Output of the compound flood merge."""

    compound_depth_path: str     # Combined flood depth raster
    overlap_mask_path: str       # Binary raster: 1 where both surge + rain
    max_depth_m: float
    max_surge_depth_m: float
    max_rain_depth_m: float
    flooded_cells: int
    overlap_cells: int           # Cells flooded by BOTH sources
    total_cells: int
    flooded_pct: float
    overlap_pct: float           # % of flooded area that is overlap
    bounds: Tuple[float, float, float, float]
    crs: str
    s3_key: Optional[str] = None


def merge_compound_flood(
    surge_depth_path: str,
    rainfall_depth_path: str,
    output_dir: str,
    storm_id: str = "unknown",
    advisory_num: str = "000",
    timestep: int = 0,
    interaction_factor: float = 0.5,
    min_depth_m: float = 0.05,
) -> CompoundResult:
    """
    Merge surge and rainfall flood depth rasters into compound flooding.

    Overlap logic:
      - Where only surge: compound = surge_depth
      - Where only rainfall: compound = rainfall_depth
      - Where both: compound = surge + rainfall * interaction_factor
        (accounts for surge blocking drainage outflows)

    Args:
        surge_depth_path: Bathtub/surge flood depth raster
        rainfall_depth_path: HAND/rainfall flood depth raster
        output_dir: Output directory
        storm_id: For naming
        advisory_num: For naming
        timestep: Forecast hour
        interaction_factor: How much rainfall adds to surge in overlap
                           (0.0 = max only, 1.0 = fully additive)
        min_depth_m: Minimum depth threshold

    Returns:
        CompoundResult with compound depth and overlap mask
    """
    import rasterio
    from rasterio.warp import reproject, Resampling

    logger.info(
        f"Merging compound flood: surge={surge_depth_path}, "
        f"rainfall={rainfall_depth_path}, interaction={interaction_factor}"
    )

    os.makedirs(output_dir, exist_ok=True)

    # ── Read Surge Raster ──────────────────────────────────────
    with rasterio.open(surge_depth_path) as surge_src:
        surge = surge_src.read(1)
        # `src.nodata or -9999` collapses a legitimate nodata=0 — which
        # some SLOSH/P-Surge products use for "dry" — to -9999, treating
        # dry cells as real data. Use an explicit None check.
        surge_nodata = surge_src.nodata if surge_src.nodata is not None else -9999
        profile = surge_src.profile.copy()
        bounds = surge_src.bounds
        crs = str(surge_src.crs)

    # ── Read Rainfall Raster (resample to match surge grid) ────
    with rasterio.open(rainfall_depth_path) as rain_src:
        # Affine uses exact tuple equality, so two transforms that differ
        # only by floating-point epsilon (common when one was derived via
        # reproject) trigger a spurious resample. Use almost_equals to
        # tolerate 1e-6 degree jitter (~0.1 m) and avoid the extra work.
        _transforms_match = (
            rain_src.transform.almost_equals(surge_src.transform, precision=1e-6)
            if hasattr(rain_src.transform, "almost_equals")
            else rain_src.transform == surge_src.transform
        )
        if (
            rain_src.crs != surge_src.crs
            or rain_src.shape != surge.shape
            or not _transforms_match
        ):
            # Resample rainfall to match surge grid.
            # np.empty_like returns an uninitialized buffer; any destination
            # pixel that reproject doesn't touch (e.g. outside the rainfall
            # raster's footprint) would retain garbage floats and leak into
            # the downstream validity mask. Pre-fill with the dst_nodata
            # sentinel so uncovered cells are correctly classified.
            rain = np.full_like(surge, -9999, dtype=surge.dtype)
            with rasterio.open(surge_depth_path) as surge_ref:
                reproject(
                    source=rasterio.band(rain_src, 1),
                    destination=rain,
                    src_transform=rain_src.transform,
                    src_crs=rain_src.crs,
                    dst_transform=surge_ref.transform,
                    dst_crs=surge_ref.crs,
                    resampling=Resampling.bilinear,
                    src_nodata=rain_src.nodata,
                    dst_nodata=-9999,
                )
            rain_nodata = -9999
        else:
            rain = rain_src.read(1)
            rain_nodata = rain_src.nodata if rain_src.nodata is not None else -9999

    # ── Compound Flood Calculation ─────────────────────────────
    # Valid data mask
    surge_valid = surge != surge_nodata
    rain_valid = rain != rain_nodata
    valid = surge_valid | rain_valid

    # Clean up: set nodata to zero for calculation
    surge_clean = np.where(surge_valid, surge, 0).astype(np.float32)
    rain_clean = np.where(rain_valid, rain, 0).astype(np.float32)

    # Apply minimum threshold
    surge_clean[surge_clean < min_depth_m] = 0
    rain_clean[rain_clean < min_depth_m] = 0

    # Identify zones
    surge_only = (surge_clean > min_depth_m) & (rain_clean <= min_depth_m)
    rain_only = (rain_clean > min_depth_m) & (surge_clean <= min_depth_m)
    overlap = (surge_clean > min_depth_m) & (rain_clean > min_depth_m)

    # Compute compound depth
    compound = np.full_like(surge, -9999, dtype=np.float32)

    # Surge-only zones: just surge depth
    compound[surge_only] = surge_clean[surge_only]

    # Rain-only zones: just rainfall depth
    compound[rain_only] = rain_clean[rain_only]

    # Overlap zones: surge + fraction of rainfall
    # This models the effect of surge blocking drainage outlets
    compound[overlap] = (
        surge_clean[overlap]
        + rain_clean[overlap] * interaction_factor
    )

    # Unflooded valid cells
    unflooded = valid & ~surge_only & ~rain_only & ~overlap
    compound[unflooded] = 0

    # ── Overlap Mask Raster ────────────────────────────────────
    overlap_mask = np.zeros_like(surge, dtype=np.uint8)
    overlap_mask[overlap] = 1

    # ── Statistics ─────────────────────────────────────────────
    flooded = valid & (compound > min_depth_m)
    flooded_cells = int(np.sum(flooded))
    overlap_cells = int(np.sum(overlap))
    total_cells = int(np.sum(valid))
    flooded_pct = (
        (flooded_cells / total_cells * 100) if total_cells > 0 else 0
    )
    overlap_pct = (
        (overlap_cells / flooded_cells * 100) if flooded_cells > 0 else 0
    )
    max_depth = float(np.nanmax(compound[flooded])) if flooded_cells > 0 else 0.0
    max_surge = float(np.nanmax(surge_clean)) if np.any(surge_clean > 0) else 0.0
    max_rain = float(np.nanmax(rain_clean)) if np.any(rain_clean > 0) else 0.0

    logger.info(
        f"Compound result: max_depth={max_depth:.2f}ft "
        f"(surge={max_surge:.2f}ft, rain={max_rain:.2f}ft), "
        f"flooded={flooded_pct:.1f}%, "
        f"overlap={overlap_pct:.1f}% of flooded area ({overlap_cells:,} cells)"
    )

    # ── Write Compound Depth Raster ────────────────────────────
    ts_str = f"t{timestep:03d}" if timestep > 0 else "t000"

    compound_path = os.path.join(
        output_dir,
        f"depth_compound_{storm_id}_{advisory_num}_{ts_str}.tif",
    )

    write_raster(
        compound_path,
        compound,
        profile,
        tags={
            "model": "compound",
            "storm_id": storm_id,
            "advisory": advisory_num,
            "timestep": str(timestep),
            "max_depth_m": f"{max_depth:.3f}",
            "interaction_factor": f"{interaction_factor:.2f}",
            "overlap_pct": f"{overlap_pct:.2f}",
        },
    )

    # ── Write Overlap Mask ─────────────────────────────────────
    overlap_path = os.path.join(
        output_dir,
        f"overlap_{storm_id}_{advisory_num}_{ts_str}.tif",
    )

    write_raster(
        overlap_path,
        overlap_mask,
        profile,
        dtype="uint8",
        nodata=255,
        predictor=None,    # uint8 masks don't benefit from float predictor
    )

    return CompoundResult(
        compound_depth_path=compound_path,
        overlap_mask_path=overlap_path,
        max_depth_m=max_depth,
        max_surge_depth_m=max_surge,
        max_rain_depth_m=max_rain,
        flooded_cells=flooded_cells,
        overlap_cells=overlap_cells,
        total_cells=total_cells,
        flooded_pct=flooded_pct,
        overlap_pct=overlap_pct,
        bounds=(bounds.left, bounds.bottom, bounds.right, bounds.top),
        crs=crs,
    )
