"""
Cloud Optimized GeoTIFF (COG) Builder

Converts flood depth rasters into Cloud Optimized GeoTIFFs
with internal tiling and overviews for efficient HTTP range
request delivery from S3.

COGs allow the browser (via georaster or similar) to fetch
only the pixels visible in the current map viewport.
"""

from __future__ import annotations

import logging
import os
import subprocess
from dataclasses import dataclass
from typing import List, Optional, Tuple

logger = logging.getLogger(__name__)


@dataclass
class COGResult:
    """Result of COG creation."""

    path: str
    size_bytes: int
    overview_levels: List[int]
    tile_size: int
    bounds: Tuple[float, float, float, float]
    crs: str
    s3_key: Optional[str] = None


def build_cog(
    input_path: str,
    output_path: str,
    tile_size: int = 256,
    overview_levels: Optional[List[int]] = None,
    resampling: str = "average",
    target_crs: str = "EPSG:3857",
    dtype: str = "float32",
) -> COGResult:
    """
    Convert a GeoTIFF to a Cloud Optimized GeoTIFF.

    Uses rasterio/GDAL to:
      1. Reproject to Web Mercator (for map tile alignment)
      2. Add internal tiling
      3. Build overviews (pyramid levels)
      4. Write as COG with DEFLATE compression

    Args:
        input_path: Source GeoTIFF
        output_path: Destination COG path
        tile_size: Internal tile size (256 or 512)
        overview_levels: Overview decimation levels
        resampling: Resampling method for overviews
        target_crs: Output CRS (EPSG:3857 for web serving)
        dtype: Output data type

    Returns:
        COGResult with metadata
    """
    import rasterio
    from rasterio.warp import calculate_default_transform, reproject, Resampling
    from rasterio.shutil import copy as rio_copy
    import numpy as np

    if overview_levels is None:
        overview_levels = [2, 4, 8, 16]

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    resamp = getattr(Resampling, resampling, Resampling.average)

    logger.info(f"Building COG: {input_path} -> {output_path}")

    # Step 1: Reproject to target CRS
    with rasterio.open(input_path) as src:
        dst_transform, dst_width, dst_height = calculate_default_transform(
            src.crs, target_crs, src.width, src.height, *src.bounds
        )

        # Respect the source's declared nodata — `src.nodata or -9999`
        # would collapse a legitimate nodata=0 to -9999.
        _src_nodata = src.nodata if src.nodata is not None else -9999

        profile = {
            "driver": "GTiff",
            "crs": target_crs,
            "transform": dst_transform,
            "width": dst_width,
            "height": dst_height,
            "count": 1,
            "dtype": dtype,
            "nodata": _src_nodata,
            "compress": "deflate",
            "predictor": 3 if "float" in dtype else 2,
            "tiled": True,
            "blockxsize": tile_size,
            "blockysize": tile_size,
        }

        # Intermediate reprojected file — pid+tid keyed so concurrent
        # build_cog calls on the same output don't stomp each other's
        # temp file mid-reproject.
        import threading as _th_cog
        temp_path = f"{output_path}.tmp.{os.getpid()}.{_th_cog.get_ident()}.tif"

        with rasterio.open(temp_path, "w", **profile) as dst:
            reproject(
                source=rasterio.band(src, 1),
                destination=rasterio.band(dst, 1),
                src_transform=src.transform,
                src_crs=src.crs,
                dst_transform=dst_transform,
                dst_crs=target_crs,
                resampling=resamp,
                src_nodata=_src_nodata,
                dst_nodata=_src_nodata,
            )

    # Step 2: Build overviews
    with rasterio.open(temp_path, "r+") as ds:
        ds.build_overviews(overview_levels, resamp)
        ds.update_tags(ns="rio_overview", resampling=resampling)

    # Step 3: Copy to COG layout
    with rasterio.open(temp_path) as src:
        rio_copy(
            src,
            output_path,
            driver="GTiff",
            copy_src_overviews=True,
            tiled=True,
            blockxsize=tile_size,
            blockysize=tile_size,
            compress="deflate",
            predictor=3 if "float" in dtype else 2,
        )

    # Clean up temp file
    if os.path.exists(temp_path):
        os.remove(temp_path)

    # Get output metadata
    with rasterio.open(output_path) as out:
        bounds = out.bounds
        crs = str(out.crs)

    size_bytes = os.path.getsize(output_path)

    logger.info(
        f"COG created: {output_path} "
        f"({size_bytes / 1024 / 1024:.1f} MB, "
        f"overviews={overview_levels})"
    )

    return COGResult(
        path=output_path,
        size_bytes=size_bytes,
        overview_levels=overview_levels,
        tile_size=tile_size,
        bounds=(bounds.left, bounds.bottom, bounds.right, bounds.top),
        crs=crs,
    )


def build_classified_cog(
    input_path: str,
    output_path: str,
    depth_breaks: Optional[List[float]] = None,
) -> COGResult:
    """
    Build a classified (uint8) COG from a float depth raster.

    Classifies continuous flood depth into discrete bins for
    smaller file sizes on the free tier. Each class maps to a
    color in the frontend.

    Default breaks (feet):
        0: No flooding
        1: 0-1 ft (Minor)
        2: 1-3 ft (Moderate)
        3: 3-6 ft (Major)
        4: 6-10 ft (Severe)
        5: 10+ ft  (Catastrophic)
    """
    import rasterio
    import numpy as np

    if depth_breaks is None:
        # Depth breaks in meters
        depth_breaks = [0, 0.3, 0.9, 1.8, 3.0]  # ~0, 1, 3, 6, 10 ft

    with rasterio.open(input_path) as src:
        depth = src.read(1)
        nodata = src.nodata if src.nodata is not None else -9999
        valid = depth != nodata

        classified = np.zeros_like(depth, dtype=np.uint8)
        for i, threshold in enumerate(depth_breaks):
            classified[valid & (depth > threshold)] = i + 1

        classified[~valid] = 255  # NoData

        profile = src.profile.copy()

    # Write classified raster
    temp_path = output_path + ".tmp.tif"
    profile.update(
        dtype="uint8",
        nodata=255,
        compress="deflate",
        predictor=2,
        tiled=True,
        blockxsize=256,
        blockysize=256,
    )

    with rasterio.open(temp_path, "w", **profile) as dst:
        dst.write(classified, 1)

    # Build as COG
    return build_cog(
        temp_path,
        output_path,
        dtype="uint8",
        overview_levels=[2, 4, 8, 16],
        resampling="nearest",
        target_crs="EPSG:3857",
    )
