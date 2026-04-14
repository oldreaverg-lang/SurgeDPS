"""
Raster I/O Utilities for SurgeDPS Flood Models

Provides read_raster() / write_raster() helpers that encapsulate the
common rasterio boilerplate used across bathtub, compound, rainfall,
and hand_model modules:

    open → read(1) → profile.copy() → profile.update(…) → write → update_tags

Usage
-----
    from flood_model.raster_utils import read_raster, write_raster

    info = read_raster("depth.tif")
    # info.data      → np.ndarray (band 1)
    # info.profile   → dict (rasterio profile, copy)
    # info.nodata    → float
    # info.bounds    → rasterio BoundingBox
    # info.crs       → str
    # info.transform → affine.Affine
    # info.shape     → (rows, cols)

    write_raster(
        "output.tif",
        data=depth_array,
        profile=info.profile,
        tags={"model": "bathtub", "max_depth_m": "1.23"},
    )
"""

from __future__ import annotations

from typing import Any, Dict, NamedTuple, Optional, Tuple

import numpy as np


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Data container
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class RasterInfo(NamedTuple):
    """All metadata returned by read_raster()."""

    data: np.ndarray            # Band-1 pixel values
    profile: dict               # rasterio profile (copy — safe to mutate)
    nodata: float               # Resolved nodata value
    bounds: Any                 # rasterio.coords.BoundingBox
    crs: str                    # CRS as string, e.g. "EPSG:4326"
    transform: Any              # affine.Affine
    shape: Tuple[int, int]      # (rows, cols)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Read helper
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def read_raster(path: str, nodata_fallback: float = -9999) -> RasterInfo:
    """
    Open a single-band GeoTIFF and return data + all metadata.

    Args:
        path: Path to the GeoTIFF file.
        nodata_fallback: Value to use when the file has no nodata tag.

    Returns:
        RasterInfo named tuple.
    """
    import rasterio

    with rasterio.open(path) as src:
        return RasterInfo(
            data=src.read(1),
            profile=src.profile.copy(),
            nodata=src.nodata if src.nodata is not None else nodata_fallback,
            bounds=src.bounds,
            crs=str(src.crs),
            transform=src.transform,
            shape=src.shape,
        )


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Write helper
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def write_raster(
    path: str,
    data: np.ndarray,
    profile: dict,
    tags: Optional[Dict[str, str]] = None,
    dtype: str = "float32",
    nodata: float = -9999,
    compress: str = "deflate",
    predictor: Optional[int] = 3,
    tiled: bool = False,
) -> None:
    """
    Write a single-band raster with standard compression settings.

    Copies *profile* before mutating it so the caller's dict is untouched.

    Args:
        path:      Output file path.
        data:      2-D array to write as band 1.
        profile:   Base rasterio profile (typically from read_raster or
                   built from scratch for synthetic rasters).
        tags:      Optional dict of string tags written via update_tags().
        dtype:     Output pixel type.  "float32" for depth rasters,
                   "uint8" for binary masks.
        nodata:    Nodata sentinel.  Use -9999 for float, 255 for uint8.
        compress:  Compression algorithm (default "deflate").
        predictor: TIFF predictor. 3 = floating-point (float32 depth),
                   2 = horizontal differencing (integer/uint8),
                   None = omit (useful for uint8 masks where 2 is marginal).
        tiled:     Write as tiled TIFF (useful for large rasters).
    """
    import rasterio

    out_profile = profile.copy()
    updates: dict = {
        "dtype": dtype,
        "nodata": nodata,
        "compress": compress,
        "count": 1,
    }
    if predictor is not None:
        updates["predictor"] = predictor
    if tiled:
        updates["tiled"] = True
    out_profile.update(**updates)

    with rasterio.open(path, "w", **out_profile) as dst:
        dst.write(np.asarray(data, dtype=dtype), 1)
        if tags:
            dst.update_tags(**tags)
