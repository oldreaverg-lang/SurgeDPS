"""
Synthetic HEC-RAS Results Generator

Generates realistic-looking flood depth rasters for development
and testing when HEC-RAS binaries are not available.

Produces output that matches the format of the result_extractor
so downstream tile generation and publishing works identically.
"""

from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path
from typing import Optional

import numpy as np

logger = logging.getLogger(__name__)


def generate_synthetic_depth(
    dem_path: Optional[str],
    surge_path: Optional[str],
    output_dir: str,
    storm_id: str = "UNKNOWN",
    num_timesteps: int = 13,  # 0, 6, 12, ..., 72h
    crs_epsg: int = 5070,
) -> str:
    """
    Generate synthetic flood depth rasters that mimic HEC-RAS 2D output.

    Combines bathtub surge with a hydraulic decay factor and adds
    channel-following flood patterns for realistic appearance.

    Args:
        dem_path: Path to terrain DEM GeoTIFF
        surge_path: Path to P-Surge GeoTIFF (max surge)
        output_dir: Directory for output rasters
        storm_id: Storm identifier
        num_timesteps: Number of output timesteps
        crs_epsg: Output CRS

    Returns:
        Path to the output directory containing GeoTIFFs
    """
    import rasterio
    from rasterio.crs import CRS
    from rasterio.transform import from_bounds

    os.makedirs(output_dir, exist_ok=True)

    # Read DEM
    if dem_path and os.path.exists(dem_path):
        with rasterio.open(dem_path) as src:
            dem = src.read(1).astype(np.float32)
            transform = src.transform
            crs = src.crs
            height, width = dem.shape
    else:
        # Synthetic terrain: coastal gradient 0-15m over 500x500 grid
        height, width = 500, 500
        dem = np.zeros((height, width), dtype=np.float32)
        for row in range(height):
            dem[row, :] = (row / height) * 15.0  # 0m at coast, 15m inland
        # Add some noise for realism
        dem += np.random.normal(0, 0.3, dem.shape).astype(np.float32)
        dem = np.maximum(dem, -2.0)
        transform = from_bounds(0, 0, 15000, 15000, width, height)
        crs = CRS.from_epsg(crs_epsg)

    # Read surge
    max_surge = 3.5  # Default: 3.5m (Cat 2-3 storm)
    if surge_path and os.path.exists(surge_path):
        try:
            with rasterio.open(surge_path) as src:
                surge_data = src.read(1)
                valid = surge_data[surge_data > -9999]
                if len(valid) > 0:
                    max_surge = float(np.percentile(valid, 95))
        except Exception:
            pass

    logger.info(f"Generating synthetic HEC-RAS results: max_surge={max_surge}m, grid={height}x{width}")

    # Generate max depth envelope (enhanced bathtub with hydraulic effects)
    # Add a channel-following pattern for realism
    channel_mask = _generate_channel_network(height, width)
    roughness_factor = 1.0 + 0.3 * channel_mask  # Deeper in channels

    # Bathtub base with hydraulic attenuation
    surge_surface = max_surge * np.ones_like(dem)
    # Surge decays exponentially inland from coast (row 0 = coast)
    for row in range(height):
        inland_km = (row / height) * 15.0  # 15km domain
        attenuation = np.exp(-0.15 * inland_km)  # ~22% per km
        surge_surface[row, :] *= attenuation

    # Flood depth = surge surface - ground elevation (where positive)
    depth_envelope = np.maximum(0, surge_surface - dem) * roughness_factor
    depth_envelope = depth_envelope.astype(np.float32)

    # Write max depth envelope
    max_path = os.path.join(output_dir, "hecras_max_depth.tif")
    _write_tif(max_path, depth_envelope, transform, crs)

    # Generate timestep snapshots
    for i in range(num_timesteps):
        t_hours = i * 6
        # Temporal scaling: peak at T+24h
        if t_hours <= 24:
            t_frac = (t_hours / 24.0) ** 1.5
        else:
            t_frac = np.exp(-0.04 * (t_hours - 24))

        ts_depth = (depth_envelope * t_frac).astype(np.float32)
        ts_path = os.path.join(output_dir, f"hecras_depth_t{i:03d}.tif")
        _write_tif(ts_path, ts_depth, transform, crs)

    logger.info(
        f"Generated {num_timesteps + 1} rasters in {output_dir}, "
        f"max depth = {float(np.max(depth_envelope)):.2f}m"
    )
    return output_dir


def _generate_channel_network(height: int, width: int) -> np.ndarray:
    """
    Create a synthetic channel network mask (0-1) for realistic
    flood patterns. Channels are meandering paths from top to bottom.
    """
    mask = np.zeros((height, width), dtype=np.float32)

    # Main channel down the center with meandering
    center_x = width // 2
    amplitude = width * 0.15
    frequency = 3.0  # Number of meander wavelengths

    for row in range(height):
        frac = row / height
        x = center_x + int(amplitude * np.sin(2 * np.pi * frequency * frac))
        # Channel width decreases upstream
        chan_width = max(3, int(15 * (1 - 0.5 * frac)))
        x_start = max(0, x - chan_width // 2)
        x_end = min(width, x + chan_width // 2)
        mask[row, x_start:x_end] = 1.0

    # Add a tributary
    for row in range(height // 3, 2 * height // 3):
        frac = (row - height // 3) / (height // 3)
        x = int(width * 0.2 + frac * (center_x - width * 0.2))
        w = max(2, int(8 * (1 - frac)))
        x_start = max(0, x - w // 2)
        x_end = min(width, x + w // 2)
        mask[row, x_start:x_end] = 0.7

    # Gaussian blur for smooth edges
    try:
        from scipy.ndimage import gaussian_filter
        mask = gaussian_filter(mask, sigma=3)
    except ImportError:
        pass

    return mask


def _write_tif(
    path: str,
    data: np.ndarray,
    transform,
    crs,
) -> None:
    """Write a GeoTIFF with model metadata."""
    import rasterio

    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=data.shape[0],
        width=data.shape[1],
        count=1,
        dtype=data.dtype,
        crs=crs,
        transform=transform,
        compress="deflate",
        tiled=True,
        blockxsize=256,
        blockysize=256,
    ) as dst:
        dst.write(data, 1)
        dst.update_tags(
            MODEL="SurgeDPS Synthetic HEC-RAS",
            VARIABLE="flood_depth_m",
        )


# ── CLI entry point ──────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Generate synthetic HEC-RAS flood depth results"
    )
    parser.add_argument("--project-dir", required=True)
    parser.add_argument("--dem-file", default=None)
    parser.add_argument("--surge-file", default=None)
    parser.add_argument("--output-dir", default=None)
    parser.add_argument("--storm-id", default="SYNTHETIC")
    parser.add_argument("--timesteps", type=int, default=13)

    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)

    output_dir = args.output_dir or os.path.join(args.project_dir, "output")
    generate_synthetic_depth(
        dem_path=args.dem_file,
        surge_path=args.surge_file,
        output_dir=output_dir,
        storm_id=args.storm_id,
        num_timesteps=args.timesteps,
    )


if __name__ == "__main__":
    main()
