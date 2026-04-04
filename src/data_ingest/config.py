"""
Configuration for the data ingestion pipeline.

Defines S3 paths for base datasets, NOAA endpoints,
and processing parameters.
"""

import os
from dataclasses import dataclass, field
from typing import List


@dataclass(frozen=True)
class IngestConfig:
    """Immutable configuration for data ingestion."""

    # ── S3 Base Data Locations ─────────────────────────────────────
    data_bucket: str = field(
        default_factory=lambda: os.getenv("DATA_BUCKET", "surgedps-data")
    )

    # Pre-processed COG DEMs organized by USGS quad tile
    dem_10m_prefix: str = "base/dem/10m/"
    dem_1m_prefix: str = "base/dem/1m/"

    # Pre-computed HAND rasters by HUC-8
    hand_prefix: str = "base/hand/"

    # Manning's roughness coefficient raster (derived from NLCD)
    mannings_key: str = "base/mannings_n.tif"

    # NHDPlus flowlines (FlatGeobuf)
    nhdplus_key: str = "base/nhdplus_flowlines.fgb"

    # Storm data output prefix
    storm_output_prefix: str = "storms/"

    # ── NOAA Data Sources ──────────────────────────────────────────
    # National Water Model on AWS Open Data
    nwm_bucket: str = "noaa-nwm-pds"
    nwm_short_range_prefix: str = "nwm.{date}/short_range/"
    nwm_medium_range_prefix: str = "nwm.{date}/medium_range_mem1/"

    # USGS 3DEP LiDAR on AWS Open Data
    usgs_lidar_bucket: str = "usgs-lidar-public"

    # P-Surge / SLOSH base URL
    psurge_base_url: str = "https://www.nhc.noaa.gov/gis/"

    # WPC QPF (Quantitative Precipitation Forecast) GRIB endpoint
    qpf_base_url: str = (
        "https://ftp.wpc.ncep.noaa.gov/2p5km_qpf/"
    )

    # USGS Water Services API
    usgs_water_api: str = "https://waterservices.usgs.gov/nwis/iv/"

    # ── Processing Parameters ──────────────────────────────────────
    # Buffer distance (km) around the storm cone for data fetch
    cone_buffer_km: float = float(os.getenv("CONE_BUFFER_KM", "50"))

    # Target CRS for all modeling (CONUS Albers Equal Area)
    model_crs: str = "EPSG:5070"

    # Target CRS for tile serving (Web Mercator)
    tile_crs: str = "EPSG:3857"

    # Processing tile size (km) for parallel subdivisions
    tile_size_km: float = float(os.getenv("TILE_SIZE_KM", "10"))

    # Overlap buffer (m) between processing tiles
    tile_overlap_m: float = 500.0

    # Forecast timesteps (hours)
    timesteps: List[int] = field(
        default_factory=lambda: [0, 6, 12, 18, 24, 30, 36, 42, 48, 54, 60, 66, 72]
    )

    # ── Local Scratch Space ────────────────────────────────────────
    scratch_dir: str = os.getenv("SCRATCH_DIR", "/tmp/surgedps")

    # ── Networking ─────────────────────────────────────────────────
    http_timeout: int = int(os.getenv("HTTP_TIMEOUT", "60"))
    user_agent: str = (
        "SurgeDPS-DataIngest/1.0 (flood-risk-research; contact@surgedps.com)"
    )

    dry_run: bool = os.getenv("DRY_RUN", "false").lower() == "true"
