"""
NOAA Data Fetchers

Fetches dynamic storm data from NOAA services:
  - P-Surge / SLOSH storm surge forecasts
  - National Water Model (NWM) river discharge forecasts
  - WPC Quantitative Precipitation Forecasts (QPF)
  - USGS real-time stream gauges

All fetchers clip their output to the storm processing extent
to minimize data transfer and downstream compute.
"""

from __future__ import annotations

import io
import logging
import os
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

import numpy as np
import requests

from .config import IngestConfig

logger = logging.getLogger(__name__)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Data Classes
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


@dataclass
class SurgeData:
    """P-Surge / SLOSH storm surge data for the storm area."""

    path: str                # Local path to surge height raster
    source: str              # "psurge", "slosh_meow", or "synthetic"
    exceedance: str          # e.g. "10pct" (10% exceedance = 90% confidence)
    max_surge_m: float       # Maximum surge height in meters (NAVD88)
    bounds: Tuple[float, float, float, float]
    crs: str
    s3_key: Optional[str] = None


@dataclass
class NWMDischargeData:
    """National Water Model forecast discharge for stream reaches."""

    path: str                # Local path to discharge CSV/NetCDF
    reach_count: int         # Number of NHDPlus reaches with forecasts
    max_discharge_cms: float # Peak forecast discharge (m3/s)
    forecast_hours: int      # Forecast horizon
    s3_key: Optional[str] = None


@dataclass
class QPFData:
    """Quantitative Precipitation Forecast raster."""

    path: str                # Local path to accumulated rainfall raster
    total_precip_mm: float   # Maximum accumulated precipitation (mm)
    duration_hours: int      # Accumulation period
    bounds: Tuple[float, float, float, float]
    crs: str
    s3_key: Optional[str] = None


@dataclass
class GaugeObservation:
    """Single USGS stream gauge observation."""

    site_id: str
    site_name: str
    lat: float
    lon: float
    stage_ft: Optional[float]
    discharge_cfs: Optional[float]
    timestamp: str


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# P-Surge / SLOSH Fetcher
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class PSurgeFetcher:
    """
    Fetches probabilistic storm surge data from NHC.

    P-Surge provides surge height grids at various exceedance
    levels (10%, 20%, 50%). When P-Surge isn't available (early
    in the storm lifecycle), falls back to SLOSH MEOW/MOM lookup
    or generates a synthetic surge field.
    """

    def __init__(self, config: IngestConfig, s3_client=None):
        self.config = config
        self.s3 = s3_client
        self.session = requests.Session()
        self.session.headers["User-Agent"] = config.user_agent

    def fetch(
        self,
        storm_id: str,
        advisory_num: str,
        storm_geometry: dict,
        output_dir: str,
        exceedance: str = "10pct",
    ) -> SurgeData:
        """
        Fetch P-Surge data, clipped to the storm area.

        Falls back to synthetic data if P-Surge is unavailable.
        """
        os.makedirs(output_dir, exist_ok=True)

        # Try P-Surge first
        psurge_data = self._try_psurge(
            storm_id, advisory_num, storm_geometry, output_dir, exceedance
        )
        if psurge_data:
            return psurge_data

        # Fallback: generate synthetic surge from storm parameters
        logger.info("P-Surge unavailable — generating synthetic surge field")
        return self._generate_synthetic_surge(
            storm_id, advisory_num, storm_geometry, output_dir
        )

    def _try_psurge(
        self,
        storm_id: str,
        advisory_num: str,
        storm_geometry: dict,
        output_dir: str,
        exceedance: str,
    ) -> Optional[SurgeData]:
        """Attempt to download P-Surge grid from NHC."""
        # P-Surge URL pattern (may vary by storm)
        url = (
            f"{self.config.psurge_base_url}"
            f"forecast/archive/{storm_id.lower()}_"
            f"{exceedance}_{advisory_num}.zip"
        )

        try:
            response = self.session.get(
                url, timeout=self.config.http_timeout
            )
            if response.status_code == 404:
                logger.info(f"P-Surge not found at {url}")
                return None
            response.raise_for_status()

            # Extract and process the surge grid
            import zipfile

            zip_path = os.path.join(output_dir, "psurge_raw.zip")
            with open(zip_path, "wb") as f:
                f.write(response.content)

            extract_dir = os.path.join(output_dir, "psurge_raw")
            with zipfile.ZipFile(zip_path) as zf:
                zf.extractall(extract_dir)

            # Find the raster file (.tif or .asc)
            surge_raster = self._find_raster(extract_dir)
            if not surge_raster:
                logger.warning("No raster found in P-Surge archive")
                return None

            # Clip to storm extent and convert to standard format
            return self._clip_surge_raster(
                surge_raster,
                storm_geometry,
                output_dir,
                storm_id,
                advisory_num,
                exceedance,
                source="psurge",
            )

        except requests.RequestException as e:
            logger.info(f"P-Surge fetch failed: {e}")
            return None

    def _generate_synthetic_surge(
        self,
        storm_id: str,
        advisory_num: str,
        storm_geometry: dict,
        output_dir: str,
    ) -> SurgeData:
        """
        Generate a synthetic surge height raster for development/fallback.

        Models surge as a distance-decaying field from the coast,
        with peak surge at the landfall point.
        """
        import rasterio
        from rasterio.transform import from_bounds
        from shapely.geometry import shape

        storm_shape = shape(storm_geometry)
        bounds = storm_shape.bounds  # (minx, miny, maxx, maxy)

        # Resolution: ~500m
        res = 0.005
        width = min(int((bounds[2] - bounds[0]) / res), 1500)
        height = min(int((bounds[3] - bounds[1]) / res), 1500)

        # Generate surge field: peak at center-south, decaying outward
        cx = (bounds[0] + bounds[2]) / 2
        cy = bounds[1] + (bounds[3] - bounds[1]) * 0.3  # Lower third

        x = np.linspace(bounds[0], bounds[2], width)
        y = np.linspace(bounds[1], bounds[3], height)
        xx, yy = np.meshgrid(x, y)

        # Distance from peak surge point (in degrees, ~111km per degree)
        dist = np.sqrt((xx - cx) ** 2 + (yy - cy) ** 2) * 111.0

        # Surge height: exponential decay from peak
        peak_surge_m = 4.5  # meters
        surge = peak_surge_m * np.exp(-dist / 80.0)

        # Zero out areas far from coast (assume coast is at west edge)
        inland_dist = (xx - bounds[0]) / (bounds[2] - bounds[0])
        surge *= np.clip(1.0 - inland_dist * 1.5, 0, 1)

        # Add noise
        rng = np.random.default_rng(123)
        surge += rng.normal(0, 0.2, surge.shape)
        surge = np.clip(surge, 0, None).astype(np.float32)

        output_path = os.path.join(
            output_dir, f"surge_{storm_id}_{advisory_num}.tif"
        )
        transform = from_bounds(
            bounds[0], bounds[1], bounds[2], bounds[3], width, height
        )

        with rasterio.open(
            output_path,
            "w",
            driver="GTiff",
            height=height,
            width=width,
            count=1,
            dtype="float32",
            crs="EPSG:4326",
            transform=transform,
            nodata=-9999,
            compress="deflate",
        ) as dst:
            dst.write(surge, 1)

        logger.info(
            f"Synthetic surge: peak={peak_surge_m}m, "
            f"shape={width}x{height}, path={output_path}"
        )

        return SurgeData(
            path=output_path,
            source="synthetic",
            exceedance="synthetic",
            max_surge_m=float(np.nanmax(surge)),
            bounds=bounds,
            crs="EPSG:4326",
        )

    def _clip_surge_raster(
        self,
        raster_path: str,
        storm_geometry: dict,
        output_dir: str,
        storm_id: str,
        advisory_num: str,
        exceedance: str,
        source: str,
    ) -> SurgeData:
        """Clip a surge raster to the storm extent."""
        import rasterio
        from rasterio.mask import mask as rasterio_mask
        from shapely.geometry import shape, mapping

        storm_shape = shape(storm_geometry)
        buffered = storm_shape.buffer(self.config.cone_buffer_km / 111.0)

        output_path = os.path.join(
            output_dir, f"surge_{storm_id}_{advisory_num}.tif"
        )

        with rasterio.open(raster_path) as src:
            out_image, out_transform = rasterio_mask(
                src, [mapping(buffered)], crop=True, nodata=-9999
            )
            profile = src.profile.copy()
            profile.update(
                transform=out_transform,
                width=out_image.shape[2],
                height=out_image.shape[1],
                nodata=-9999,
                compress="deflate",
            )
            with rasterio.open(output_path, "w", **profile) as dst:
                dst.write(out_image)

        max_surge = float(np.nanmax(out_image[out_image != -9999]))

        return SurgeData(
            path=output_path,
            source=source,
            exceedance=exceedance,
            max_surge_m=max_surge,
            bounds=tuple(buffered.bounds),
            crs=str(src.crs),
        )

    @staticmethod
    def _find_raster(directory: str) -> Optional[str]:
        """Find the first raster file in a directory."""
        for ext in (".tif", ".tiff", ".asc", ".nc"):
            for root, _, files in os.walk(directory):
                for f in files:
                    if f.lower().endswith(ext):
                        return os.path.join(root, f)
        return None


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# National Water Model Fetcher
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class NWMFetcher:
    """
    Fetches National Water Model discharge forecasts from AWS Open Data.

    The NWM provides short-range (18h) and medium-range (10-day)
    streamflow forecasts for every NHDPlus reach in the US.
    We extract forecasts only for reaches within the storm area.
    """

    def __init__(self, config: IngestConfig, s3_client=None):
        self.config = config
        self.s3 = s3_client

    def fetch(
        self,
        storm_geometry: dict,
        reach_ids: List[int],
        output_dir: str,
        forecast_date: Optional[str] = None,
    ) -> NWMDischargeData:
        """
        Fetch NWM discharge forecasts for specified reaches.

        Args:
            storm_geometry: Storm extent for spatial filtering
            reach_ids: NHDPlus COMID reach identifiers in the storm area
            output_dir: Local output directory
            forecast_date: Date string YYYYMMDD (default: today)

        Returns:
            NWMDischargeData with reach-level discharge forecasts
        """
        os.makedirs(output_dir, exist_ok=True)

        if not forecast_date:
            forecast_date = datetime.utcnow().strftime("%Y%m%d")

        # Try to read NWM from S3 Open Data
        discharge_data = self._fetch_from_s3(
            forecast_date, reach_ids, output_dir
        )

        if discharge_data is not None:
            return discharge_data

        # Fallback: generate synthetic discharge
        logger.info("NWM data unavailable — generating synthetic discharge")
        return self._generate_synthetic_discharge(
            reach_ids, output_dir
        )

    def _fetch_from_s3(
        self,
        forecast_date: str,
        reach_ids: List[int],
        output_dir: str,
    ) -> Optional[NWMDischargeData]:
        """
        Fetch NWM channel output from s3://noaa-nwm-pds.

        NWM NetCDF files contain discharge for all ~2.7M reaches.
        We read only the reaches in our storm area.
        """
        if not self.s3:
            return None

        try:
            import xarray as xr

            # Medium-range forecast, member 1, hour 1
            prefix = self.config.nwm_medium_range_prefix.format(
                date=forecast_date
            )
            key = f"{prefix}nwm.t00z.medium_range.channel_rt_1.f001.conus.nc"

            # Download the NetCDF
            local_path = os.path.join(output_dir, "nwm_channel.nc")
            self.s3.download_file(
                self.config.nwm_bucket, key, local_path
            )

            # Open and filter to our reaches
            ds = xr.open_dataset(local_path)
            reach_id_set = set(reach_ids)
            mask = ds.feature_id.isin(list(reach_id_set))
            filtered = ds.where(mask, drop=True)

            # Extract discharge values
            discharge = filtered["streamflow"].values
            max_q = float(np.nanmax(discharge)) if len(discharge) > 0 else 0

            # Save filtered data
            output_path = os.path.join(output_dir, "nwm_discharge.nc")
            filtered.to_netcdf(output_path)

            logger.info(
                f"NWM: {len(discharge)} reaches, peak Q={max_q:.1f} m3/s"
            )

            return NWMDischargeData(
                path=output_path,
                reach_count=len(discharge),
                max_discharge_cms=max_q,
                forecast_hours=240,  # 10-day
            )

        except Exception as e:
            logger.warning(f"NWM S3 fetch failed: {e}")
            return None

    def _generate_synthetic_discharge(
        self,
        reach_ids: List[int],
        output_dir: str,
    ) -> NWMDischargeData:
        """
        Generate synthetic discharge data for development.

        Creates a CSV with reach IDs and simulated discharge values
        that follow a storm hydrograph shape.
        """
        import csv

        output_path = os.path.join(output_dir, "nwm_discharge_synthetic.csv")

        rng = np.random.default_rng(42)
        max_q = 0.0

        with open(output_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                "reach_id", "hour", "discharge_cms", "stage_m"
            ])

            for rid in reach_ids[:500]:  # Limit for dev
                # Base flow + storm pulse
                base_q = rng.uniform(5, 50)
                peak_q = base_q * rng.uniform(3, 15)
                max_q = max(max_q, peak_q)

                for hour in range(0, 73, 6):
                    # Storm hydrograph: ramp up to peak at ~36h, then decay
                    t_norm = hour / 72.0
                    if t_norm < 0.5:
                        factor = t_norm * 2
                    else:
                        factor = 1.0 - (t_norm - 0.5) * 1.5
                    factor = max(0, min(1, factor))

                    q = base_q + (peak_q - base_q) * factor
                    stage = 0.3 * (q ** 0.4)  # Synthetic rating curve

                    writer.writerow([rid, hour, f"{q:.2f}", f"{stage:.2f}"])

        n = min(len(reach_ids), 500)
        logger.info(
            f"Synthetic NWM: {n} reaches, peak Q={max_q:.1f} m3/s"
        )

        return NWMDischargeData(
            path=output_path,
            reach_count=n,
            max_discharge_cms=max_q,
            forecast_hours=72,
        )


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# QPF (Rainfall) Fetcher
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class QPFFetcher:
    """
    Fetches WPC Quantitative Precipitation Forecasts.

    QPF grids provide forecast rainfall accumulations at 2.5km
    resolution. We download the 72-hour accumulation period
    and clip to the storm extent.
    """

    def __init__(self, config: IngestConfig, s3_client=None):
        self.config = config
        self.s3 = s3_client
        self.session = requests.Session()
        self.session.headers["User-Agent"] = config.user_agent

    def fetch(
        self,
        storm_geometry: dict,
        output_dir: str,
        duration_hours: int = 72,
    ) -> QPFData:
        """
        Fetch and clip QPF data for the storm area.

        Falls back to synthetic rainfall if WPC data is unavailable.
        """
        os.makedirs(output_dir, exist_ok=True)

        # Try WPC QPF
        qpf = self._try_wpc(storm_geometry, output_dir, duration_hours)
        if qpf:
            return qpf

        # Fallback: synthetic rainfall
        logger.info("QPF unavailable — generating synthetic rainfall")
        return self._generate_synthetic_rainfall(
            storm_geometry, output_dir, duration_hours
        )

    def _try_wpc(
        self,
        storm_geometry: dict,
        output_dir: str,
        duration_hours: int,
    ) -> Optional[QPFData]:
        """Build a real N-hour QPF by summing WPC's 24-hour QPF tiles.

        WPC publishes 24h accumulations at f024/f048/f072/... relative to
        each 00/06/12/18Z cycle. We walk back through recent cycles until
        we find one that has all the forecast-hour files we need
        (fh ∈ {24, 48, 72, …}), download them, clip each to the storm
        bbox, and sum into one GeoTIFF. No native `p72m_latest.grb` file
        exists on ftp.wpc.ncep.noaa.gov — this is how they assemble it.

        Returns None if no complete cycle is found in the search window
        or if any download/parse fails. Caller falls back to synthetic.
        """
        try:
            # Which 24h forecast hours do we need to sum?
            # 72h → [24, 48, 72]; 48h → [24, 48]; 24h → [24].
            fhrs = [(i + 1) * 24 for i in range(max(1, duration_hours // 24))]
            if not fhrs:
                return None

            # Try recent cycles until one has every fhr we need. WPC posts
            # new cycles ~50 min after cycle time; probe up to ~48h back.
            now = datetime.now(timezone.utc).replace(tzinfo=None)
            cycle = now.replace(minute=0, second=0, microsecond=0)
            # snap to nearest past 00/06/12/18 cycle
            cycle = cycle.replace(hour=(cycle.hour // 6) * 6)

            import threading as _th
            tid = _th.get_ident()

            def _head_ok(u: str) -> bool:
                try:
                    return self.session.head(u, timeout=10, allow_redirects=True).status_code == 200
                except Exception:
                    return False

            grib_paths: list[str] = []
            picked_cycle: Optional[datetime] = None
            for step in range(8):  # 8 cycles × 6 h = 48 h lookback
                c = cycle - timedelta(hours=6 * step)
                cycle_tag = c.strftime("%Y%m%d%H")
                candidates = [
                    (fh, f"{self.config.qpf_base_url}p24m_{cycle_tag}f{fh:03d}.grb")
                    for fh in fhrs
                ]
                # Probe in parallel — 3 sequential HEADs × 8 cycles × 10s
                # timeout = 240s worst case. Parallel cuts this to ~80s.
                with ThreadPoolExecutor(max_workers=len(candidates)) as ex:
                    ok = list(ex.map(_head_ok, [u for _, u in candidates]))
                if not all(ok):
                    continue

                picked_cycle = c
                grib_paths = []
                for fh, url in candidates:
                    resp = self.session.get(
                        url, timeout=self.config.http_timeout
                    )
                    # Sanity check: WPC 24h QPF files are 150–500 KB.
                    # Anything under 10 KB is almost certainly an error page.
                    if resp.status_code != 200 or not resp.content or len(resp.content) < 10_000:
                        logger.info(
                            "WPC QPF: bad response for f%03d (status=%d, len=%d)",
                            fh, resp.status_code, len(resp.content) if resp.content else 0,
                        )
                        grib_paths = []
                        break
                    gp = os.path.join(output_dir, f"qpf_{cycle_tag}_f{fh:03d}.grb")
                    tmp = f"{gp}.tmp.{os.getpid()}.{tid}"
                    with open(tmp, "wb") as f:
                        f.write(resp.content)
                    os.replace(tmp, gp)
                    grib_paths.append(gp)
                if grib_paths:
                    break

            if not grib_paths or picked_cycle is None:
                logger.info(
                    "WPC QPF: no complete cycle with fhrs %s in last 48 h — "
                    "falling back to synthetic", fhrs,
                )
                return None

            logger.info(
                "WPC QPF: summing %d×24h tiles from cycle %sZ for %dh total",
                len(grib_paths), picked_cycle.strftime("%Y-%m-%d %H"), duration_hours,
            )
            return self._sum_and_clip_grib(
                grib_paths, storm_geometry, output_dir, duration_hours,
            )

        except Exception as e:
            logger.info(f"WPC QPF fetch failed: {e}")
            return None

    def _sum_and_clip_grib(
        self,
        grib_paths: List[str],
        storm_geometry: dict,
        output_dir: str,
        duration_hours: int,
    ) -> Optional[QPFData]:
        """Clip each grib to the storm bbox, sum, and write one GeoTIFF.

        All input gribs must be on the same WPC 2.5 km grid (they are —
        same cycle, same product). We clip each to the buffered storm
        bounds, sum pixelwise (treating nodata as 0), and write an
        atomic GeoTIFF.
        """
        try:
            import rasterio
            from rasterio.mask import mask as rasterio_mask
            from rasterio.warp import transform_geom
            from shapely.geometry import shape, mapping

            storm_shape = shape(storm_geometry)
            buffered = storm_shape.buffer(self.config.cone_buffer_km / 111.0)
            wgs84_geom = mapping(buffered)

            accumulator: Optional[np.ndarray] = None
            ref_transform = None
            ref_profile: Optional[dict] = None
            ref_crs = None

            for gp in grib_paths:
                with rasterio.open(gp) as src:
                    # WPC QPF is on a Lambert Conformal Conic grid (meters).
                    # Reproject the WGS84 storm polygon into the grid's CRS
                    # before clipping — otherwise rasterio.mask reads the
                    # lat/lon coords as meters in LCC and returns a 1×1
                    # nodata tile.
                    src_geom = transform_geom("EPSG:4326", src.crs, wgs84_geom)
                    out_image, out_transform = rasterio_mask(
                        src, [src_geom], crop=True, nodata=-9999
                    )
                    # (1, H, W) → (H, W); zero-out nodata so sums are clean
                    arr = out_image[0].astype(np.float32)
                    arr = np.where(arr == -9999, 0.0, arr)
                    if accumulator is None:
                        accumulator = arr
                        ref_transform = out_transform
                        ref_profile = src.profile.copy()
                        ref_crs = src.crs
                    else:
                        if arr.shape != accumulator.shape:
                            logger.warning(
                                "WPC QPF tile shape mismatch %s vs %s — "
                                "skipping tile", arr.shape, accumulator.shape,
                            )
                            continue
                        accumulator = accumulator + arr

            if accumulator is None or accumulator.size == 0 or min(accumulator.shape) == 0:
                logger.info("WPC QPF: empty clip — storm bbox outside grid")
                return None

            import threading as _th
            output_path = os.path.join(output_dir, "qpf_rainfall.tif")
            tmp_path = f"{output_path}.tmp.{os.getpid()}.{_th.get_ident()}"
            ref_profile.update(
                driver="GTiff",
                transform=ref_transform,
                width=accumulator.shape[1],
                height=accumulator.shape[0],
                count=1,
                dtype="float32",
                nodata=-9999,
                compress="deflate",
            )
            with rasterio.open(tmp_path, "w", **ref_profile) as dst:
                dst.write(accumulator.astype(np.float32), 1)
            os.replace(tmp_path, output_path)

            max_precip = float(np.nanmax(accumulator))
            return QPFData(
                path=output_path,
                total_precip_mm=max_precip,
                duration_hours=duration_hours,
                bounds=tuple(buffered.bounds),
                crs=str(ref_crs),
            )

        except Exception as e:
            logger.error(f"WPC QPF sum/clip failed: {e}")
            return None

    def _process_grib(
        self,
        grib_path: str,
        storm_geometry: dict,
        output_dir: str,
        duration_hours: int,
    ) -> Optional[QPFData]:
        """Convert GRIB to clipped GeoTIFF."""
        try:
            import rasterio
            from rasterio.mask import mask as rasterio_mask
            from shapely.geometry import shape, mapping

            storm_shape = shape(storm_geometry)
            buffered = storm_shape.buffer(self.config.cone_buffer_km / 111.0)

            output_path = os.path.join(output_dir, "qpf_rainfall.tif")

            with rasterio.open(grib_path) as src:
                out_image, out_transform = rasterio_mask(
                    src, [mapping(buffered)], crop=True, nodata=-9999
                )
                profile = src.profile.copy()
                profile.update(
                    driver="GTiff",
                    transform=out_transform,
                    width=out_image.shape[2],
                    height=out_image.shape[1],
                    nodata=-9999,
                    compress="deflate",
                )
                with rasterio.open(output_path, "w", **profile) as dst:
                    dst.write(out_image)

            max_precip = float(
                np.nanmax(out_image[out_image != -9999])
            )

            return QPFData(
                path=output_path,
                total_precip_mm=max_precip,
                duration_hours=duration_hours,
                bounds=tuple(buffered.bounds),
                crs=str(src.crs),
            )

        except Exception as e:
            logger.error(f"GRIB processing failed: {e}")
            return None

    def _generate_synthetic_rainfall(
        self,
        storm_geometry: dict,
        output_dir: str,
        duration_hours: int,
    ) -> QPFData:
        """Generate a synthetic rainfall accumulation raster."""
        import rasterio
        from rasterio.transform import from_bounds
        from shapely.geometry import shape

        storm_shape = shape(storm_geometry)
        bounds = storm_shape.bounds

        res = 0.005  # ~500m
        width = min(int((bounds[2] - bounds[0]) / res), 1500)
        height = min(int((bounds[3] - bounds[1]) / res), 1500)

        # Rainfall pattern: concentrated band right of storm center
        cx = (bounds[0] + bounds[2]) / 2 + (bounds[2] - bounds[0]) * 0.15
        cy = (bounds[1] + bounds[3]) / 2

        x = np.linspace(bounds[0], bounds[2], width)
        y = np.linspace(bounds[1], bounds[3], height)
        xx, yy = np.meshgrid(x, y)

        dist = np.sqrt((xx - cx) ** 2 + ((yy - cy) * 1.5) ** 2) * 111.0
        rainfall_mm = 300.0 * np.exp(-dist / 60.0)

        rng = np.random.default_rng(77)
        rainfall_mm += rng.normal(0, 15, rainfall_mm.shape)
        rainfall_mm = np.clip(rainfall_mm, 0, None).astype(np.float32)

        output_path = os.path.join(output_dir, "qpf_rainfall.tif")
        transform = from_bounds(
            bounds[0], bounds[1], bounds[2], bounds[3], width, height
        )

        with rasterio.open(
            output_path, "w", driver="GTiff",
            height=height, width=width, count=1, dtype="float32",
            crs="EPSG:4326", transform=transform, nodata=-9999,
            compress="deflate",
        ) as dst:
            dst.write(rainfall_mm, 1)

        max_rain = float(np.nanmax(rainfall_mm))
        logger.info(
            f"Synthetic rainfall: peak={max_rain:.0f}mm over {duration_hours}h"
        )

        return QPFData(
            path=output_path,
            total_precip_mm=max_rain,
            duration_hours=duration_hours,
            bounds=bounds,
            crs="EPSG:4326",
        )


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# USGS Stream Gauge Fetcher
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class USGSGaugeFetcher:
    """
    Fetches real-time stream gauge observations from USGS NWIS.

    Used for ground truth validation and real-time calibration
    of model outputs.
    """

    def __init__(self, config: IngestConfig):
        self.config = config
        self.session = requests.Session()
        self.session.headers["User-Agent"] = config.user_agent

    def fetch_gauges_in_area(
        self,
        bounds: Tuple[float, float, float, float],
    ) -> List[GaugeObservation]:
        """
        Fetch current stage and discharge for all USGS gauges
        within the given bounding box.

        Args:
            bounds: (west, south, east, north) in EPSG:4326

        Returns:
            List of GaugeObservation objects
        """
        west, south, east, north = bounds

        params = {
            "format": "json",
            "bBox": f"{west},{south},{east},{north}",
            "parameterCd": "00060,00065",  # discharge + stage
            "siteType": "ST",              # streams only
            "siteStatus": "active",
        }

        try:
            response = self.session.get(
                self.config.usgs_water_api,
                params=params,
                timeout=self.config.http_timeout,
            )
            response.raise_for_status()
            data = response.json()
            return self._parse_response(data)

        except Exception as e:
            logger.error(f"USGS gauge fetch failed: {e}")
            return []

    def _parse_response(self, data: dict) -> List[GaugeObservation]:
        """Parse USGS NWIS JSON response into GaugeObservation objects."""
        observations = {}  # keyed by site_id to merge stage + discharge

        time_series = data.get("value", {}).get("timeSeries", [])

        for ts in time_series:
            site_info = ts.get("sourceInfo", {})
            site_id = site_info.get("siteCode", [{}])[0].get("value", "")
            site_name = site_info.get("siteName", "")

            geo = site_info.get("geoLocation", {}).get(
                "geogLocation", {}
            )
            lat = geo.get("latitude", 0)
            lon = geo.get("longitude", 0)

            variable = ts.get("variable", {})
            var_code = variable.get("variableCode", [{}])[0].get(
                "value", ""
            )

            values = ts.get("values", [{}])[0].get("value", [])
            if not values:
                continue

            # Get most recent value
            latest = values[-1]
            val = float(latest.get("value", -999999))
            timestamp = latest.get("dateTime", "")

            if val == -999999:
                continue

            if site_id not in observations:
                observations[site_id] = GaugeObservation(
                    site_id=site_id,
                    site_name=site_name,
                    lat=lat,
                    lon=lon,
                    stage_ft=None,
                    discharge_cfs=None,
                    timestamp=timestamp,
                )

            obs = observations[site_id]
            if var_code == "00065":   # Stage
                obs.stage_ft = val
            elif var_code == "00060":  # Discharge
                obs.discharge_cfs = val

        result = list(observations.values())
        logger.info(f"Fetched {len(result)} USGS gauge observations")
        return result
