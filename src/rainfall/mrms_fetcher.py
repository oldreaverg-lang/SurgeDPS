"""
MRMS QPE Fetcher
================
Fetches NOAA Multi-Radar Multi-Sensor Quantitative Precipitation Estimate data
for a storm's footprint.

Two access paths:
  REAL-TIME (active storm, last ~2 days):
    NCEP NOMADS — https://mrms.ncep.noaa.gov/data/2D/
    ~2-minute delivery lag from radar scan. Use Pass1 (radar-only) for speed,
    Pass2 (gauge-adjusted) for accuracy once gauges report.

  HISTORICAL (post-event validation):
    AWS S3 open data — s3://noaa-mrms-pds/CONUS/
    No auth required. Available from 2020-10 onward.
    Naming: MRMS_MultiSensor_QPE_{DUR}H_{PASS}_00.00_{YYYYMMDD}-{HHMMSS}.grib2.gz

Products used:
  MultiSensor_QPE_01H_Pass1   — 1-hour accumulation, radar-only, real-time
  MultiSensor_QPE_24H_Pass2   — 24-hour accumulation, gauge-adjusted (best accuracy)
  MultiSensor_QPE_72H_Pass2   — 72-hour accumulation (Harvey / Florence type events)

Grid: 0.01° × 0.01° (~1 km), CONUS. GRIB2 format (gzip-compressed).
Parser: cfgrib + xarray. Falls back to wgrib2 CLI if cfgrib not installed.

Usage:
    from rainfall.mrms_fetcher import MRMSFetcher
    fetcher = MRMSFetcher(cache_dir="/path/to/cache")

    # Real-time (last 6 hours):
    result = fetcher.fetch_storm_accumulation(
        storm_bbox=(-98.0, 27.0, -94.0, 31.0),
        duration_hr=72,
        pass_level=2,
        realtime=True,
    )
    print(result.max_precip_mm, result.clipped_tif_path)

    # Historical (Harvey):
    result = fetcher.fetch_historical(
        storm_bbox=(-98.0, 27.0, -94.0, 31.0),
        start_date="2017-08-25",
        end_date="2017-09-01",
        duration_hr=72,
    )
"""

from __future__ import annotations

import gzip
import hashlib
import logging
import os
import shutil
import tempfile
import threading
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Tuple

logger = logging.getLogger(__name__)

# ── S3 open-data base (no auth, no rate limit) ──────────────────────────────
_S3_BASE = "https://noaa-mrms-pds.s3.amazonaws.com/CONUS"

# ── NCEP NOMADS real-time (last ~2 days) ────────────────────────────────────
_NCEP_BASE = "https://mrms.ncep.noaa.gov/data/2D"

# ── IEM (Iowa Env Mesonet) MRMS archive ─────────────────────────────────────
# NOAA's S3 MRMS bucket only goes back to 2020-10-14. Iowa State mirrors
# hourly GaugeCorr_QPE_01H grib2 files from mid-2015 onward, which covers
# every modern US landfalling hurricane (Matthew 2016, Harvey/Irma/Maria 2017,
# Florence/Michael 2018, Dorian 2019, pre-Oct-2020 storms). We sum N hourly
# files to build an N-hour accumulation — slower than S3's pre-aggregated
# 72H product (~70 downloads of 750 KB each) but the result is real
# gauge-corrected observation, not a parametric model.
_IEM_BASE = "https://mtarchive.geol.iastate.edu"

# Product path templates
_PRODUCT_PATHS = {
    (1,  1): "MultiSensor_QPE_01H_Pass1_00.00",
    (1,  2): "MultiSensor_QPE_01H_Pass2_00.00",
    (24, 1): "MultiSensor_QPE_24H_Pass1_00.00",
    (24, 2): "MultiSensor_QPE_24H_Pass2_00.00",
    (48, 2): "MultiSensor_QPE_48H_Pass2_00.00",
    (72, 2): "MultiSensor_QPE_72H_Pass2_00.00",
}


@dataclass
class MRMSResult:
    """Output of an MRMS QPE fetch operation."""
    clipped_tif_path: str          # Storm-footprint GeoTIFF (mm precip)
    raw_grib_path: Optional[str]   # Full CONUS GRIB2 (may be deleted after clip)
    max_precip_mm: float
    avg_precip_mm: float
    product: str                   # e.g. "MultiSensor_QPE_72H_Pass2_00.00"
    valid_time: datetime           # Timestamp of the accumulation end
    duration_hr: int
    bbox: Tuple[float, float, float, float]  # (lon_min, lat_min, lon_max, lat_max)
    crs: str = "EPSG:4326"
    source: str = "mrms_s3"        # "mrms_s3" | "mrms_ncep"


class MRMSFetcher:
    """
    Fetch and clip MRMS QPE GRIB2 files to a storm bounding box.

    Args:
        cache_dir: Directory for caching downloaded GRIB2 and clipped GeoTIFFs.
        keep_raw_grib: If False (default), delete the full CONUS GRIB2 after
                       clipping to save disk space (~3-8 MB per file).
        request_timeout: HTTP timeout in seconds.
    """

    def __init__(
        self,
        cache_dir: str = "/tmp/mrms_cache",
        keep_raw_grib: bool = False,
        request_timeout: int = 30,
    ):
        self.cache_dir = cache_dir
        self.keep_raw_grib = keep_raw_grib
        self.timeout = request_timeout
        os.makedirs(cache_dir, exist_ok=True)

    # ── Public interface ─────────────────────────────────────────────────────

    def fetch_storm_accumulation(
        self,
        storm_bbox: Tuple[float, float, float, float],
        duration_hr: int = 72,
        pass_level: int = 2,
        realtime: bool = True,
        valid_time: Optional[datetime] = None,
    ) -> Optional[MRMSResult]:
        """
        Fetch the MRMS QPE accumulation product covering the storm bbox.

        For real-time use (active storm): fetches the most recent available file.
        For historical use: fetches the file matching valid_time.

        Args:
            storm_bbox: (lon_min, lat_min, lon_max, lat_max)
            duration_hr: Accumulation window — 1, 24, 48, or 72 hours.
            pass_level: 1 = radar-only (faster), 2 = gauge-adjusted (more accurate).
            realtime: True → NCEP NOMADS (last 2 days); False → S3 (historical).
            valid_time: For historical fetch, the accumulation end timestamp (UTC).
                        If None and realtime=False, uses the most recent available.

        Returns:
            MRMSResult, or None if no data is available.
        """
        product_key = (duration_hr, pass_level)
        if product_key not in _PRODUCT_PATHS:
            available = sorted(_PRODUCT_PATHS.keys())
            raise ValueError(
                f"Unsupported (duration_hr={duration_hr}, pass={pass_level}). "
                f"Available: {available}"
            )

        product_name = _PRODUCT_PATHS[product_key]

        if realtime:
            return self._fetch_ncep(product_name, storm_bbox, duration_hr)
        else:
            return self._fetch_s3(
                product_name, storm_bbox, duration_hr, valid_time
            )

    def fetch_historical(
        self,
        storm_bbox: Tuple[float, float, float, float],
        start_date: str,
        end_date: str,
        duration_hr: int = 72,
    ) -> Optional[MRMSResult]:
        """
        Fetch the peak-accumulation MRMS QPE file within a date range.

        Selects the 72H Pass2 file at the midpoint of the storm window
        (or the end of it, which captures the full accumulation).

        Args:
            storm_bbox: (lon_min, lat_min, lon_max, lat_max)
            start_date: "YYYY-MM-DD"
            end_date:   "YYYY-MM-DD"
            duration_hr: Accumulation period (72 recommended for multi-day storms).

        Returns:
            MRMSResult with the highest accumulation file in the window, or None.
        """
        end_dt = datetime.strptime(end_date, "%Y-%m-%d").replace(
            hour=12, tzinfo=timezone.utc
        )
        return self._fetch_s3(
            _PRODUCT_PATHS[(duration_hr, 2)],
            storm_bbox,
            duration_hr,
            valid_time=end_dt,
        )

    def list_available_dates(
        self,
        duration_hr: int = 24,
        pass_level: int = 2,
    ) -> List[str]:
        """
        List dates available in the S3 bucket for a given product.
        Returns sorted list of "YYYYMMDD" strings.
        """
        product_name = _PRODUCT_PATHS.get((duration_hr, pass_level))
        if not product_name:
            return []
        url = f"{_S3_BASE}/{product_name}/?list-type=2&delimiter=/"
        try:
            import re
            with urllib.request.urlopen(url, timeout=self.timeout) as resp:
                content = resp.read().decode()
            prefix = f"CONUS/{product_name}/"
            dates = re.findall(
                r'<Prefix>' + re.escape(prefix) + r'(\d{8})/', content
            )
            return sorted(dates)
        except Exception as exc:
            logger.warning("Failed to list MRMS dates: %s", exc)
            return []

    # ── Internal: NCEP NOMADS (real-time) ────────────────────────────────────

    def _fetch_ncep(
        self,
        product_name: str,
        storm_bbox: Tuple[float, float, float, float],
        duration_hr: int,
    ) -> Optional[MRMSResult]:
        """Fetch most recent file from NCEP NOMADS."""
        base_url = f"{_NCEP_BASE}/{product_name}/"

        # Get directory listing to find latest file
        try:
            with urllib.request.urlopen(base_url, timeout=self.timeout) as resp:
                html = resp.read().decode()
        except Exception as exc:
            logger.warning("NCEP NOMADS unreachable (%s), trying S3 fallback", exc)
            return self._fetch_s3(product_name, storm_bbox, duration_hr, valid_time=None)

        # Parse filenames from HTML directory listing
        import re
        fnames = re.findall(
            r'(MRMS_' + re.escape(product_name) + r'_\d{8}-\d{6}\.grib2\.gz)',
            html,
        )
        if not fnames:
            logger.warning("No MRMS files found at NCEP for %s", product_name)
            return None

        latest_fname = sorted(fnames)[-1]
        url = f"{base_url}{latest_fname}"
        valid_time = self._parse_timestamp(latest_fname)

        return self._download_and_clip(
            url=url,
            fname=latest_fname,
            product_name=product_name,
            storm_bbox=storm_bbox,
            duration_hr=duration_hr,
            valid_time=valid_time,
            source="mrms_ncep",
        )

    # ── Internal: S3 open data (historical) ──────────────────────────────────

    def _fetch_s3(
        self,
        product_name: str,
        storm_bbox: Tuple[float, float, float, float],
        duration_hr: int,
        valid_time: Optional[datetime] = None,
    ) -> Optional[MRMSResult]:
        """Fetch from S3. Picks the file closest to valid_time (or most recent)."""
        if valid_time is None:
            valid_time = datetime.now(tz=timezone.utc)

        date_str = valid_time.strftime("%Y%m%d")
        prefix = f"CONUS/{product_name}/{date_str}/"
        list_url = (
            f"{_S3_BASE.replace('https://noaa-mrms-pds.s3.amazonaws.com/CONUS', 'https://noaa-mrms-pds.s3.amazonaws.com')}"
            f"?list-type=2&prefix={prefix}&max-keys=50"
        )

        try:
            import re
            with urllib.request.urlopen(list_url, timeout=self.timeout) as resp:
                content = resp.read().decode()
            keys = re.findall(r'<Key>(CONUS/' + re.escape(product_name) + r'/\d{8}/[^<]+\.grib2\.gz)</Key>', content)
        except Exception as exc:
            logger.warning("S3 listing failed for %s/%s: %s", product_name, date_str, exc)
            return None

        if not keys:
            # Pre-2020-10-14 dates have no MRMS archive on S3 — historical storms
            # (Harvey 2017, Michael 2018, etc.) always hit this branch and fall back
            # to the Lonfat parametric rainfall model. Log at INFO, not WARNING,
            # so it doesn't look like a failure in the Railway dashboard.
            logger.info("No MRMS files on S3 for %s on %s (expected for pre-2020 storms; parametric fallback will run)", product_name, date_str)
            return None

        # Pick the file whose timestamp is closest to valid_time
        target_hour = valid_time.hour
        best_key = None
        best_delta = float("inf")
        for key in keys:
            fname = os.path.basename(key)
            ts = self._parse_timestamp(fname)
            if ts is None:
                continue
            delta = abs((ts - valid_time).total_seconds())
            if delta < best_delta:
                best_delta = delta
                best_key = key
                best_ts = ts

        if best_key is None:
            return None

        fname = os.path.basename(best_key)
        url = f"https://noaa-mrms-pds.s3.amazonaws.com/{best_key}"
        return self._download_and_clip(
            url=url,
            fname=fname,
            product_name=product_name,
            storm_bbox=storm_bbox,
            duration_hr=duration_hr,
            valid_time=best_ts,
            source="mrms_s3",
        )

    # ── Internal: Download + clip ─────────────────────────────────────────────

    def _download_and_clip(
        self,
        url: str,
        fname: str,
        product_name: str,
        storm_bbox: Tuple[float, float, float, float],
        duration_hr: int,
        valid_time: Optional[datetime],
        source: str,
    ) -> Optional[MRMSResult]:
        """Download a GRIB2 file, clip to storm_bbox, write GeoTIFF, return result."""
        # Cache key = hash of (url, bbox) so different storm windows get separate clips
        bbox_str = "_".join(f"{v:.3f}" for v in storm_bbox)
        cache_key = hashlib.md5(f"{url}|{bbox_str}".encode()).hexdigest()[:12]
        clipped_tif = os.path.join(self.cache_dir, f"mrms_{cache_key}.tif")

        if os.path.exists(clipped_tif):
            logger.info("MRMS cache hit: %s", clipped_tif)
            return self._result_from_tif(
                clipped_tif, product_name, valid_time, duration_hr, storm_bbox, source
            )

        # Download
        gz_path = os.path.join(self.cache_dir, fname)
        grib_path = gz_path.replace(".gz", "")

        if not os.path.exists(grib_path):
            logger.info("Downloading MRMS: %s", url)
            try:
                req = urllib.request.Request(
                    url, headers={"User-Agent": "SurgeDPS/1.0 (surgedps.com)"}
                )
                with urllib.request.urlopen(req, timeout=60) as resp:
                    with open(gz_path, "wb") as f:
                        shutil.copyfileobj(resp, f)
                # Decompress
                with gzip.open(gz_path, "rb") as gz_in:
                    with open(grib_path, "wb") as f_out:
                        shutil.copyfileobj(gz_in, f_out)
                os.remove(gz_path)
            except Exception as exc:
                logger.error("MRMS download failed: %s", exc)
                for p in (gz_path, grib_path):
                    if os.path.exists(p):
                        os.remove(p)
                return None

        # Parse GRIB2 and clip to bbox
        clipped = self._grib_to_clipped_tif(grib_path, storm_bbox, clipped_tif)

        if not self.keep_raw_grib and os.path.exists(grib_path):
            os.remove(grib_path)

        if not clipped:
            return None

        return self._result_from_tif(
            clipped_tif, product_name, valid_time, duration_hr, storm_bbox, source
        )

    def _grib_to_clipped_tif(
        self,
        grib_path: str,
        bbox: Tuple[float, float, float, float],
        out_tif: str,
    ) -> bool:
        """Convert GRIB2 → clipped GeoTIFF using cfgrib + rasterio."""
        lon_min, lat_min, lon_max, lat_max = bbox

        # cfgrib path (preferred)
        try:
            import xarray as xr
            import numpy as np
            import rasterio
            from rasterio.transform import from_bounds

            ds = xr.open_dataset(grib_path, engine="cfgrib", indexpath="")
            # Field is named 'tp' (total precipitation, mm)
            if "tp" in ds:
                da = ds["tp"]
            elif "unknown" in ds:
                da = ds["unknown"]
            else:
                da = list(ds.data_vars.values())[0]

            # MRMS uses 0–360 longitude convention on some products
            if float(da.longitude.max()) > 180:
                da = da.assign_coords(
                    longitude=(da.longitude + 180) % 360 - 180
                ).sortby("longitude")

            # Clip to bbox
            da_clip = da.sel(
                latitude=slice(lat_max, lat_min),
                longitude=slice(lon_min, lon_max),
            )
            data = da_clip.values.astype("float32")
            data[data < 0] = 0  # MRMS nodata is -999; zero out
            lats = da_clip.latitude.values
            lons = da_clip.longitude.values

            n_rows, n_cols = data.shape
            transform = from_bounds(
                float(lons.min()), float(lats.min()),
                float(lons.max()), float(lats.max()),
                n_cols, n_rows,
            )

            with rasterio.open(
                out_tif, "w",
                driver="GTiff", dtype="float32", count=1,
                width=n_cols, height=n_rows,
                crs="EPSG:4326", transform=transform,
                nodata=-9999,
                compress="deflate", predictor=3,
            ) as dst:
                dst.write(data, 1)
                dst.update_tags(
                    source="MRMS_QPE",
                    product=os.path.basename(grib_path),
                    units="mm",
                )
            ds.close()
            return True

        except ImportError:
            logger.warning("cfgrib/xarray not installed; trying wgrib2 CLI")
        except Exception as exc:
            logger.warning("cfgrib parse failed: %s — trying wgrib2", exc)

        # wgrib2 fallback (CLI tool)
        try:
            import subprocess
            import numpy as np
            import rasterio
            from rasterio.transform import from_bounds

            csv_path = grib_path + ".csv"
            cmd = [
                "wgrib2", grib_path,
                "-latlon",
                f"{lon_min}:{int((lon_max-lon_min)/0.01)}:0.01",
                f"{lat_min}:{int((lat_max-lat_min)/0.01)}:0.01",
                "-csv", csv_path,
            ]
            subprocess.run(cmd, check=True, capture_output=True)

            lons_u, lats_u, vals = [], [], []
            with open(csv_path) as f:
                next(f)  # header
                for line in f:
                    _, _, lat, lon, val = line.strip().split(",")
                    lats_u.append(float(lat))
                    lons_u.append(float(lon))
                    vals.append(max(0.0, float(val)))

            import numpy as np
            lats_arr = np.unique(sorted(set(lats_u), reverse=True))
            lons_arr = np.unique(sorted(set(lons_u)))
            data = np.full((len(lats_arr), len(lons_arr)), -9999, dtype=np.float32)
            lat_idx = {v: i for i, v in enumerate(lats_arr)}
            lon_idx = {v: i for i, v in enumerate(lons_arr)}
            for lat, lon, val in zip(lats_u, lons_u, vals):
                r, c = lat_idx.get(lat), lon_idx.get(lon)
                if r is not None and c is not None:
                    data[r, c] = val

            transform = from_bounds(
                float(lons_arr.min()), float(lats_arr.min()),
                float(lons_arr.max()), float(lats_arr.max()),
                len(lons_arr), len(lats_arr),
            )
            with rasterio.open(
                out_tif, "w",
                driver="GTiff", dtype="float32", count=1,
                width=len(lons_arr), height=len(lats_arr),
                crs="EPSG:4326", transform=transform,
                nodata=-9999,
                compress="deflate", predictor=3,
            ) as dst:
                dst.write(data, 1)
            os.remove(csv_path)
            return True
        except Exception as exc:
            logger.error("wgrib2 fallback failed: %s", exc)
            return False

    # ── Internal: IEM archive (pre-2020 historical) ──────────────────────────

    def fetch_iem_historical(
        self,
        storm_bbox: Tuple[float, float, float, float],
        valid_time: datetime,
        duration_hr: int = 72,
    ) -> Optional["MRMSResult"]:
        """
        Build a duration_hr accumulation GeoTIFF by summing IEM's hourly
        GaugeCorr_QPE_01H archive. Covers storms back to ~2015 — the gap
        between pre-S3 (2020-10-14) and early MRMS deployment.

        Returns None if too few hourly files are retrievable (< 50%) or if
        the grib parser fails on every file.
        """
        import numpy as np

        # Cache key tied to (valid_time, duration_hr, bbox) so Harvey at
        # 2017-08-26 18Z and 2017-08-27 18Z can coexist on disk.
        bbox_str = "_".join(f"{v:.3f}" for v in storm_bbox)
        cache_token = f"iem|{valid_time.isoformat()}|{duration_hr}|{bbox_str}"
        cache_key = hashlib.md5(cache_token.encode()).hexdigest()[:12]
        clipped_tif = os.path.join(self.cache_dir, f"iem_{cache_key}.tif")

        if os.path.exists(clipped_tif):
            logger.info("IEM cache hit: %s", clipped_tif)
            return self._result_from_tif(
                clipped_tif,
                f"IEM_GaugeCorr_QPE_{duration_hr:02d}H",
                valid_time, duration_hr, storm_bbox, source="mrms_iem",
            )

        # Build target URLs for every hour in the window.
        hour_targets = []
        for i in range(duration_hr):
            t = valid_time - timedelta(hours=i)
            url = (
                f"{_IEM_BASE}/{t.year:04d}/{t.month:02d}/{t.day:02d}"
                f"/mrms/ncep/GaugeCorr_QPE_01H"
                f"/GaugeCorr_QPE_01H_00.00_{t.strftime('%Y%m%d-%H%M%S')}.grib2.gz"
            )
            fname = os.path.basename(url)
            gz_path = os.path.join(self.cache_dir, fname)
            grib_path = gz_path[:-3]
            hour_targets.append((url, fname, gz_path, grib_path))

        # Parallel downloads — 72 sequential HTTPs takes >30 s (Railway request
        # timeout territory); 12 workers runs the whole window in ~5-8 s.
        from concurrent.futures import ThreadPoolExecutor, as_completed

        def _download_one(entry):
            # Atomic-write pattern: download → decompress → rename. Prevents
            # a concurrent request from seeing a half-written grib_path when
            # two users hit /api/rainfall for the same storm simultaneously
            # (ThreadingHTTPServer fans out requests across threads).
            url, fname, gz_path, grib_path = entry
            if os.path.exists(grib_path) and os.path.getsize(grib_path) > 0:
                return grib_path  # cached from prior fetch (files are immutable)
            tid = threading.get_ident()
            gz_tmp = f"{gz_path}.tmp.{os.getpid()}.{tid}"
            grib_tmp = f"{grib_path}.tmp.{os.getpid()}.{tid}"
            try:
                req = urllib.request.Request(
                    url, headers={"User-Agent": "SurgeDPS/1.0 (surgedps.com)"}
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    with open(gz_tmp, "wb") as f:
                        shutil.copyfileobj(resp, f)
                with gzip.open(gz_tmp, "rb") as gz_in, open(grib_tmp, "wb") as f_out:
                    shutil.copyfileobj(gz_in, f_out)
                os.remove(gz_tmp)
                # os.replace is atomic on POSIX — a concurrent reader either
                # sees the fully-written file or the older cached one.
                os.replace(grib_tmp, grib_path)
                return grib_path
            except Exception as exc:
                logger.debug("IEM miss %s: %s", fname, exc)
                for p in (gz_tmp, grib_tmp):
                    if os.path.exists(p):
                        try: os.remove(p)
                        except OSError: pass
                return None

        downloaded_paths: list[str] = []
        with ThreadPoolExecutor(max_workers=24) as ex:
            for fut in as_completed([ex.submit(_download_one, t) for t in hour_targets]):
                p = fut.result()
                if p is not None:
                    downloaded_paths.append(p)

        # Parse sequentially. cfgrib materializes the full CONUS grid
        # (~300 MB) on each open — threading it just thrashes memory and
        # got OOM-killed under Railway's container limits in testing.
        parsed: list[tuple] = []
        shape_mismatch = 0
        for gp in downloaded_paths:
            arr, lats, lons = self._grib_to_clipped_array(gp, storm_bbox)
            if arr is not None:
                parsed.append((arr, lats, lons))

        # Reduce sequentially — adding float32 arrays isn't worth parallelizing
        # at this size (~500x500 per hour) and keeps shape-check logic simple.
        accumulator = None
        accum_lats = accum_lons = None
        valid_mask_any = None
        hours_accumulated = 0

        for arr, lats, lons in parsed:
            if accumulator is None:
                accumulator = np.zeros_like(arr, dtype=np.float32)
                valid_mask_any = np.zeros_like(arr, dtype=bool)
                accum_lats, accum_lons = lats, lons
            if arr.shape == accumulator.shape:
                hr_valid = arr >= 0
                accumulator[hr_valid] += arr[hr_valid]
                valid_mask_any |= hr_valid
                hours_accumulated += 1
            else:
                # Shouldn't happen on a stable MRMS grid + fixed bbox, but
                # surface it if it ever does rather than silently dropping.
                shape_mismatch += 1

        if shape_mismatch:
            logger.warning(
                "IEM aggregator: dropped %d hourly files whose clip shape didn't match",
                shape_mismatch,
            )

        if accumulator is None or hours_accumulated < max(4, duration_hr // 2):
            logger.info(
                "IEM fetch gave up: only %d/%d hourly files retrievable",
                hours_accumulated, duration_hr,
            )
            return None

        # Guard against an all-empty clip (bbox entirely outside CONUS, or the
        # lat/lon slice came back as a 0-row array). Writing a 0-dim GeoTIFF
        # succeeds silently but the tile server can't render anything from it.
        if accumulator.size == 0 or min(accumulator.shape) == 0:
            logger.warning(
                "IEM clip produced an empty array for bbox=%s — outside CONUS?",
                storm_bbox,
            )
            return None

        # Mark non-observed pixels as nodata so the tile server draws them
        # transparent instead of zero-green.
        accumulator[~valid_mask_any] = -9999

        # Write the summed raster as a clipped GeoTIFF (units: mm). Atomic
        # write so a concurrent /api/rainfall caller can't see a partially
        # written tif through the cache-hit fast path.
        tif_tmp = f"{clipped_tif}.tmp.{os.getpid()}.{threading.get_ident()}"
        try:
            import rasterio
            from rasterio.transform import from_bounds
            n_rows, n_cols = accumulator.shape
            transform = from_bounds(
                float(accum_lons.min()), float(accum_lats.min()),
                float(accum_lons.max()), float(accum_lats.max()),
                n_cols, n_rows,
            )
            with rasterio.open(
                tif_tmp, "w",
                driver="GTiff", dtype="float32", count=1,
                width=n_cols, height=n_rows,
                crs="EPSG:4326", transform=transform,
                nodata=-9999,
                compress="deflate", predictor=3,
            ) as dst:
                dst.write(accumulator, 1)
                dst.update_tags(
                    source="MRMS_IEM_archive",
                    product=f"GaugeCorr_QPE_{duration_hr:02d}H_sum",
                    units="mm",
                    hours_summed=str(hours_accumulated),
                )
            os.replace(tif_tmp, clipped_tif)
        except Exception as exc:
            logger.error("IEM GeoTIFF write failed: %s", exc)
            if os.path.exists(tif_tmp):
                try: os.remove(tif_tmp)
                except OSError: pass
            return None

        logger.info(
            "IEM historical: summed %d/%d hours ending %s → %s",
            hours_accumulated, duration_hr, valid_time.isoformat(), clipped_tif,
        )
        return self._result_from_tif(
            clipped_tif,
            f"IEM_GaugeCorr_QPE_{duration_hr:02d}H",
            valid_time, duration_hr, storm_bbox, source="mrms_iem",
        )

    def _grib_to_clipped_array(
        self,
        grib_path: str,
        bbox: Tuple[float, float, float, float],
    ) -> Tuple[Optional["np.ndarray"], Optional["np.ndarray"], Optional["np.ndarray"]]:
        """Parse a single hourly GRIB2 → (data, lats, lons) clipped to bbox.

        Returns (None, None, None) on any parse failure. Used by the IEM
        historical aggregator; the non-aggregating S3/NCEP path writes
        straight to GeoTIFF via _grib_to_clipped_tif instead.
        """
        lon_min, lat_min, lon_max, lat_max = bbox
        try:
            import xarray as xr
            import numpy as np
            ds = xr.open_dataset(grib_path, engine="cfgrib", indexpath="")
            if "tp" in ds:
                da = ds["tp"]
            elif "unknown" in ds:
                da = ds["unknown"]
            else:
                da = list(ds.data_vars.values())[0]
            if float(da.longitude.max()) > 180:
                da = da.assign_coords(
                    longitude=(da.longitude + 180) % 360 - 180
                ).sortby("longitude")
            da_clip = da.sel(
                latitude=slice(lat_max, lat_min),
                longitude=slice(lon_min, lon_max),
            )
            data = da_clip.values.astype("float32")
            lats = da_clip.latitude.values
            lons = da_clip.longitude.values
            ds.close()
            return data, lats, lons
        except Exception as exc:
            logger.debug("IEM grib parse failed on %s: %s", os.path.basename(grib_path), exc)
            return None, None, None

    def _result_from_tif(
        self,
        tif_path: str,
        product_name: str,
        valid_time: Optional[datetime],
        duration_hr: int,
        bbox: Tuple[float, float, float, float],
        source: str,
    ) -> MRMSResult:
        """Read stats from a clipped GeoTIFF and build MRMSResult.

        Also: `src.nodata or -9999` collapses a legitimate nodata=0 to
        -9999. Use an explicit None check.

        Iterates the raster block-by-block rather than pulling the whole
        array into RAM — a continental IEM accumulation is ~4000×6000
        float32 (~100 MB) and we compute stats on every /api/rainfall
        hit under ThreadingHTTPServer.
        """
        max_mm, avg_mm = 0.0, 0.0
        try:
            import rasterio
            total = 0.0
            count = 0
            running_max = float("-inf")
            with rasterio.open(tif_path) as src:
                nodata = src.nodata if src.nodata is not None else -9999
                for _, window in src.block_windows(1):
                    block = src.read(1, window=window)
                    valid = block[(block != nodata) & (block >= 0)]
                    if valid.size:
                        total += float(valid.sum())
                        count += int(valid.size)
                        m = float(valid.max())
                        if m > running_max:
                            running_max = m
            if count:
                max_mm = running_max
                avg_mm = total / count
        except Exception:
            max_mm, avg_mm = 0.0, 0.0

        return MRMSResult(
            clipped_tif_path=tif_path,
            raw_grib_path=None,
            max_precip_mm=max_mm,
            avg_precip_mm=avg_mm,
            product=product_name,
            valid_time=valid_time or datetime.now(tz=timezone.utc),
            duration_hr=duration_hr,
            bbox=bbox,
            source=source,
        )

    @staticmethod
    def _parse_timestamp(fname: str) -> Optional[datetime]:
        """Extract datetime from MRMS filename like ...20250101-120000.grib2.gz"""
        import re
        m = re.search(r'(\d{8})-(\d{6})', fname)
        if not m:
            return None
        try:
            return datetime.strptime(m.group(1) + m.group(2), "%Y%m%d%H%M%S").replace(
                tzinfo=timezone.utc
            )
        except ValueError:
            return None


# ── Convenience helpers ──────────────────────────────────────────────────────

def point_precip_mm(tif_path: str, lat: float, lon: float) -> Optional[float]:
    """
    Sample MRMS QPE accumulation (mm) at a single lat/lon point.
    Returns None if outside the raster or data invalid.
    """
    try:
        import rasterio
        with rasterio.open(tif_path) as src:
            row, col = src.index(lon, lat)
            data = src.read(1)
            nodata = src.nodata or -9999
            val = float(data[row, col])
            if val == nodata or val < 0:
                return None
            return val
    except Exception:
        return None


def storm_bbox_from_catalog_entry(
    landfall_lat: float,
    landfall_lon: float,
    buffer_deg: float = 4.0,
) -> Tuple[float, float, float, float]:
    """
    Build a bounding box around a storm landfall point.
    Default buffer 4° (≈ 440 km) captures most TC rain fields.
    """
    return (
        landfall_lon - buffer_deg,
        landfall_lat - buffer_deg,
        landfall_lon + buffer_deg,
        landfall_lat + buffer_deg,
    )
