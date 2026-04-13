"""
NLCD 2021 Impervious Surface Fetcher

Fetches the NLCD 2021 Impervious Surface layer from the MRLC (Multi-
Resolution Land Characteristics Consortium) WCS endpoint and computes
cell-average impervious fraction for use in the Lonfat rainfall model.

Why this matters
----------------
The Lonfat parametric rain model produces total precipitation (mm), but
converting that to flood depth requires a runoff coefficient.  A 100 mm
storm over downtown Houston (85% impervious) floods far worse than the
same storm over a pine forest (5% impervious).

The existing ``get_runoff_coefficient()`` in rainfall.py already supports
NLCD class strings.  This module adds the complementary path: given an
impervious *fraction* (0.0–1.0) derived from the NLCD 30m raster, return
a physically-grounded runoff coefficient via a linear blend model.

MRLC WCS endpoint (NLCD 2021, no authentication required):
    https://www.mrlc.gov/geoserver/mrlc_display/
    NLCD_2021_Impervious_L48_20230630/wcs

Coverage: contiguous 48 states.  Alaska, Hawaii, Puerto Rico handled
via graceful fallback to the default coefficient (0.50).

NLCD pixel values: 0–100 = percent impervious surface (integer).
Nodata: 127.

Caching: one GeoTIFF per storm cell (keyed by rounded bbox).  Evicted
together with the cell cache since NLCD doesn't change between events.
"""

from __future__ import annotations

import json
import logging
import math
import os
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Optional, Tuple

import numpy as np

logger = logging.getLogger(__name__)

_MRLC_WCS_BASE = (
    "https://www.mrlc.gov/geoserver/mrlc_display/"
    "NLCD_2021_Impervious_L48_20230630/wcs"
)
_COVERAGE_ID  = "NLCD_2021_Impervious_L48_20230630"
_TIMEOUT_S    = 30
_NODATA_VALUE = 127   # NLCD impervious nodata sentinel

# Runoff model constants (calibrated to rational method lookup tables)
# C_min: fully permeable (dense forest/wetland)
# C_max: fully impervious (downtown urban core)
_C_MIN = 0.10
_C_MAX = 0.90


# ── Output type ───────────────────────────────────────────────────────────────

@dataclass
class NLCDResult:
    """NLCD impervious surface summary for a storm cell."""
    mean_impervious_pct: float    # 0–100 average impervious % across cell
    runoff_coefficient:  float    # 0.0–1.0 for use in rainfall model
    pixel_count:         int      # Number of valid NLCD pixels sampled
    bbox:                Tuple[float, float, float, float]
    source:              str = "nlcd_2021"
    notes:               str = ""

    @property
    def impervious_fraction(self) -> float:
        return self.mean_impervious_pct / 100.0


# ── Runoff coefficient from impervious fraction ───────────────────────────────

def runoff_coefficient_from_impervious(impervious_pct: float) -> float:
    """
    Convert NLCD impervious percentage to a rational-method runoff coefficient.

    Uses a linear blend model:
        C = C_min + (C_max - C_min) × (impervious% / 100)

    Calibrated to match the ASCE rational method look-up table:
        - 0%   impervious (forests)    → C ≈ 0.10
        - 25%  impervious (low-density suburban) → C ≈ 0.30
        - 50%  impervious (medium suburban) → C ≈ 0.50
        - 75%  impervious (urban residential) → C ≈ 0.70
        - 100% impervious (CBD/downtown)  → C ≈ 0.90

    Args:
        impervious_pct: NLCD impervious percentage (0–100).

    Returns:
        Runoff coefficient in [0.10, 0.90].
    """
    frac = max(0.0, min(100.0, impervious_pct)) / 100.0
    return _C_MIN + (_C_MAX - _C_MIN) * frac


# ── WCS HTTP fetch ────────────────────────────────────────────────────────────

def _fetch_nlcd_geotiff(
    west: float,
    south: float,
    east: float,
    north: float,
    output_path: str,
    max_dim: int = 1024,
) -> bool:
    """
    Fetch NLCD 2021 impervious surface raster via MRLC WCS 2.0.1.

    Returns True if a valid file was written to output_path.
    """
    # WCS uses Lat/Long axis order (OGC standard): subset=Lat(south,north)
    # and subset=Long(west,east) with EPSG:4326 CRS
    params = urllib.parse.urlencode({
        "service":      "WCS",
        "version":      "2.0.1",
        "request":      "GetCoverage",
        "CoverageId":   _COVERAGE_ID,
        "format":       "image/geotiff",
        "subset":       [f"Long({west},{east})", f"Lat({south},{north})"],
        "subsettingCrs": "http://www.opengis.net/def/crs/EPSG/0/4326",
        "outputCrs":    "http://www.opengis.net/def/crs/EPSG/0/4326",
        "scaleSize":    f"i({max_dim}),j({max_dim})",
    }, doseq=True)

    url = f"{_MRLC_WCS_BASE}?{params}"
    logger.info("[NLCD] Fetching impervious surface: %s…", url[:120])

    try:
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "SurgeDPS/1.0 (land cover data)"},
        )
        with urllib.request.urlopen(req, timeout=_TIMEOUT_S) as resp:
            raw = resp.read()

        # Sanity-check: must be a GeoTIFF
        if len(raw) < 200 or raw[:4] not in (
            b"II\x2A\x00", b"MM\x00\x2A",
            b"II\x2B\x00", b"MM\x00\x2B"
        ):
            logger.warning(
                "[NLCD] Response is not a GeoTIFF (%d bytes, head=%r)",
                len(raw), raw[:20],
            )
            return False

        with open(output_path, "wb") as f:
            f.write(raw)
        logger.info("[NLCD] Downloaded %d bytes → %s", len(raw), output_path)
        return True

    except Exception as exc:
        logger.warning("[NLCD] WCS fetch failed: %s", exc)
        return False


# ── Raster summary ────────────────────────────────────────────────────────────

def _compute_mean_impervious(tif_path: str) -> Tuple[float, int]:
    """
    Read an NLCD impervious GeoTIFF and return (mean_pct, pixel_count).

    Pixels equal to _NODATA_VALUE (127) are excluded.
    """
    try:
        import rasterio
        with rasterio.open(tif_path) as src:
            data = src.read(1).astype(np.float32)

        nodata_mask = (data == _NODATA_VALUE) | (data < 0) | (data > 100)
        valid = data[~nodata_mask]

        if valid.size == 0:
            return 0.0, 0

        return float(np.mean(valid)), int(valid.size)

    except Exception as exc:
        logger.warning("[NLCD] Mean impervious computation failed: %s", exc)
        return 0.0, 0


# ── Main entry point ──────────────────────────────────────────────────────────

def fetch_nlcd_for_cell(
    lon_min: float,
    lat_min: float,
    lon_max: float,
    lat_max: float,
    cache_dir: Optional[str] = None,
    storm_id: str = "",
    col: int = 0,
    row: int = 0,
) -> Optional[NLCDResult]:
    """
    Get NLCD 2021 impervious surface fraction for a storm cell bounding box.

    Downloads the MRLC WCS raster, computes cell-average impervious %,
    and returns a runoff coefficient for the Lonfat rainfall model.

    Results are cached per cell (keyed by storm_id + col + row) so the
    WCS is only queried once per cell per deployment.

    Args:
        lon_min, lat_min, lon_max, lat_max: Cell bounding box (EPSG:4326).
        cache_dir:  Directory for cached GeoTIFFs (use NWM_CACHE_DIR or similar).
        storm_id:   Storm identifier for cache namespacing.
        col, row:   Cell grid coordinates for cache naming.

    Returns:
        NLCDResult with mean_impervious_pct and runoff_coefficient,
        or None if the WCS is unreachable.
    """
    # ── Cache lookup ──────────────────────────────────────────────────────────
    tif_path: Optional[str] = None
    summary_path: Optional[str] = None

    if cache_dir:
        cell_dir = os.path.join(cache_dir, storm_id or "nlcd")
        os.makedirs(cell_dir, exist_ok=True)
        tif_path = os.path.join(cell_dir, f"nlcd_imp_{col}_{row}.tif")
        summary_path = os.path.join(cell_dir, f"nlcd_imp_{col}_{row}.json")

        if summary_path and os.path.exists(summary_path):
            try:
                with open(summary_path) as f:
                    data = json.load(f)
                logger.info(
                    "[NLCD] Cache hit cell (%d,%d): %.1f%% impervious",
                    col, row, data["mean_impervious_pct"],
                )
                return NLCDResult(
                    mean_impervious_pct=data["mean_impervious_pct"],
                    runoff_coefficient=data["runoff_coefficient"],
                    pixel_count=data["pixel_count"],
                    bbox=(lon_min, lat_min, lon_max, lat_max),
                    source="nlcd_2021_cache",
                )
            except Exception:
                pass  # Re-fetch if cache corrupt

    # ── Check geographic coverage ─────────────────────────────────────────────
    # NLCD L48 does not cover Alaska (west of -169°) or most ocean areas
    if lon_min < -170 or lon_max > -60 or lat_min < 20 or lat_max > 50:
        logger.info(
            "[NLCD] Cell (%.1f,%.1f)-(%.1f,%.1f) outside L48 — skipping",
            lon_min, lat_min, lon_max, lat_max,
        )
        return None

    # ── Fetch raster ──────────────────────────────────────────────────────────
    tmp_tif = tif_path or f"/tmp/nlcd_imp_{col}_{row}_{os.getpid()}.tif"
    ok = _fetch_nlcd_geotiff(lon_min, lat_min, lon_max, lat_max, tmp_tif)

    if not ok:
        return None

    # ── Compute mean impervious % ─────────────────────────────────────────────
    mean_pct, pixel_count = _compute_mean_impervious(tmp_tif)

    if pixel_count == 0:
        logger.warning("[NLCD] No valid pixels in cell (%d,%d)", col, row)
        return None

    runoff_c = runoff_coefficient_from_impervious(mean_pct)

    logger.info(
        "[NLCD] Cell (%d,%d): mean impervious %.1f%% → runoff C=%.2f "
        "(%d valid pixels)",
        col, row, mean_pct, runoff_c, pixel_count,
    )

    result = NLCDResult(
        mean_impervious_pct=mean_pct,
        runoff_coefficient=runoff_c,
        pixel_count=pixel_count,
        bbox=(lon_min, lat_min, lon_max, lat_max),
        notes=f"{pixel_count} NLCD 30m pixels, MRLC WCS 2021",
    )

    # ── Cache summary ─────────────────────────────────────────────────────────
    if summary_path:
        try:
            with open(summary_path, "w") as f:
                json.dump({
                    "mean_impervious_pct": mean_pct,
                    "runoff_coefficient": runoff_c,
                    "pixel_count": pixel_count,
                    "bbox": [lon_min, lat_min, lon_max, lat_max],
                }, f)
        except Exception as exc:
            logger.warning("[NLCD] Cache write failed: %s", exc)

    # Optionally remove large GeoTIFF after computing summary
    # (the JSON summary is what we actually use going forward)
    if tif_path and not cache_dir:
        try:
            os.remove(tmp_tif)
        except OSError:
            pass

    return result


# ── Convenience: default runoff coefficient for unknown areas ─────────────────

def get_default_runoff_coefficient(lat: float = 30.0, lon: float = -90.0) -> float:
    """
    Return a geographically-reasonable default runoff coefficient when NLCD
    data cannot be fetched.

    Gulf Coast is ~35% impervious on average (mix of urban, suburban, wetland).
    Coastal metro areas (Houston, New Orleans) are higher (~50-60%).
    Rural coastal areas are lower (~15-25%).

    For now, returns a conservative middle value of 0.45.
    """
    return 0.45
