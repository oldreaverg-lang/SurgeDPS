"""
CFIM / NOAA OWP FIM HAND Raster Fetcher

Downloads pre-computed HAND (Height Above Nearest Drainage) rasters from
the NOAA Office of Water Prediction Flood Inundation Mapping (FIM) dataset,
available as a public S3 bucket at:

    s3://noaa-nws-owp-fim/hand_fim/

Per-HUC8 files used:
  rem_zeroed_masked_{huc8}.tif
      HAND raster — height above nearest drainage in meters.
      A cell value of 0 means the cell IS the stream channel.
      A cell value of 3.5 means it's 3.5m above the nearest stream.

  gw_catchments_reaches_filtered_addedAttributes_{huc8}.gpkg
      NHDPlus catchment polygons.  Each polygon corresponds to one
      NHDPlus reach and carries a `feature_id` column (= NHDPlus COMID).
      We rasterize this to a catchment ID raster matching the HAND grid.

Workflow per storm cell:
  1. Find which HUC8 watersheds overlap the cell bounding box
     (via the USGS Watershed Boundary Dataset REST API).
  2. For each HUC8 not already cached, download HAND + catchments and
     rasterize catchments → GeoTIFF on the persistent volume.
  3. Mosaic/clip the per-HUC8 rasters to the cell extent.
  4. Return (hand_path, catchment_path) ready for run_hand_model().

Public HTTPS access (no auth required):
  https://noaa-nws-owp-fim.s3.amazonaws.com/hand_fim/{version}/{huc8}/{file}

WBD API (HUC8 lookup):
  https://hydro.nationalmap.gov/arcgis/rest/services/wbd/MapServer/4/query
"""

from __future__ import annotations

import gzip
import json
import logging
import os
import shutil
import time
import urllib.request
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import numpy as np

logger = logging.getLogger(__name__)

# ── NOAA OWP FIM bucket ───────────────────────────────────────────────────────
_FIM_BASE    = "https://noaa-nws-owp-fim.s3.amazonaws.com/hand_fim"
# FIM versions to try in order (newest first); we fall through until one works.
_FIM_VERSIONS = [
    "hand_fim_4_4_0_0",
    "hand_fim_4_3_6_1",
    "hand_fim_3_0_26_0",
]
_HAND_FNAME    = "rem_zeroed_masked_{huc8}.tif"
_CATCH_FNAME   = "gw_catchments_reaches_filtered_addedAttributes_{huc8}.gpkg"

# ── WBD API (HUC8 lookup by bbox) ────────────────────────────────────────────
_WBD_URL = (
    "https://hydro.nationalmap.gov/arcgis/rest/services/wbd/MapServer/4/query"
    "?geometry={xmin},{ymin},{xmax},{ymax}"
    "&geometryType=esriGeometryEnvelope"
    "&spatialRel=esriSpatialRelIntersects"
    "&f=json"
    "&outFields=huc8,name"
    "&returnGeometry=false"
)

_TIMEOUT_S = 30
_CHUNK     = 1 << 16  # 64 KB read chunks


# ── Result types ─────────────────────────────────────────────────────────────

@dataclass
class HANDFiles:
    """Paths to the HAND and catchment rasters for one cell extent."""
    hand_path: str         # HAND GeoTIFF (meters above nearest drainage)
    catchment_path: str    # Catchment ID GeoTIFF (NHDPlus COMID per cell)
    huc8s: List[str]       # HUC8s that were mosaicked
    from_cache: bool = False


# ── Helpers ──────────────────────────────────────────────────────────────────

def _http_get_json(url: str) -> Optional[dict]:
    try:
        req = urllib.request.Request(
            url, headers={"User-Agent": "SurgeDPS/1.0 (flood model)"}
        )
        with urllib.request.urlopen(req, timeout=_TIMEOUT_S) as resp:
            return json.loads(resp.read().decode())
    except Exception as exc:
        logger.debug("HTTP GET failed %s: %s", url, exc)
        return None


def _download_file(url: str, local_path: str) -> bool:
    """Download url → local_path. Returns True on success."""
    import threading as _th_cf
    tmp = f"{local_path}.tmp.{os.getpid()}.{_th_cf.get_ident()}"
    try:
        req = urllib.request.Request(
            url, headers={"User-Agent": "SurgeDPS/1.0 (flood model)"}
        )
        with urllib.request.urlopen(req, timeout=_TIMEOUT_S) as resp, \
             open(tmp, "wb") as fout:
            while True:
                chunk = resp.read(_CHUNK)
                if not chunk:
                    break
                fout.write(chunk)
        os.replace(tmp, local_path)
        return True
    except Exception as exc:
        logger.debug("Download failed %s: %s", url, exc)
        if os.path.exists(tmp):
            os.remove(tmp)
        return False


def get_huc8s_for_bbox(
    lon_min: float, lat_min: float, lon_max: float, lat_max: float
) -> List[str]:
    """
    Return HUC8 codes for watersheds that intersect the given bounding box
    using the USGS Watershed Boundary Dataset REST API.
    """
    url = _WBD_URL.format(
        xmin=lon_min, ymin=lat_min, xmax=lon_max, ymax=lat_max
    )
    data = _http_get_json(url)
    if not data:
        return []

    huc8s = []
    for feat in data.get("features", []):
        attrs = feat.get("attributes", {})
        h8 = attrs.get("huc8")
        if h8:
            huc8s.append(str(h8).zfill(8))

    logger.info("[CFIM] HUC8s for bbox: %s", huc8s)
    return huc8s


def _find_fim_url(huc8: str, filename: str) -> Optional[str]:
    """
    Try FIM version URLs in order; return the first that returns HTTP 200.
    HEAD requests fail on this bucket so we try a small range request.
    """
    for version in _FIM_VERSIONS:
        url = f"{_FIM_BASE}/{version}/{huc8}/{filename}"
        try:
            req = urllib.request.Request(url, method="HEAD",
                headers={"User-Agent": "SurgeDPS/1.0"})
            with urllib.request.urlopen(req, timeout=10):
                return url
        except Exception:
            continue
    return None


def _rasterize_catchments(
    gpkg_path: str,
    hand_path: str,
    output_path: str,
) -> bool:
    """
    Rasterize the catchment GeoPackage to a GeoTIFF matching the HAND raster.

    Burns the NHDPlus COMID (feature_id) value into each cell, producing
    a raster where every pixel holds the COMID of its NHDPlus reach.
    Returns True on success.
    """
    try:
        import rasterio
        from rasterio.features import rasterize
        from rasterio.transform import from_bounds
        import fiona

        # Read reference grid from HAND raster
        with rasterio.open(hand_path) as src:
            profile = src.profile.copy()
            shape   = (src.height, src.width)
            transform = src.transform
            crs = src.crs

        # Read catchment polygons
        with fiona.open(gpkg_path) as ds:
            # Look for the COMID column (varies by FIM version)
            schema_props = ds.schema.get("properties", {})
            id_col = next(
                (c for c in ("feature_id", "HydroID", "COMID", "NHDPlusID")
                 if c in schema_props),
                None,
            )
            if id_col is None:
                logger.warning("[CFIM] No COMID column found in %s; cols=%s",
                               gpkg_path, list(schema_props.keys()))
                return False

            shapes = [
                (feat["geometry"], int(feat["properties"][id_col]))
                for feat in ds
                if feat["geometry"] is not None
                   and feat["properties"].get(id_col) is not None
            ]

        if not shapes:
            logger.warning("[CFIM] No catchment polygons in %s", gpkg_path)
            return False

        # Rasterize
        catch_raster = rasterize(
            shapes,
            out_shape=shape,
            transform=transform,
            fill=0,           # 0 = no catchment
            dtype=np.int32,
        )

        # Write output
        out_profile = profile.copy()
        out_profile.update(dtype="int32", nodata=0, count=1,
                           compress="deflate")
        with rasterio.open(output_path, "w", **out_profile) as dst:
            dst.write(catch_raster.astype(np.int32), 1)

        logger.info("[CFIM] Rasterized %d catchments → %s",
                    len(shapes), output_path)
        return True

    except ImportError as exc:
        logger.warning("[CFIM] Rasterization deps missing: %s", exc)
        return False
    except Exception as exc:
        logger.warning("[CFIM] Rasterization failed: %s", exc)
        return False


def _fetch_huc8(huc8: str, huc8_cache_dir: str) -> Optional[Tuple[str, str]]:
    """
    Download and prepare HAND + catchment rasters for one HUC8.
    Returns (hand_tif_path, catchment_tif_path) or None on failure.
    Results are cached in huc8_cache_dir permanently.
    """
    hand_cache    = os.path.join(huc8_cache_dir, f"hand_{huc8}.tif")
    catch_cache   = os.path.join(huc8_cache_dir, f"catchments_{huc8}.tif")

    # Already cached?
    if os.path.exists(hand_cache) and os.path.exists(catch_cache):
        logger.info("[CFIM] HUC8 %s: cache hit", huc8)
        return hand_cache, catch_cache

    os.makedirs(huc8_cache_dir, exist_ok=True)

    # ── Download HAND raster ────────────────────────────────────────────────
    hand_url = _find_fim_url(huc8, _HAND_FNAME.format(huc8=huc8))
    if hand_url is None:
        logger.info("[CFIM] HUC8 %s not found in any FIM version — skipping", huc8)
        return None

    logger.info("[CFIM] Downloading HAND raster for HUC8 %s", huc8)
    if not _download_file(hand_url, hand_cache):
        return None

    # ── Download catchments GeoPackage ─────────────────────────────────────
    gpkg_local = os.path.join(huc8_cache_dir, f"catchments_{huc8}.gpkg")
    if not os.path.exists(gpkg_local):
        catch_url = _find_fim_url(huc8, _CATCH_FNAME.format(huc8=huc8))
        if catch_url is None:
            logger.warning("[CFIM] No catchments file for HUC8 %s", huc8)
            # Cleanup orphaned HAND
            if os.path.exists(hand_cache):
                os.remove(hand_cache)
            return None

        logger.info("[CFIM] Downloading catchments for HUC8 %s", huc8)
        if not _download_file(catch_url, gpkg_local):
            if os.path.exists(hand_cache):
                os.remove(hand_cache)
            return None

    # ── Rasterize catchments → GeoTIFF ─────────────────────────────────────
    if not _rasterize_catchments(gpkg_local, hand_cache, catch_cache):
        # Keep the gpkg for a retry later; clean up partial tif
        if os.path.exists(catch_cache):
            os.remove(catch_cache)
        if os.path.exists(hand_cache):
            os.remove(hand_cache)
        return None

    # GeoPackage no longer needed after rasterization (saves ~200 MB/HUC8)
    try:
        os.remove(gpkg_local)
    except OSError:
        pass

    return hand_cache, catch_cache


# ── Multi-HUC8 mosaic ────────────────────────────────────────────────────────

def _mosaic_to_cell(
    huc8_rasters: List[Tuple[str, str]],   # [(hand_path, catch_path), ...]
    lon_min: float, lat_min: float, lon_max: float, lat_max: float,
    output_dir: str,
    cell_tag: str,
) -> Optional[Tuple[str, str]]:
    """
    Mosaic and clip multiple HUC8 HAND/catchment rasters to a cell extent.
    Returns (hand_cell_path, catchment_cell_path) or None on failure.
    """
    try:
        import rasterio
        from rasterio.merge import merge
        from rasterio.mask import mask as rmask
        from shapely.geometry import box
        import shapely

        hand_out  = os.path.join(output_dir, f"hand_cell_{cell_tag}.tif")
        catch_out = os.path.join(output_dir, f"catchments_cell_{cell_tag}.tif")
        bbox_geom = [box(lon_min, lat_min, lon_max, lat_max).__geo_interface__]

        for paths, out_path in [
            ([h for h, _ in huc8_rasters], hand_out),
            ([c for _, c in huc8_rasters], catch_out),
        ]:
            srcs = [rasterio.open(p) for p in paths]
            try:
                if len(srcs) == 1:
                    mosaic, mo_transform = srcs[0].read(), srcs[0].transform
                    mo_profile = srcs[0].profile.copy()
                    # Clip to cell bbox
                    clipped, clip_transform = rmask(
                        srcs[0], bbox_geom, crop=True, nodata=srcs[0].nodata
                    )
                    mo_profile.update(
                        height=clipped.shape[1], width=clipped.shape[2],
                        transform=clip_transform,
                    )
                    with rasterio.open(out_path, "w", **mo_profile) as dst:
                        dst.write(clipped)
                else:
                    mosaic, mo_transform = merge(srcs)
                    mo_profile = srcs[0].profile.copy()
                    mo_profile.update(
                        height=mosaic.shape[1], width=mosaic.shape[2],
                        transform=mo_transform,
                    )
                    # Write mosaic then re-open to clip
                    tmp = out_path + ".mosaic.tif"
                    with rasterio.open(tmp, "w", **mo_profile) as dst:
                        dst.write(mosaic)
                    with rasterio.open(tmp) as mo_src:
                        clipped, clip_transform = rmask(
                            mo_src, bbox_geom, crop=True, nodata=mo_src.nodata
                        )
                        mo_profile.update(
                            height=clipped.shape[1], width=clipped.shape[2],
                            transform=clip_transform,
                        )
                        with rasterio.open(out_path, "w", **mo_profile) as dst:
                            dst.write(clipped)
                    os.remove(tmp)
            finally:
                for s in srcs:
                    s.close()

        logger.info("[CFIM] Cell rasters written: %s, %s", hand_out, catch_out)
        return hand_out, catch_out

    except Exception as exc:
        logger.warning("[CFIM] Mosaic/clip failed: %s", exc)
        return None


# ── Public API ───────────────────────────────────────────────────────────────

def get_hand_for_cell(
    lon_min: float,
    lat_min: float,
    lon_max: float,
    lat_max: float,
    hand_cache_dir: str,
    col: int,
    row: int,
    storm_id: str,
) -> Optional[HANDFiles]:
    """
    Return HAND + catchment rasters clipped to a cell bounding box.

    Downloads and caches HUC8 rasters on first use (tens of MB per HUC8,
    kept permanently).  Cell-level mosaics are kept per-storm in the cell
    cache alongside surge/damage files.

    Returns None if the data is not available for any HUC8 in the cell.
    """
    cell_tag = f"{storm_id}_{col}_{row}"

    # Cell-level mosaics
    cell_dir  = os.path.join(hand_cache_dir, "_cells", storm_id)
    os.makedirs(cell_dir, exist_ok=True)
    hand_cell  = os.path.join(cell_dir, f"hand_cell_{cell_tag}.tif")
    catch_cell = os.path.join(cell_dir, f"catchments_cell_{cell_tag}.tif")

    if os.path.exists(hand_cell) and os.path.exists(catch_cell):
        logger.info("[CFIM] Cell (%d,%d) HAND cache hit", col, row)
        return HANDFiles(
            hand_path=hand_cell,
            catchment_path=catch_cell,
            huc8s=[],
            from_cache=True,
        )

    # Find overlapping HUC8s
    huc8s = get_huc8s_for_bbox(lon_min, lat_min, lon_max, lat_max)
    if not huc8s:
        logger.info("[CFIM] No HUC8s found for cell (%d,%d) bbox", col, row)
        return None

    # Download/cache per-HUC8 rasters
    huc8_pairs: List[Tuple[str, str]] = []
    for huc8 in huc8s:
        huc8_dir = os.path.join(hand_cache_dir, huc8)
        result = _fetch_huc8(huc8, huc8_dir)
        if result is not None:
            huc8_pairs.append(result)
        time.sleep(0.1)  # polite delay between HUC8 downloads

    if not huc8_pairs:
        logger.info("[CFIM] No HAND data available for cell (%d,%d)", col, row)
        return None

    # Mosaic + clip to cell extent
    paths = _mosaic_to_cell(
        huc8_pairs, lon_min, lat_min, lon_max, lat_max, cell_dir, cell_tag
    )
    if paths is None:
        return None

    hand_path, catch_path = paths
    return HANDFiles(
        hand_path=hand_path,
        catchment_path=catch_path,
        huc8s=huc8s,
        from_cache=False,
    )
