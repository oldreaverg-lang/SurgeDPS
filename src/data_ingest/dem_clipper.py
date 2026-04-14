"""
DEM Clipper

Clips pre-processed Cloud Optimized GeoTIFF DEMs to the storm
processing extent (cone of uncertainty + buffer). Uses rasterio's
windowed reading to only fetch the pixels we need from S3.

This is the most important cost-saving step in the pipeline:
by clipping to the storm area, we avoid processing the entire
coastline on every storm.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import List, Optional, Tuple

import numpy as np

from .config import IngestConfig

logger = logging.getLogger(__name__)


@dataclass
class ClippedDEM:
    """Result of a DEM clip operation."""

    path: str               # Local path to the clipped GeoTIFF
    bounds: Tuple[float, float, float, float]  # (west, south, east, north)
    crs: str                # CRS of the output raster
    resolution: float       # Cell size in CRS units (meters for EPSG:5070)
    shape: Tuple[int, int]  # (rows, cols)
    nodata: float           # NoData value
    s3_key: Optional[str] = None


class DEMClipper:
    """
    Clips DEM tiles to a storm processing extent.

    In production, reads COGs from S3 via GDAL's /vsis3/ driver.
    In dry-run/dev mode, works with local files.
    """

    def __init__(self, config: IngestConfig, s3_client=None):
        self.config = config
        self.s3 = s3_client

    def clip_to_extent(
        self,
        storm_geometry: dict,
        output_dir: str,
        resolution: str = "10m",
        storm_id: str = "unknown",
        advisory_num: str = "000",
    ) -> ClippedDEM:
        """
        Clip DEM to the storm extent polygon.

        Args:
            storm_geometry: GeoJSON geometry of the storm cone (buffered)
            output_dir: Directory to write the clipped DEM
            resolution: "10m" or "1m"
            storm_id: For naming the output file
            advisory_num: For naming the output file

        Returns:
            ClippedDEM with path and metadata
        """
        import rasterio
        from rasterio.mask import mask as rasterio_mask
        from rasterio.warp import calculate_default_transform, reproject
        from shapely.geometry import shape, mapping
        from shapely.ops import transform as shapely_transform
        import pyproj

        logger.info(
            f"Clipping {resolution} DEM for {storm_id} advisory {advisory_num}"
        )

        # Buffer the storm geometry
        storm_shape = shape(storm_geometry)
        buffered = storm_shape.buffer(self.config.cone_buffer_km / 111.0)
        bounds = buffered.bounds  # (minx, miny, maxx, maxy) in EPSG:4326

        logger.info(
            f"Processing extent: {bounds[0]:.2f}W, {bounds[1]:.2f}S, "
            f"{bounds[2]:.2f}E, {bounds[3]:.2f}N"
        )

        # Determine DEM source path(s)
        dem_prefix = (
            self.config.dem_10m_prefix
            if resolution == "10m"
            else self.config.dem_1m_prefix
        )

        # For S3-based COGs, use /vsis3/ GDAL virtual filesystem
        # For local dev, use local paths
        dem_vrt_path = self._build_dem_vrt(dem_prefix, bounds, output_dir)

        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(
            output_dir,
            f"dem_{resolution}_{storm_id}_{advisory_num}.tif",
        )

        # Clip and reproject to model CRS
        with rasterio.open(dem_vrt_path) as src:
            # Create clip geometry in the DEM's CRS
            if str(src.crs) != "EPSG:4326":
                transformer = pyproj.Transformer.from_crs(
                    "EPSG:4326", src.crs, always_xy=True
                )
                clip_geom = shapely_transform(transformer.transform, buffered)
            else:
                clip_geom = buffered

            # Mask (clip) to storm extent.
            # `src.nodata or -9999` collapses nodata=0 to -9999 — for DEMs
            # that declare 0 as nodata this wrongly treats sea-level cells
            # as valid elevations. Respect the source's declared nodata.
            _dem_nodata = src.nodata if src.nodata is not None else -9999
            out_image, out_transform = rasterio_mask(
                src,
                [mapping(clip_geom)],
                crop=True,
                nodata=_dem_nodata,
                filled=True,
            )

            # Calculate transform for reprojection to model CRS
            dst_crs = self.config.model_crs
            dst_transform, dst_width, dst_height = calculate_default_transform(
                src.crs,
                dst_crs,
                out_image.shape[2],
                out_image.shape[1],
                *rasterio.transform.array_bounds(
                    out_image.shape[1],
                    out_image.shape[2],
                    out_transform,
                ),
            )

            # Write reprojected clipped DEM
            profile = src.profile.copy()
            profile.update(
                driver="GTiff",
                crs=dst_crs,
                transform=dst_transform,
                width=dst_width,
                height=dst_height,
                nodata=_dem_nodata,
                compress="deflate",
                tiled=True,
                blockxsize=256,
                blockysize=256,
            )

            # Pre-fill with nodata instead of np.empty — cells outside the
            # reprojection source footprint otherwise keep uninitialized
            # float bytes that can read as extreme elevations downstream.
            dst_data = np.full(
                (src.count, dst_height, dst_width),
                _dem_nodata,
                dtype=src.dtypes[0],
            )

            reproject(
                source=out_image,
                destination=dst_data,
                src_transform=out_transform,
                src_crs=src.crs,
                dst_transform=dst_transform,
                dst_crs=dst_crs,
                src_nodata=_dem_nodata,
                dst_nodata=_dem_nodata,
            )

            with rasterio.open(output_path, "w", **profile) as dst:
                dst.write(dst_data)

        # Get output metadata
        with rasterio.open(output_path) as out:
            result_bounds = out.bounds
            result_res = out.res[0]
            result_shape = (out.height, out.width)
            result_nodata = out.nodata

        logger.info(
            f"Clipped DEM written: {output_path} "
            f"({result_shape[1]}x{result_shape[0]} px, "
            f"{result_res:.1f}m resolution)"
        )

        # Upload to S3 if available
        s3_key = None
        if self.s3 and not self.config.dry_run:
            s3_key = (
                f"{self.config.storm_output_prefix}{storm_id}/"
                f"advisory_{advisory_num}/dem/{os.path.basename(output_path)}"
            )
            self.s3.upload_file(output_path, self.config.data_bucket, s3_key)
            logger.info(f"Uploaded DEM to s3://{self.config.data_bucket}/{s3_key}")

        return ClippedDEM(
            path=output_path,
            bounds=(
                result_bounds.left,
                result_bounds.bottom,
                result_bounds.right,
                result_bounds.top,
            ),
            crs=self.config.model_crs,
            resolution=result_res,
            shape=result_shape,
            nodata=result_nodata,
            s3_key=s3_key,
        )

    def _build_dem_vrt(
        self,
        prefix: str,
        bounds: Tuple[float, float, float, float],
        output_dir: str,
    ) -> str:
        """
        Build a GDAL VRT (Virtual Raster) referencing only the COG tiles
        that intersect the storm bounds.

        In production, this queries an S3 spatial index or tile grid.
        For development, it uses local files.
        """
        from osgeo import gdal

        os.makedirs(output_dir, exist_ok=True)
        vrt_path = os.path.join(output_dir, "dem_mosaic.vrt")

        # In production: list S3 COG tiles that intersect bounds
        # For now: if local files exist, use them; otherwise create synthetic
        tile_paths = self._find_tiles_for_bounds(prefix, bounds)

        if not tile_paths:
            logger.warning(
                "No DEM tiles found for extent — generating synthetic DEM"
            )
            synth_path = self._generate_synthetic_dem(bounds, output_dir)
            return synth_path

        # Build VRT from matching tiles
        gdal.BuildVRT(vrt_path, tile_paths)
        logger.info(f"Built VRT from {len(tile_paths)} tiles: {vrt_path}")
        return vrt_path

    def _find_tiles_for_bounds(
        self,
        prefix: str,
        bounds: Tuple[float, float, float, float],
    ) -> List[str]:
        """
        Find DEM tile paths that intersect the given bounds.

        Production: query S3 listing or a spatial index.
        Development: scan local directory.
        """
        # Check for local tiles
        local_dir = os.path.join(self.config.scratch_dir, "base_dem")
        if os.path.isdir(local_dir):
            import glob

            all_tiles = glob.glob(os.path.join(local_dir, "*.tif"))
            return self._filter_tiles_by_bounds(all_tiles, bounds)

        # S3-based lookup
        if self.s3 and not self.config.dry_run:
            try:
                response = self.s3.list_objects_v2(
                    Bucket=self.config.data_bucket,
                    Prefix=prefix,
                    MaxKeys=1000,
                )
                all_tiles = []
                for obj in response.get("Contents", []):
                    key = obj["Key"]
                    if key.endswith(".tif"):
                        s3_path = (
                            f"/vsis3/{self.config.data_bucket}/{key}"
                        )
                        all_tiles.append(s3_path)
                return self._filter_tiles_by_bounds(all_tiles, bounds)
            except Exception as e:
                logger.error(f"S3 tile listing failed: {e}")

        return []

    def _filter_tiles_by_bounds(
        self,
        tile_paths: List[str],
        query_bounds: Tuple[float, float, float, float],
    ) -> List[str]:
        """
        Filter tile paths to only those intersecting the query bounds.

        Uses two strategies:
            1. Parse tile grid coordinates from filename
               (e.g., "n29w083_10m.tif" → 29N, 83W)
            2. Fall back to reading GeoTIFF bounds via rasterio
        """
        if not tile_paths:
            return []

        qw, qs, qe, qn = query_bounds
        matching = []

        for path in tile_paths:
            fname = os.path.basename(path).lower()

            # Strategy 1: Parse USGS 3DEP tile naming convention
            # Format: nYYwXXX (e.g., n29w083) or nYYeXXX
            tile_bounds = self._parse_tile_name(fname)
            if tile_bounds:
                tw, ts, te, tn = tile_bounds
                # Check intersection
                if tw < qe and te > qw and ts < qn and tn > qs:
                    matching.append(path)
                continue

            # Strategy 2: Read bounds from GeoTIFF header
            # (works for any naming convention, slower for S3/vsis3)
            try:
                import rasterio
                with rasterio.open(path) as src:
                    tb = src.bounds
                    if tb.left < qe and tb.right > qw and tb.bottom < qn and tb.top > qs:
                        matching.append(path)
            except Exception:
                # If we can't check, include it to be safe
                matching.append(path)

        logger.info(
            f"Filtered {len(tile_paths)} tiles to {len(matching)} "
            f"intersecting query bounds"
        )
        return matching

    @staticmethod
    def _parse_tile_name(
        filename: str,
    ) -> Optional[Tuple[float, float, float, float]]:
        """
        Parse USGS 3DEP tile name to extract bounding box.

        Supports formats:
            n29w083_10m.tif → (lat=29, lon=-83, 1°x1° tile)
            USGS_13_n29w083.tif → same
        """
        import re

        match = re.search(r'n(\d{2,3})([ew])(\d{2,3})', filename)
        if not match:
            return None

        lat = float(match.group(1))
        lon_dir = match.group(2)
        lon = float(match.group(3))

        if lon_dir == 'w':
            lon = -lon

        # USGS 3DEP tiles are typically 1°x1°
        return (lon - 1.0, lat - 1.0, lon, lat)

    def _fetch_3dep_dem(
        self,
        bounds: Tuple[float, float, float, float],
        output_dir: str,
        max_pixels: int = 2000,
    ) -> Optional[str]:
        """
        Fetch a real DEM from USGS 3DEP via the TNM ImageServer WCS endpoint.

        The National Map (TNM) 3DEP 1/3 arc-second (~10m) elevation service
        returns a Cloud-Optimized GeoTIFF for any CONUS bounding box.
        No authentication required.

        Endpoint:
            https://elevation.nationalmap.gov/arcgis/rest/services/
            3DEPElevation/ImageServer/exportImage

        Args:
            bounds: (west, south, east, north) in EPSG:4326 decimal degrees.
            output_dir: Directory to write output GeoTIFF.
            max_pixels: Maximum image dimension (width or height).
                        TNM caps at 4096; we default to 2000 for speed.

        Returns:
            Path to downloaded GeoTIFF, or None if fetch fails.
        """
        import urllib.request
        import urllib.parse

        west, south, east, north = bounds

        # Compute pixel dimensions at ~10m (1/3 arc-second ≈ 0.0000926°)
        # but cap at max_pixels to avoid huge downloads
        deg_per_pixel = 0.0000926  # 1/3 arc-second
        width  = int(min((east - west)  / deg_per_pixel, max_pixels))
        height = int(min((north - south) / deg_per_pixel, max_pixels))
        width  = max(width, 64)
        height = max(height, 64)

        bbox = f"{west},{south},{east},{north}"
        params = urllib.parse.urlencode({
            "bbox":       bbox,
            "bboxSR":     "4326",
            "size":       f"{width},{height}",
            "imageSR":    "4326",
            "format":     "tiff",
            "pixelType":  "F32",
            "noData":     "-9999",
            "noDataInterpretation": "esriNoDataMatchAny",
            "interpolation": "RSP_BilinearInterpolation",
            "f":          "image",
        })
        url = (
            "https://elevation.nationalmap.gov/arcgis/rest/services/"
            f"3DEPElevation/ImageServer/exportImage?{params}"
        )

        logger.info("[3DEP] Fetching %dx%d px DEM for bbox %s", width, height, bbox)

        output_path = os.path.join(output_dir, "dem_3dep.tif")
        try:
            req = urllib.request.Request(
                url,
                headers={"User-Agent": "SurgeDPS/1.0 (elevation data)"},
            )
            with urllib.request.urlopen(req, timeout=60) as resp:
                raw = resp.read()

            # Verify it's a GeoTIFF (starts with TIFF magic bytes)
            if len(raw) < 100 or raw[:4] not in (b"II\x2A\x00", b"MM\x00\x2A",
                                                   b"II\x2B\x00", b"MM\x00\x2B"):
                logger.warning(
                    "[3DEP] Response doesn't look like a TIFF (%d bytes, "
                    "header: %s)", len(raw), raw[:20]
                )
                return None

            with open(output_path, "wb") as f:
                f.write(raw)

            logger.info(
                "[3DEP] Downloaded DEM: %s (%d bytes, %dx%d px)",
                output_path, len(raw), width, height,
            )
            return output_path

        except Exception as exc:
            logger.warning("[3DEP] Fetch failed: %s", exc)
            return None

    def _generate_synthetic_dem(
        self,
        bounds: Tuple[float, float, float, float],
        output_dir: str,
    ) -> str:
        """
        Generate a synthetic DEM as a last-resort fallback.

        First tries to fetch a real DEM from USGS 3DEP.  Only falls back to
        the synthetic terrain if 3DEP is unreachable (e.g., during testing).

        Creates a gentle slope from coast inland with some noise,
        representative of flat Gulf Coast terrain.
        """
        import rasterio
        from rasterio.transform import from_bounds

        # ── Attempt real 3DEP fetch first ────────────────────────────────────
        real_path = self._fetch_3dep_dem(bounds, output_dir)
        if real_path:
            return real_path

        # ── Fallback: synthetic DEM for dev / offline environments ───────────
        logger.warning(
            "[DEM] 3DEP unavailable — falling back to synthetic DEM "
            "(results will be approximate)"
        )

        # 10m resolution in degrees (approximate)
        res = 0.0001  # ~11m
        west, south, east, north = bounds

        width = int((east - west) / res)
        height = int((north - south) / res)

        # Clamp to reasonable size for dev
        max_pixels = 2000
        if width > max_pixels:
            scale = max_pixels / width
            width = max_pixels
            height = int(height * scale)

        # Generate terrain: coastal areas low, inland higher
        # Simple west-to-east elevation gradient with noise
        x = np.linspace(0, 1, width)
        y = np.linspace(0, 1, height)
        xx, yy = np.meshgrid(x, y)

        # Base elevation: 0-10m, increasing eastward (inland)
        elevation = xx * 8.0 + yy * 2.0

        # Add some noise for realistic terrain
        rng = np.random.default_rng(42)
        elevation += rng.normal(0, 0.5, elevation.shape)

        # Coastal areas (west edge) at or below sea level
        elevation[:, :int(width * 0.1)] = np.clip(
            elevation[:, :int(width * 0.1)] - 2.0, -1.0, 2.0
        )

        elevation = elevation.astype(np.float32)

        transform = from_bounds(west, south, east, north, width, height)
        output_path = os.path.join(output_dir, "synthetic_dem.tif")

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
            dst.write(elevation, 1)

        logger.info(
            "[DEM] Synthetic DEM: %s (%dx%d px, %.2f to %.2f lon)",
            output_path, width, height, west, east,
        )
        return output_path
