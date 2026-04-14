"""
build_landuse_pmtiles.py — offline bake for the SurgeDPS land-use overlay.

The runtime default is to serve NLCD 2021 live from the USGS/MRLC WMS
(`https://www.mrlc.gov/geoserver/...`). That ships with zero bake work,
but the raster has to paint on every pan/zoom and the MRLC geoserver
occasionally hiccups. This script produces an offline-friendly
PMTiles file with the same data — optional, drop it at
`ui/public/landuse.pmtiles` and flip the frontend source over to
the pmtiles protocol.

Pipeline
────────
  1. Download NLCD 2021 Land Cover (L48) GeoTIFF — one-time, ~1.6 GB.
  2. Clip to the coastal-states bbox used by the rest of SurgeDPS
     (everywhere a storm can land → 18 states + DC).
  3. Reclassify the 16 Anderson Level II classes down to the 5 that
     emergency managers plan around:
        1 = Developed           (NLCD 21, 22, 23, 24)
        2 = Agriculture         (NLCD 81, 82)
        3 = Forest / Wetland    (NLCD 41-43, 90, 95)
        4 = Open / Shrub / Grass (NLCD 31, 51-52, 71-74)
        5 = Water               (NLCD 11, 12)
  4. Polygonize the classified raster with gdal_polygonize.
  5. Simplify geometries (topology-preserving) at 0.0003° ≈ 30 m.
  6. Write GeoJSON then convert to PMTiles with `tippecanoe`:
        tippecanoe -zg --drop-densest-as-needed --coalesce \
                   --layer=landuse -o landuse.pmtiles landuse.geojson

Runtime expectations
────────────────────
  Download:       ~5 min on a 100 Mbps connection.
  Clip + reclass: ~2 min  (needs `rasterio` + `numpy`).
  Polygonize:     30-90 min on a modern laptop (this is the long step).
  Tippecanoe:     ~5 min  for the whole coastal bake.
  Output size:    ~80-150 MB PMTiles depending on simplification.

Dependencies
────────────
  pip install --break-system-packages rasterio shapely fiona requests
  brew install gdal tippecanoe   # or apt-get install gdal-bin tippecanoe

Usage
─────
  python scripts/build_landuse_pmtiles.py \
      --out-dir ui/public/ \
      --bbox "-100.0,24.0,-66.0,48.0"   # coastal CONUS default

  Drop-in to the frontend:
      src/App.tsx — swap the `tiles=[...MRLC WMS...]` block for:
          <Source id="nlcd-landuse" type="raster"
                  url="pmtiles://./landuse.pmtiles"
                  tileSize={256} />

This script is intentionally not wired into CI. It's a one-off bake
that runs when NLCD publishes a new year (2021 → 2024 next).
"""

from __future__ import annotations

import argparse
import logging
import os
import subprocess
import sys
from pathlib import Path

logger = logging.getLogger(__name__)

# ── Source URLs ──────────────────────────────────────────────────────
NLCD_2021_URL = (
    "https://s3-us-west-2.amazonaws.com/mrlc/"
    "nlcd_2021_land_cover_l48_20230630.zip"
)

# ── Reclassification map: NLCD 16-class → SurgeDPS 5-class ───────────
# Each tuple is (NLCD values, new class code, display label, hex color).
LANDUSE_SCHEMA = [
    ((21, 22, 23, 24),       1, "Developed",         "#C73A3A"),
    ((81, 82),               2, "Agriculture",       "#DCD939"),
    ((41, 42, 43, 90, 95),   3, "Forest / Wetland",  "#498A4C"),
    ((31, 51, 52, 71, 72, 73, 74),
                             4, "Open / Shrub",      "#CEC5A1"),
    ((11, 12),               5, "Water",             "#5475A8"),
]


def download_nlcd(cache_dir: Path) -> Path:
    """Fetch the CONUS NLCD 2021 zip and return the path to the .tif inside."""
    cache_dir.mkdir(parents=True, exist_ok=True)
    tif_path = cache_dir / "nlcd_2021_land_cover_l48.tif"
    if tif_path.exists():
        logger.info("[NLCD] Using cached raster at %s", tif_path)
        return tif_path

    zip_path = cache_dir / "nlcd_2021.zip"
    if not zip_path.exists():
        logger.info("[NLCD] Downloading NLCD 2021 (~1.6 GB) from %s", NLCD_2021_URL)
        import requests
        with requests.get(NLCD_2021_URL, stream=True, timeout=600) as r:
            r.raise_for_status()
            with open(zip_path, "wb") as fh:
                for chunk in r.iter_content(chunk_size=1 << 20):
                    fh.write(chunk)

    logger.info("[NLCD] Unzipping to %s", cache_dir)
    import zipfile
    with zipfile.ZipFile(zip_path) as z:
        z.extractall(cache_dir)

    candidates = list(cache_dir.glob("**/*.tif"))
    if not candidates:
        raise RuntimeError("NLCD tif not found after unzip")
    candidates[0].rename(tif_path)
    return tif_path


def clip_and_reclassify(src_tif: Path, out_tif: Path, bbox: tuple[float, float, float, float]) -> None:
    """Clip to `bbox` (lon/lat) and collapse 16 classes down to 5."""
    import numpy as np
    import rasterio
    from rasterio.windows import from_bounds

    # NLCD is EPSG:5070 (Albers). We clip by transforming the lon/lat bbox
    # through the raster CRS.
    from rasterio.warp import transform_bounds

    with rasterio.open(src_tif) as src:
        bounds = transform_bounds("EPSG:4326", src.crs, *bbox, densify_pts=21)
        window = from_bounds(*bounds, transform=src.transform).round_offsets().round_lengths()
        data = src.read(1, window=window)
        transform = src.window_transform(window)

        # Build lookup: NLCD code (0-255) → our class
        lut = np.zeros(256, dtype=np.uint8)
        for nlcd_codes, new_code, _label, _color in LANDUSE_SCHEMA:
            for c in nlcd_codes:
                lut[c] = new_code
        reclassified = lut[data]

        profile = src.profile.copy()
        profile.update({
            "height": data.shape[0],
            "width": data.shape[1],
            "transform": transform,
            "dtype": "uint8",
            "nodata": 0,
            "compress": "deflate",
        })
        with rasterio.open(out_tif, "w", **profile) as dst:
            dst.write(reclassified, 1)

    logger.info("[NLCD] Reclassified raster written to %s", out_tif)


def polygonize(src_tif: Path, out_geojson: Path) -> None:
    """Vectorize the raster with gdal_polygonize.py."""
    logger.info("[NLCD] Polygonizing — this is the long step (30-90 min).")
    cmd = [
        "gdal_polygonize.py",
        str(src_tif),
        "-b", "1",
        "-f", "GeoJSON",
        str(out_geojson),
        "landuse",  # layer name
        "class",    # field name
    ]
    subprocess.run(cmd, check=True)


def to_pmtiles(geojson: Path, pmtiles: Path) -> None:
    """Pack GeoJSON into a PMTiles archive with tippecanoe defaults tuned
    for a mid-zoom land-cover overlay."""
    logger.info("[NLCD] Running tippecanoe → %s", pmtiles)
    cmd = [
        "tippecanoe",
        "-zg",                          # auto-choose max zoom
        "--minimum-zoom", "4",
        "--drop-densest-as-needed",
        "--coalesce-smallest-as-needed",
        "--simplification", "10",
        "--layer", "landuse",
        "--force",
        "-o", str(pmtiles),
        str(geojson),
    ]
    subprocess.run(cmd, check=True)


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out-dir", default="ui/public", help="Where to drop the final PMTiles.")
    parser.add_argument("--cache-dir", default=".cache/nlcd", help="Where to stash the raw NLCD raster.")
    parser.add_argument("--bbox", default="-100.0,24.0,-66.0,48.0",
                        help="lon_min,lat_min,lon_max,lat_max — coastal CONUS default.")
    parser.add_argument("--keep-intermediate", action="store_true",
                        help="Keep clipped.tif + landuse.geojson for debugging.")
    args = parser.parse_args()

    out_dir = Path(args.out_dir); out_dir.mkdir(parents=True, exist_ok=True)
    cache_dir = Path(args.cache_dir)
    bbox = tuple(float(x) for x in args.bbox.split(","))
    if len(bbox) != 4:
        parser.error("--bbox must be lon_min,lat_min,lon_max,lat_max")

    src_tif = download_nlcd(cache_dir)
    clipped_tif = cache_dir / "nlcd_reclass.tif"
    geojson_path = cache_dir / "landuse.geojson"
    pmtiles_path = out_dir / "landuse.pmtiles"

    clip_and_reclassify(src_tif, clipped_tif, bbox)
    polygonize(clipped_tif, geojson_path)
    to_pmtiles(geojson_path, pmtiles_path)

    if not args.keep_intermediate:
        for p in (clipped_tif, geojson_path):
            if p.exists():
                p.unlink()

    logger.info("[NLCD] Done. PMTiles → %s (%.1f MB)",
                pmtiles_path, pmtiles_path.stat().st_size / 1e6)
    return 0


if __name__ == "__main__":
    sys.exit(main())
