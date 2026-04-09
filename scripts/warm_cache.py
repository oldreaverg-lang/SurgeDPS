"""
Pre-warm 3×3 grid cache for all storms visible in the sidebar.

Generates the surge raster, flood GeoJSON, building data, and damage
model output for the full 3×3 grid (9 cells) around each storm's
landfall so the initial view is fully loaded on first activation.

Run at deploy time (background process alongside the API server):
    python scripts/warm_cache.py &
    python scripts/api_server.py

The script is idempotent — already-cached cells are skipped, so
re-deploys only generate new/missing data.
"""

import json
import os
import sys
import time

# ── Path setup (same as api_server.py) ──
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(os.path.join(BASE_DIR, 'src'))

from storm_catalog.catalog import (
    StormEntry, CELL_WIDTH, CELL_HEIGHT, HISTORICAL_STORMS,
)
from storm_catalog.hurdat2_parser import (
    get_seasons, get_storms_for_year,
)
from storm_catalog.surge_model import generate_surge_raster
from tile_gen.pmtiles_builder import raster_to_geojson
from data_ingest.building_fetcher import fetch_buildings
from damage_model.depth_damage import estimate_damage_from_raster

CACHE_DIR = os.path.join(BASE_DIR, 'tmp_integration', 'cells')
os.makedirs(CACHE_DIR, exist_ok=True)

# Season accordion cutoff — must match api_server.py
SEASON_MIN_YEAR = 2015


def _storm_cache_dir(storm: StormEntry) -> str:
    d = os.path.join(CACHE_DIR, storm.storm_id)
    os.makedirs(d, exist_ok=True)
    return d


# 3×3 grid: all cells visible on the default zoom level
WARM_CELLS = [
    (col, row)
    for row in range(-1, 2)
    for col in range(-1, 2)
]


def _cached_cells(storm: StormEntry) -> set[tuple[int, int]]:
    """Return which of the 3×3 cells are already cached."""
    sdir = _storm_cache_dir(storm)
    cached = set()
    for col, row in WARM_CELLS:
        if (os.path.exists(os.path.join(sdir, f'cell_{col}_{row}_damage.geojson')) and
                os.path.exists(os.path.join(sdir, f'cell_{col}_{row}_flood.geojson'))):
            cached.add((col, row))
    return cached


def warm_cell(storm: StormEntry, col: int, row: int) -> bool:
    """Generate data for a single cell of a storm. Returns True on success."""
    sdir = _storm_cache_dir(storm)

    origin_lon = storm.grid_origin_lon
    origin_lat = storm.grid_origin_lat
    lon_min = origin_lon + col * CELL_WIDTH
    lat_min = origin_lat + row * CELL_HEIGHT
    lon_max = lon_min + CELL_WIDTH
    lat_max = lat_min + CELL_HEIGHT

    try:
        # 1. Surge raster
        raster_path = os.path.join(sdir, f'cell_{col}_{row}_depth.tif')
        if not os.path.exists(raster_path):
            generate_surge_raster(
                lon_min=lon_min, lat_min=lat_min,
                lon_max=lon_max, lat_max=lat_max,
                output_path=raster_path,
                landfall_lon=storm.landfall_lon,
                landfall_lat=storm.landfall_lat,
                max_wind_kt=storm.max_wind_kt,
                min_pressure_mb=storm.min_pressure_mb,
                heading_deg=storm.heading_deg,
                speed_kt=storm.speed_kt,
            )

        # 2. Flood polygons
        flood_path = os.path.join(sdir, f'cell_{col}_{row}_flood.geojson')
        if not os.path.exists(flood_path):
            raster_to_geojson(raster_path, flood_path)

        # 3. OSM buildings
        buildings_path = os.path.join(sdir, f'cell_{col}_{row}_buildings.json')
        fetch_buildings(lon_min, lat_min, lon_max, lat_max, buildings_path, cache=True)

        with open(buildings_path) as f:
            buildings_data = json.load(f)

        # 4. HAZUS damage model
        damage_path = os.path.join(sdir, f'cell_{col}_{row}_damage.geojson')
        if buildings_data.get('features'):
            estimate_damage_from_raster(raster_path, buildings_path, damage_path)
        else:
            with open(damage_path, 'w') as f:
                json.dump({"type": "FeatureCollection", "features": []}, f)

        return True

    except Exception as e:
        print(f"    ERROR cell ({col},{row}): {e}")
        return False


def collect_sidebar_storms() -> list[StormEntry]:
    """
    Collect every storm that appears in the sidebar accordion:
      - Historic Storms (curated)
      - Season-by-season (2015+)
    De-duplicates by storm_id.
    """
    seen: set[str] = set()
    storms: list[StormEntry] = []

    # Curated historic storms
    for s in HISTORICAL_STORMS:
        if s.storm_id not in seen:
            seen.add(s.storm_id)
            storms.append(s)

    # Season accordion (2015+)
    try:
        seasons = get_seasons(min_year=SEASON_MIN_YEAR)
        for season in seasons:
            year = season['year']
            year_storms = get_storms_for_year(year)
            for s in year_storms:
                if s.storm_id not in seen:
                    seen.add(s.storm_id)
                    storms.append(s)
    except Exception as e:
        print(f"WARNING: Could not load HURDAT2 seasons: {e}")

    return storms


def main():
    print("=" * 60)
    print("SurgeDPS Cache Warmer")
    print("=" * 60)

    storms = collect_sidebar_storms()
    total_cells = len(storms) * len(WARM_CELLS)
    print(f"Found {len(storms)} storms × {len(WARM_CELLS)} cells = {total_cells} cells to warm")

    storms_cached = 0
    cells_generated = 0
    cells_failed = 0
    t0 = time.time()

    for i, storm in enumerate(storms, 1):
        tag = f"[{i}/{len(storms)}] {storm.storm_id}"
        already = _cached_cells(storm)

        if len(already) == len(WARM_CELLS):
            print(f"  {tag} — all 9 cells cached, skipping")
            storms_cached += 1
            continue

        missing = [c for c in WARM_CELLS if c not in already]
        print(f"  {tag} — generating {len(missing)} cell(s) ({len(already)} cached)...")
        t1 = time.time()

        for col, row in missing:
            ok = warm_cell(storm, col, row)
            if ok:
                cells_generated += 1
            else:
                cells_failed += 1

        elapsed = time.time() - t1
        print(f"  {tag} — done ({elapsed:.1f}s)")

    total = time.time() - t0
    print()
    print(f"Warm-up complete in {total:.0f}s")
    print(f"  {storms_cached} storms fully cached (skipped)")
    print(f"  {cells_generated} cells newly generated")
    print(f"  {cells_failed} cells failed")
    print("=" * 60)


if __name__ == '__main__':
    main()
