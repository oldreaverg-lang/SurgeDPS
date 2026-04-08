"""
Pre-warm center cell cache for all storms visible in the sidebar.

Generates the surge raster, flood GeoJSON, building data, and damage
model output for cell (0,0) of each storm so the first user activation
is instant instead of a 2+ minute cold-start.

Run at deploy time before starting the API server:
    python scripts/warm_cache.py && python scripts/api_server.py

The script is idempotent — if a storm's cache already exists it is
skipped, so re-deploys only generate new/missing storms.
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


def _is_cached(storm: StormEntry) -> bool:
    """Check if center cell already has both damage and flood GeoJSON."""
    sdir = _storm_cache_dir(storm)
    return (
        os.path.exists(os.path.join(sdir, 'cell_0_0_damage.geojson')) and
        os.path.exists(os.path.join(sdir, 'cell_0_0_flood.geojson'))
    )


def warm_storm(storm: StormEntry) -> bool:
    """Generate center cell data for a single storm. Returns True on success."""
    sdir = _storm_cache_dir(storm)

    origin_lon = storm.grid_origin_lon
    origin_lat = storm.grid_origin_lat
    lon_min = origin_lon
    lat_min = origin_lat
    lon_max = origin_lon + CELL_WIDTH
    lat_max = origin_lat + CELL_HEIGHT

    try:
        # 1. Surge raster
        raster_path = os.path.join(sdir, 'cell_0_0_depth.tif')
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
        flood_path = os.path.join(sdir, 'cell_0_0_flood.geojson')
        if not os.path.exists(flood_path):
            raster_to_geojson(raster_path, flood_path)

        # 3. OSM buildings
        buildings_path = os.path.join(sdir, 'cell_0_0_buildings.json')
        fetch_buildings(lon_min, lat_min, lon_max, lat_max, buildings_path, cache=True)

        with open(buildings_path) as f:
            buildings_data = json.load(f)

        # 4. HAZUS damage model
        damage_path = os.path.join(sdir, 'cell_0_0_damage.geojson')
        if buildings_data.get('features'):
            estimate_damage_from_raster(raster_path, buildings_path, damage_path)
        else:
            with open(damage_path, 'w') as f:
                json.dump({"type": "FeatureCollection", "features": []}, f)

        return True

    except Exception as e:
        print(f"  ERROR warming {storm.storm_id}: {e}")
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
    print(f"Found {len(storms)} storms in sidebar")

    cached = 0
    generated = 0
    failed = 0
    t0 = time.time()

    for i, storm in enumerate(storms, 1):
        tag = f"[{i}/{len(storms)}] {storm.storm_id}"
        if _is_cached(storm):
            print(f"  {tag} — cached, skipping")
            cached += 1
            continue

        print(f"  {tag} — generating center cell...")
        t1 = time.time()
        ok = warm_storm(storm)
        elapsed = time.time() - t1

        if ok:
            print(f"  {tag} — done ({elapsed:.1f}s)")
            generated += 1
        else:
            failed += 1

    total = time.time() - t0
    print()
    print(f"Warm-up complete in {total:.0f}s")
    print(f"  {cached} already cached")
    print(f"  {generated} newly generated")
    print(f"  {failed} failed")
    print("=" * 60)


if __name__ == '__main__':
    main()
