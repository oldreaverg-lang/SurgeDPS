"""
Integration Orchestration Script - Hurricane Ike Macro Simulation

Generates a surge raster across Galveston, TX, fetches real building
footprints from OpenStreetMap, and runs the full damage pipeline.

Usage:
    python scripts/build_frontend_assets.py              # real OSM buildings (default)
    python scripts/build_frontend_assets.py --synthetic   # original 5k random buildings
"""
import argparse
import json
import os
import shutil
import sys

import numpy as np

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))
from damage_model.depth_damage import estimate_damage_from_raster
from tile_gen.pmtiles_builder import generate_tiles_for_layer

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
UI_PUBLIC_DIR = os.path.join(BASE_DIR, 'ui', 'public')
TMP_DIR = os.environ.get('PERSISTENT_DATA_DIR', os.path.join(BASE_DIR, 'tmp_integration'))

# Galveston Bounding Box
LON_MIN, LAT_MIN = -95.0, 29.2
LON_MAX, LAT_MAX = -94.6, 29.5

os.makedirs(UI_PUBLIC_DIR, exist_ok=True)
os.makedirs(TMP_DIR, exist_ok=True)


def generate_surge_raster() -> str:
    """Generate simulated Hurricane Ike storm surge GeoTIFF."""
    import rasterio
    from rasterio.transform import from_bounds

    raster_path = os.path.join(TMP_DIR, 'ike_depth.tif')

    print("Generating simulated Hurricane Ike Surge Raster (160k points)...")
    rows, cols = 400, 400
    y = np.linspace(LAT_MIN, LAT_MAX, rows)
    x = np.linspace(LON_MIN, LON_MAX, cols)
    X, Y = np.meshgrid(x, y)

    # Surge model: deeply flooded in south-east (ocean/coast), fading north-west
    dist_from_coast = (Y - LAT_MIN) + (LON_MAX - X)
    depth = 7.0 - (dist_from_coast * 12.0) + np.random.normal(0, 0.4, (rows, cols))
    data = np.clip(depth, 0, 7.0).astype(np.float32)

    transform = from_bounds(LON_MIN, LAT_MIN, LON_MAX, LAT_MAX, cols, rows)
    data = np.flipud(data)

    with rasterio.open(
        raster_path, 'w', driver='GTiff', height=rows, width=cols,
        count=1, dtype=data.dtype, crs='+proj=latlong', transform=transform
    ) as dst:
        dst.write(data, 1)

    return raster_path


def generate_synthetic_buildings() -> str:
    """Generate 5,000 random building points (legacy fallback)."""
    buildings_path = os.path.join(TMP_DIR, 'ike_buildings.json')

    print("Generating 5,000 synthetic residential/commercial structures...")
    buildings = {"type": "FeatureCollection", "features": []}
    btypes = ["RES1-1SNB", "RES1-1SNB", "RES1-1SNB", "RES1-2SNB", "COM", "IND", "RES1-1SWB"]

    np.random.seed(42)
    points_lon = np.random.uniform(LON_MIN + 0.05, LON_MAX - 0.05, 5000)
    points_lat = np.random.uniform(LAT_MIN + 0.05, LAT_MAX - 0.05, 5000)
    points_types = np.random.choice(btypes, 5000)

    for i in range(5000):
        buildings["features"].append({
            "type": "Feature",
            "properties": {"id": f"Ike_{i}", "type": points_types[i]},
            "geometry": {"type": "Point", "coordinates": [float(points_lon[i]), float(points_lat[i])]}
        })

    with open(buildings_path, 'w') as f:
        json.dump(buildings, f)

    return buildings_path


def fetch_real_buildings() -> str:
    """Fetch actual building footprints from OpenStreetMap."""
    from data_ingest.building_fetcher import fetch_buildings

    buildings_path = os.path.join(TMP_DIR, 'ike_buildings_osm.json')

    print("Fetching real building footprints from OpenStreetMap...")
    fetch_buildings(
        lon_min=LON_MIN, lat_min=LAT_MIN,
        lon_max=LON_MAX, lat_max=LAT_MAX,
        output_path=buildings_path,
        cache=True,
    )
    return buildings_path


def main():
    parser = argparse.ArgumentParser(description="SurgeDPS Hurricane Ike Pipeline")
    parser.add_argument(
        '--synthetic', action='store_true',
        help='Use 5,000 random synthetic buildings instead of real OSM data'
    )
    args = parser.parse_args()

    print("=== Step 1: Simulating Hurricane Ike Surge Raster ===")
    raster_path = generate_surge_raster()

    print("\n=== Step 2: Loading Building Inventory ===")
    if args.synthetic:
        buildings_path = generate_synthetic_buildings()
    else:
        buildings_path = fetch_real_buildings()

    print("\n=== Step 3: Processing Buildings through DPI Engine ===")
    damage_output = os.path.join(UI_PUBLIC_DIR, 'buildings_damage.geojson')
    estimate_damage_from_raster(raster_path, buildings_path, damage_output)

    print("\n=== Step 4: Generating MapLibre Overlays ===")
    tiles_result = generate_tiles_for_layer(
        depth_raster_path=raster_path,
        output_dir=TMP_DIR,
        layer_name='flood',
        storm_id='IKE',
        advisory_num='99',
        timestep=0
    )

    if tiles_result.pmtiles_free:
        output_filename = 'flood.geojson' if tiles_result.pmtiles_free.endswith('.geojson') else 'flood.pmtiles'
        final_pmtiles_dest = os.path.join(UI_PUBLIC_DIR, output_filename)
        shutil.copy2(tiles_result.pmtiles_free, final_pmtiles_dest)

    print("\n=== Run Complete! Texas Coast simulation deployed to the UI ===")


if __name__ == '__main__':
    main()
