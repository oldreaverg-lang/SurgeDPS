"""
Pre-warm grid cache for all storms visible in the sidebar.

- Curated historic storms (Katrina, Harvey, Ian, etc.): 5×5 grid (25 cells)
- All other sidebar storms (2015+ HURDAT2): 3×3 grid (9 cells)

Generates surge raster, flood GeoJSON, building data, and HAZUS damage
model output per cell. Run at deploy time as a background process.

The script is idempotent — already-cached cells are skipped, so
re-deploys only generate new/missing data. Failed cells are retried
up to 3 times with a 30s delay between sweeps.
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
from storm_catalog.surge_model import generate_surge_raster, SURGE_MODEL_VERSION, validate_surge_model
from tile_gen.pmtiles_builder import raster_to_geojson
from data_ingest.building_fetcher import fetch_buildings
from damage_model.depth_damage import estimate_damage_from_raster

from persistent_paths import CELLS_DIR, PERSISTENT_DATA_DIR as PERSISTENT_DIR
CACHE_DIR = str(CELLS_DIR)  # backward compat — some functions use os.path.join

# Season accordion cutoff — must match api_server.py
SEASON_MIN_YEAR = 2015

# ── Loss sanity reference ──────────────────────────────────────────────────────
# Reported total economic losses (USD billions) from authoritative post-event
# assessments (NOAA, Munich Re, Swiss Re).  These are all-cause (wind + surge +
# rain) so we deliberately set a wide lower bound — surge-only will always be
# a fraction of total.  Upper bound is 2× reported to catch formula overcount.
#
# Format: storm_id → (lower_B, upper_B, source note)
_LOSS_REFERENCE_B: dict[str, tuple[float, float, str]] = {
    "sandy_2012":   (5.0,  40.0,  "NOAA: $65B all-cause; surge ~$20B"),
    "katrina_2005": (20.0, 125.0, "NOAA: $125B all-cause; surge ~$40-60B"),
    "ike_2008":     (5.0,  40.0,  "NOAA: $30B all-cause; surge ~$10-20B"),
    "harvey_2017":  (5.0,  130.0, "NOAA: $125B all-cause; mostly rain-flood"),
    "ian_2022":     (10.0, 113.0, "NOAA: $113B all-cause; surge significant"),
}


def _check_storm_losses(storm_id: str, sdir: str, target_cells: list) -> list[str]:
    """
    Sum total modeled losses across all cached cells for a storm and compare
    against the known reference range.  Returns a list of warning strings.
    Called after a storm's cells finish generating.
    """
    if storm_id not in _LOSS_REFERENCE_B:
        return []

    lower_B, upper_B, note = _LOSS_REFERENCE_B[storm_id]
    total_loss = 0.0
    cells_read = 0

    for col, row in target_cells:
        damage_path = os.path.join(sdir, f'cell_{col}_{row}_damage.geojson')
        if not os.path.exists(damage_path):
            continue
        try:
            with open(damage_path) as f:
                data = json.load(f)
            for feat in data.get('features', []):
                total_loss += feat.get('properties', {}).get('loss_usd', 0) or 0
            cells_read += 1
        except Exception:
            pass

    if cells_read == 0:
        return []

    total_B = total_loss / 1e9
    warnings = []
    status = "✓" if lower_B <= total_B <= upper_B else "✗ OUT OF RANGE"
    print(
        f"    Loss check {storm_id}: ${total_B:.1f}B modeled across {cells_read} cells "
        f"(expected ${lower_B:.0f}–${upper_B:.0f}B)  {status}"
    )
    if note:
        print(f"    Note: {note}")

    if total_B > upper_B:
        warnings.append(
            f"LOSS SANITY WARNING — {storm_id}: modeled ${total_B:.1f}B exceeds "
            f"upper bound ${upper_B:.0f}B. Surge formula may be overcounting. "
            f"Check surge_model.py."
        )
    elif total_B < lower_B:
        warnings.append(
            f"LOSS SANITY WARNING — {storm_id}: modeled ${total_B:.1f}B is below "
            f"lower bound ${lower_B:.0f}B. Surge formula may be too conservative or "
            f"NSI building data is missing."
        )
    return warnings


def _storm_cache_dir(storm: StormEntry) -> str:
    d = os.path.join(CACHE_DIR, storm.storm_id)
    os.makedirs(d, exist_ok=True)
    return d


# 3×3 grid: standard for all storms
WARM_CELLS_3x3 = [
    (col, row)
    for row in range(-1, 2)
    for col in range(-1, 2)
]

# All storms get 3×3 for pre-warming (fits within Railway 5 GB volume limit).
# Users can still expand coverage on-demand by clicking grid borders.
_HISTORIC_IDS = {s.storm_id for s in HISTORICAL_STORMS}


def _warm_cells_for(storm: StormEntry) -> list[tuple[int, int]]:
    """Return which cells to warm: 3×3 for all storms."""
    return WARM_CELLS_3x3


def _cached_cells(storm: StormEntry) -> set[tuple[int, int]]:
    """Return which of the storm's target cells are current (not stale).

    Cells whose damage.geojson was built with an older surge formula are
    deleted so they get regenerated on this run.
    """
    sdir = _storm_cache_dir(storm)
    target = _warm_cells_for(storm)
    cached = set()
    for col, row in target:
        damage_path = os.path.join(sdir, f'cell_{col}_{row}_damage.geojson')
        flood_path = os.path.join(sdir, f'cell_{col}_{row}_flood.geojson')
        if os.path.exists(damage_path) and os.path.exists(flood_path):
            # Check version stamp — stale cells must be regenerated
            try:
                with open(damage_path) as f:
                    data = json.load(f)
                if data.get('surge_model_version') == SURGE_MODEL_VERSION:
                    cached.add((col, row))
                else:
                    print(f"    Stale cell ({col},{row}) for {storm.storm_id} "
                          f"(version {data.get('surge_model_version')!r} → {SURGE_MODEL_VERSION!r}), deleting...")
                    for stale in (damage_path, flood_path):
                        try:
                            os.remove(stale)
                        except OSError:
                            pass
            except Exception:
                pass  # unreadable — treat as missing, will regenerate
    return cached


def _update_building_index(storm_id: str, col: int, row: int, count: int):
    """
    Write per-cell building count to a lightweight JSON index file.
    The index lives at <storm_cache_dir>/building_index.json and maps
    "col,row" → building_count.  _compute_confidence reads this instead
    of scanning/parsing every damage GeoJSON on each request.
    """
    index_path = os.path.join(CACHE_DIR, storm_id, 'building_index.json')
    index = {}
    if os.path.exists(index_path):
        try:
            with open(index_path) as f:
                index = json.load(f)
        except (json.JSONDecodeError, IOError):
            index = {}
    index[f'{col},{row}'] = count
    with open(index_path, 'w') as f:
        json.dump(index, f)


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
                storm_rmax_nm=storm.rmax_nm,
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
        # CRITICAL: must pass the SAME storm parameters the live API passes
        # (api_server.py /load_cell). Without them the damage model short-
        # circuits to surge-only — no parametric wind field, no parametric
        # rainfall — and pre-cached cells end up systematically different
        # from live-computed cells. Visible as a hard rectangular boundary
        # where pre-cached cells meet live-loaded cells.
        damage_path = os.path.join(sdir, f'cell_{col}_{row}_damage.geojson')
        if buildings_data.get('features'):
            estimate_damage_from_raster(
                raster_path, buildings_path, damage_path,
                storm_id=storm.storm_id,
                landfall_lat=storm.landfall_lat,
                landfall_lon=storm.landfall_lon,
                max_wind_kt=storm.max_wind_kt,
                storm_speed_kt=storm.speed_kt,
                storm_heading_deg=storm.heading_deg,
            )
            # Stamp the surge model version so stale-cache detection works
            try:
                with open(damage_path) as f:
                    damage_data = json.load(f)
                damage_data['surge_model_version'] = SURGE_MODEL_VERSION
                with open(damage_path, 'w') as f:
                    json.dump(damage_data, f)
            except Exception:
                pass  # non-fatal; cell will just be regenerated next time
        else:
            with open(damage_path, 'w') as f:
                json.dump({
                    "type": "FeatureCollection",
                    "features": [],
                    "surge_model_version": SURGE_MODEL_VERSION,
                }, f)

        # 5. Record building count in lightweight index so _compute_confidence
        #    can do an O(1) lookup instead of re-reading multi-MB GeoJSON files.
        n_buildings = len(buildings_data.get('features', []))
        _update_building_index(storm.storm_id, col, row, n_buildings)

        # 6. Clean up intermediate files to save volume space.
        #    The API only needs damage.geojson + flood.geojson for cache hits.
        #    depth.tif and buildings.json can be regenerated on-demand if needed.
        for tmp in (raster_path, buildings_path):
            try:
                if os.path.exists(tmp):
                    os.remove(tmp)
            except OSError:
                pass

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

    # Season accordion (2015+) — only pre-warm Category 1+ hurricanes.
    # Tropical storms produce minimal surge and aren't worth the volume space.
    try:
        seasons = [s for s in get_seasons() if s['year'] >= SEASON_MIN_YEAR]
        for season in seasons:
            year = season['year']
            year_storms = get_storms_for_year(year)
            for s in year_storms:
                if s.storm_id not in seen and s.category >= 1:
                    seen.add(s.storm_id)
                    storms.append(s)
    except Exception as e:
        print(f"WARNING: Could not load HURDAT2 seasons: {e}")

    return storms


def main():
    print("=" * 60)
    print("SurgeDPS Cache Warmer")
    print("=" * 60)

    # ── Step 0: Surge formula sanity check (fast, no network) ──
    print("\n[Step 0] Validating surge formula against historical observations...")
    surge_warnings = validate_surge_model()
    if surge_warnings:
        for w in surge_warnings:
            print(f"  *** {w}")
        print("  *** Aborting warm — fix the surge formula before caching.")
        sys.exit(1)
    print()

    MAX_RETRIES = 3
    RETRY_DELAY = 30  # seconds between retry sweeps
    all_loss_warnings: list[str] = []

    storms = collect_sidebar_storms()
    historic_count = sum(1 for s in storms if s.storm_id in _HISTORIC_IDS)
    other_count = len(storms) - historic_count
    total_cells = len(storms) * len(WARM_CELLS_3x3)
    print(f"Found {len(storms)} storms ({historic_count} curated + {other_count} season Cat1+, all 3×3) = {total_cells} cells to warm")

    cells_generated = 0
    storms_cached = 0
    failed_cells: list[tuple[StormEntry, int, int]] = []
    t0 = time.time()

    # ── Main pass ──
    for i, storm in enumerate(storms, 1):
        tag = f"[{i}/{len(storms)}] {storm.storm_id}"
        target = _warm_cells_for(storm)
        already = _cached_cells(storm)

        if len(already) == len(target):
            grid_label = '3×3'
            print(f"  {tag} — all {len(target)} cells ({grid_label}) cached, skipping")
            storms_cached += 1
            continue

        missing = [c for c in target if c not in already]
        grid_label = '3×3'
        print(f"  {tag} — {grid_label}: generating {len(missing)} cell(s) ({len(already)} cached)...")
        t1 = time.time()

        for col, row in missing:
            ok = warm_cell(storm, col, row)
            if ok:
                cells_generated += 1
            else:
                failed_cells.append((storm, col, row))

        elapsed = time.time() - t1
        print(f"  {tag} — done ({elapsed:.1f}s)")

        # Per-storm loss sanity check (only for reference storms)
        sdir = _storm_cache_dir(storm)
        loss_warns = _check_storm_losses(storm.storm_id, sdir, target)
        all_loss_warnings.extend(loss_warns)

    # ── Retry failed cells ──
    for attempt in range(1, MAX_RETRIES + 1):
        if not failed_cells:
            break
        print(f"\n--- Retry pass {attempt}/{MAX_RETRIES}: {len(failed_cells)} failed cell(s) ---")
        print(f"    Waiting {RETRY_DELAY}s before retrying...")
        time.sleep(RETRY_DELAY)

        still_failed: list[tuple[StormEntry, int, int]] = []
        for storm, col, row in failed_cells:
            # Check if it was somehow cached in the meantime
            sdir = _storm_cache_dir(storm)
            if (os.path.exists(os.path.join(sdir, f'cell_{col}_{row}_damage.geojson')) and
                    os.path.exists(os.path.join(sdir, f'cell_{col}_{row}_flood.geojson'))):
                cells_generated += 1
                continue
            print(f"    Retrying {storm.storm_id} cell ({col},{row})...")
            ok = warm_cell(storm, col, row)
            if ok:
                cells_generated += 1
            else:
                still_failed.append((storm, col, row))
        failed_cells = still_failed

    total = time.time() - t0
    print()
    print(f"Warm-up complete in {total:.0f}s")
    print(f"  {storms_cached} storms fully cached (skipped)")
    print(f"  {cells_generated} cells newly generated")
    if failed_cells:
        print(f"  {len(failed_cells)} cells STILL FAILED after {MAX_RETRIES} retries:")
        for storm, col, row in failed_cells:
            print(f"    - {storm.storm_id} cell ({col},{row})")
    else:
        print(f"  0 failures — all cells cached successfully")

    if all_loss_warnings:
        print()
        print("  ⚠️  LOSS SANITY WARNINGS (review before deploying to production):")
        for w in all_loss_warnings:
            print(f"    *** {w}")
    else:
        print(f"  Loss sanity checks passed for all reference storms ✓")
    print("=" * 60)


if __name__ == '__main__':
    main()
