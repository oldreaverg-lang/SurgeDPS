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
# peril_timeseries import deferred along with the lazy-ticks endpoint.

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

    # Track files this cell touched so we can clean up on failure — a half-
    # finished cell that leaves a truncated depth.tif or buildings.json on
    # disk would trip the next warm pass (depth.tif "not recognized",
    # buildings.json "Unterminated string").
    artifacts_this_run: list[str] = []

    # Precompute artifact paths up front so cleanup-on-failure can nuke
    # every one of them regardless of where in the pipeline we died.
    raster_path    = os.path.join(sdir, f'cell_{col}_{row}_depth.tif')
    flood_path     = os.path.join(sdir, f'cell_{col}_{row}_flood.geojson')
    buildings_path = os.path.join(sdir, f'cell_{col}_{row}_buildings.json')
    damage_path    = os.path.join(sdir, f'cell_{col}_{row}_damage.geojson')
    all_cell_artifacts = [raster_path, flood_path, buildings_path, damage_path]

    # Validate any existing cached files by actually opening them. A file
    # that passes a size check can still be an unrecoverable truncated
    # GeoTIFF ("not recognized as being in a supported file format") or a
    # 0-byte JSON ("Expecting value: line 1 column 1"). Delete corrupt
    # caches so the pipeline below regenerates them cleanly.
    if os.path.exists(raster_path):
        _ok = False
        try:
            import rasterio  # local import — module is used below too
            with rasterio.open(raster_path) as _src:
                if _src.width > 0 and _src.height > 0:
                    _ok = True
        except Exception:
            _ok = False
        if not _ok:
            try:
                os.remove(raster_path)
            except OSError:
                pass
    if os.path.exists(buildings_path):
        _ok = False
        try:
            if os.path.getsize(buildings_path) >= 2:
                with open(buildings_path) as _f:
                    json.load(_f)
                _ok = True
        except Exception:
            _ok = False
        if not _ok:
            try:
                os.remove(buildings_path)
            except OSError:
                pass
    # A flood.geojson paired with a now-missing raster is suspect — drop
    # it so it gets rebuilt from a fresh raster.
    if os.path.exists(flood_path) and not os.path.exists(raster_path):
        try:
            os.remove(flood_path)
        except OSError:
            pass

    try:
        # 1. Surge raster
        if not os.path.exists(raster_path):
            artifacts_this_run.append(raster_path)
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
        if not os.path.exists(flood_path):
            artifacts_this_run.append(flood_path)
            raster_to_geojson(raster_path, flood_path)

        # 3. OSM buildings
        if not os.path.exists(buildings_path):
            artifacts_this_run.append(buildings_path)
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
        if not os.path.exists(damage_path):
            artifacts_this_run.append(damage_path)
        if buildings_data.get('features'):
            # Final-tick HAZUS only. Per-tick bundle is lazy — generated on
            # first /cell_ticks request from the frontend slider.
            estimate_damage_from_raster(
                depth_raster_path=raster_path,
                buildings_geojson_path=buildings_path,
                output_path=damage_path,
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
        # Nuke every artifact for this cell — not just what we wrote this
        # pass. A corrupt cached file we inherited (depth.tif that rasterio
        # can't open, buildings.json with 0 bytes) would otherwise survive
        # and re-trigger the same exception on the retry pass.
        for _p in all_cell_artifacts:
            try:
                if os.path.exists(_p):
                    os.remove(_p)
            except OSError:
                pass
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

    # ── Phase 2: warm historical AHPS gauge caches ───────────────────────
    # One cache file per storm under PERSISTENT_DIR/cache/gauges_historical/.
    # Each fetch hits USGS NWIS IV + NWPS (~30–60s per storm), so we
    # serialise them with a small sleep between to stay polite.
    try:
        from rainfall.ahps_historical import (
            fetch_historical_gauges, cache_exists,
        )
    except Exception as _e:
        print(f"\n[gauges] warm phase skipped — import failed: {_e}")
        return

    print()
    print("=" * 60)
    print("Phase 2: Warming AHPS historical gauge archive")
    print("=" * 60)
    g_ok = g_skip = g_fail = 0
    for storm in HISTORICAL_STORMS:
        if not getattr(storm, 'landfall_date', None):
            continue
        if cache_exists(storm.storm_id, str(PERSISTENT_DIR)):
            g_skip += 1
            continue
        try:
            t_s = time.time()
            r = fetch_historical_gauges(
                storm_id=storm.storm_id,
                landfall_lat=storm.landfall_lat,
                landfall_lon=storm.landfall_lon,
                landfall_date=storm.landfall_date,
                radius_deg=4.0,
                persistent_dir=str(PERSISTENT_DIR),
            )
            n = r.get('gauge_count', 0)
            print(f"  [gauges] {storm.storm_id:18s} → {n:4d} gauges "
                  f"({time.time()-t_s:.1f}s)")
            g_ok += 1
            time.sleep(1.0)   # polite spacing between NWIS queries
        except Exception as e:
            g_fail += 1
            print(f"  [gauges] {storm.storm_id:18s} FAILED — {e}")
    print(f"  Gauge warm summary: {g_ok} fetched · {g_skip} already cached · {g_fail} failed")
    print("=" * 60)

    # ── Phase 3: FEMA NFHL flood zone tile cache ─────────────────────────
    # Pre-fetch FEMA National Flood Hazard Layer for a 4° × 4° grid of
    # 2° × 2° sub-tiles around each historical storm's landfall.  The cache
    # key exactly matches what api_server.py /api/flood_zones writes, so
    # subsequent browser requests within prewarmed tiles are instant reads.
    #
    # Tile strategy: 16 tiles per storm (4 cols × 4 rows, 2° each) covering
    # an 8° × 8° box around landfall.  At 2° resolution the per-tile
    # feature count stays well below FEMA's 2,000-record limit even for
    # dense coastal zones.  Total at ~20 historical storms: ~320 requests
    # × ~2 s each ≈ 10 min, but only for un-cached tiles.
    print()
    print("=" * 60)
    print("Phase 3: Warming FEMA NFHL flood zone tile cache")
    print("=" * 60)

    import urllib.parse as _fz_parse
    import json as _fz_json
    import requests as _fz_requests

    _FEMA_URL = (
        'https://hazards.fema.gov/arcgis/rest/services/public/NFHL/MapServer/28/query'
    )
    _FEMA_UA = 'SurgeDPS/1.0 (+https://stormdps.com) warm_cache'
    _FZ_TILE_DEG = 2.0      # fetch one 2° × 2° tile per request
    _FZ_RADIUS_DEG = 4.0    # cover ±4° around landfall in each axis → 4×4 grid
    _FZ_TIMEOUT = 45        # seconds per FEMA request (raised; hazards.fema.gov is slow)

    fz_cache_dir = os.path.join(str(PERSISTENT_DIR), 'cache', 'flood_zones')
    os.makedirs(fz_cache_dir, exist_ok=True)
    fz_ok = fz_skip = fz_fail = 0

    for storm in HISTORICAL_STORMS:
        lat0 = storm.landfall_lat
        lon0 = storm.landfall_lon

        # Build a 4×4 grid of 2° × 2° tiles covering [lon0-4°, lon0+4°] ×
        # [lat0-4°, lat0+4°].  Snap each edge to 0.01° to match the cache
        # key quantization used by api_server.py's /api/flood_zones handler.
        tiles_fetched = tiles_skipped = tiles_failed = 0
        n_cols = n_rows = int(2 * _FZ_RADIUS_DEG / _FZ_TILE_DEG)  # 4

        for row in range(n_rows):
            for col in range(n_cols):
                raw_w = lon0 - _FZ_RADIUS_DEG + col * _FZ_TILE_DEG
                raw_s = lat0 - _FZ_RADIUS_DEG + row * _FZ_TILE_DEG
                raw_e = raw_w + _FZ_TILE_DEG
                raw_n = raw_s + _FZ_TILE_DEG

                # Quantize exactly as api_server.py does
                qw = round(raw_w, 2)
                qs = round(raw_s, 2)
                qe = round(raw_e, 2)
                qn = round(raw_n, 2)

                cache_name = f"fz_{qw:+.2f}_{qs:+.2f}_{qe:+.2f}_{qn:+.2f}.json"
                cache_path = os.path.join(fz_cache_dir, cache_name)

                if os.path.exists(cache_path):
                    tiles_skipped += 1
                    continue

                envelope = _fz_json.dumps({
                    'xmin': qw, 'ymin': qs,
                    'xmax': qe, 'ymax': qn,
                    'spatialReference': {'wkid': 4326},
                })
                qs_str = _fz_parse.urlencode({
                    'where': '1=1',
                    'geometry': envelope,
                    'geometryType': 'esriGeometryEnvelope',
                    'inSR': '4326',
                    'outSR': '4326',
                    'spatialRel': 'esriSpatialRelIntersects',
                    'outFields': 'FLD_ZONE,SFHA_TF,FLOODWAY',
                    'returnGeometry': 'true',
                    'resultRecordCount': '2000',
                    'f': 'geojson',
                })
                fema_url = f'{_FEMA_URL}?{qs_str}'
                try:
                    # requests handles TLS session resumption and unexpected
                    # EOF far more gracefully than urllib's raw SSL socket.
                    _fz_resp = _fz_requests.get(
                        fema_url,
                        headers={'User-Agent': _FEMA_UA},
                        timeout=_FZ_TIMEOUT,
                    )
                    _fz_resp.raise_for_status()
                    raw = _fz_resp.content
                    # Validate JSON before caching (FEMA occasionally returns
                    # HTML error pages with HTTP 200).
                    parsed = _fz_json.loads(raw)
                    if 'error' in parsed:
                        raise ValueError(f"FEMA error: {parsed['error']}")
                    # Atomic write — same pattern as api_server.py
                    import threading as _fz_th
                    _tmp = f'{cache_path}.tmp.{os.getpid()}.{_fz_th.get_ident()}'
                    with open(_tmp, 'wb') as fh:
                        fh.write(raw)
                    os.replace(_tmp, cache_path)
                    tiles_fetched += 1
                    time.sleep(0.5)  # polite rate limit — FEMA is a shared public API
                except Exception as _fe:
                    tiles_failed += 1
                    print(f"    FEMA tile ({qw:+.2f},{qs:+.2f})→({qe:+.2f},{qn:+.2f})"
                          f" {storm.storm_id}: {_fe}")

        n_features = 0
        # Quick count of cached features for this storm to print summary
        for row in range(n_rows):
            for col in range(n_cols):
                raw_w = lon0 - _FZ_RADIUS_DEG + col * _FZ_TILE_DEG
                raw_s = lat0 - _FZ_RADIUS_DEG + row * _FZ_TILE_DEG
                qw = round(raw_w, 2); qs = round(raw_s, 2)
                qe = round(raw_w + _FZ_TILE_DEG, 2); qn = round(raw_s + _FZ_TILE_DEG, 2)
                cp = os.path.join(fz_cache_dir, f"fz_{qw:+.2f}_{qs:+.2f}_{qe:+.2f}_{qn:+.2f}.json")
                if os.path.exists(cp):
                    try:
                        with open(cp) as _cf:
                            n_features += len(_fz_json.load(_cf).get('features', []))
                    except Exception:
                        pass

        status = f"{tiles_fetched} fetched · {tiles_skipped} cached · {tiles_failed} failed"
        print(f"  [fema] {storm.storm_id:18s} → {n_features:5d} features  ({status})")
        fz_ok += tiles_fetched
        fz_skip += tiles_skipped
        fz_fail += tiles_failed

    print(f"  FEMA tile summary: {fz_ok} newly fetched · {fz_skip} already cached · {fz_fail} failed")
    print("=" * 60)

    # ── Phase 4: Compound raster mosaics ─────────────────────────────────
    # For every storm whose 3×3 cells include compound tifs (produced during
    # Phase 1 by flood_model.compound.merge_compound_flood), merge them into
    # a single storm_compound.tif mosaic.  The mosaic is what
    # /api/compound_tile renders on demand — prebuilding it here means the
    # first compound-overlay tile request is served from cache instead of
    # triggering an in-request rebuild.
    #
    # The rebuild check is: mosaic missing OR any cell tif is newer than it.
    # This is the same heuristic api_server.py uses, so repeated deploys are
    # cheap (no-op if cells haven't changed).
    print()
    print("=" * 60)
    print("Phase 4: Building compound raster mosaics")
    print("=" * 60)

    try:
        import glob as _glob
        import rasterio as _rio
        from rasterio.merge import merge as _rio_merge
        import numpy as _np_cm
        _rasterio_available = True
    except ImportError as _rim:
        print(f"  [compound] rasterio not available — skipping ({_rim})")
        _rasterio_available = False

    if _rasterio_available:
        cm_built = cm_skip = cm_fail = 0

        all_storm_dirs = []
        # HISTORICAL_STORMS covers all curated storms; also sweep the cells
        # directory for any season storms whose cells were pre-warmed.
        for entry in os.scandir(str(CELLS_DIR)):
            if entry.is_dir():
                all_storm_dirs.append(entry.path)

        for storm_dir in sorted(all_storm_dirs):
            storm_id = os.path.basename(storm_dir)
            cell_tifs = sorted(_glob.glob(os.path.join(storm_dir, 'cell_*_compound.tif')))
            if not cell_tifs:
                continue  # no compound data for this storm yet

            mosaic_path = os.path.join(storm_dir, 'storm_compound.tif')

            # Check if mosaic is fresh vs all cell tifs
            if os.path.exists(mosaic_path):
                mo_mtime = os.path.getmtime(mosaic_path)
                if max(os.path.getmtime(t) for t in cell_tifs) <= mo_mtime:
                    cm_skip += 1
                    continue  # mosaic is up to date

            # Build / rebuild the mosaic
            datasets: list = []
            try:
                for t in cell_tifs:
                    datasets.append(_rio.open(t))
                mosaic, transform = _rio_merge(datasets)
                profile = datasets[0].profile.copy()
                profile.update(
                    driver='GTiff',
                    height=mosaic.shape[1],
                    width=mosaic.shape[2],
                    transform=transform,
                    count=1,
                    compress='deflate',
                    tiled=True,
                    blockxsize=256,
                    blockysize=256,
                )
                import threading as _cm_th
                _tmp = f'{mosaic_path}.tmp.{os.getpid()}.{_cm_th.get_ident()}'
                with _rio.open(_tmp, 'w', **profile) as dst:
                    dst.write(mosaic[0], 1)
                os.replace(_tmp, mosaic_path)

                # Quick stats for the summary line
                arr = mosaic[0]
                valid = arr > 0
                max_ft = float(arr[valid].max()) if valid.any() else 0.0
                avg_ft = float(arr[valid].mean()) if valid.any() else 0.0
                print(f"  [compound] {storm_id:18s} → {len(cell_tifs):2d} cells merged"
                      f"  max={max_ft:.1f}ft  avg={avg_ft:.2f}ft")
                cm_built += 1
            except Exception as _cm_err:
                print(f"  [compound] {storm_id:18s} FAILED — {_cm_err}")
                cm_fail += 1
            finally:
                for _d in datasets:
                    try:
                        _d.close()
                    except Exception:
                        pass

        print(f"  Compound mosaic summary: {cm_built} built · {cm_skip} up to date · {cm_fail} failed")
    print("=" * 60)

    # ── Phase 5: MRMS QPE prewarming ──────────────────────────────────────
    # For every historical storm that has a landfall_date (IEM archive
    # coverage goes back to ~2015), pre-build the accumulated rainfall
    # GeoTIFF and store it under PERSISTENT_DIR/mrms/.  Subsequent
    # /api/rainfall requests are then cache hits: no 90-120s IEM download.
    #
    # Strategy:
    #   • Skip storms without landfall_date (parametric-only; Katrina etc.)
    #   • Skip storms whose IEM TIF already exists (idempotent redeploys)
    #   • Run each storm sequentially to keep peak RAM low
    #   • MRMSFetcher itself retries individual hourly files and writes the
    #     clipped TIF atomically, so a crash mid-storm is safe to re-run.
    print()
    print("=" * 60)
    print("Phase 5: Prewarming MRMS QPE rainfall GeoTIFFs")
    print("=" * 60)

    try:
        from rainfall.mrms_fetcher import MRMSFetcher, storm_bbox_from_catalog_entry
        from datetime import datetime, timezone as _utc, timedelta as _td
        import hashlib as _hl
        import glob as _mrms_glob
        _mrms_available = True
    except Exception as _mrms_imp_err:
        print(f"  [mrms] MRMSFetcher import failed — skipping ({_mrms_imp_err})")
        _mrms_available = False

    if _mrms_available:
        mrms_ok = mrms_skip = mrms_fail = 0
        mrms_dir = os.path.join(str(PERSISTENT_DIR), 'mrms')
        os.makedirs(mrms_dir, exist_ok=True)

        for storm in HISTORICAL_STORMS:
            sid = storm.storm_id
            landfall_date = getattr(storm, 'landfall_date', None)
            if not landfall_date:
                # No date → parametric fallback only; nothing to prewarm
                mrms_skip += 1
                continue

            # Resolve the same valid_time that api_server.py uses so the cache
            # key (md5 of "iem|<valid_time>|<duration_hr>|<bbox_str>") matches.
            duration_hr = 72
            try:
                valid_time = datetime.strptime(landfall_date, '%Y-%m-%d').replace(
                    hour=18, tzinfo=_utc.utc
                ) + _td(hours=48)
            except ValueError:
                print(f"  [mrms] {sid:20s} — bad landfall_date {landfall_date!r}, skipping")
                mrms_skip += 1
                continue

            bbox = storm_bbox_from_catalog_entry(
                storm.landfall_lat, storm.landfall_lon, buffer_deg=4.0
            )
            bbox_str = "_".join(f"{v:.3f}" for v in bbox)
            cache_token = f"iem|{valid_time.isoformat()}|{duration_hr}|{bbox_str}"
            ck = _hl.md5(cache_token.encode()).hexdigest()[:12]
            iem_tif = os.path.join(mrms_dir, f'iem_{ck}.tif')

            if os.path.exists(iem_tif):
                print(f"  [mrms] {sid:20s} — already cached ({iem_tif}), skipping")
                mrms_skip += 1
                continue

            print(f"  [mrms] {sid:20s} — fetching IEM accumulation (landfall {landfall_date}, "
                  f"valid_time {valid_time.date()})...")
            t_s = time.time()
            try:
                fetcher = MRMSFetcher(cache_dir=mrms_dir, keep_raw_grib=False)
                result = fetcher.fetch_iem_historical(
                    storm_bbox=bbox,
                    valid_time=valid_time,
                    duration_hr=duration_hr,
                )
                if result and result.clipped_tif_path and os.path.exists(result.clipped_tif_path):
                    print(f"  [mrms] {sid:20s} — OK  max={result.max_precip_mm:.1f}mm "
                          f"avg={result.avg_precip_mm:.1f}mm  ({time.time()-t_s:.0f}s)  "
                          f"source={result.source}")
                    mrms_ok += 1
                else:
                    print(f"  [mrms] {sid:20s} — returned no TIF ({time.time()-t_s:.0f}s)")
                    mrms_fail += 1
            except Exception as _mrms_err:
                print(f"  [mrms] {sid:20s} — FAILED: {_mrms_err} ({time.time()-t_s:.0f}s)")
                mrms_fail += 1

            # Brief pause between storms — IEM mtarchive is a shared public service
            time.sleep(2.0)

        print(f"  MRMS prewarm summary: {mrms_ok} fetched · {mrms_skip} skipped · {mrms_fail} failed")
    print("=" * 60)


if __name__ == '__main__':
    main()
