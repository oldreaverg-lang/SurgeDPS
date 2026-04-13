"""
SurgeDPS Cell API Server

Lightweight HTTP server for on-demand storm analysis.  The React
frontend calls this when the user:
  1. Opens the storm selector   → GET /api/storms
  2. Picks a storm              → GET /api/storm/{id}/activate
  3. Clicks a grid cell to load → GET /api/cell?col=N&row=N

Each cell request fetches real OSM buildings, generates a parametric
surge raster based on the active storm's real parameters, runs the
HAZUS damage model, and returns both flood polygons and damage points.

Usage:
    python scripts/api_server.py          # starts on port 8000
"""

import json
import mimetypes
import os
import sys
from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn
from urllib.parse import urlparse, parse_qs

# Built React frontend lives at <repo_root>/ui/dist/
_STATIC_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..', 'ui', 'dist')
)

import numpy as np

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))
from damage_model.depth_damage import estimate_damage_from_raster
from damage_model.peril_timeseries import estimate_damage_timeseries_from_raster
from data_ingest.building_fetcher import fetch_buildings
from tile_gen.pmtiles_builder import raster_to_geojson
from storm_catalog.catalog import (
    StormEntry, CELL_WIDTH, CELL_HEIGHT,
    fetch_active_storms, HISTORICAL_STORMS,
)
from storm_catalog.hurdat2_parser import (
    get_seasons, get_storms_for_year, search_storms,
    get_storm_by_id, get_all_hurdat2_storms,
)

# Season accordion cutoff — only show 2015+ in the year-by-year browser
SEASON_MIN_YEAR = 2015
from storm_catalog.surge_model import generate_surge_raster
from data_ingest.census_fetcher import get_population_context
from validation.run_ledger import record_from_activation
from validation.backtester import run_backtest, score_storm, predict_loss_range
from validation.ground_truth import get_ground_truth
from storm_catalog.forecast_track import fetch_forecast_track, fetch_forecast_cone

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
from persistent_paths import CELLS_DIR, GEOCODE_DIR, PERSISTENT_DATA_DIR
PERSISTENT_DIR = str(PERSISTENT_DATA_DIR)
CACHE_DIR = str(CELLS_DIR)

# ── DPS Score Lookup (from StormDPS compiled_bundle) ──
_DPS_SCORES: dict = {}
_dps_path = os.path.join(BASE_DIR, 'data', 'dps_scores.json')
if os.path.exists(_dps_path):
    with open(_dps_path) as _f:
        _DPS_SCORES = json.load(_f)
    print(f"Loaded {len(_DPS_SCORES)} DPS scores from dps_scores.json")


def _compute_confidence(storm_id: str) -> dict:
    """
    R5: Compute validation confidence based on cached building count.
    Returns {'confidence': 'high'|'medium'|'low'|'unvalidated', 'building_count': int}

    Fast path: reads building_index.json (tiny file written during cell
    generation by both warm_cache.py and load_cell()).
    Fallback: scans *_damage.geojson for cells generated before the index
    existed — and backfills the index so subsequent lookups are instant.
    """
    sdir = os.path.join(CACHE_DIR, storm_id)
    if not os.path.isdir(sdir):
        return {'confidence': 'unvalidated', 'building_count': 0}

    # Fast path: read the lightweight index
    index_path = os.path.join(sdir, 'building_index.json')
    if os.path.exists(index_path):
        try:
            with open(index_path) as f:
                index = json.load(f)
            total = sum(index.values())
            level = 'high' if total > 500 else ('medium' if total >= 50 else 'low')
            return {'confidence': level, 'building_count': total}
        except (json.JSONDecodeError, IOError):
            pass

    # Fallback: scan damage GeoJSONs and backfill the index
    total = 0
    index = {}
    for fname in os.listdir(sdir):
        if fname.endswith('_damage.geojson'):
            try:
                with open(os.path.join(sdir, fname)) as f:
                    data = json.load(f)
                count = len(data.get('features', []))
                total += count
                # Extract col,row from filename like "cell_0_-1_damage.geojson"
                parts = fname.replace('_damage.geojson', '').replace('cell_', '').split('_')
                if len(parts) == 2:
                    index[f'{parts[0]},{parts[1]}'] = count
            except Exception:
                pass
    # Backfill the index for future instant lookups
    if index:
        try:
            with open(index_path, 'w') as f:
                json.dump(index, f)
        except IOError:
            pass

    level = 'high' if total > 500 else ('medium' if total >= 50 else 'low')
    return {'confidence': level, 'building_count': total}


def _compute_eli(dps_score: float, building_count: int) -> dict:
    """
    R8: Expected Loss Index = sqrt(DPS) * sqrt(buildings).
    Correlates r=0.95 with actual HAZUS loss vs DPS's r=0.12.
    Returns ELI value and severity tier.
    """
    import math
    if dps_score <= 0 or building_count <= 0:
        return {'eli': 0.0, 'eli_tier': 'unavailable'}
    eli = math.sqrt(dps_score) * math.sqrt(building_count)
    if eli >= 400:
        tier = 'extreme'
    elif eli >= 250:
        tier = 'very_high'
    elif eli >= 100:
        tier = 'high'
    elif eli >= 50:
        tier = 'moderate'
    else:
        tier = 'low'
    return {'eli': round(eli, 1), 'eli_tier': tier}


import math as _math
import hashlib as _hashlib
import urllib.request as _urllib_request

# ── Persistent Nominatim geocoding cache ──
_GEOCODE_CACHE_DIR = str(GEOCODE_DIR)  # imported from storage.py
_NOMINATIM_HEADERS = {'User-Agent': 'SurgeDPS/1.0 (surgedps.com)'}
_last_nominatim_call = 0.0  # rate-limit: 1 req/sec


def _geocode_cache_path(key: str) -> str:
    h = _hashlib.md5(key.encode()).hexdigest()
    return os.path.join(_GEOCODE_CACHE_DIR, f'{h}.json')


def _rate_limit_nominatim():
    """Ensure at least 1 second between Nominatim requests."""
    global _last_nominatim_call
    elapsed = _time.time() - _last_nominatim_call
    if elapsed < 1.0:
        _time.sleep(1.0 - elapsed)
    _last_nominatim_call = _time.time()


def _geocode_reverse(lat: float, lon: float) -> dict:
    """Reverse-geocode via Nominatim with persistent disk cache."""
    cache_key = f'reverse:{lat:.5f},{lon:.5f}'
    cp = _geocode_cache_path(cache_key)
    if os.path.exists(cp):
        with open(cp) as f:
            return json.load(f)

    _rate_limit_nominatim()
    url = f'https://nominatim.openstreetmap.org/reverse?lat={lat}&lon={lon}&format=json'
    req = _urllib_request.Request(url, headers=_NOMINATIM_HEADERS)
    with _urllib_request.urlopen(req, timeout=10) as resp:
        data = json.loads(resp.read())

    addr = data.get('address', {})
    parts = [addr.get('house_number'), addr.get('road'),
             addr.get('city') or addr.get('town') or addr.get('village') or addr.get('hamlet')]
    label = ', '.join(p for p in parts if p) or None
    result = {'label': label, 'address': addr}

    with open(cp, 'w') as f:
        json.dump(result, f)
    return result


def _geocode_forward(query: str) -> dict:
    """Forward-geocode via Nominatim with persistent disk cache."""
    cache_key = f'forward:{query.lower()}'
    cp = _geocode_cache_path(cache_key)
    if os.path.exists(cp):
        with open(cp) as f:
            return json.load(f)

    _rate_limit_nominatim()
    q = _urllib_request.quote(query)
    url = f'https://nominatim.openstreetmap.org/search?q={q}&format=json&limit=1'
    req = _urllib_request.Request(url, headers=_NOMINATIM_HEADERS)
    with _urllib_request.urlopen(req, timeout=10) as resp:
        data = json.loads(resp.read())

    result = {'results': data}
    with open(cp, 'w') as f:
        json.dump(result, f)
    return result

# R11: Regional building count baselines (median from HAZUS data per region)
_REGIONAL_BLDG_BASELINE = {
    'Tampa Bay': 10000, 'Mid-Atlantic': 8000, 'Carolinas': 5000,
    'SE Florida': 8000, 'NE Florida / Georgia': 4000, 'SW Florida': 3000,
    'Texas': 2000, 'Louisiana / Mississippi': 1500, 'Alabama / FL Panhandle': 2000,
    'FL Big Bend': 800, 'Northeast': 6000, 'North Carolina': 3000,
    'Mississippi': 1000, 'Leeward Islands': 500, 'Puerto Rico / USVI': 2000,
    'Windward Islands': 300, 'Bahamas': 400, 'Cuba / Jamaica': 500,
    'Mexico / Central America': 300,
}

def _compute_validated_dps(dps_score: float, building_count: int, exposure_region: str) -> dict:
    """
    R11: Dynamic exposure reclassification.
    If actual building count deviates >3x from regional baseline, adjust DPS.
    Returns adjusted_dps, adjustment_factor, and explanation.
    """
    if dps_score <= 0 or building_count <= 0:
        return {'validated_dps': dps_score, 'dps_adjustment': 0.0, 'dps_adj_reason': ''}
    baseline = _REGIONAL_BLDG_BASELINE.get(exposure_region, 2000)
    ratio = building_count / baseline
    if ratio > 3.0:
        # More buildings than expected — boost DPS
        adj = min(_math.log2(ratio) * 0.03, 0.15)
        validated = min(100.0, dps_score * (1 + adj))
        reason = f'+{adj:.0%} ({building_count:,} bldgs vs {baseline:,} baseline)'
    elif ratio < 0.33:
        # Fewer buildings than expected — reduce DPS relevance
        adj = -min(_math.log2(1/ratio) * 0.03, 0.10)
        validated = max(0.0, dps_score * (1 + adj))
        reason = f'{adj:.0%} ({building_count:,} bldgs vs {baseline:,} baseline)'
    else:
        return {'validated_dps': round(dps_score, 1), 'dps_adjustment': 0.0, 'dps_adj_reason': ''}
    return {'validated_dps': round(validated, 1), 'dps_adjustment': round(adj, 3), 'dps_adj_reason': reason}


def _inject_dps(storm_dict: dict) -> dict:
    """Inject dps_score into a storm dict if not already set (or 0)."""
    if storm_dict.get('dps_score', 0) > 0:
        return storm_dict
    sid = storm_dict.get('storm_id', '')
    score = _DPS_SCORES.get(sid, 0)
    if score == 0:
        # Try name_year lookup (for custom IDs like 'katrina_2005')
        name = storm_dict.get('name', '').lower()
        name = name.replace('hurricane ', '').replace('tropical storm ', '').replace('tropical depression ', '').strip()
        year = storm_dict.get('year', 0)
        score = _DPS_SCORES.get(f'{name}_{year}', 0)
    storm_dict['dps_score'] = score
    return storm_dict

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Active Storm State
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

import threading as _threading
_active_storm_lock = _threading.Lock()
_active_storm: StormEntry | None = None
_active_exposure_region: str = ''  # R11: cached for cell-load lookups

# ── Progress tracking for long-running activation ──
import time as _time
_progress: dict = {'step': '', 'step_num': 0, 'total_steps': 4, 'started_at': 0.0, 'storm_id': ''}


def _storm_cache_dir(storm: StormEntry) -> str:
    d = os.path.join(CACHE_DIR, storm.storm_id)
    os.makedirs(d, exist_ok=True)
    return d


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


def cell_bbox(col: int, row: int):
    """Convert grid (col, row) to bbox using the active storm's grid origin."""
    if _active_storm is None:
        raise RuntimeError("No storm active")
    origin_lon = _active_storm.grid_origin_lon
    origin_lat = _active_storm.grid_origin_lat
    lon_min = origin_lon + col * CELL_WIDTH
    lat_min = origin_lat + row * CELL_HEIGHT
    return lon_min, lat_min, lon_min + CELL_WIDTH, lat_min + CELL_HEIGHT


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Cell Loading
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def load_cell(col: int, row: int) -> dict:
    """
    Generate damage + flood data for a grid cell under the active storm.
    """
    storm = _active_storm
    if storm is None:
        return {"buildings": _empty_fc(), "flood": _empty_fc()}

    sdir = _storm_cache_dir(storm)
    damage_path = os.path.join(sdir, f'cell_{col}_{row}_damage.geojson')
    flood_path = os.path.join(sdir, f'cell_{col}_{row}_flood.geojson')
    ticks_path  = os.path.join(sdir, f'cell_{col}_{row}_ticks.json')

    # Check cache. Requires ticks.json as well now — cells generated by the
    # old pre-peril pipeline only have damage.geojson + flood.geojson and
    # need to be regenerated so the time-slider has data to show.
    if (os.path.exists(damage_path) and os.path.exists(flood_path)
            and os.path.exists(ticks_path)):
        with open(damage_path) as f:
            damage_data = json.load(f)
        with open(flood_path) as f:
            flood_data = json.load(f)
        print(f"  [cache hit] cell ({col},{row}) for {storm.storm_id}")
        _progress.update(step='Complete', step_num=4, storm_id=storm.storm_id)
        return {"buildings": damage_data, "flood": flood_data}

    lon_min, lat_min, lon_max, lat_max = cell_bbox(col, row)
    print(f"[{storm.storm_id} cell {col},{row}] "
          f"bbox=({lon_min:.2f},{lat_min:.2f})->({lon_max:.2f},{lat_max:.2f})")

    # 1. Parametric surge raster using real storm parameters
    _progress.update(step='Generating surge model', step_num=1, storm_id=storm.storm_id)
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

    # 2. Rainfall raster (parametric, always available) + optional HAND fluvial
    _progress.update(step='Generating rainfall model', step_num=2)
    rainfall_raster_path = os.path.join(sdir, f'cell_{col}_{row}_rainfall.tif')
    compound_raster_path = os.path.join(sdir, f'cell_{col}_{row}_compound.tif')
    _rainfall_raster_available = False

    # 2a-pre. NLCD impervious surface → runoff coefficient
    #   Fetch the NLCD 2021 impervious surface fraction for this cell and
    #   use it to calibrate the rainfall runoff coefficient before running
    #   the parametric rainfall model.  Falls back to 0.45 if unavailable.
    _nlcd_runoff_coeff = 0.45  # default: mixed coastal landscape
    try:
        from rainfall.nlcd_fetcher import fetch_nlcd_for_cell
        from persistent_paths import NWM_CACHE_DIR
        _nlcd = fetch_nlcd_for_cell(
            lon_min=lon_min, lat_min=lat_min,
            lon_max=lon_max, lat_max=lat_max,
            cache_dir=str(NWM_CACHE_DIR),
            storm_id=storm.storm_id,
            col=col, row=row,
        )
        if _nlcd is not None:
            _nlcd_runoff_coeff = _nlcd.runoff_coefficient
            print(f"  [NLCD] Cell ({col},{row}): {_nlcd.mean_impervious_pct:.1f}% "
                  f"impervious → runoff C={_nlcd_runoff_coeff:.2f}")
    except Exception as _nlcd_err:
        print(f"  [NLCD] Impervious fetch skipped (non-fatal): {_nlcd_err}")

    if not os.path.exists(rainfall_raster_path):
        try:
            from flood_model.rainfall import estimate_rainfall_flooding
            rain_result = estimate_rainfall_flooding(
                center_lat=storm.landfall_lat,
                center_lon=storm.landfall_lon,
                max_wind_kt=storm.max_wind_kt,
                storm_speed_kt=storm.speed_kt,
                rmax_nm=getattr(storm, 'rmax_nm', 20.0),
                heading_deg=storm.heading_deg,
                output_dir=sdir,
                storm_id=f'cell_{col}_{row}_{storm.storm_id}',
                # Clip to cell extent
                extent_km=max(
                    abs(lon_max - storm.landfall_lon) * 111,
                    abs(lat_max - storm.landfall_lat) * 111,
                ) + 20,
                runoff_coefficient=_nlcd_runoff_coeff,
            )
            # The rainfall module writes to its own path; symlink to our expected name
            if os.path.exists(rain_result.depth_raster_path):
                import shutil as _shutil
                _shutil.copy2(rain_result.depth_raster_path, rainfall_raster_path)
                os.remove(rain_result.depth_raster_path)
                os.remove(rain_result.total_precip_path)
                _rainfall_raster_available = True
        except Exception as _rain_err:
            print(f"  [rainfall] Parametric raster failed (non-fatal): {_rain_err}")
    else:
        _rainfall_raster_available = True

    # 2a-post. Atlas 14 return-period classification
    #   After computing max_precip from the Lonfat model, classify the storm's
    #   rainfall magnitude against NOAA PFDS frequency data so the CAT report
    #   can say e.g. "~500-year rainfall event".
    _rain_return_period_label = "unknown"
    try:
        from rainfall.atlas14_fetcher import get_return_period_for_storm
        from persistent_paths import ATLAS14_DIR
        _atlas14_cache = str(ATLAS14_DIR)
        # Estimate peak precip for classification (use Lonfat model if rain_result available)
        _peak_precip_mm = 0.0
        if '_rainfall_raster_available' and '_rain_result_ref' in dir():
            pass  # Would use rain_result.max_precip_mm if captured
        else:
            # Estimate from parametric model parameters directly
            from flood_model.rainfall import (
                estimate_rain_rate_mm_hr, estimate_storm_duration_hr,
                estimate_total_precip_mm
            )
            _r = estimate_rain_rate_mm_hr(50.0, storm.max_wind_kt, storm.speed_kt, "right")
            _dur = estimate_storm_duration_hr(storm.speed_kt)
            _peak_precip_mm = estimate_total_precip_mm(_r, storm.speed_kt, _dur)

        _rp = get_return_period_for_storm(
            storm_lat=storm.landfall_lat,
            storm_lon=storm.landfall_lon,
            total_precip_mm=_peak_precip_mm,
            storm_speed_kt=storm.speed_kt,
            cache_dir=_atlas14_cache,
        )
        _rain_return_period_label = _rp.label
        print(f"  [Atlas14] Return period: {_rain_return_period_label} "
              f"(~{_peak_precip_mm:.0f} mm peak)")
    except Exception as _a14_err:
        print(f"  [Atlas14] Classification skipped (non-fatal): {_a14_err}")

    # 2b. NWM streamflow + CFIM HAND fluvial inundation layer
    #     ─────────────────────────────────────────────────────
    #     Fetch gaged streamflow via AHPS gauges (NWPS API) → NLDI COMID lookup,
    #     then run the HAND model on the downloaded NOAA OWP FIM rasters for each
    #     HUC8 that overlaps the cell.  The resulting fluvial depth raster is merged
    #     with surge and parametric rainfall into a 3-way compound raster used for
    #     flood polygon display.  Failure here is fully non-fatal.
    _fluvial_raster_path = os.path.join(sdir, f'cell_{col}_{row}_fluvial.tif')
    _fluvial_available   = False

    if not os.path.exists(_fluvial_raster_path):
        try:
            from persistent_paths import HAND_DIR, NWM_CACHE_DIR
            from rainfall.nwm_http_fetcher import fetch_nwm_for_cell
            from rainfall.cfim_fetcher    import get_hand_for_cell
            from flood_model.hand_model   import run_hand_model

            # Step 2b-i: NWM discharge (AHPS gauges → NLDI → discharge dict)
            _nwm = fetch_nwm_for_cell(
                lon_min=lon_min, lat_min=lat_min,
                lon_max=lon_max, lat_max=lat_max,
                landfall_lat=storm.landfall_lat,
                landfall_lon=storm.landfall_lon,
                nwm_cache_dir=str(NWM_CACHE_DIR),
                storm_id=storm.storm_id,
                col=col, row=row,
                radius_deg=4.0,
                min_flood_category='action',
                cache_ttl_seconds=1800,
            )
            _discharge_dict = _nwm.as_discharge_dict() if _nwm else {}

            if _discharge_dict:
                # Step 2b-ii: CFIM HAND rasters (download HUC8 tiles, mosaic to cell)
                _hand_files = get_hand_for_cell(
                    lon_min=lon_min, lat_min=lat_min,
                    lon_max=lon_max, lat_max=lat_max,
                    hand_cache_dir=str(HAND_DIR),
                    col=col, row=row,
                    storm_id=storm.storm_id,
                )

                if _hand_files is not None:
                    # Step 2b-iii: HAND inundation model
                    _hand_result = run_hand_model(
                        hand_path=_hand_files.hand_path,
                        catchment_path=_hand_files.catchment_path,
                        discharge_data=_discharge_dict,
                        output_dir=sdir,
                        storm_id=storm.storm_id,
                    )
                    if _hand_result.max_depth_m > 0:
                        import shutil as _shutil
                        _shutil.copy2(_hand_result.depth_path, _fluvial_raster_path)
                        _fluvial_available = True
                        print(
                            f"  [HAND] fluvial layer: max={_hand_result.max_depth_m:.2f}m, "
                            f"reaches={_hand_result.reaches_flooded}, "
                            f"huc8s={_hand_files.huc8s}"
                        )
                else:
                    print(f"  [HAND] CFIM rasters not available for cell ({col},{row})")
            else:
                print(f"  [HAND] No NWM discharge data for cell ({col},{row})")

        except Exception as _hand_err:
            print(f"  [HAND] Fluvial layer failed (non-fatal): {_hand_err}")
    else:
        _fluvial_available = True

    # Optional: merge surge + rainfall + fluvial into compound raster for flood polygon display
    if not os.path.exists(compound_raster_path):
        try:
            from flood_model.compound import merge_compound_flood

            # Choose the best available rainfall/fluvial source
            # Priority: fluvial HAND > parametric rainfall > surge only
            _rain_source = None
            if _fluvial_available:
                _rain_source = _fluvial_raster_path
            elif _rainfall_raster_available:
                _rain_source = rainfall_raster_path

            if _rain_source:
                comp = merge_compound_flood(
                    surge_depth_path=raster_path,
                    rainfall_depth_path=_rain_source,
                    output_dir=sdir,
                    storm_id=storm.storm_id,
                    interaction_factor=0.5,
                )
                import shutil as _shutil
                _shutil.copy2(comp.compound_depth_path, compound_raster_path)
                for _p in (comp.compound_depth_path, comp.overlap_mask_path):
                    if os.path.exists(_p) and _p != compound_raster_path:
                        try:
                            os.remove(_p)
                        except OSError:
                            pass
        except Exception as _comp_err:
            print(f"  [compound] Merge failed (non-fatal): {_comp_err}")

    # 3. Flood polygons — use compound raster when available, fall back to surge
    _progress.update(step='Building flood map', step_num=3)
    flood_source = compound_raster_path if os.path.exists(compound_raster_path) else raster_path
    if not os.path.exists(flood_path):
        raster_to_geojson(flood_source, flood_path)
    with open(flood_path) as f:
        flood_data = json.load(f)

    # 4. Fetch real OSM/NSI buildings
    _progress.update(step='Fetching building footprints', step_num=4)
    buildings_path = os.path.join(sdir, f'cell_{col}_{row}_buildings.json')
    fetch_buildings(lon_min, lat_min, lon_max, lat_max, buildings_path, cache=True)

    with open(buildings_path) as f:
        buildings_data = json.load(f)
    n_buildings = len(buildings_data.get("features", []))

    if not n_buildings:
        empty = _empty_fc()
        with open(damage_path, 'w') as f:
            json.dump(empty, f)
        _update_building_index(storm.storm_id, col, row, 0)
        _progress.update(step='Complete', step_num=5)
        return {"buildings": empty, "flood": flood_data}

    # 5. Run HAZUS damage model as a time-series (per-tick surge/rainfall/
    #    cumulative). Emits two artifacts:
    #      - cell_C_R_ticks.json: compact per-tick per-peril bundle for the
    #        frontend time-slider + peril toggle.
    #      - cell_C_R_damage.geojson: final-tick GeoJSON (backwards-compatible
    #        with layers that aren't tick-aware yet).
    _progress.update(step='Running damage model', step_num=5)
    ticks_path = damage_path.replace('_damage.geojson', '_ticks.json')
    estimate_damage_timeseries_from_raster(
        depth_raster_path=raster_path,
        buildings_geojson_path=buildings_path,
        ticks_output_path=ticks_path,
        final_geojson_path=damage_path,
        storm_id=storm.storm_id,
        landfall_lat=storm.landfall_lat,
        landfall_lon=storm.landfall_lon,
        max_wind_kt=storm.max_wind_kt,
        storm_speed_kt=storm.speed_kt,
        storm_heading_deg=storm.heading_deg,
    )

    with open(damage_path) as f:
        damage_data = json.load(f)

    # Inject cell-level metadata into the FeatureCollection root so CAT reports
    # can display rainfall return period, impervious fraction, etc.
    if isinstance(damage_data, dict):
        damage_data.setdefault("metadata", {})
        damage_data["metadata"].update({
            "rain_return_period":   _rain_return_period_label,
            "nlcd_runoff_coeff":    round(_nlcd_runoff_coeff, 3),
            "fluvial_available":    _fluvial_available,
        })

    # 6. Record building count in lightweight index (instant confidence lookups).
    _update_building_index(storm.storm_id, col, row, n_buildings)

    # 7. Clean up intermediate files to save volume space.
    #    Keep: damage.geojson, flood.geojson (required for cache hits)
    #    Remove: surge depth.tif, rainfall.tif, compound.tif, buildings.json
    for tmp in (raster_path, buildings_path, rainfall_raster_path,
                compound_raster_path, _fluvial_raster_path):
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except OSError:
            pass

    return {"buildings": damage_data, "flood": flood_data}


def _empty_fc():
    return {"type": "FeatureCollection", "features": []}

def _empty_fc_pair():
    return {"buildings": _empty_fc(), "flood": _empty_fc()}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# HTTP Server
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class CellHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path.rstrip('/')
        params = parse_qs(parsed.query)

        # ── GET /api/seasons ── list of {year, count} for accordion (2015+)
        if path == '/api/seasons':
            try:
                data = [s for s in get_seasons() if s['year'] >= SEASON_MIN_YEAR]
                self._send_raw(200, json.dumps(data).encode())
            except Exception as e:
                self._send_error(500, str(e))
            return

        # ── GET /api/storms/historic ── curated notable storms (pre-2015 ok)
        if path == '/api/storms/historic':
            try:
                data = [_inject_dps(s.to_dict()) for s in HISTORICAL_STORMS]
                self._send_raw(200, json.dumps(data).encode())
            except Exception as e:
                self._send_error(500, str(e))
            return

        # ── GET /api/season/<year> ── all storms for a year
        if path.startswith('/api/season/'):
            try:
                year = int(path.split('/')[3])
                storms = get_storms_for_year(year)
                self._send_raw(200, json.dumps([_inject_dps(s.to_dict()) for s in storms]).encode())
            except (ValueError, IndexError):
                self._send_error(400, 'Invalid year')
            except Exception as e:
                self._send_error(500, str(e))
            return

        # ── GET /api/storms/search?q=katrina ── search by name or ID
        if path == '/api/storms/search':
            q = params.get('q', [''])[0]
            if not q:
                self._send_error(400, 'Missing ?q= parameter')
                return
            try:
                ql = q.lower().strip()
                # Search curated HISTORICAL_STORMS first (better names + DPS scores)
                seen_ids = set()
                results = []
                for s in HISTORICAL_STORMS:
                    if ql in s.name.lower() or ql in s.storm_id.lower():
                        results.append(_inject_dps(s.to_dict()))
                        seen_ids.add(s.storm_id)
                # Then fill remaining slots from HURDAT2
                for s in search_storms(q):
                    if s.storm_id not in seen_ids:
                        results.append(_inject_dps(s.to_dict()))
                        seen_ids.add(s.storm_id)
                    if len(results) >= 20:
                        break
                self._send_raw(200, json.dumps(results).encode())
            except Exception as e:
                self._send_error(500, str(e))
            return

        # ── GET /api/storms/active ── currently active NHC storms
        if path == '/api/storms/active':
            try:
                active = fetch_active_storms()
                self._send_raw(200, json.dumps([_inject_dps(s.to_dict()) for s in active]).encode())
            except Exception as e:
                self._send_error(500, str(e))
            return

        # ── GET /api/storm/<id>/activate ── select a storm for analysis
        if path.startswith('/api/storm/') and path.endswith('/activate'):
            storm_id = path.split('/')[3]
            # Check HURDAT2 first, then curated historic list
            storm = get_storm_by_id(storm_id)
            if storm is None:
                # Try curated historic storms (different ID format)
                for hs in HISTORICAL_STORMS:
                    if hs.storm_id == storm_id:
                        storm = hs
                        break
            if storm is None:
                self._send_error(404, f"Storm '{storm_id}' not found")
                return

            global _active_storm, _active_exposure_region
            with _active_storm_lock:
                _active_storm = storm
            print(f"\n{'='*60}")
            print(f"ACTIVATED: {storm.name} ({storm.year}) — Cat {storm.category}")
            print(f"  Landfall: ({storm.landfall_lon}, {storm.landfall_lat})")
            print(f"  Wind: {storm.max_wind_kt} kt  Pressure: {storm.min_pressure_mb} mb")
            print(f"  Grid origin: ({storm.grid_origin_lon}, {storm.grid_origin_lat})")
            print(f"{'='*60}\n")

            # Load all pre-cached cells around landfall (3×3 for all storms
            # to stay within Railway 5 GB volume limit).  Cached cells return
            # instantly; uncached ones are generated on the fly.  Users can
            # expand coverage on-demand by clicking grid borders.
            _ACTIVATE_CELLS = [(c, r) for r in range(-1, 2) for c in range(-1, 2)]
            total_act = len(_ACTIVATE_CELLS) * 4  # 4 steps per cell
            _progress.update(step='Initializing', step_num=0, total_steps=total_act,
                             started_at=_time.time(), storm_id=storm.storm_id)

            grid_cells = {}
            for idx, (c, r) in enumerate(_ACTIVATE_CELLS):
                _progress.update(step=f'Loading cell ({c},{r})', step_num=idx * 4)
                print(f"  Loading cell ({c},{r})...")
                grid_cells[f'{c},{r}'] = load_cell(c, r)

            center_data = grid_cells.get('0,0')
            _progress.update(step='Complete', step_num=total_act)

            # R5: Attach validation confidence after cell load
            conf = _compute_confidence(storm.storm_id)
            storm_data = _inject_dps(storm.to_dict())
            storm_data['confidence'] = conf['confidence']
            storm_data['building_count'] = conf['building_count']
            # R8: Compute Expected Loss Index
            eli = _compute_eli(storm_data.get('dps_score', 0), conf['building_count'])
            storm_data['eli'] = eli['eli']
            storm_data['eli_tier'] = eli['eli_tier']
            # R11: Dynamic exposure reclassification
            _active_exposure_region = storm_data.get('exposure_region', '')
            vdps = _compute_validated_dps(storm_data.get('dps_score', 0), conf['building_count'], _active_exposure_region)
            storm_data['validated_dps'] = vdps['validated_dps']
            storm_data['dps_adjustment'] = vdps['dps_adjustment']
            storm_data['dps_adj_reason'] = vdps['dps_adj_reason']
            adj_note = f"  Validated DPS: {vdps['validated_dps']:.1f} ({vdps['dps_adj_reason']})" if vdps['dps_adjustment'] != 0 else ""
            print(f"  Confidence: {conf['confidence']} ({conf['building_count']} buildings)  ELI: {eli['eli']:.1f} ({eli['eli_tier']}){adj_note}")

            # Population context (Census Bureau)
            try:
                pop_ctx = get_population_context(storm.landfall_lat, storm.landfall_lon)
                if pop_ctx:
                    storm_data['population'] = pop_ctx
                    print(f"  Population: {pop_ctx.get('pop_label', '?')} in {pop_ctx.get('county_name', '?')}, {pop_ctx.get('state_code', '?')}")
            except Exception as e:
                print(f"  [warn] Census population lookup failed: {e}")

            # Record model run in validation ledger
            try:
                model_run = record_from_activation(storm.storm_id, grid_cells, storm_data)
                print(f"  Validation: logged run — ${model_run.modeled_loss/1e6:,.1f}M modeled, "
                      f"{model_run.building_count} bldgs ({model_run.nsi_count} NSI / {model_run.osm_count} OSM)")
                # Attach ground truth comparison if available
                gt = get_ground_truth(storm.storm_id)
                if gt:
                    storm_data['ground_truth'] = {
                        'actual_total_B': gt.actual_damage_B,
                        'surge_fraction': gt.surge_fraction,
                        'surge_damage_B': gt.surge_damage_B,
                        'source': gt.source,
                    }
            except Exception as e:
                print(f"  [warn] Validation ledger failed: {e}")

            response_data = {
                "storm": storm_data,
                "center_cell": center_data,
            }
            # R6: Include all grid cells if 3x3 was loaded
            if grid_cells:
                response_data["grid_cells"] = grid_cells
            body = json.dumps(response_data).encode()
            self._send_raw(200, body)
            return

        # ── GET /api/cell?col=N&row=N ── load a grid cell
        if path == '/api/cell':
            try:
                col = int(params['col'][0])
                row = int(params['row'][0])
            except (KeyError, ValueError, IndexError):
                self._send_error(400, 'Missing or invalid col/row')
                return

            if _active_storm is None:
                self._send_error(400, 'No storm active')
                return

            try:
                print(f"\n--- Loading cell ({col}, {row}) for {_active_storm.name} ---")
                data = load_cell(col, row)
                # R5: Include updated confidence after cell load
                conf = _compute_confidence(_active_storm.storm_id)
                data['confidence'] = conf['confidence']
                data['building_count'] = conf['building_count']
                # R8: Updated ELI with new building count
                dps_val = _DPS_SCORES.get(_active_storm.storm_id, 0) or _DPS_SCORES.get(_active_storm.storm_id.lower(), 0)
                eli = _compute_eli(dps_val, conf['building_count'])
                data['eli'] = eli['eli']
                data['eli_tier'] = eli['eli_tier']
                # R11: Updated validated DPS
                vdps = _compute_validated_dps(dps_val, conf['building_count'], _active_exposure_region)
                data['validated_dps'] = vdps['validated_dps']
                data['dps_adjustment'] = vdps['dps_adjustment']
                data['dps_adj_reason'] = vdps['dps_adj_reason']
                body = json.dumps(data).encode()
                self._send_raw(200, body)
                n = len(data.get('buildings', {}).get('features', []))
                print(f"--- Cell ({col},{row}): {n} buildings | Confidence: {conf['confidence']} ({conf['building_count']} total) ---")
            except Exception as e:
                print(f"Error loading cell ({col},{row}): {e}")
                import traceback; traceback.print_exc()
                self._send_error(500, str(e))
            return

        # ── GET /api/cell_ticks?col=N&row=N ── time-series peril bundle
        # Served as a *separate* fetch so the main /api/cell response stays
        # small and renders the final-state damage immediately. The frontend
        # pulls this in the background to activate the time slider + peril
        # toggle. Returns 404 if the bundle hasn't been generated yet (e.g.
        # legacy cells warmed before the peril pipeline shipped).
        if path == '/api/cell_ticks':
            try:
                col = int(params['col'][0])
                row = int(params['row'][0])
            except (KeyError, ValueError, IndexError):
                self._send_error(400, 'Missing or invalid col/row')
                return
            if _active_storm is None:
                self._send_error(400, 'No storm active')
                return
            sdir = _storm_cache_dir(_active_storm)
            ticks_path = os.path.join(sdir, f'cell_{col}_{row}_ticks.json')
            if not os.path.exists(ticks_path):
                self._send_error(404, 'No ticks bundle for this cell yet')
                return
            try:
                with open(ticks_path, 'rb') as f:
                    self._send_raw(200, f.read())
            except Exception as e:
                self._send_error(500, str(e))
            return

        # ── GET /api/progress ── poll current processing step
        if path == '/api/progress':
            elapsed = round(_time.time() - _progress['started_at'], 1) if _progress['started_at'] else 0
            self._send_json(200, {
                'step': _progress['step'],
                'step_num': _progress['step_num'],
                'total_steps': _progress['total_steps'],
                'elapsed': elapsed,
                'storm_id': _progress['storm_id'],
            })
            return

        # ── GET /api/geocode/reverse?lat=N&lon=N ── cached reverse geocoding
        if path == '/api/geocode/reverse':
            lat = params.get('lat', [''])[0]
            lon = params.get('lon', [''])[0]
            if not lat or not lon:
                self._send_error(400, 'Missing lat/lon')
                return
            try:
                result = _geocode_reverse(float(lat), float(lon))
                self._send_json(200, result)
            except Exception as e:
                self._send_error(500, str(e))
            return

        # ── GET /api/geocode/search?q=address ── cached forward geocoding
        if path == '/api/geocode/search':
            q = params.get('q', [''])[0]
            if not q:
                self._send_error(400, 'Missing ?q= parameter')
                return
            try:
                result = _geocode_forward(q.strip())
                self._send_json(200, result)
            except Exception as e:
                self._send_error(500, str(e))
            return

        # ── GET /api/forecast/track ── forecast track points + cone for active storms
        if path == '/api/forecast/track':
            try:
                tracks = fetch_forecast_track()
                cones = fetch_forecast_cone()
                result = []
                for t in tracks:
                    td = t.to_dict()
                    cone_key = t.storm_name.upper()
                    td['cone'] = cones.get(cone_key)
                    result.append(td)
                self._send_json(200, result)
            except Exception as e:
                self._send_error(500, str(e))
            return

        # ── GET /api/simulate?lat=N&lon=N&wind=N&pressure=N ── what-if scenario
        if path == '/api/simulate':
            if _active_storm is None:
                self._send_error(400, 'No storm active — activate a storm first')
                return
            try:
                sim_lat = float(params.get('lat', [str(_active_storm.landfall_lat)])[0])
                sim_lon = float(params.get('lon', [str(_active_storm.landfall_lon)])[0])
                sim_wind = int(params.get('wind', [str(_active_storm.max_wind_kt)])[0])
                sim_pressure = int(params.get('pressure', [str(_active_storm.min_pressure_mb)])[0])
                sim_heading = float(params.get('heading', [str(_active_storm.heading_deg)])[0])
                sim_speed = float(params.get('speed', [str(_active_storm.speed_kt)])[0])
            except (ValueError, TypeError):
                self._send_error(400, 'Invalid simulation parameters')
                return

            print(f"\n{'='*60}")
            print(f"SIMULATION: {_active_storm.name} — What-if at ({sim_lon:.2f}, {sim_lat:.2f})")
            print(f"  Wind: {sim_wind} kt  Pressure: {sim_pressure} mb")
            print(f"{'='*60}")

            # Build a temporary StormEntry with the user's parameters
            sim_storm = StormEntry(
                storm_id=f"{_active_storm.storm_id}_sim",
                name=_active_storm.name,
                year=_active_storm.year,
                category=_active_storm.category,
                status="simulation",
                landfall_lon=sim_lon,
                landfall_lat=sim_lat,
                max_wind_kt=sim_wind,
                min_pressure_mb=sim_pressure,
                heading_deg=sim_heading,
                speed_kt=sim_speed,
                basin=_active_storm.basin,
                advisory="simulation",
            )

            # Run center cell only (fast ~15-30s)
            _progress.update(step='Running simulation', step_num=0, total_steps=4,
                             started_at=_time.time(), storm_id=sim_storm.storm_id)

            sim_cache = os.path.join(CACHE_DIR, sim_storm.storm_id)
            os.makedirs(sim_cache, exist_ok=True)

            col, row = 0, 0
            origin_lon = sim_storm.grid_origin_lon
            origin_lat = sim_storm.grid_origin_lat
            lon_min = origin_lon + col * CELL_WIDTH
            lat_min = origin_lat + row * CELL_HEIGHT
            lon_max = lon_min + CELL_WIDTH
            lat_max = lat_min + CELL_HEIGHT

            # 1. Surge raster
            _progress.update(step='Generating surge model', step_num=1)
            raster_path = os.path.join(sim_cache, f'sim_depth.tif')
            generate_surge_raster(
                lon_min=lon_min, lat_min=lat_min,
                lon_max=lon_max, lat_max=lat_max,
                output_path=raster_path,
                landfall_lon=sim_storm.landfall_lon,
                landfall_lat=sim_storm.landfall_lat,
                max_wind_kt=sim_storm.max_wind_kt,
                min_pressure_mb=sim_storm.min_pressure_mb,
                heading_deg=sim_storm.heading_deg,
                speed_kt=sim_storm.speed_kt,
            )

            # 2. Flood polygons
            _progress.update(step='Building flood map', step_num=2)
            flood_path = os.path.join(sim_cache, f'sim_flood.geojson')
            raster_to_geojson(raster_path, flood_path)
            with open(flood_path) as f:
                flood_data = json.load(f)

            # 3. Buildings
            _progress.update(step='Fetching building footprints', step_num=3)
            buildings_path = os.path.join(sim_cache, f'sim_buildings.json')
            fetch_buildings(lon_min, lat_min, lon_max, lat_max, buildings_path, cache=True)

            # 4. Damage model
            _progress.update(step='Running damage model', step_num=4)
            damage_path = os.path.join(sim_cache, f'sim_damage.geojson')
            with open(buildings_path) as f:
                buildings_data = json.load(f)
            if buildings_data.get("features"):
                estimate_damage_from_raster(
                    raster_path, buildings_path, damage_path,
                    storm_id=sim_storm.storm_id,
                    landfall_lat=sim_storm.landfall_lat,
                    landfall_lon=sim_storm.landfall_lon,
                    max_wind_kt=sim_wind,
                    storm_speed_kt=sim_speed,
                    storm_heading_deg=sim_heading,
                )
            else:
                with open(damage_path, 'w') as f:
                    json.dump({"type": "FeatureCollection", "features": []}, f)

            with open(damage_path) as f:
                damage_data = json.load(f)

            # Compute quick summary
            total_loss = sum(f['properties'].get('estimated_loss_usd', 0) or 0
                             for f in damage_data.get('features', []))
            n_buildings = len(damage_data.get('features', []))
            n_damaged = sum(1 for f in damage_data.get('features', [])
                           if (f['properties'].get('total_damage_pct', 0) or 0) > 0)

            _progress.update(step='Complete', step_num=4)

            # Population context
            pop_ctx = None
            try:
                pop_ctx = get_population_context(sim_lat, sim_lon)
            except Exception:
                pass

            sim_result = {
                "simulation": True,
                "parameters": {
                    "lat": sim_lat, "lon": sim_lon,
                    "wind_kt": sim_wind, "pressure_mb": sim_pressure,
                    "heading_deg": sim_heading, "speed_kt": sim_speed,
                },
                "summary": {
                    "total_loss": round(total_loss, 2),
                    "total_loss_M": round(total_loss / 1e6, 1),
                    "buildings_assessed": n_buildings,
                    "buildings_damaged": n_damaged,
                    "scope": "center_cell",
                },
                "population": pop_ctx,
                "buildings": damage_data,
                "flood": flood_data,
            }

            # Confidence interval from backtesting
            try:
                pred = predict_loss_range(total_loss)
                sim_result["prediction"] = pred
            except Exception:
                pass

            print(f"  Simulation complete: ${total_loss/1e6:,.1f}M loss, {n_buildings} buildings")
            self._send_json(200, sim_result)
            return

        # ── GET /api/validation/backtest ── full backtest report
        if path == '/api/validation/backtest':
            try:
                report = run_backtest()
                self._send_json(200, report.to_dict())
            except Exception as e:
                self._send_error(500, str(e))
            return

        # ── GET /api/validation/storm/<id> ── score a single storm
        if path.startswith('/api/validation/storm/'):
            try:
                sid = path.split('/')[4]
                score = score_storm(sid)
                if score:
                    self._send_json(200, score.to_dict())
                else:
                    self._send_json(200, {'error': 'No ground truth or model run for this storm',
                                           'storm_id': sid,
                                           'has_ground_truth': get_ground_truth(sid) is not None})
            except Exception as e:
                self._send_error(500, str(e))
            return

        # ── GET /api/validation/predict?loss=N ── confidence interval
        if path == '/api/validation/predict':
            try:
                loss = float(params.get('loss', ['0'])[0])
                result = predict_loss_range(loss)
                self._send_json(200, result)
            except Exception as e:
                self._send_error(500, str(e))
            return

        # ── GET /api/rainfall?storm_id=<id>&duration=72&pass=2 ──
        # Returns MRMS QPE overlay GeoTIFF stats + bounding box for the active storm.
        # Used by the frontend Rainfall (obs) map toggle.
        if path == '/api/rainfall':
            if _active_storm is None:
                self._send_error(400, 'No storm active')
                return
            try:
                sys.path.insert(0, os.path.join(BASE_DIR, 'src'))
                from rainfall.mrms_fetcher import MRMSFetcher, storm_bbox_from_catalog_entry
                duration_hr = int(params.get('duration', ['72'])[0])
                pass_level  = int(params.get('pass',     ['2'])[0])
                realtime    = params.get('realtime', ['0'])[0] == '1'
                bbox = storm_bbox_from_catalog_entry(
                    _active_storm.landfall_lat, _active_storm.landfall_lon, buffer_deg=4.0
                )
                mrms_cache = os.path.join(PERSISTENT_DIR, 'mrms')
                os.makedirs(mrms_cache, exist_ok=True)
                fetcher = MRMSFetcher(cache_dir=mrms_cache, keep_raw_grib=False)
                result = fetcher.fetch_storm_accumulation(
                    storm_bbox=bbox,
                    duration_hr=duration_hr,
                    pass_level=pass_level,
                    realtime=realtime,
                )
                if result is None:
                    self._send_json(200, {'available': False, 'storm_id': _active_storm.storm_id})
                    return
                self._send_json(200, {
                    'available': True,
                    'storm_id': _active_storm.storm_id,
                    'product': result.product,
                    'valid_time': result.valid_time.isoformat() if result.valid_time else None,
                    'duration_hr': result.duration_hr,
                    'max_precip_mm': round(result.max_precip_mm, 1),
                    'avg_precip_mm': round(result.avg_precip_mm, 1),
                    'max_precip_in': round(result.max_precip_mm / 25.4, 2),
                    'bbox': list(result.bbox),
                    'tif_path': result.clipped_tif_path,
                    'source': result.source,
                })
            except Exception as e:
                self._send_error(500, str(e))
            return

        # ── GET /api/qpf ──
        # Returns WPC 72-hour Quantitative Precipitation Forecast stats for the
        # active storm area as a planning overlay.
        #
        # IMPORTANT: WPC QPF is a DETERMINISTIC forecast best suited for
        # fast-moving extratropical and post-tropical systems.  For slow-moving
        # tropical cyclones (≤ 5 kt forward speed), the WPC QPF systematically
        # underestimates total accumulation because it uses a synoptic-scale
        # NWP model that doesn't capture TC stall dynamics well.  The API
        # response includes a `caveat` field indicating model reliability.
        #
        # The QPF raster is NOT used as a model input — it is served as a
        # read-only planning overlay for the frontend map panel.
        if path == '/api/qpf':
            if _active_storm is None:
                self._send_error(400, 'No storm active')
                return
            try:
                sys.path.insert(0, os.path.join(BASE_DIR, 'src'))
                from data_ingest.noaa_fetchers import QPFFetcher
                from data_ingest.config import IngestConfig
                import time as _time_qpf

                qpf_cache = os.path.join(PERSISTENT_DIR, 'qpf')
                os.makedirs(qpf_cache, exist_ok=True)
                cache_meta = os.path.join(qpf_cache, 'latest_meta.json')

                # Return cached result if fresh (< 6 hours for WPC QPF)
                if os.path.exists(cache_meta):
                    try:
                        with open(cache_meta) as _f:
                            _cached = json.load(_f)
                        age_hr = (_time_qpf.time() - _cached.get('fetched_at', 0)) / 3600
                        if age_hr < 6 and _cached.get('storm_id') == _active_storm.storm_id:
                            self._send_json(200, _cached)
                            return
                    except Exception:
                        pass

                config = IngestConfig()
                fetcher = QPFFetcher(config)

                # Build a simple storm polygon from landfall + 4° buffer
                _clat = _active_storm.landfall_lat
                _clon = _active_storm.landfall_lon
                storm_geom = {
                    "type": "Polygon",
                    "coordinates": [[
                        [_clon - 4, _clat - 4], [_clon + 4, _clat - 4],
                        [_clon + 4, _clat + 4], [_clon - 4, _clat + 4],
                        [_clon - 4, _clat - 4],
                    ]],
                }

                qpf_result = fetcher.fetch(storm_geom, qpf_cache, duration_hours=72)

                # Determine reliability caveat based on storm forward speed.
                # Thresholds: <5 kt = nearly stationary (low), ≤10 kt = slow-
                # moving tropical (medium), >10 kt = fast-moving (high).
                # Use ≤10 (not <10) so that Harvey-class storms (catalog 10 kt)
                # correctly land in "medium" rather than "high" — at 10 kt a TC
                # is still within NHC's "slow-moving" definition and WPC QPF
                # is known to underestimate rainfall for such storms.
                _spd = getattr(_active_storm, 'speed_kt', 10.0) or 10.0
                if _spd < 5:
                    caveat = ("WPC QPF unreliable for nearly-stationary storms — "
                              "use MRMS observed QPE instead")
                    reliability = "low"
                elif _spd <= 10:
                    caveat = ("WPC QPF may underestimate totals for slow-moving "
                              "tropical systems (≤10 kt) — verify against MRMS QPE")
                    reliability = "medium"
                else:
                    caveat = "WPC QPF reliable for fast-moving post-tropical/extratropical systems"
                    reliability = "high"

                _meta = {
                    'available': qpf_result is not None,
                    'storm_id': _active_storm.storm_id,
                    'storm_speed_kt': round(_spd, 1),
                    'reliability': reliability,
                    'caveat': caveat,
                    'fetched_at': _time_qpf.time(),
                }
                if qpf_result is not None:
                    _meta.update({
                        'duration_hr': 72,
                        'max_precip_mm': round(getattr(qpf_result, 'total_precip_mm', 0), 1),
                        'max_precip_in': round(getattr(qpf_result, 'total_precip_mm', 0) / 25.4, 2),
                        'tif_path': getattr(qpf_result, 'path', None),
                        'source': 'wpc_qpf_72hr',
                    })

                # Cache the response
                try:
                    with open(cache_meta, 'w') as _f:
                        _out = {k: v for k, v in _meta.items() if k != 'fetched_at'}
                        json.dump({**_out, 'fetched_at': _meta['fetched_at']}, _f)
                except Exception:
                    pass

                self._send_json(200, _meta)
            except Exception as e:
                self._send_error(500, str(e))
            return

        # ── GET /api/gauges?lat=N&lon=N&radius=4 ──
        # Returns active flood gauges near the storm as GeoJSON.
        # Used by the frontend Flood Warnings map toggle.
        if path == '/api/gauges':
            if _active_storm is None:
                self._send_error(400, 'No storm active')
                return
            try:
                from rainfall.ahps_gauges import AHPSClient
                radius  = float(params.get('radius', ['4.0'])[0])
                min_cat = params.get('category', ['action'])[0]
                client = AHPSClient(cache_ttl_seconds=300)
                gauges = client.get_gauges_for_storm(
                    landfall_lat=_active_storm.landfall_lat,
                    landfall_lon=_active_storm.landfall_lon,
                    radius_deg=radius,
                    min_flood_category=min_cat,
                )
                geojson = client.to_geojson(gauges)
                body = json.dumps({
                    'storm_id': _active_storm.storm_id,
                    'gauge_count': len(gauges),
                    'at_or_above_major': sum(1 for g in gauges if g.flood_category == 'major'),
                    'at_or_above_moderate': sum(1 for g in gauges if g.flood_category in ('moderate', 'major')),
                    'at_or_above_minor': sum(1 for g in gauges if g.flood_category in ('minor', 'moderate', 'major')),
                    'gauges': geojson,
                }).encode()
                self._send_raw(200, body)
            except Exception as e:
                self._send_error(500, str(e))
            return

        # ── GET /api/health ──
        if path == '/api/health':
            self._send_json(200, {'status': 'ok', 'active_storm': _active_storm.storm_id if _active_storm else None})
            return

        # ── GET /api/health/storage ──
        if path == '/api/health/storage':
            from persistent_paths import storage_summary
            self._send_json(200, storage_summary())
            return

        # ── Static file serving (built React frontend) ──
        # Strip query string; map "/" → "/index.html"
        static_path = parsed.path.rstrip('/') or '/index.html'
        if static_path == '':
            static_path = '/index.html'
        file_path = os.path.join(_STATIC_DIR, static_path.lstrip('/'))
        # SPA fallback: unknown paths → index.html (client-side routing)
        if not os.path.isfile(file_path):
            file_path = os.path.join(_STATIC_DIR, 'index.html')
        if os.path.isfile(file_path):
            mime, _ = mimetypes.guess_type(file_path)
            mime = mime or 'application/octet-stream'
            with open(file_path, 'rb') as fh:
                body = fh.read()
            try:
                self.send_response(200)
                self.send_header('Content-Type', mime)
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            except BrokenPipeError:
                pass
        else:
            self._send_error(404, 'Not found')

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()

    def _send_raw(self, code, body: bytes):
        # Gzip compress large responses (>1 KB) if client supports it
        try:
            accept_enc = self.headers.get('Accept-Encoding', '')
            if len(body) > 1024 and 'gzip' in accept_enc:
                import gzip as _gzip
                body = _gzip.compress(body, compresslevel=6)
                self.send_response(code)
                self.send_header('Content-Encoding', 'gzip')
            else:
                self.send_response(code)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except BrokenPipeError:
            # Client disconnected before we finished writing (e.g., browser
            # timed out during a long cell activation).  Safe to ignore —
            # the data was cached, so the next request will be fast.
            pass

    def _send_json(self, code, data):
        self._send_raw(code, json.dumps(data).encode())

    def _send_error(self, code, message):
        self._send_json(code, {'error': message})

    def log_message(self, format, *args):
        pass


class ThreadingHTTPServer(ThreadingMixIn, HTTPServer):
    """Handle each request in a new thread so long cell loads don't block health checks."""
    daemon_threads = True


def main():
    # Railway injects PORT at runtime; SURGE_API_PORT is a local-dev override.
    port = int(os.environ.get('PORT', 8000))
    server = ThreadingHTTPServer(('0.0.0.0', port), CellHandler)
    print(f"SurgeDPS Cell API running on http://localhost:{port}")
    print(f"Cell size: {CELL_WIDTH}° x {CELL_HEIGHT}°")
    print(f"Cache dir: {CACHE_DIR}")
    # Pre-load HURDAT2 on startup
    get_seasons()

    print(f"\nEndpoints:")
    print(f"  GET /api/seasons               — season list for browser")
    print(f"  GET /api/season/<year>          — storms for a year")
    print(f"  GET /api/storms/search?q=name   — search storms")
    print(f"  GET /api/storms/active          — active NHC storms")
    print(f"  GET /api/storm/<id>/activate    — select a storm")
    print(f"  GET /api/cell?col=N&row=N       — load a grid cell")
    print(f"\nWaiting for requests...\n")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down.")
        server.server_close()


if __name__ == '__main__':
    main()
