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
# peril_timeseries import deferred — will land with the lazy-ticks endpoint
# + slider UI. Kept out of the hot path so activate stays under 5 min.
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

# ── Rainfall tile server state ──
# Keyed by storm_id, value is the absolute path to the MRMS-clipped GeoTIFF
# that /api/rainfall_tile serves via rio-tiler. Populated on the first
# /api/rainfall hit per storm (MRMSFetcher caches the raw GRIB + clipped tif
# under PERSISTENT_DIR/mrms). Keeping it in-memory means tiles don't have
# to re-run the MRMS fetch; the tif is already on disk.
_rainfall_tif_by_storm: dict[str, str] = {}
_rainfall_tif_lock = _threading.Lock()

# ── Compound tile server state ──
# Keyed by storm_id, value is the absolute path to the mosaic VRT/tif that
# /api/compound_tile serves. Built lazily from the per-cell compound tifs
# in <cache>/<storm_id>/cell_*_compound.tif the first time /api/compound
# is hit, then rebuilt whenever new cells have been processed (detected by
# comparing the mosaic's mtime against the newest per-cell tif).
_compound_mosaic_by_storm: dict[str, str] = {}
_compound_lock = _threading.Lock()

# ── QPF forecast tile server state ──
# Parallel to _rainfall_tif_by_storm but for the WPC QPF forecast raster.
# Populated on the first /api/qpf hit per storm; served by /api/qpf_tile.
_qpf_tif_by_storm: dict[str, str] = {}
_qpf_tif_lock = _threading.Lock()

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

    # Check cache. Ticks bundle is generated lazily by /api/cell_ticks on
    # demand, so we only require damage + flood here.
    if (os.path.exists(damage_path) and os.path.exists(flood_path)):
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

    # 5. Run HAZUS damage model for the final (cumulative-peak) state. The
    #    per-tick time-series bundle is generated LAZILY on first /cell_ticks
    #    request so that the activate hot-path stays fast enough for the
    #    frontend's 5-min timeout. Cold Harvey's 3×3 grid with per-tick HAZUS
    #    blew past that; see commit history for background.
    _progress.update(step='Running damage model', step_num=5)
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
    #    Keep: damage.geojson, flood.geojson (required for cache hits);
    #          compound.tif (required for the /api/compound_tile map overlay —
    #          we stitch all per-cell compound tifs into a storm-wide mosaic
    #          on first tile request, so the individual cell files stay).
    #    Remove: surge depth.tif, rainfall.tif, buildings.json, fluvial.tif
    for tmp in (raster_path, buildings_path, rainfall_raster_path,
                _fluvial_raster_path):
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
# Rainfall XYZ tile rendering (NWS standard precipitation colormap)
#
# Serves PNG tiles reprojected from the MRMS-clipped GeoTIFF. The
# clipped tif is written to disk by MRMSFetcher; /api/rainfall
# registers its path, this module reads and tiles it on demand.
#
# Values in the source raster are mm of accumulated precipitation.
# We convert to inches for colormap lookup because the NWS ramp is
# inch-denominated. nodata and sub-threshold pixels render fully
# transparent so the basemap shows through.
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# NWS standard precipitation stops — matches weather.gov / MRMS
# product pages users have seen elsewhere. Stop = lower bound in inches.
_NWS_RAIN_STOPS_IN = [0.01, 0.10, 0.25, 0.50, 0.75, 1.00, 1.50, 2.00, 3.00, 4.00, 6.00, 8.00, 10.00, 15.00]
_NWS_RAIN_COLORS_RGB = [
    (200, 255, 200),   # 0.01"  very light green
    (100, 230, 100),   # 0.10"  light green
    (50,  180, 50),    # 0.25"  green
    (0,   130, 0),     # 0.50"  dark green
    (170, 200, 60),    # 0.75"  yellow-green
    (255, 255, 0),     # 1.00"  yellow
    (255, 200, 0),     # 1.50"
    (255, 140, 0),     # 2.00"  orange
    (255, 60,  0),     # 3.00"
    (200, 0,   0),     # 4.00"  red
    (150, 0,   100),   # 6.00"  magenta
    (110, 0,   180),   # 8.00"
    (70,  0,   200),   # 10.00" purple
    (255, 255, 255),   # 15.00"+ white/pink cap
]
_TILE_ALPHA = 200  # semi-transparent so basemap + flood polygons stay visible


def _nws_rainfall_rgba(mm, valid_mask):
    """Apply the NWS rainfall colormap to an (H, W) mm array.

    valid_mask: boolean (H, W) — True where the source reports a value.
    Returns (H, W, 4) uint8 RGBA. Pixels below the first stop OR invalid
    render fully transparent; above the top stop clamp to the top color.
    """
    import numpy as _np
    inches = mm / 25.4
    rgba = _np.zeros((*inches.shape, 4), dtype=_np.uint8)
    # searchsorted returns insertion index; idx==0 means below first stop
    idx = _np.searchsorted(_NWS_RAIN_STOPS_IN, inches, side='right')
    for bucket in range(1, len(_NWS_RAIN_STOPS_IN) + 1):
        sel = (idx == bucket) & valid_mask
        if not sel.any():
            continue
        r, g, b = _NWS_RAIN_COLORS_RGB[bucket - 1]
        rgba[sel] = (r, g, b, _TILE_ALPHA)
    return rgba


_TRANSPARENT_TILE_PNG_CACHE: bytes | None = None

def _transparent_tile_png() -> bytes:
    """Cached all-transparent 256x256 PNG for empty/out-of-bounds tiles."""
    global _TRANSPARENT_TILE_PNG_CACHE
    if _TRANSPARENT_TILE_PNG_CACHE is None:
        from PIL import Image as _Image
        import io as _io
        buf = _io.BytesIO()
        _Image.new('RGBA', (256, 256), (0, 0, 0, 0)).save(buf, format='PNG', optimize=True)
        _TRANSPARENT_TILE_PNG_CACHE = buf.getvalue()
    return _TRANSPARENT_TILE_PNG_CACHE


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Compound flood raster — surge + rainfall + fluvial at each cell's DEM
# resolution, stitched across processed cells. Values are depth in feet.
#
# Uses a distinct "compound hazard" ramp (pale cyan → teal → indigo →
# dark violet) so the layer visually reads different from the existing
# surge polygons (yellow→red) and the rainfall accumulation raster
# (green→magenta). Avoids the risk of confusing users who see three
# overlapping hazard views.
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

_COMPOUND_STOPS_FT = [0.25, 0.5, 1.0, 2.0, 3.0, 5.0, 7.0, 10.0, 14.0, 20.0]
_COMPOUND_COLORS_RGB = [
    (200, 240, 245),   # 0.25 ft — pale cyan (nuisance ponding)
    (150, 215, 230),   # 0.5
    (100, 190, 220),   # 1.0
    (60,  155, 210),   # 2.0 — mid teal
    (40,  120, 200),   # 3.0
    (55,  80,  190),   # 5.0 — indigo
    (85,  50,  170),   # 7.0
    (110, 30,  145),   # 10.0 — purple
    (130, 20,  110),   # 14.0
    (150, 10,  80),    # 20.0+ — dark violet
]
_COMPOUND_TILE_ALPHA = 200


def _compound_rgba(depth_ft, valid_mask):
    """Apply the compound-hazard colormap to an (H, W) ft array."""
    import numpy as _np
    rgba = _np.zeros((*depth_ft.shape, 4), dtype=_np.uint8)
    idx = _np.searchsorted(_COMPOUND_STOPS_FT, depth_ft, side='right')
    for bucket in range(1, len(_COMPOUND_STOPS_FT) + 1):
        sel = (idx == bucket) & valid_mask
        if not sel.any():
            continue
        r, g, b = _COMPOUND_COLORS_RGB[bucket - 1]
        rgba[sel] = (r, g, b, _COMPOUND_TILE_ALPHA)
    return rgba


def _build_storm_compound_mosaic(storm_id: str) -> tuple[str | None, dict]:
    """Merge all per-cell compound tifs for a storm into a single mosaic.

    Returns (mosaic_path, stats) where stats carries cell_count, max_depth_ft,
    and avg_depth_ft. If no cells have compound tifs yet the mosaic_path is
    None and the caller should respond with available=False.

    Cheap rebuild heuristic: the mosaic gets regenerated whenever any cell
    tif's mtime is newer than the mosaic's. In practice this triggers once
    per new cell load — the cost (rasterio.merge of ~100 small tifs) is
    sub-second on storm-scale data.
    """
    import glob as _glob, os as _os
    import rasterio as _rio
    from rasterio.merge import merge as _merge
    import numpy as _np

    cache_dir = _os.path.join(CACHE_DIR, storm_id)
    if not _os.path.isdir(cache_dir):
        return None, {'cell_count': 0}
    cell_tifs = sorted(_glob.glob(_os.path.join(cache_dir, 'cell_*_compound.tif')))
    if not cell_tifs:
        return None, {'cell_count': 0}

    mosaic_path = _os.path.join(cache_dir, 'storm_compound.tif')
    # Rebuild if mosaic missing or stale vs any cell tif.
    rebuild = True
    if _os.path.exists(mosaic_path):
        mo_mtime = _os.path.getmtime(mosaic_path)
        cell_mtime_max = max(_os.path.getmtime(t) for t in cell_tifs)
        rebuild = cell_mtime_max > mo_mtime

    if rebuild:
        datasets = [_rio.open(t) for t in cell_tifs]
        try:
            mosaic, transform = _merge(datasets)
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
            with _rio.open(mosaic_path, 'w', **profile) as dst:
                dst.write(mosaic[0], 1)
        finally:
            for d in datasets:
                d.close()

    # Summary stats for the response badge.
    with _rio.open(mosaic_path) as src:
        arr = src.read(1, masked=True)
        valid = ~arr.mask if hasattr(arr, 'mask') else _np.ones_like(arr, dtype=bool)
        data = _np.asarray(arr)
        max_ft = float(data[valid].max()) if valid.any() else 0.0
        avg_ft = float(data[valid].mean()) if valid.any() else 0.0
    return mosaic_path, {
        'cell_count': len(cell_tifs),
        'max_depth_ft': round(max_ft, 1),
        'avg_depth_ft': round(avg_ft, 2),
    }


def _render_compound_tile(mosaic_path: str, z: int, x: int, y: int) -> bytes:
    import io as _io
    from PIL import Image as _Image
    try:
        from rio_tiler.io import Reader as _Reader
        from rio_tiler.errors import TileOutsideBounds as _TileOOB
    except ImportError:
        return _transparent_tile_png()
    try:
        with _Reader(mosaic_path) as src:
            tile = src.tile(x, y, z, tilesize=256)
    except _TileOOB:
        return _transparent_tile_png()
    data = tile.data[0].astype('float32')
    valid = tile.mask > 0 if tile.mask is not None else (data > 0)
    rgba = _compound_rgba(data, valid)
    img = _Image.fromarray(rgba, 'RGBA')
    buf = _io.BytesIO()
    img.save(buf, format='PNG', optimize=True)
    return buf.getvalue()


def _render_rainfall_tile(tif_path: str, z: int, x: int, y: int) -> bytes:
    """Render a single XYZ tile as PNG bytes using rio-tiler + NWS colormap."""
    import io as _io
    from PIL import Image as _Image
    try:
        from rio_tiler.io import Reader as _Reader
        from rio_tiler.errors import TileOutsideBounds as _TileOOB
    except ImportError:
        # rio-tiler missing (dev env) — degrade gracefully to transparent.
        return _transparent_tile_png()

    try:
        with _Reader(tif_path) as src:
            tile = src.tile(x, y, z, tilesize=256)
    except _TileOOB:
        return _transparent_tile_png()

    # tile.data shape: (bands, H, W). We take band 0.
    data = tile.data[0].astype('float32')
    # rio-tiler's mask: 255 = valid, 0 = nodata.
    valid = tile.mask > 0 if tile.mask is not None else (data > 0)
    rgba = _nws_rainfall_rgba(data, valid)

    img = _Image.fromarray(rgba, 'RGBA')
    buf = _io.BytesIO()
    img.save(buf, format='PNG', optimize=True)
    return buf.getvalue()


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
        # Ticks generation is deferred — not produced by the activate hot
        # path (would exceed frontend's 5-min timeout). Returns 404 until
        # the lazy-generation path or background job is wired in. Frontend
        # handles 404 gracefully (slider stays hidden).
        if path == '/api/cell_ticks':
            self._send_error(404, 'No ticks bundle for this cell yet')
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
                # Register the clipped GeoTIFF with the tile server so
                # /api/rainfall_tile/{z}/{x}/{y}.png?storm_id=… can find it.
                if result.clipped_tif_path and os.path.exists(result.clipped_tif_path):
                    with _rainfall_tif_lock:
                        _rainfall_tif_by_storm[_active_storm.storm_id] = result.clipped_tif_path
                tile_url_template = (
                    f"/api/rainfall_tile/{{z}}/{{x}}/{{y}}.png"
                    f"?storm_id={_active_storm.storm_id}"
                ) if result.clipped_tif_path else None
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
                    'tile_url_template': tile_url_template,
                    'source': result.source,
                })
            except Exception as e:
                self._send_error(500, str(e))
            return

        # ── GET /api/rainfall_tile/{z}/{x}/{y}.png?storm_id=<id> ──
        # On-demand XYZ PNG tile server for the MRMS rainfall raster.
        # Backed by the clipped GeoTIFF registered by /api/rainfall.
        # Colormap: NWS standard precipitation ramp (green→yellow→red→magenta).
        # Tiles are cache-friendly (Cache-Control: public, 24h) because the
        # MRMS product is immutable once archived.
        if path.startswith('/api/rainfall_tile/'):
            try:
                storm_id = (params.get('storm_id') or [''])[0]
                if not storm_id:
                    self._send_error(400, 'Missing storm_id')
                    return
                with _rainfall_tif_lock:
                    tif_path = _rainfall_tif_by_storm.get(storm_id)
                if not tif_path or not os.path.exists(tif_path):
                    # Client probably hit the tile endpoint before /api/rainfall.
                    # Return a transparent 256x256 PNG so MapLibre doesn't
                    # flood the console with errors.
                    self._send_raw(200, _transparent_tile_png(), content_type='image/png',
                                   cache_control='no-cache')
                    return
                # Parse /api/rainfall_tile/{z}/{x}/{y}.png
                parts = path[len('/api/rainfall_tile/'):].split('/')
                if len(parts) != 3 or not parts[2].endswith('.png'):
                    self._send_error(400, 'Expected /api/rainfall_tile/{z}/{x}/{y}.png')
                    return
                z = int(parts[0]); x = int(parts[1]); y = int(parts[2][:-4])
                png_bytes = _render_rainfall_tile(tif_path, z, x, y)
                self._send_raw(200, png_bytes, content_type='image/png',
                               cache_control='public, max-age=86400')
            except Exception as e:
                self._send_error(500, f'tile error: {e}')
            return

        # ── GET /api/qpf_tile/{z}/{x}/{y}.png?storm_id=<id> ──
        # On-demand PNG tile server for the WPC QPF forecast raster.
        # Shares the NWS precipitation colormap with /api/rainfall_tile
        # (both surfaces are precip totals in mm, so the same ramp reads
        # correctly). Cached for 1 hour because QPF refreshes every 6 hours.
        if path.startswith('/api/qpf_tile/'):
            try:
                storm_id = (params.get('storm_id') or [''])[0]
                if not storm_id:
                    self._send_error(400, 'Missing storm_id')
                    return
                with _qpf_tif_lock:
                    tif_path = _qpf_tif_by_storm.get(storm_id)
                if not tif_path or not os.path.exists(tif_path):
                    self._send_raw(200, _transparent_tile_png(), content_type='image/png',
                                   cache_control='no-cache')
                    return
                parts = path[len('/api/qpf_tile/'):].split('/')
                if len(parts) != 3 or not parts[2].endswith('.png'):
                    self._send_error(400, 'Expected /api/qpf_tile/{z}/{x}/{y}.png')
                    return
                z = int(parts[0]); x = int(parts[1]); y = int(parts[2][:-4])
                png_bytes = _render_rainfall_tile(tif_path, z, x, y)
                self._send_raw(200, png_bytes, content_type='image/png',
                               cache_control='public, max-age=3600')
            except Exception as e:
                self._send_error(500, f'qpf tile error: {e}')
            return

        # ── GET /api/compound?storm_id=<id> ──
        # Returns storm-wide compound flood raster (surge + rainfall + fluvial)
        # stats and a XYZ tile URL template. The mosaic is built lazily from
        # the per-cell compound tifs process_cell writes; only cells the user
        # has already loaded contribute. Empty-storm case returns
        # available=false with a hint to load some cells first.
        # ── GET /api/shelters?radius_km=<n>&include_far=<0|1> ──
        # Returns shelter capacity/occupancy data for the active storm.
        # v1 source: an optional shelters.geojson dropped into
        # PERSISTENT_DIR/shelters/ by operators (exported from state EM
        # portals, Red Cross iAM, or FEMA NSS). If the file is absent
        # returns available=false with guidance. Properties expected on
        # each Feature: id, name, capacity, occupancy (nullable),
        # operator, accessible (bool), pet_friendly (bool), last_updated.
        if path == '/api/shelters':
            if _active_storm is None:
                self._send_error(400, 'No storm active'); return
            try:
                shelters_dir = os.path.join(PERSISTENT_DIR, 'shelters')
                sfile = os.path.join(shelters_dir, 'shelters.geojson')
                radius_km = float((params.get('radius_km') or ['200'])[0])
                if not os.path.exists(sfile):
                    self._send_json(200, {
                        'available': False,
                        'shelters': [],
                        'total_capacity': 0,
                        'total_occupancy': None,
                        'notes': (
                            'No shelter manifest found. Drop a shelters.geojson '
                            'at PERSISTENT_DIR/shelters/shelters.geojson (Red Cross iAM '
                            'export, state EM feed, or FEMA NSS) to enable this layer.'
                        ),
                    })
                    return
                with open(sfile) as _sf:
                    gj = json.load(_sf)
                feats = gj.get('features', []) if isinstance(gj, dict) else []
                clat = _active_storm.landfall_lat; clon = _active_storm.landfall_lon
                out = []; total_cap = 0; any_unknown = False; total_occ = 0
                # Rough km-per-degree at the landfall latitude.
                import math as _m
                km_per_deg_lat = 111.32
                km_per_deg_lon = 111.32 * max(_m.cos(_m.radians(clat)), 0.1)
                rad_deg_lat = radius_km / km_per_deg_lat
                rad_deg_lon = radius_km / km_per_deg_lon
                for f in feats:
                    g = f.get('geometry') or {}
                    if g.get('type') != 'Point': continue
                    lon, lat = g.get('coordinates', [None, None])[:2]
                    if lon is None or lat is None: continue
                    if abs(lat - clat) > rad_deg_lat or abs(lon - clon) > rad_deg_lon:
                        continue
                    p = f.get('properties') or {}
                    cap = int(p.get('capacity') or 0)
                    if cap <= 0: continue
                    occ_raw = p.get('occupancy')
                    occ = int(occ_raw) if (occ_raw is not None and str(occ_raw).strip() != '') else None
                    if occ is None: any_unknown = True
                    else: total_occ += occ
                    total_cap += cap
                    out.append({
                        'id': str(p.get('id') or f.get('id') or f'{lat:.4f},{lon:.4f}'),
                        'name': p.get('name') or 'Unnamed shelter',
                        'lat': lat, 'lon': lon,
                        'capacity': cap,
                        'occupancy': occ,
                        'operator': p.get('operator') or 'Unknown',
                        'is_accessible': bool(p.get('accessible') or p.get('is_accessible')),
                        'is_pet_friendly': bool(p.get('pet_friendly') or p.get('is_pet_friendly')),
                        'last_updated': p.get('last_updated'),
                        'notes': p.get('notes'),
                    })
                self._send_json(200, {
                    'available': True,
                    'shelters': out,
                    'total_capacity': total_cap,
                    'total_occupancy': None if any_unknown else total_occ,
                    'notes': f'{len(out)} shelter{"" if len(out)==1 else "s"} within {radius_km:.0f} km of landfall',
                })
            except Exception as e:
                self._send_error(500, f'shelters error: {e}')
            return

        # ── GET /api/vendor_coverage ──
        # Reads PERSISTENT_DIR/vendors/vendors.json (an array of
        # { vendor_id, vendor_name, specialties, contact_url, notes,
        #   service_area: <GeoJSON Polygon|MultiPolygon> }) and computes
        # each vendor's coverage % against the active storm footprint
        # (4° box around landfall — intentionally coarse for v1).
        # Future: compute against the union of hotspot polygons.
        if path == '/api/vendor_coverage':
            if _active_storm is None:
                self._send_error(400, 'No storm active'); return
            try:
                vdir = os.path.join(PERSISTENT_DIR, 'vendors')
                vfile = os.path.join(vdir, 'vendors.json')
                if not os.path.exists(vfile):
                    self._send_json(200, {
                        'available': False, 'vendors': [],
                        'notes': (
                            'No vendor manifest. Drop a vendors.json at '
                            'PERSISTENT_DIR/vendors/vendors.json — each entry needs '
                            'a GeoJSON service_area polygon. See PHASE5_DATA_CONTRACTS.md §3.'
                        ),
                    })
                    return
                from shapely.geometry import shape as _shape, box as _shbox
                with open(vfile) as _vf:
                    vendors_in = json.load(_vf)
                if not isinstance(vendors_in, list):
                    vendors_in = vendors_in.get('vendors', []) if isinstance(vendors_in, dict) else []
                clat = _active_storm.landfall_lat; clon = _active_storm.landfall_lon
                storm_footprint = _shbox(clon - 4, clat - 4, clon + 4, clat + 4)
                storm_area = storm_footprint.area or 1.0
                out = []
                for v in vendors_in:
                    sa = v.get('service_area')
                    if not sa:
                        coverage = 0.0
                    else:
                        try:
                            poly = _shape(sa)
                            inter = poly.intersection(storm_footprint)
                            coverage = float(inter.area / storm_area * 100.0)
                        except Exception:
                            coverage = 0.0
                    out.append({
                        'vendor_id': str(v.get('vendor_id') or v.get('id') or v.get('vendor_name', '')),
                        'vendor_name': v.get('vendor_name') or v.get('name') or 'Unknown vendor',
                        'specialties': v.get('specialties') or [],
                        'coverage_pct': round(max(0.0, min(100.0, coverage)), 1),
                        'contact_url': v.get('contact_url'),
                        'notes': v.get('notes'),
                    })
                out.sort(key=lambda r: r['coverage_pct'], reverse=True)
                self._send_json(200, {
                    'available': True, 'vendors': out,
                    'notes': f'Coverage computed against a 4° bbox around landfall ({len(out)} vendor(s) in manifest).',
                })
            except Exception as e:
                self._send_error(500, f'vendor_coverage error: {e}')
            return

        # ── GET /api/time_to_access?ranks=1,2,3 ──
        # Hotspot access-time heuristic. For v1 this is a deliberately
        # coarse estimate (confidence='low') based on the storm's
        # max_surge and each hotspot's rank: higher rank ≈ more damage
        # and/or more inland, so longer access window. The contract
        # documents a POST endpoint, but because the payload is tiny
        # (a handful of small integers) a GET with a comma-separated
        # list is simpler and avoids a POST handler. Shape is
        # identical to the documented TimeToAccessLayer.
        if path == '/api/time_to_access':
            if _active_storm is None:
                self._send_error(400, 'No storm active'); return
            try:
                raw = (params.get('ranks') or [''])[0]
                ranks = [int(r) for r in raw.split(',') if r.strip().isdigit()]
                if not ranks:
                    self._send_json(200, {
                        'available': False, 'estimates': [], 'generated_at': None,
                        'notes': 'No ranks supplied. Pass ?ranks=1,2,3 with the hotspot rank list.',
                    })
                    return
                # Simple model: base ETA scales with max_surge; each rank
                # adds ~2h because higher-rank hotspots are more damaged.
                try:
                    max_surge = float(getattr(_active_storm, 'max_surge_ft', 0) or 0)
                except Exception:
                    max_surge = 0.0
                base_hr = 6.0 + max_surge * 2.0   # ~6h for no surge, ~36h at 15ft
                import datetime as _dt
                estimates = []
                for r in ranks:
                    eta = base_hr + r * 2.0
                    # Above rank 10 or very high surge, label surge as limiting;
                    # otherwise debris/road closures dominate.
                    if max_surge >= 10:
                        limiting = 'surge'
                    elif r > 10:
                        limiting = 'debris'
                    else:
                        limiting = 'road_closure'
                    estimates.append({
                        'hotspot_rank': r,
                        'eta_hours': round(eta, 1),
                        'limiting_factor': limiting,
                        'confidence': 'low',
                        'notes': 'Heuristic — depth × road-network model pending (see PHASE5_DATA_CONTRACTS.md §4).',
                    })
                self._send_json(200, {
                    'available': True,
                    'estimates': estimates,
                    'generated_at': _dt.datetime.utcnow().isoformat() + 'Z',
                    'notes': f'Heuristic ETAs for {len(estimates)} hotspot(s) based on max_surge={max_surge:.1f} ft.',
                })
            except Exception as e:
                self._send_error(500, f'time_to_access error: {e}')
            return

        if path == '/api/compound':
            if _active_storm is None:
                self._send_error(400, 'No storm active')
                return
            try:
                with _compound_lock:
                    mosaic_path, stats = _build_storm_compound_mosaic(_active_storm.storm_id)
                    if mosaic_path:
                        _compound_mosaic_by_storm[_active_storm.storm_id] = mosaic_path
                if mosaic_path is None:
                    self._send_json(200, {
                        'available': False,
                        'storm_id': _active_storm.storm_id,
                        'cell_count': 0,
                        'notes': 'No cells loaded yet — load at least one cell to see compound flooding.',
                    })
                    return
                self._send_json(200, {
                    'available': True,
                    'storm_id': _active_storm.storm_id,
                    'cell_count': stats['cell_count'],
                    'max_depth_ft': stats['max_depth_ft'],
                    'avg_depth_ft': stats['avg_depth_ft'],
                    'tile_url_template': (
                        f'/api/compound_tile/{{z}}/{{x}}/{{y}}.png?storm_id={_active_storm.storm_id}'
                    ),
                    'notes': f"Compound mosaic of {stats['cell_count']} cell(s) — "
                             f"surge + rainfall + fluvial combined.",
                })
            except Exception as e:
                self._send_error(500, str(e))
            return

        # ── GET /api/compound_tile/{z}/{x}/{y}.png?storm_id=<id> ──
        # XYZ PNG tile server for the compound flood mosaic. Mirrors
        # /api/rainfall_tile but reads the storm_compound.tif mosaic and
        # uses the depth colormap instead of NWS precipitation.
        if path.startswith('/api/compound_tile/'):
            try:
                storm_id = (params.get('storm_id') or [''])[0]
                if not storm_id:
                    self._send_error(400, 'Missing storm_id')
                    return
                with _compound_lock:
                    mosaic_path = _compound_mosaic_by_storm.get(storm_id)
                    # Lazy first build if client hit the tile endpoint directly.
                    if mosaic_path is None or not os.path.exists(mosaic_path):
                        mosaic_path, _ = _build_storm_compound_mosaic(storm_id)
                        if mosaic_path:
                            _compound_mosaic_by_storm[storm_id] = mosaic_path
                if not mosaic_path or not os.path.exists(mosaic_path):
                    self._send_raw(200, _transparent_tile_png(), content_type='image/png',
                                   cache_control='no-cache')
                    return
                parts = path[len('/api/compound_tile/'):].split('/')
                if len(parts) != 3 or not parts[2].endswith('.png'):
                    self._send_error(400, 'Expected /api/compound_tile/{z}/{x}/{y}.png')
                    return
                z = int(parts[0]); x = int(parts[1]); y = int(parts[2][:-4])
                png_bytes = _render_compound_tile(mosaic_path, z, x, y)
                # Mosaic changes as cells load, so cache is shorter than rainfall.
                self._send_raw(200, png_bytes, content_type='image/png',
                               cache_control='public, max-age=300')
            except Exception as e:
                self._send_error(500, f'tile error: {e}')
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
                            # Re-register the cached tif with the tile server
                            # so /api/qpf_tile works after a restart.
                            _cached_tif = _cached.get('tif_path')
                            if _cached_tif and os.path.exists(_cached_tif):
                                with _qpf_tif_lock:
                                    _qpf_tif_by_storm[_active_storm.storm_id] = _cached_tif
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
                    _qpf_tif = getattr(qpf_result, 'path', None)
                    _qpf_tile_tmpl = None
                    if _qpf_tif and os.path.exists(_qpf_tif) and _active_storm is not None:
                        with _qpf_tif_lock:
                            _qpf_tif_by_storm[_active_storm.storm_id] = _qpf_tif
                        _qpf_tile_tmpl = (
                            f"/api/qpf_tile/{{z}}/{{x}}/{{y}}.png"
                            f"?storm_id={_active_storm.storm_id}"
                        )
                    _meta.update({
                        'duration_hr': 72,
                        'max_precip_mm': round(getattr(qpf_result, 'total_precip_mm', 0), 1),
                        'max_precip_in': round(getattr(qpf_result, 'total_precip_mm', 0) / 25.4, 2),
                        'tif_path': _qpf_tif,
                        'tile_url_template': _qpf_tile_tmpl,
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

    def _send_raw(self, code, body: bytes, content_type: str = 'application/json', cache_control: str | None = None):
        # Gzip compress large TEXT responses if client supports it. Skip
        # binary formats (PNG, etc.) — they're already compressed and
        # layering gzip on top just bloats the byte count.
        gzippable = content_type.startswith(('application/json', 'text/', 'application/xml'))
        try:
            accept_enc = self.headers.get('Accept-Encoding', '')
            if gzippable and len(body) > 1024 and 'gzip' in accept_enc:
                import gzip as _gzip
                body = _gzip.compress(body, compresslevel=6)
                self.send_response(code)
                self.send_header('Content-Encoding', 'gzip')
            else:
                self.send_response(code)
            self.send_header('Content-Type', content_type)
            self.send_header('Access-Control-Allow-Origin', '*')
            if cache_control:
                self.send_header('Cache-Control', cache_control)
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
