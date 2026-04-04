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
from urllib.parse import urlparse, parse_qs

# Built React frontend lives at <repo_root>/ui/dist/
_STATIC_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..', 'ui', 'dist')
)

import numpy as np

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))
from damage_model.depth_damage import estimate_damage_from_raster
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

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
CACHE_DIR = os.path.join(BASE_DIR, 'tmp_integration', 'cells')
os.makedirs(CACHE_DIR, exist_ok=True)

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
    """
    sdir = os.path.join(CACHE_DIR, storm_id)
    if not os.path.isdir(sdir):
        return {'confidence': 'unvalidated', 'building_count': 0}
    total = 0
    for fname in os.listdir(sdir):
        if fname.endswith('_buildings.json'):
            try:
                with open(os.path.join(sdir, fname)) as f:
                    data = json.load(f)
                total += len(data.get('features', []))
            except Exception:
                pass
    if total > 500:
        level = 'high'
    elif total >= 50:
        level = 'medium'
    else:
        level = 'low'
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

_active_storm: StormEntry | None = None
_active_exposure_region: str = ''  # R11: cached for cell-load lookups


def _storm_cache_dir(storm: StormEntry) -> str:
    d = os.path.join(CACHE_DIR, storm.storm_id)
    os.makedirs(d, exist_ok=True)
    return d


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

    # Check cache
    if os.path.exists(damage_path) and os.path.exists(flood_path):
        with open(damage_path) as f:
            damage_data = json.load(f)
        with open(flood_path) as f:
            flood_data = json.load(f)
        print(f"  [cache hit] cell ({col},{row}) for {storm.storm_id}")
        return {"buildings": damage_data, "flood": flood_data}

    lon_min, lat_min, lon_max, lat_max = cell_bbox(col, row)
    print(f"[{storm.storm_id} cell {col},{row}] "
          f"bbox=({lon_min:.2f},{lat_min:.2f})->({lon_max:.2f},{lat_max:.2f})")

    # 1. Parametric surge raster using real storm parameters
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
    if not os.path.exists(flood_path):
        raster_to_geojson(raster_path, flood_path)
    with open(flood_path) as f:
        flood_data = json.load(f)

    # 3. Fetch real OSM buildings
    buildings_path = os.path.join(sdir, f'cell_{col}_{row}_buildings.json')
    fetch_buildings(lon_min, lat_min, lon_max, lat_max, buildings_path, cache=True)

    with open(buildings_path) as f:
        buildings_data = json.load(f)
    if not buildings_data.get("features"):
        empty = _empty_fc()
        with open(damage_path, 'w') as f:
            json.dump(empty, f)
        return {"buildings": empty, "flood": flood_data}

    # 4. Run HAZUS damage model
    estimate_damage_from_raster(raster_path, buildings_path, damage_path)

    with open(damage_path) as f:
        damage_data = json.load(f)

    return {"buildings": damage_data, "flood": flood_data}


def _empty_fc():
    return {"type": "FeatureCollection", "features": []}


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
                results = search_storms(q)
                self._send_raw(200, json.dumps([_inject_dps(s.to_dict()) for s in results]).encode())
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
            _active_storm = storm
            print(f"\n{'='*60}")
            print(f"ACTIVATED: {storm.name} ({storm.year}) — Cat {storm.category}")
            print(f"  Landfall: ({storm.landfall_lon}, {storm.landfall_lat})")
            print(f"  Wind: {storm.max_wind_kt} kt  Pressure: {storm.min_pressure_mb} mb")
            print(f"  Grid origin: ({storm.grid_origin_lon}, {storm.grid_origin_lat})")
            print(f"{'='*60}\n")

            # R6: Auto-load 3x3 grid for Cat 3+ storms to capture full damage footprint
            # For Cat 1-2, just load the eye cell (0,0)
            if storm.category >= 3:
                print(f"Pre-loading 3x3 grid for Cat {storm.category} storm...")
                grid_cells = {}
                for dc in [-1, 0, 1]:
                    for dr in [-1, 0, 1]:
                        k = f"{dc},{dr}"
                        print(f"  Loading cell ({dc},{dr})...", end=" ", flush=True)
                        grid_cells[k] = load_cell(dc, dr)
                        n = len(grid_cells[k].get('buildings', {}).get('features', []))
                        print(f"{n} buildings")
                center_data = grid_cells.get("0,0", load_cell(0, 0))
            else:
                print("Pre-loading eye cell (0,0)...")
                center_data = load_cell(0, 0)
                grid_cells = None

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

        # ── GET /api/health ──
        if path == '/api/health':
            self._send_json(200, {'status': 'ok', 'active_storm': _active_storm.storm_id if _active_storm else None})
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
            self.send_response(200)
            self.send_header('Content-Type', mime)
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            self._send_error(404, 'Not found')

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()

    def _send_raw(self, code, body: bytes):
        self.send_response(code)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_json(self, code, data):
        self._send_raw(code, json.dumps(data).encode())

    def _send_error(self, code, message):
        self._send_json(code, {'error': message})

    def log_message(self, format, *args):
        pass


def main():
    port = int(os.environ.get('SURGE_API_PORT', 8000))
    server = HTTPServer(('0.0.0.0', port), CellHandler)
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
