"""
SurgeDPS FastAPI server — drop-in replacement for api_server.py.

Wraps the exact same business logic (all imports and shared state live in
api_server.py) but uses FastAPI + Uvicorn instead of BaseHTTPRequestHandler.
This gives us:
  • Typed route handlers with automatic validation
  • Automatic OpenAPI docs at /docs
  • Async endpoints so long cell loads don't block health checks
  • Proper HTTP 422 on bad params instead of custom _send_error
  • Middleware (CORS, GZip, request timing)

Run with:
  uvicorn api_server_fastapi:app --host 0.0.0.0 --port 8000 --workers 4

Or for development (auto-reload):
  uvicorn api_server_fastapi:app --reload --port 8000
"""

from __future__ import annotations

import json
import os
import sys
import time
from contextlib import asynccontextmanager
from typing import Optional

# ── Add src/ to Python path (mirrors api_server.py's sys.path.insert) ─────────
_SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))
_BASE_DIR     = os.path.dirname(_SCRIPTS_DIR)
sys.path.insert(0, os.path.join(_BASE_DIR, 'src'))
sys.path.insert(0, _SCRIPTS_DIR)

from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse, StreamingResponse

# ── Import shared state and all business logic from the original module ────────
# We import api_server as a module (NOT run via __name__=='__main__') so we
# get all the module-level globals and functions without starting the old HTTP
# server. Any mutation to api_server._active_storm etc. is visible here.
import api_server as _s  # _s.<name> = api_server.<name>

# ── Lifespan: warm up HURDAT2 catalog on startup ──────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Pre-load HURDAT2 so the first /api/seasons request is instant
    _s.get_seasons()
    print(f"SurgeDPS FastAPI ready — cache: {_s.CACHE_DIR}")
    yield
    print("SurgeDPS FastAPI shutting down.")


# ── App ───────────────────────────────────────────────────────────────────────

app = FastAPI(
    title="SurgeDPS API",
    description="Storm surge damage assessment — FastAPI backend",
    version="2.0.0",
    lifespan=lifespan,
    # Keep the /surgedps prefix consistent with the Vite proxy config
    root_path="/surgedps",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "OPTIONS"],
    allow_headers=["*"],
)
app.add_middleware(GZipMiddleware, minimum_size=1024)


# ── Helper: pull active storm or raise ────────────────────────────────────────

def _require_active_storm():
    with _s._active_storm_lock:
        storm = _s._active_storm
    if storm is None:
        raise HTTPException(status_code=400, detail="No storm active — activate a storm first")
    return storm


# ─────────────────────────────────────────────────────────────────────────────
# Storm browser endpoints
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/api/seasons")
def get_seasons():
    """List of {year, count} for the season accordion (2015+)."""
    data = [s for s in _s.get_seasons() if s["year"] >= _s.SEASON_MIN_YEAR]
    return data


@app.get("/api/storms/historic")
def get_historic_storms():
    """Curated notable historic storms (pre-2015 included)."""
    return [_s._inject_dps(s.to_dict()) for s in _s.HISTORICAL_STORMS]


@app.get("/api/season/{year}")
def get_season_storms(year: int):
    """All storms for a given season year."""
    storms = _s.get_storms_for_year(year)
    return [_s._inject_dps(s.to_dict()) for s in storms]


@app.get("/api/storms/search")
def search_storms(q: str = Query(..., description="Storm name or ID")):
    """Search HURDAT2 + curated catalog by name or ID (up to 20 results)."""
    ql = q.lower().strip()
    seen: set[str] = set()
    results = []
    for s in _s.HISTORICAL_STORMS:
        if ql in s.name.lower() or ql in s.storm_id.lower():
            results.append(_s._inject_dps(s.to_dict()))
            seen.add(s.storm_id)
    for s in _s.search_storms(q):
        if s.storm_id not in seen:
            results.append(_s._inject_dps(s.to_dict()))
            seen.add(s.storm_id)
        if len(results) >= 20:
            break
    return results


@app.get("/api/storms/active")
def get_active_storms():
    """Currently active NHC tropical cyclones."""
    return [_s._inject_dps(s.to_dict()) for s in _s.fetch_active_storms()]


# ─────────────────────────────────────────────────────────────────────────────
# Storm activation
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/api/storm/{storm_id}/activate")
def activate_storm(storm_id: str):
    """
    Select a storm for analysis. Loads a 3×3 grid of cells around landfall,
    computes confidence/ELI/validated-DPS, fetches Census population context,
    and records the model run in the validation ledger.
    """
    storm = _s.get_storm_by_id(storm_id)
    if storm is None:
        for hs in _s.HISTORICAL_STORMS:
            if hs.storm_id == storm_id:
                storm = hs
                break
    if storm is None:
        raise HTTPException(status_code=404, detail=f"Storm '{storm_id}' not found")

    with _s._active_storm_lock:
        _s._active_storm = storm
    _s._active_exposure_region = ''

    print(f"\n{'='*60}")
    print(f"ACTIVATED: {storm.name} ({storm.year}) — Cat {storm.category}")
    print(f"  Landfall: ({storm.landfall_lon}, {storm.landfall_lat})")
    print(f"{'='*60}\n")

    _ACTIVATE_CELLS = [(c, r) for r in range(-1, 2) for c in range(-1, 2)]
    total_act = len(_ACTIVATE_CELLS) * 4
    _s._progress.update(
        step="Initializing", step_num=0, total_steps=total_act,
        started_at=time.time(), storm_id=storm.storm_id,
    )

    grid_cells: dict = {}
    for idx, (c, r) in enumerate(_ACTIVATE_CELLS):
        _s._progress.update(step=f"Loading cell ({c},{r})", step_num=idx * 4)
        grid_cells[f"{c},{r}"] = _s.load_cell(c, r)

    _s._progress.update(step="Complete", step_num=total_act)

    conf = _s._compute_confidence(storm.storm_id)
    storm_data = _s._inject_dps(storm.to_dict())
    storm_data["confidence"] = conf["confidence"]
    storm_data["building_count"] = conf["building_count"]

    eli = _s._compute_eli(storm_data.get("dps_score", 0), conf["building_count"])
    storm_data["eli"] = eli["eli"]
    storm_data["eli_tier"] = eli["eli_tier"]

    _s._active_exposure_region = storm_data.get("exposure_region", "")
    vdps = _s._compute_validated_dps(
        storm_data.get("dps_score", 0), conf["building_count"], _s._active_exposure_region
    )
    storm_data["validated_dps"] = vdps["validated_dps"]
    storm_data["dps_adjustment"] = vdps["dps_adjustment"]
    storm_data["dps_adj_reason"] = vdps["dps_adj_reason"]

    try:
        pop_ctx = _s.get_population_context(storm.landfall_lat, storm.landfall_lon)
        if pop_ctx:
            storm_data["population"] = pop_ctx
    except Exception as e:
        print(f"  [warn] Census population lookup failed: {e}")

    try:
        model_run = _s.record_from_activation(storm.storm_id, grid_cells, storm_data)
        gt = _s.get_ground_truth(storm.storm_id)
        if gt:
            storm_data["ground_truth"] = {
                "actual_total_B": gt.actual_damage_B,
                "surge_fraction": gt.surge_fraction,
                "surge_damage_B": gt.surge_damage_B,
                "source": gt.source,
            }
    except Exception as e:
        print(f"  [warn] Validation ledger failed: {e}")

    return {"storm": storm_data, "center_cell": grid_cells.get("0,0"), "grid_cells": grid_cells}


# ─────────────────────────────────────────────────────────────────────────────
# Grid cell loading
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/api/cell")
def get_cell(col: int = Query(...), row: int = Query(...)):
    """Load (or cache-hit) a single grid cell for the active storm."""
    storm = _require_active_storm()
    data = _s.load_cell(col, row)

    conf = _s._compute_confidence(storm.storm_id)
    data["confidence"] = conf["confidence"]
    data["building_count"] = conf["building_count"]

    dps_val = _s._DPS_SCORES.get(storm.storm_id, 0) or _s._DPS_SCORES.get(storm.storm_id.lower(), 0)
    eli = _s._compute_eli(dps_val, conf["building_count"])
    data["eli"] = eli["eli"]
    data["eli_tier"] = eli["eli_tier"]

    vdps = _s._compute_validated_dps(dps_val, conf["building_count"], _s._active_exposure_region)
    data["validated_dps"] = vdps["validated_dps"]
    data["dps_adjustment"] = vdps["dps_adjustment"]
    data["dps_adj_reason"] = vdps["dps_adj_reason"]
    return data


@app.get("/api/cell_ticks")
def get_cell_ticks(
    col: int = Query(...),
    row: int = Query(...),
    storm_id: Optional[str] = Query(None),
):
    """
    Per-building HAZUS damage timeseries for a cell.
    Generated lazily on first request; cached to disk thereafter.
    Returns the raw JSON bundle (TicksBundle schema).
    """
    storm = _require_active_storm()
    if storm_id and storm_id != storm.storm_id:
        raise HTTPException(status_code=404, detail=f"Storm '{storm_id}' not active")
    if not (-500 < col < 500 and -500 < row < 500):
        raise HTTPException(status_code=400, detail="col/row out of range")

    sdir = _s._storm_cache_dir(storm)
    ticks_path = os.path.join(sdir, f"cell_{col}_{row}_ticks.json")
    depth_path = os.path.join(sdir, f"cell_{col}_{row}_depth.tif")
    bldgs_path = os.path.join(sdir, f"cell_{col}_{row}_buildings.json")
    damage_path = os.path.join(sdir, f"cell_{col}_{row}_damage.geojson")

    def _is_ticks_fresh() -> bool:
        if not os.path.exists(ticks_path):
            return False
        if os.path.exists(bldgs_path):
            try:
                return os.path.getmtime(bldgs_path) <= os.path.getmtime(ticks_path)
            except OSError:
                pass
        return True

    def _read_ticks_bytes() -> Optional[bytes]:
        try:
            with open(ticks_path, "rb") as f:
                return f.read()
        except OSError:
            return None

    if _is_ticks_fresh():
        raw = _read_ticks_bytes()
        if raw:
            return Response(content=raw, media_type="application/json",
                            headers={"Cache-Control": "public, max-age=3600"})

    if not os.path.exists(depth_path) or not os.path.exists(bldgs_path):
        raise HTTPException(
            status_code=404,
            detail="No ticks data for this cell. Re-load the cell to regenerate.",
        )

    lock = _s._get_cell_ticks_lock(storm.storm_id, col, row)
    with lock:
        if _is_ticks_fresh():
            raw = _read_ticks_bytes()
            if raw:
                return Response(content=raw, media_type="application/json",
                                headers={"Cache-Control": "public, max-age=3600"})

        from damage_model.peril_timeseries import (
            estimate_damage_timeseries_from_raster as _run_ts,
        )
        import threading as _th_tk
        ticks_tmp = f"{ticks_path}.tmp.{os.getpid()}.{_th_tk.get_ident()}"
        _run_ts(
            depth_raster_path=depth_path,
            buildings_geojson_path=bldgs_path,
            ticks_output_path=ticks_tmp,
            final_geojson_path=damage_path,
            storm_id=storm.storm_id,
            landfall_lat=storm.landfall_lat,
            landfall_lon=storm.landfall_lon,
            max_wind_kt=storm.max_wind_kt,
            storm_speed_kt=storm.speed_kt,
            storm_heading_deg=storm.heading_deg,
        )
        if not os.path.exists(ticks_tmp):
            raise HTTPException(status_code=500, detail="Ticks generation produced no output")
        os.replace(ticks_tmp, ticks_path)

    raw = _read_ticks_bytes()
    if not raw:
        raise HTTPException(status_code=500, detail="Ticks file unreadable after generation")
    return Response(content=raw, media_type="application/json",
                    headers={"Cache-Control": "public, max-age=3600"})


# ─────────────────────────────────────────────────────────────────────────────
# Progress / polling
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/api/progress")
def get_progress():
    elapsed = round(time.time() - _s._progress["started_at"], 1) if _s._progress["started_at"] else 0
    return {
        "step": _s._progress["step"],
        "step_num": _s._progress["step_num"],
        "total_steps": _s._progress["total_steps"],
        "elapsed": elapsed,
        "storm_id": _s._progress["storm_id"],
    }


# ─────────────────────────────────────────────────────────────────────────────
# Geocoding
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/api/geocode/reverse")
def geocode_reverse(lat: float = Query(...), lon: float = Query(...)):
    return _s._geocode_reverse(lat, lon)


@app.get("/api/geocode/search")
def geocode_search(q: str = Query(..., description="Address or place name")):
    return _s._geocode_forward(q.strip())


# ─────────────────────────────────────────────────────────────────────────────
# Forecast track
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/api/forecast/track")
def get_forecast_track():
    tracks = _s.fetch_forecast_track()
    cones = _s.fetch_forecast_cone()
    result = []
    for t in tracks:
        td = t.to_dict()
        td["cone"] = cones.get(t.storm_name.upper())
        result.append(td)
    return result


# ─────────────────────────────────────────────────────────────────────────────
# Simulation (what-if)
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/api/simulate")
def simulate(
    lat: Optional[float] = Query(None),
    lon: Optional[float] = Query(None),
    wind: Optional[int] = Query(None),
    pressure: Optional[int] = Query(None),
    heading: Optional[float] = Query(None),
    speed: Optional[float] = Query(None),
):
    """Run a what-if simulation for the active storm at the given landfall point."""
    storm = _require_active_storm()

    sim_lat = lat if lat is not None else storm.landfall_lat
    sim_lon = lon if lon is not None else storm.landfall_lon
    sim_wind = wind if wind is not None else storm.max_wind_kt
    sim_pressure = pressure if pressure is not None else storm.min_pressure_mb
    sim_heading = heading if heading is not None else storm.heading_deg
    sim_speed = speed if speed is not None else storm.speed_kt

    from storm_catalog.catalog import StormEntry
    from storm_catalog.surge_model import generate_surge_raster
    from tile_gen.pmtiles_builder import raster_to_geojson
    from data_ingest.building_fetcher import fetch_buildings
    from damage_model.depth_damage import estimate_damage_from_raster
    from validation.backtester import predict_loss_range

    sim_storm = StormEntry(
        storm_id=f"{storm.storm_id}_sim",
        name=storm.name,
        year=storm.year,
        category=storm.category,
        status="simulation",
        landfall_lon=sim_lon,
        landfall_lat=sim_lat,
        max_wind_kt=sim_wind,
        min_pressure_mb=sim_pressure,
        heading_deg=sim_heading,
        speed_kt=sim_speed,
        basin=storm.basin,
        advisory="simulation",
    )

    CACHE_DIR = _s.CACHE_DIR
    CELL_WIDTH = _s.CELL_WIDTH
    CELL_HEIGHT = _s.CELL_HEIGHT
    sim_cache = os.path.join(CACHE_DIR, sim_storm.storm_id)
    os.makedirs(sim_cache, exist_ok=True)

    origin_lon = sim_storm.grid_origin_lon
    origin_lat = sim_storm.grid_origin_lat
    lon_min = origin_lon
    lat_min = origin_lat
    lon_max = lon_min + CELL_WIDTH
    lat_max = lat_min + CELL_HEIGHT

    _s._progress.update(step="Running simulation", step_num=0, total_steps=4,
                        started_at=time.time(), storm_id=sim_storm.storm_id)

    raster_path = os.path.join(sim_cache, "sim_depth.tif")
    _s._progress.update(step="Generating surge model", step_num=1)
    generate_surge_raster(
        lon_min=lon_min, lat_min=lat_min, lon_max=lon_max, lat_max=lat_max,
        output_path=raster_path,
        landfall_lon=sim_storm.landfall_lon, landfall_lat=sim_storm.landfall_lat,
        max_wind_kt=sim_wind, min_pressure_mb=sim_pressure,
        heading_deg=sim_heading, speed_kt=sim_speed,
    )

    flood_path = os.path.join(sim_cache, "sim_flood.geojson")
    _s._progress.update(step="Building flood map", step_num=2)
    raster_to_geojson(raster_path, flood_path)
    with open(flood_path) as f:
        flood_data = json.load(f)

    buildings_path = os.path.join(sim_cache, "sim_buildings.json")
    _s._progress.update(step="Fetching building footprints", step_num=3)
    fetch_buildings(lon_min, lat_min, lon_max, lat_max, buildings_path, cache=True)

    damage_path = os.path.join(sim_cache, "sim_damage.geojson")
    _s._progress.update(step="Running damage model", step_num=4)
    with open(buildings_path) as f:
        buildings_data = json.load(f)
    if buildings_data.get("features"):
        estimate_damage_from_raster(
            raster_path, buildings_path, damage_path,
            storm_id=sim_storm.storm_id,
            landfall_lat=sim_lat, landfall_lon=sim_lon,
            max_wind_kt=sim_wind, storm_speed_kt=sim_speed,
            storm_heading_deg=sim_heading,
        )
    else:
        with open(damage_path, "w") as f:
            json.dump({"type": "FeatureCollection", "features": []}, f)

    with open(damage_path) as f:
        damage_data = json.load(f)

    total_loss = sum(
        feat["properties"].get("estimated_loss_usd", 0) or 0
        for feat in damage_data.get("features", [])
    )
    n_buildings = len(damage_data.get("features", []))
    n_damaged = sum(
        1 for feat in damage_data.get("features", [])
        if (feat["properties"].get("total_damage_pct", 0) or 0) > 0
    )
    _s._progress.update(step="Complete", step_num=4)

    pop_ctx = None
    try:
        pop_ctx = _s.get_population_context(sim_lat, sim_lon)
    except Exception:
        pass

    result = {
        "simulation": True,
        "parameters": {
            "lat": sim_lat, "lon": sim_lon, "wind_kt": sim_wind,
            "pressure_mb": sim_pressure, "heading_deg": sim_heading, "speed_kt": sim_speed,
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
    try:
        result["prediction"] = predict_loss_range(total_loss)
    except Exception:
        pass

    return result


# ─────────────────────────────────────────────────────────────────────────────
# Validation
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/api/validation/backtest")
def get_backtest():
    report = _s.run_backtest()
    return report.to_dict()


@app.get("/api/validation/storm/{storm_id}")
def get_validation_storm(storm_id: str):
    score = _s.score_storm(storm_id)
    if score:
        return score.to_dict()
    return {
        "error": "No ground truth or model run for this storm",
        "storm_id": storm_id,
        "has_ground_truth": _s.get_ground_truth(storm_id) is not None,
    }


@app.get("/api/validation/predict")
def get_loss_prediction(loss: float = Query(0.0)):
    return _s.predict_loss_range(loss)


# ─────────────────────────────────────────────────────────────────────────────
# Hazard overlays (rainfall, QPF, compound, gauges, shelters, vendor)
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/api/rainfall")
def get_rainfall(
    duration: int = Query(72),
    passes: int = Query(2, alias="pass"),
    realtime: int = Query(0),
):
    storm = _require_active_storm()
    # Delegate to the same inline logic as api_server.py's /api/rainfall handler
    try:
        from rainfall.mrms_fetcher import MRMSFetcher, storm_bbox_from_catalog_entry
        bbox = storm_bbox_from_catalog_entry(storm)
        fetcher = MRMSFetcher(bbox=bbox)
        result = fetcher.fetch(duration_h=duration, n_passes=passes, realtime=bool(realtime))
        with _s._rainfall_tif_lock:
            if result.get("tif_path"):
                _s._lru_set(_s._rainfall_tif_by_storm, storm.storm_id, result["tif_path"])
        tile_url = f"/api/rainfall_tile/{{z}}/{{x}}/{{y}}.png?storm_id={storm.storm_id}" \
                   if result.get("tif_path") else None
        return {
            "available": bool(result.get("tif_path")),
            "bbox": result.get("bbox"),
            "max_precip_mm": result.get("max_precip_mm"),
            "tile_url_template": tile_url,
            "source": result.get("source"),
            "notes": result.get("notes", ""),
        }
    except Exception as e:
        return {"available": False, "notes": str(e)}


@app.get("/api/compound")
def get_compound(storm_id: Optional[str] = Query(None)):
    storm = _require_active_storm()
    if storm_id and storm_id != storm.storm_id:
        raise HTTPException(status_code=404, detail="Storm not active")
    try:
        from rainfall.compound_mosaic import build_compound_mosaic
        sdir = _s._storm_cache_dir(storm)
        mosaic_path = build_compound_mosaic(sdir, storm.storm_id)
        with _s._compound_lock:
            _s._lru_set(_s._compound_mosaic_by_storm, storm.storm_id, mosaic_path)
        tile_url = f"/api/compound_tile/{{z}}/{{x}}/{{y}}.png?storm_id={storm.storm_id}"
        return {"available": True, "tile_url_template": tile_url, "mosaic_path": mosaic_path}
    except Exception as e:
        return {"available": False, "notes": str(e)}


@app.get("/api/gauges")
def get_gauges(
    radius: float = Query(4.0, description="Search radius in degrees"),
    category: int = Query(0, description="Minimum flood category (0=all)"),
):
    _require_active_storm()
    try:
        from betaLayers import _fetch_gauges_impl
        return _fetch_gauges_impl(radius, category)
    except Exception as e:
        return {"available": False, "gauges": [], "notes": str(e)}


@app.get("/api/shelters")
def get_shelters(
    radius_km: float = Query(200.0),
    include_far: int = Query(0),
):
    _require_active_storm()
    try:
        from betaLayers import _fetch_shelters_impl
        return _fetch_shelters_impl(radius_km, bool(include_far))
    except Exception as e:
        return {"available": False, "shelters": [], "notes": str(e)}


@app.get("/api/vendor_coverage")
def get_vendor_coverage():
    _require_active_storm()
    try:
        from betaLayers import _fetch_vendor_coverage_impl
        return _fetch_vendor_coverage_impl()
    except Exception as e:
        return {"available": False, "vendors": [], "notes": str(e)}


@app.get("/api/time_to_access")
def get_time_to_access(
    ranks: str = Query("", description="Comma-separated hotspot ranks"),
    coords: str = Query("", description="Semicolon-separated lon,lat pairs"),
):
    _require_active_storm()
    try:
        from betaLayers import _fetch_time_to_access_impl
        return _fetch_time_to_access_impl(ranks, coords)
    except Exception as e:
        return {"available": False, "notes": str(e)}


# ─────────────────────────────────────────────────────────────────────────────
# Raster tile endpoints (PNG)
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/api/rainfall_tile/{z}/{x}/{y}.png")
def rainfall_tile(z: int, x: int, y: int, storm_id: str = Query(...)):
    with _s._rainfall_tif_lock:
        tif_path = _s._rainfall_tif_by_storm.get(storm_id)
    if not tif_path or not os.path.exists(tif_path):
        raise HTTPException(status_code=404, detail="No rainfall raster for this storm")
    try:
        from tile_gen.raster_tiles import render_xyz_tile
        png_bytes = render_xyz_tile(tif_path, z, x, y, colormap="nws_precip")
        return Response(content=png_bytes, media_type="image/png",
                        headers={"Cache-Control": "public, max-age=86400"})
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/qpf_tile/{z}/{x}/{y}.png")
def qpf_tile(z: int, x: int, y: int, storm_id: str = Query(...)):
    with _s._qpf_tif_lock:
        tif_path = _s._qpf_tif_by_storm.get(storm_id)
    if not tif_path or not os.path.exists(tif_path):
        raise HTTPException(status_code=404, detail="No QPF raster for this storm")
    try:
        from tile_gen.raster_tiles import render_xyz_tile
        png_bytes = render_xyz_tile(tif_path, z, x, y, colormap="nws_precip")
        return Response(content=png_bytes, media_type="image/png",
                        headers={"Cache-Control": "public, max-age=3600"})
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/compound_tile/{z}/{x}/{y}.png")
def compound_tile(z: int, x: int, y: int, storm_id: str = Query(...)):
    with _s._compound_lock:
        tif_path = _s._compound_mosaic_by_storm.get(storm_id)
    if not tif_path or not os.path.exists(tif_path):
        raise HTTPException(status_code=404, detail="No compound mosaic for this storm")
    try:
        from tile_gen.raster_tiles import render_xyz_tile
        png_bytes = render_xyz_tile(tif_path, z, x, y, colormap="depth_ft")
        return Response(content=png_bytes, media_type="image/png",
                        headers={"Cache-Control": "public, max-age=86400"})
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─────────────────────────────────────────────────────────────────────────────
# Health checks
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/api/health")
def health():
    return {"status": "ok", "server": "fastapi"}


@app.get("/api/health/storage")
def health_storage():
    try:
        import shutil
        total, used, free = shutil.disk_usage(_s.CACHE_DIR)
        return {
            "cache_dir": _s.CACHE_DIR,
            "total_gb": round(total / 1e9, 2),
            "used_gb": round(used / 1e9, 2),
            "free_gb": round(free / 1e9, 2),
        }
    except Exception as e:
        return {"error": str(e)}


# ─────────────────────────────────────────────────────────────────────────────
# Private validation namespace (token-gated)
# ─────────────────────────────────────────────────────────────────────────────

@app.api_route("/__val/{path:path}", methods=["GET"])
@app.api_route("/__val", methods=["GET"])
async def validation_namespace(request: Request, path: str = ""):
    """
    Delegates to the existing private_routes handler. Returns 404 on bad token
    (same behaviour as the BaseHTTPRequestHandler version — obscurity over 401).
    """
    from validation.private_routes import _token_ok, _validation_dashboard, _validation_metrics
    token = request.headers.get("X-Validation-Token") or request.query_params.get("token", "")
    if not _token_ok(token):
        raise HTTPException(status_code=404, detail="Not found")
    full_path = f"/__val/{path}".rstrip("/")
    if full_path == "/__val" or full_path == "/__val/":
        html = _validation_dashboard()
        return Response(content=html, media_type="text/html")
    if full_path == "/__val/metrics":
        return _validation_metrics()
    raise HTTPException(status_code=404, detail="Not found")


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("api_server_fastapi:app", host="0.0.0.0", port=port, workers=4, log_level="info")
