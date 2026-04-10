"""
Active Storm Monitor

Runs as a persistent background process alongside the API server.
Polls NHC every 30 minutes for active tropical cyclones, fetches
their forecast tracks, and runs the full SurgeDPS pipeline at the
predicted landfall point — not just the current position.

Pipeline per storm per advisory:
  1. Fetch active storms from NHC RSS
  2. Fetch forecast track from NOAA ArcGIS (predicted positions/winds)
  3. Identify predicted landfall point from the forecast track
  4. Run surge model at the predicted landfall location
  5. Fetch NSI buildings + run HAZUS damage for 3×3 grid
  6. Record model run in the validation ledger (tagged with advisory #)
  7. Compute confidence-interval prediction from backtesting data

Each advisory creates a new ledger entry, building an advisory-by-
advisory timeline that shows how the prediction evolves as the storm
approaches. Post-event, this timeline can be compared against actual
outcomes to measure forecast-dependent accuracy.

Results are cached to the Railway persistent volume, so when a user
opens the website during an active storm, everything is pre-computed.

Usage:
    python scripts/storm_monitor.py          # runs forever, polling
    python scripts/storm_monitor.py --once   # single poll, then exit
"""

import json
import os
import sys
import time
import traceback

# ── Path setup ──
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(os.path.join(BASE_DIR, 'src'))

from storm_catalog.catalog import (
    StormEntry, CELL_WIDTH, CELL_HEIGHT,
    fetch_active_storms, HISTORICAL_STORMS,
)
from storm_catalog.surge_model import generate_surge_raster
from tile_gen.pmtiles_builder import raster_to_geojson
from data_ingest.building_fetcher import fetch_buildings
from damage_model.depth_damage import estimate_damage_from_raster
from validation.run_ledger import record_from_activation, ModelRun
from validation.backtester import predict_loss_range
from data_ingest.census_fetcher import get_population_context
from storm_catalog.forecast_track import fetch_forecast_track, ForecastTrack

PERSISTENT_DIR = os.environ.get('PERSISTENT_DATA_DIR', os.path.join(BASE_DIR, 'tmp_integration'))
CACHE_DIR = os.path.join(PERSISTENT_DIR, 'cells')
MONITOR_STATE_PATH = os.path.join(PERSISTENT_DIR, 'monitor_state.json')
os.makedirs(CACHE_DIR, exist_ok=True)

# ── Configuration ──
POLL_INTERVAL = 1800          # 30 minutes between NHC checks
MIN_WIND_KT = 34              # Tropical storm threshold
LAT_RANGE = (15.0, 45.0)      # US coastal threat zone
LON_RANGE = (-100.0, -60.0)
GRID_CELLS_3x3 = [(c, r) for r in range(-1, 2) for c in range(-1, 2)]

# Historical storm IDs — never re-process these
_HISTORIC_IDS = {s.storm_id for s in HISTORICAL_STORMS}


def _storm_cache_dir(storm: StormEntry) -> str:
    d = os.path.join(CACHE_DIR, storm.storm_id)
    os.makedirs(d, exist_ok=True)
    return d


def _load_state() -> dict:
    """Load monitor state (last poll time, processed advisories)."""
    if os.path.exists(MONITOR_STATE_PATH):
        try:
            with open(MONITOR_STATE_PATH) as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            pass
    return {"last_poll": 0, "processed": {}}


def _save_state(state: dict):
    """Persist monitor state to disk."""
    with open(MONITOR_STATE_PATH, 'w') as f:
        json.dump(state, f, indent=2)


def is_actionable(storm: StormEntry) -> bool:
    """Check if a storm warrants automatic pipeline execution."""
    if storm.storm_id in _HISTORIC_IDS:
        return False
    if storm.max_wind_kt < MIN_WIND_KT:
        return False
    if not (LAT_RANGE[0] <= storm.landfall_lat <= LAT_RANGE[1]):
        return False
    if not (LON_RANGE[0] <= storm.landfall_lon <= LON_RANGE[1]):
        return False
    return True


def run_pipeline(storm: StormEntry, forecast: ForecastTrack = None) -> dict:
    """
    Run the full SurgeDPS pipeline for an active storm.

    If a forecast track is provided, the pipeline runs at the
    predicted landfall point with forecast wind/pressure rather
    than the storm's current position. This produces meaningful
    damage estimates days before the storm arrives.

    Returns a summary dict with modeled loss, building count, and
    confidence interval.
    """
    # ── Use forecast landfall if available ──
    if forecast and forecast.predicted_landfall:
        lf = forecast.predicted_landfall
        # Override storm parameters with forecast landfall values
        storm = StormEntry(
            storm_id=storm.storm_id,
            name=storm.name,
            year=storm.year,
            category=lf.category if lf.category > 0 else storm.category,
            status="active",
            landfall_lon=lf.lon,
            landfall_lat=lf.lat,
            max_wind_kt=lf.max_wind_kt if lf.max_wind_kt > 0 else storm.max_wind_kt,
            min_pressure_mb=lf.pressure_mb if lf.pressure_mb > 0 else storm.min_pressure_mb,
            heading_deg=lf.direction_deg if lf.direction_deg > 0 else storm.heading_deg,
            speed_kt=lf.speed_kt if lf.speed_kt > 0 else storm.speed_kt,
            basin=storm.basin,
            advisory=forecast.advisory_num,
        )
        print(f"    Running pipeline for {storm.name} — Cat {storm.category}")
        print(f"    Forecast landfall: ({storm.landfall_lon:.2f}, {storm.landfall_lat:.2f}) "
              f"in ~{forecast.hours_to_landfall}h")
        print(f"    Forecast wind: {storm.max_wind_kt} kt, Pressure: {storm.min_pressure_mb} mb")
    else:
        print(f"    Running pipeline for {storm.name} ({storm.year}) — Cat {storm.category}")
        print(f"    Current position: ({storm.landfall_lon:.2f}, {storm.landfall_lat:.2f})")
        print(f"    Wind: {storm.max_wind_kt} kt, Pressure: {storm.min_pressure_mb} mb")
        if forecast:
            print(f"    (no US landfall predicted in forecast track)")

    sdir = _storm_cache_dir(storm)

    grid_cells = {}

    for idx, (col, row) in enumerate(GRID_CELLS_3x3):
        cell_key = f"{col},{row}"
        print(f"    Cell ({col},{row}) [{idx+1}/{len(GRID_CELLS_3x3)}]...", end=" ", flush=True)

        origin_lon = storm.grid_origin_lon
        origin_lat = storm.grid_origin_lat
        lon_min = origin_lon + col * CELL_WIDTH
        lat_min = origin_lat + row * CELL_HEIGHT
        lon_max = lon_min + CELL_WIDTH
        lat_max = lat_min + CELL_HEIGHT

        damage_path = os.path.join(sdir, f'cell_{col}_{row}_damage.geojson')
        flood_path = os.path.join(sdir, f'cell_{col}_{row}_flood.geojson')

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
            if not os.path.exists(flood_path):
                raster_to_geojson(raster_path, flood_path)
            with open(flood_path) as f:
                flood_data = json.load(f)

            # 3. Buildings (NSI → OSM fallback)
            buildings_path = os.path.join(sdir, f'cell_{col}_{row}_buildings.json')
            fetch_buildings(lon_min, lat_min, lon_max, lat_max, buildings_path, cache=True)
            with open(buildings_path) as f:
                buildings_data = json.load(f)

            # 4. HAZUS damage model
            if buildings_data.get('features'):
                estimate_damage_from_raster(raster_path, buildings_path, damage_path)
            else:
                with open(damage_path, 'w') as f:
                    json.dump({"type": "FeatureCollection", "features": []}, f)

            with open(damage_path) as f:
                damage_data = json.load(f)

            grid_cells[cell_key] = {"buildings": damage_data, "flood": flood_data}
            n_bldgs = len(damage_data.get("features", []))
            print(f"{n_bldgs} buildings")

        except Exception as e:
            print(f"ERROR: {e}")
            grid_cells[cell_key] = {
                "buildings": {"type": "FeatureCollection", "features": []},
                "flood": {"type": "FeatureCollection", "features": []},
            }

    # Build storm_data dict for the validation ledger
    storm_data = storm.to_dict()

    # Census population context
    try:
        pop_ctx = get_population_context(storm.landfall_lat, storm.landfall_lon)
        if pop_ctx:
            storm_data['population'] = pop_ctx
            print(f"    Population: {pop_ctx.get('pop_label', '?')} in {pop_ctx.get('county_name', '?')}, {pop_ctx.get('state_code', '?')}")
    except Exception:
        pass

    # Record to validation ledger
    model_run = record_from_activation(storm.storm_id, grid_cells, storm_data)

    # Compute confidence interval from backtesting
    prediction = predict_loss_range(model_run.modeled_loss)

    summary = {
        "storm_id": storm.storm_id,
        "name": storm.name,
        "category": storm.category,
        "max_wind_kt": storm.max_wind_kt,
        "modeled_loss": model_run.modeled_loss,
        "modeled_loss_M": round(model_run.modeled_loss / 1e6, 1),
        "building_count": model_run.building_count,
        "nsi_count": model_run.nsi_count,
        "prediction_low": prediction.get("low", 0),
        "prediction_high": prediction.get("high", 0),
        "confidence_note": prediction.get("confidence", ""),
        "timestamp": time.time(),
        # Forecast metadata
        "advisory_num": forecast.advisory_num if forecast else None,
        "hours_to_landfall": forecast.hours_to_landfall if forecast else None,
        "forecast_landfall_lat": storm.landfall_lat,
        "forecast_landfall_lon": storm.landfall_lon,
        "used_forecast_track": forecast is not None and forecast.predicted_landfall is not None,
    }

    print(f"    ✓ Modeled loss: ${model_run.modeled_loss/1e6:,.1f}M "
          f"({model_run.building_count} buildings, {model_run.nsi_count} NSI)")
    print(f"    ✓ Prediction range: ${prediction.get('low', 0)/1e6:,.0f}M – ${prediction.get('high', 0)/1e6:,.0f}M")

    return summary


def poll_once() -> list[dict]:
    """
    Single poll cycle: check NHC, run pipeline for qualifying storms.
    Returns list of summary dicts for storms that were processed.
    """
    print(f"\n{'─'*60}")
    print(f"NHC Poll — {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}")
    print(f"{'─'*60}")

    state = _load_state()
    results = []

    try:
        active = fetch_active_storms()
    except Exception as e:
        print(f"  ERROR fetching NHC feeds: {e}")
        return results

    if not active:
        print("  No active tropical systems in the Atlantic basin.")
        state["last_poll"] = time.time()
        _save_state(state)
        return results

    print(f"  Found {len(active)} active system(s)")

    # Fetch forecast tracks from NOAA ArcGIS
    forecast_tracks: dict[str, ForecastTrack] = {}
    try:
        tracks = fetch_forecast_track()
        for t in tracks:
            forecast_tracks[t.storm_name.upper()] = t
        if tracks:
            print(f"  Fetched {len(tracks)} forecast track(s) from NOAA")
    except Exception as e:
        print(f"  [warn] Forecast track fetch failed: {e} — using current positions")

    for storm in active:
        tag = f"{storm.name} ({storm.storm_id})"

        if not is_actionable(storm):
            reason = []
            if storm.max_wind_kt < MIN_WIND_KT:
                reason.append(f"wind {storm.max_wind_kt}kt < {MIN_WIND_KT}kt threshold")
            if not (LAT_RANGE[0] <= storm.landfall_lat <= LAT_RANGE[1]):
                reason.append(f"lat {storm.landfall_lat:.1f} outside US zone")
            if not (LON_RANGE[0] <= storm.landfall_lon <= LON_RANGE[1]):
                reason.append(f"lon {storm.landfall_lon:.1f} outside Atlantic zone")
            print(f"  ⏭ {tag} — skipping: {', '.join(reason)}")
            continue

        # Match forecast track by storm name
        forecast = forecast_tracks.get(storm.name.upper().replace("HURRICANE ", "")
                                                          .replace("TROPICAL STORM ", ""))

        # Use forecast advisory number if available, else fall back to RSS
        current_advisory = (forecast.advisory_num if forecast else None) or storm.advisory or ""

        # Check if we already processed this advisory
        last_advisory = state.get("processed", {}).get(storm.storm_id, "")
        if last_advisory == current_advisory and current_advisory:
            print(f"  ⏭ {tag} — advisory {current_advisory} already processed")
            continue

        print(f"\n  ▶ {tag} — Cat {storm.category}, {storm.max_wind_kt} kt")
        if forecast:
            lf = forecast.predicted_landfall
            if lf:
                print(f"    Forecast: landfall in ~{forecast.hours_to_landfall}h "
                      f"at ({lf.lon:.2f}, {lf.lat:.2f}), {lf.max_wind_kt} kt")
            else:
                print(f"    Forecast: {len(forecast.points)} track points, no US landfall predicted")

        try:
            summary = run_pipeline(storm, forecast=forecast)
            results.append(summary)

            # Mark advisory as processed
            if "processed" not in state:
                state["processed"] = {}
            state["processed"][storm.storm_id] = current_advisory

        except Exception as e:
            print(f"    PIPELINE ERROR: {e}")
            traceback.print_exc()

    state["last_poll"] = time.time()
    _save_state(state)

    if results:
        print(f"\n  Processed {len(results)} storm(s) this cycle")
    return results


def main():
    """Run the monitor loop. Pass --once for a single poll."""
    single = "--once" in sys.argv

    print("=" * 60)
    print("SurgeDPS Active Storm Monitor")
    print(f"  Poll interval: {POLL_INTERVAL}s ({POLL_INTERVAL // 60} min)")
    print(f"  Wind threshold: {MIN_WIND_KT} kt")
    print(f"  Mode: {'single poll' if single else 'continuous'}")
    print("=" * 60)

    if single:
        poll_once()
        return

    # Continuous monitoring loop
    while True:
        try:
            poll_once()
        except Exception as e:
            print(f"\nUNHANDLED ERROR in poll cycle: {e}")
            traceback.print_exc()

        print(f"\nNext poll in {POLL_INTERVAL // 60} minutes...")
        time.sleep(POLL_INTERVAL)


if __name__ == '__main__':
    main()
