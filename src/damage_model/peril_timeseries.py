"""
Time-series peril pipeline.

Wraps the per-cell damage model so it emits a *bundle of tick snapshots*
instead of a single final-state GeoJSON. For each tick in the schedule
(default: 0, 3, 6, ..., 72 h post-landfall) every building is evaluated
three ways — surge-only, rainfall-only, cumulative — using the current
accumulated rainfall fraction from ``rainfall_accumulation``. Surge and
wind are held constant at their peak/landfall values; rainfall scales
with the Gamma accumulation curve.

Output format (``cell_C_R_ticks.json``):

    {
      "tick_hours":   [0, 3, 6, ..., 72],
      "duration_h":   72,
      "peril_fields": ["s_ft","r_ft","c_ft","s_state","r_state","c_state","s_loss","r_loss","c_loss"],
      "buildings": [
          {
            "id": "bldg_42",
            "lat": 29.71, "lon": -95.4,
            "ticks": [[s_ft,r_ft,c_ft,s_state,r_state,c_state,s_loss,r_loss,c_loss], ...]
          },
          ...
      ]
    }

Compact array encoding keeps each tick row to ~60 bytes/building. 25 ticks
× 100 bldg/cell × 1500 cells ≈ 200 MB per storm (vs. ~5 GB if we dumped
full GeoJSON per tick).

The final tick also gets written out as a regular ``_damage.geojson`` for
backwards compatibility with the existing frontend layers that aren't
tick-aware yet.
"""
from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, List, Optional

import rasterio

from damage_model.depth_damage import (
    DEFAULT_BUILDING_TYPE,
    DEFAULT_FFH_FT,
    STRUCTURE_DAMAGE,
    _get_centroid,
    estimate_building_damage,
)
from damage_model.rainfall_accumulation import (
    DEFAULT_DURATION_H,
    default_tick_hours,
    rainfall_fraction_at_hour,
)

logger = logging.getLogger(__name__)

# Schema version — bump when the output layout changes so frontends can
# detect stale cached bundles.
TICKS_SCHEMA_VERSION = "v1"


# ── helpers ──────────────────────────────────────────────────────────────────
def _state_code(pct: float) -> str:
    """Compact two-char damage-state code for tick records."""
    if pct >= 60: return "sv"   # severe
    if pct >= 40: return "mj"   # major
    if pct >= 20: return "mo"   # moderate
    if pct >=  5: return "mi"   # minor
    return "no"


def _run_hazus(
    depth_m: float,
    rainfall_m: Optional[float],
    wind_mph: Optional[float],
    bldg_kwargs: dict,
) -> tuple[float, float, str]:
    """Single HAZUS call returning (depth_ft, loss_usd, state_code).

    Non-fatal on errors — returns zeros so the tick loop keeps going.
    """
    try:
        dmg = estimate_building_damage(
            depth_m=depth_m,
            rainfall_depth_m=rainfall_m,
            wind_speed_mph=wind_mph,
            **bldg_kwargs,
        )
        d_ft = (depth_m or 0.0) * 3.28084
        loss = float(dmg.estimated_loss_usd or 0.0)
        pct  = float(dmg.total_damage_pct or 0.0)
        return d_ft, loss, _state_code(pct)
    except Exception as exc:
        logger.debug("HAZUS call failed on building: %s", exc)
        return 0.0, 0.0, "no"


def estimate_damage_timeseries_from_raster(
    depth_raster_path: str,
    buildings_geojson_path: str,
    ticks_output_path: str,
    final_geojson_path: str = "",
    storm_id: Optional[str] = None,
    landfall_lat: Optional[float] = None,
    landfall_lon: Optional[float] = None,
    max_wind_kt: Optional[float] = None,
    storm_speed_kt: Optional[float] = None,
    storm_heading_deg: Optional[float] = None,
    tick_hours: Optional[List[float]] = None,
    duration_hours: float = DEFAULT_DURATION_H,
    building_type: str = DEFAULT_BUILDING_TYPE,
) -> Dict[str, Any]:
    """Run the damage model across a tick schedule and write a bundle.

    Returns a small summary dict for logging / health endpoints; the
    meaningful output is the ``ticks_output_path`` JSON file.
    """
    tick_hours = tick_hours or default_tick_hours(duration_h=duration_hours)

    with open(buildings_geojson_path) as f:
        buildings_data = json.load(f)
    features = buildings_data.get("features") or []
    if not features:
        _write_empty_bundle(ticks_output_path, tick_hours, duration_hours)
        return {"buildings": 0, "ticks": len(tick_hours)}

    # ── Setup phase (run once) ──────────────────────────────────────────────
    # Raster, wind field, rainfall model are identical across all ticks;
    # rainfall_fraction is the only thing that varies.
    wind_snapshot = None
    if storm_id and landfall_lat is not None and landfall_lon is not None:
        try:
            from damage_model.wind_field import load_landfall_snapshot
            wind_snapshot = load_landfall_snapshot(storm_id, landfall_lat, landfall_lon)
        except Exception as exc:
            logger.info("[peril_ts] wind field init failed: %s", exc)

    _estimate_rain = None
    rain_params = None
    if max_wind_kt and storm_speed_kt is not None and storm_heading_deg is not None:
        try:
            from flood_model.rainfall import estimate_rainfall_at_point
            _estimate_rain = estimate_rainfall_at_point
            rain_params = dict(
                center_lat=landfall_lat, center_lon=landfall_lon,
                max_wind_kt=max_wind_kt,
                storm_speed_kt=storm_speed_kt,
                heading_deg=storm_heading_deg,
            )
        except Exception as exc:
            logger.info("[peril_ts] rainfall model unavailable: %s", exc)

    # ── Per-building setup: sample surge, compute full-total rainfall & wind ─
    per_bldg: List[Dict[str, Any]] = []
    with rasterio.open(depth_raster_path) as src:
        depth_band = src.read(1)
        transform = src.transform
        nodata = src.nodata or -9999

        for i, feat in enumerate(features):
            props = feat.get("properties") or {}
            lon, lat = _get_centroid(feat.get("geometry") or {})
            if lon == 0 and lat == 0:
                continue

            try:
                r, c = rasterio.transform.rowcol(transform, lon, lat)
                surge_m = float(depth_band[r, c]) if (
                    0 <= r < depth_band.shape[0] and 0 <= c < depth_band.shape[1]
                ) else 0.0
            except Exception:
                surge_m = 0.0
            if surge_m == nodata or surge_m <= 0.0:
                surge_m = 0.0

            rain_total_m = 0.0
            if _estimate_rain is not None and rain_params is not None:
                try:
                    r_m = _estimate_rain(point_lat=lat, point_lon=lon, **rain_params)
                    rain_total_m = float(r_m or 0.0)
                    if rain_total_m < 0.01:
                        rain_total_m = 0.0
                except Exception:
                    rain_total_m = 0.0

            wind_mph = None
            if wind_snapshot is not None:
                try:
                    from damage_model.wind_field import get_wind_speed_at_point
                    wind_mph = get_wind_speed_at_point(wind_snapshot, lat, lon)
                except Exception:
                    pass

            btype = props.get("building_type", props.get("type", building_type))
            if btype not in STRUCTURE_DAMAGE:
                btype = building_type
            bid = str(props.get("id", props.get("building_id", i)))

            bldg_kwargs = dict(
                lon=lon, lat=lat,
                building_type=btype, building_id=bid,
                sqft=float(props["area_sqft"]) if props.get("area_sqft") else None,
                first_floor_ht_ft=float(props["found_ht"]) if props.get("found_ht") is not None else None,
                val_struct=float(props["val_struct"]) if props.get("val_struct") is not None else None,
                val_cont=float(props["val_cont"]) if props.get("val_cont") is not None else None,
                med_yr_blt=int(props["med_yr_blt"]) if props.get("med_yr_blt") is not None else None,
                num_story=int(props["num_story"]) if props.get("num_story") is not None else None,
                occtype=str(props["occtype"]) if props.get("occtype") else None,
                storm_speed_kt=storm_speed_kt,
                flood_zone=props.get("flood_zone"),
            )

            per_bldg.append({
                "id": bid, "lat": lat, "lon": lon,
                "surge_m": surge_m,
                "rain_total_m": rain_total_m,
                "wind_mph": wind_mph,
                "kwargs": bldg_kwargs,
                "btype": btype, "props": props,
            })

    # ── Tick loop: evaluate three perils per building per tick ──────────────
    # Surge-only damage is identical across ticks (surge = instantaneous
    # peak, does not accumulate), so we pre-compute it once and reuse.
    surge_only_by_bldg: Dict[str, tuple[float, float, str]] = {}
    for b in per_bldg:
        surge_only_by_bldg[b["id"]] = _run_hazus(
            depth_m=b["surge_m"],
            rainfall_m=None,
            wind_mph=b["wind_mph"],   # wind also constant
            bldg_kwargs=b["kwargs"],
        )

    bundle_buildings: List[Dict[str, Any]] = []
    for b in per_bldg:
        s_ft, s_loss, s_state = surge_only_by_bldg[b["id"]]
        ticks_rows: List[list] = []
        for h in tick_hours:
            frac = rainfall_fraction_at_hour(h, duration_hours=duration_hours)
            rain_m_at_tick = b["rain_total_m"] * frac

            if rain_m_at_tick > 0.01:
                r_ft, r_loss, r_state = _run_hazus(
                    depth_m=0.0,
                    rainfall_m=rain_m_at_tick,
                    wind_mph=None,
                    bldg_kwargs=b["kwargs"],
                )
                c_ft, c_loss, c_state = _run_hazus(
                    depth_m=b["surge_m"],
                    rainfall_m=rain_m_at_tick,
                    wind_mph=b["wind_mph"],
                    bldg_kwargs=b["kwargs"],
                )
            else:
                # No rainfall yet at this tick → rainfall-only is zero,
                # cumulative equals surge-only.
                r_ft, r_loss, r_state = 0.0, 0.0, "no"
                c_ft, c_loss, c_state = s_ft, s_loss, s_state

            ticks_rows.append([
                round(s_ft, 2), round(r_ft, 2), round(c_ft, 2),
                s_state, r_state, c_state,
                round(s_loss, 0), round(r_loss, 0), round(c_loss, 0),
            ])

        bundle_buildings.append({
            "id": b["id"],
            "lat": round(b["lat"], 6),
            "lon": round(b["lon"], 6),
            "ticks": ticks_rows,
        })

    # ── Write tick bundle ───────────────────────────────────────────────────
    os.makedirs(os.path.dirname(ticks_output_path) or ".", exist_ok=True)
    with open(ticks_output_path, "w") as f:
        json.dump({
            "schema_version": TICKS_SCHEMA_VERSION,
            "tick_hours": tick_hours,
            "duration_h": duration_hours,
            "peril_fields": [
                "s_ft", "r_ft", "c_ft",
                "s_state", "r_state", "c_state",
                "s_loss", "r_loss", "c_loss",
            ],
            "buildings": bundle_buildings,
        }, f, separators=(",", ":"))

    # ── Also delegate to the existing single-state estimator for the final
    # tick so backward-compatible _damage.geojson keeps flowing. Easier
    # than reconstructing the full GeoJSON schema here. ──────────────────
    if final_geojson_path:
        try:
            from damage_model.depth_damage import estimate_damage_from_raster
            estimate_damage_from_raster(
                depth_raster_path=depth_raster_path,
                buildings_geojson_path=buildings_geojson_path,
                output_path=final_geojson_path,
                storm_id=storm_id,
                landfall_lat=landfall_lat, landfall_lon=landfall_lon,
                max_wind_kt=max_wind_kt,
                storm_speed_kt=storm_speed_kt,
                storm_heading_deg=storm_heading_deg,
            )
        except Exception as exc:
            logger.warning("[peril_ts] final GeoJSON emit failed: %s", exc)

    return {
        "buildings": len(bundle_buildings),
        "ticks": len(tick_hours),
        "duration_h": duration_hours,
        "output": ticks_output_path,
    }


def _write_empty_bundle(path: str, tick_hours, duration_hours) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w") as f:
        json.dump({
            "schema_version": TICKS_SCHEMA_VERSION,
            "tick_hours": tick_hours,
            "duration_h": duration_hours,
            "peril_fields": [
                "s_ft", "r_ft", "c_ft",
                "s_state", "r_state", "c_state",
                "s_loss", "r_loss", "c_loss",
            ],
            "buildings": [],
        }, f)
