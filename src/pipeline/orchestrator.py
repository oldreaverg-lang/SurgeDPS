"""
Pipeline Orchestrator

Lambda handler that coordinates the full flood modeling pipeline.
Invoked by Step Functions at each stage of the state machine.

Pipeline stages:
  1. INGEST   — Fetch DEM, P-Surge, NWM, QPF for storm area
  2. MODEL    — Run bathtub surge, HAND rainfall, compound merge
  3. TILEGEN  — Convert depth rasters to COGs and PMTiles
  4. PUBLISH  — Upload to S3, write manifest, invalidate CDN

Each stage receives the pipeline state from Step Functions and
returns updated state for the next stage.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from typing import Any, Dict

logger = logging.getLogger(__name__)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Stage: INGEST
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def handle_ingest(event: dict, context: Any) -> dict:
    """
    Stage 1: Ingest all data for the storm area.

    Input (from storm detector):
        storm_id, advisory_number, center, gis_data

    Output (for model stage):
        dem_path, surge_path, nwm_path, qpf_path, storm_geometry
    """
    from data_ingest.config import IngestConfig
    from data_ingest.dem_clipper import DEMClipper
    from data_ingest.noaa_fetchers import PSurgeFetcher, NWMFetcher, QPFFetcher

    config = IngestConfig()
    storm_id = event["storm_id"]
    advisory_num = event["advisory_number"]
    center = event.get("center", {})
    gis_data = event.get("gis_data", {})

    logger.info(f"INGEST stage: {storm_id} advisory {advisory_num}")

    scratch = os.path.join(config.scratch_dir, storm_id, advisory_num)
    os.makedirs(scratch, exist_ok=True)

    # Build storm geometry from GIS data or center point
    storm_geometry = _build_storm_geometry(event, config)

    # Clip DEM
    dem_clipper = DEMClipper(config)
    clipped_dem = dem_clipper.clip_to_extent(
        storm_geometry,
        os.path.join(scratch, "dem"),
        resolution="10m",
        storm_id=storm_id,
        advisory_num=advisory_num,
    )

    # Fetch P-Surge
    surge_fetcher = PSurgeFetcher(config)
    surge_data = surge_fetcher.fetch(
        storm_id, advisory_num, storm_geometry,
        os.path.join(scratch, "surge"),
    )

    # Fetch NWM discharge
    # Resolve NHDPlus reach IDs from storm geometry spatial extent
    reach_ids = _find_nhdplus_reaches(storm_geometry, config)
    nwm_fetcher = NWMFetcher(config)
    nwm_data = nwm_fetcher.fetch(
        storm_geometry,
        reach_ids=reach_ids,
        output_dir=os.path.join(scratch, "nwm"),
    )

    # Fetch QPF
    qpf_fetcher = QPFFetcher(config)
    qpf_data = qpf_fetcher.fetch(
        storm_geometry,
        os.path.join(scratch, "qpf"),
    )

    # Fetch tide gauge data for bias correction
    tide_bias_m = 0.0
    tide_gauge_geojson = ""
    tide_station_count = 0
    try:
        from data_ingest.tide_gauge import TideGaugeFetcher

        tide_fetcher = TideGaugeFetcher(config=config)
        tide_result = tide_fetcher.fetch_for_storm(storm_geometry)
        tide_bias_m = tide_result.mean_tide_bias_m
        tide_station_count = tide_result.station_count

        if tide_result.station_count > 0:
            tide_gauge_geojson = os.path.join(scratch, "tide_gauges.geojson")
            tide_fetcher.write_geojson(tide_result, tide_gauge_geojson)
            logger.info(
                f"Tide gauges: {tide_result.station_count} stations, "
                f"mean bias={tide_bias_m:.3f}m"
            )
    except Exception as e:
        logger.warning(f"Tide gauge fetch failed (non-fatal): {e}")

    # Resolve NHDPlus data path for vector overlay generation
    nhdplus_path = os.path.join(config.scratch_dir, "base_data", "nhdplus_flowlines.gpkg")
    if not os.path.exists(nhdplus_path):
        nhdplus_path = ""

    # Return updated state for the next stage
    return {
        **event,
        "stage": "ingest_complete",
        "scratch_dir": scratch,
        "dem_path": clipped_dem.path,
        "dem_bounds": list(clipped_dem.bounds),
        "dem_crs": clipped_dem.crs,
        "surge_path": surge_data.path,
        "surge_max_m": surge_data.max_surge_m,
        "nwm_path": nwm_data.path,
        "nwm_reach_count": nwm_data.reach_count,
        "nwm_reach_ids": reach_ids,
        "qpf_path": qpf_data.path,
        "qpf_max_mm": qpf_data.total_precip_mm,
        "storm_geometry": storm_geometry,
        "nhdplus_path": nhdplus_path,
        "cone_shapefile_dir": gis_data.get("cone_shapefile_dir", ""),
        "tide_bias_m": tide_bias_m,
        "tide_gauge_geojson": tide_gauge_geojson,
        "tide_station_count": tide_station_count,
    }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Stage: MODEL
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def handle_model(event: dict, context: Any) -> dict:
    """
    Stage 2: Run flood models.

    Runs bathtub (surge), HAND (rainfall), and compound merge
    for each forecast timestep.
    """
    from flood_model.bathtub import run_bathtub_model
    from flood_model.compound import merge_compound_flood

    storm_id = event["storm_id"]
    advisory_num = event["advisory_number"]
    scratch = event["scratch_dir"]

    logger.info(f"MODEL stage: {storm_id} advisory {advisory_num}")

    model_dir = os.path.join(scratch, "model_output")
    os.makedirs(model_dir, exist_ok=True)

    # ── Apply Tide Bias Correction to Surge ─────────────────────
    # Add the current tide level to the P-Surge heights so the
    # bathtub model accounts for whether it's high or low tide.
    tide_bias_m = event.get("tide_bias_m", 0.0)
    surge_path = event["surge_path"]

    if tide_bias_m != 0.0 and os.path.exists(surge_path):
        try:
            import rasterio
            corrected_path = os.path.join(model_dir, "surge_tide_corrected.tif")
            with rasterio.open(surge_path) as src:
                data = src.read(1)
                nodata = src.nodata or -9999
                profile = src.profile.copy()

            valid = data != nodata
            data[valid] = data[valid] + tide_bias_m

            profile.update(dtype="float32")
            with rasterio.open(corrected_path, "w", **profile) as dst:
                dst.write(data.astype("float32"), 1)

            surge_path = corrected_path
            logger.info(f"Applied tide bias correction: {tide_bias_m:+.3f}m")
        except Exception as e:
            logger.warning(f"Tide bias correction failed (non-fatal): {e}")

    # ── Bathtub Surge Model ────────────────────────────────────
    surge_result = run_bathtub_model(
        dem_path=event["dem_path"],
        surge_path=surge_path,
        output_dir=model_dir,
        storm_id=storm_id,
        advisory_num=advisory_num,
    )

    # ── HAND Rainfall Model ────────────────────────────────────
    # In production: use pre-computed HAND + catchment rasters
    # For now: generate a synthetic rainfall depth from QPF
    rain_depth_path = _generate_rainfall_depth(
        event["qpf_path"],
        event["dem_path"],
        model_dir,
        storm_id,
        advisory_num,
    )

    # ── Compound Merge ─────────────────────────────────────────
    compound_result = merge_compound_flood(
        surge_depth_path=surge_result.depth_path,
        rainfall_depth_path=rain_depth_path,
        output_dir=model_dir,
        storm_id=storm_id,
        advisory_num=advisory_num,
    )

    # ── Tier 3: HEC-RAS 2D (Premium) ─────────────────────────────
    # Runs HEC-RAS for high-resolution compound flood modeling.
    # Falls back to synthetic results if binaries not available.
    hecras_result = None
    hecras_output_dir = ""
    enable_hecras = os.getenv("ENABLE_HECRAS", "false").lower() == "true"

    if enable_hecras:
        try:
            from hecras.runner import HECRASRunner, HECRASRunRequest

            runner = HECRASRunner(
                data_bucket=os.getenv("DATA_BUCKET", ""),
                work_dir=os.path.join(scratch, "hecras"),
            )

            center = event.get("storm_center", [0, 0])
            hecras_request = HECRASRunRequest(
                storm_id=storm_id,
                advisory_num=advisory_num,
                storm_center=tuple(center),
                surge_s3_path=event.get("surge_s3_path", ""),
                rainfall_s3_path=event.get("qpf_s3_path", ""),
                dem_s3_path=event.get("dem_s3_path", ""),
                data_bucket=os.getenv("DATA_BUCKET", ""),
            )
            hecras_result = runner.run(hecras_request)
            hecras_output_dir = hecras_result.output_dir or ""

            logger.info(
                f"HEC-RAS Tier 3: mode={hecras_result.mode}, "
                f"max_depth={hecras_result.max_depth_m:.2f}m, "
                f"files={len(hecras_result.output_files)}"
            )
        except Exception as e:
            logger.warning(f"HEC-RAS Tier 3 failed (non-fatal): {e}")

    return {
        **event,
        "stage": "model_complete",
        "model_dir": model_dir,
        "surge_depth_path": surge_result.depth_path,
        "surge_max_depth_m": surge_result.max_depth_m,
        "surge_flooded_pct": surge_result.flooded_pct,
        "rainfall_depth_path": rain_depth_path,
        "compound_depth_path": compound_result.compound_depth_path,
        "overlap_mask_path": compound_result.overlap_mask_path,
        "compound_max_depth_m": compound_result.max_depth_m,
        "compound_flooded_pct": compound_result.flooded_pct,
        "overlap_pct": compound_result.overlap_pct,
        "hecras_enabled": enable_hecras,
        "hecras_output_dir": hecras_output_dir,
        "hecras_max_depth_m": hecras_result.max_depth_m if hecras_result else 0.0,
        "hecras_mode": hecras_result.mode if hecras_result else "disabled",
    }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Stage: TILEGEN
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def handle_tilegen(event: dict, context: Any) -> dict:
    """
    Stage 3: Generate COGs and PMTiles from model outputs.

    Processes:
        - Main depth rasters (surge, rainfall, compound) for T+0
        - HEC-RAS timestep rasters if available (T+0 through T+72)
    """
    from tile_gen.pmtiles_builder import generate_tiles_for_layer

    storm_id = event["storm_id"]
    advisory_num = event["advisory_number"]
    scratch = event["scratch_dir"]

    logger.info(f"TILEGEN stage: {storm_id} advisory {advisory_num}")

    tile_dir = os.path.join(scratch, "tiles")
    os.makedirs(tile_dir, exist_ok=True)

    # Generate tiles for each Tier 1/2 layer
    layers_output = {}

    for layer_name, depth_path_key in [
        ("surge", "surge_depth_path"),
        ("rainfall", "rainfall_depth_path"),
        ("compound", "compound_depth_path"),
    ]:
        depth_path = event.get(depth_path_key)
        if not depth_path or not os.path.exists(depth_path):
            logger.warning(f"Skipping {layer_name}: no depth raster")
            continue

        result = generate_tiles_for_layer(
            depth_raster_path=depth_path,
            output_dir=tile_dir,
            layer_name=layer_name,
            storm_id=storm_id,
            advisory_num=advisory_num,
        )
        layers_output[layer_name] = result.metadata

    # Generate tiles for HEC-RAS timestep rasters (Tier 3, premium)
    hecras_output_dir = event.get("hecras_output_dir", "")
    hecras_timesteps = []
    if hecras_output_dir and os.path.isdir(hecras_output_dir):
        import glob
        timestep_files = sorted(
            glob.glob(os.path.join(hecras_output_dir, "hecras_depth_t*.tif"))
        )
        for ts_file in timestep_files:
            # Extract timestep index from filename: hecras_depth_t003.tif -> 3
            fname = os.path.basename(ts_file)
            ts_idx = int(fname.replace("hecras_depth_t", "").replace(".tif", ""))
            ts_hours = ts_idx * 6  # Each index = 6 hours

            ts_layer_name = f"hecras_t{ts_hours:02d}"
            result = generate_tiles_for_layer(
                depth_raster_path=ts_file,
                output_dir=tile_dir,
                layer_name=ts_layer_name,
                storm_id=storm_id,
                advisory_num=advisory_num,
            )
            layers_output[ts_layer_name] = result.metadata
            hecras_timesteps.append(ts_hours)

        # Also generate tiles for max depth envelope
        max_depth_file = os.path.join(hecras_output_dir, "hecras_max_depth.tif")
        if os.path.exists(max_depth_file):
            result = generate_tiles_for_layer(
                depth_raster_path=max_depth_file,
                output_dir=tile_dir,
                layer_name="hecras_max",
                storm_id=storm_id,
                advisory_num=advisory_num,
            )
            layers_output["hecras_max"] = result.metadata

        logger.info(
            f"Generated HEC-RAS tiles for {len(timestep_files)} timesteps"
        )

    # ── Vector Overlay Layers (cone, track, reaches) ──────────
    overlay_result = None
    try:
        from tile_gen.vector_overlays import build_vector_overlays

        overlay_result = build_vector_overlays(
            output_dir=tile_dir,
            storm_id=storm_id,
            advisory_num=advisory_num,
            storm_name=event.get("storm_name", ""),
            storm_geometry=event.get("storm_geometry"),
            cone_shapefile_dir=event.get("cone_shapefile_dir", ""),
            nhdplus_path=event.get("nhdplus_path", ""),
            reach_ids=event.get("nwm_reach_ids"),
            tide_gauge_geojson=event.get("tide_gauge_geojson", ""),
        )

        logger.info(
            f"Vector overlays: {len(overlay_result.layers)} layers "
            f"({', '.join(overlay_result.layer_names)})"
        )
    except Exception as e:
        logger.warning(f"Vector overlay generation failed (non-fatal): {e}")

    # ── Damage Estimation (HAZUS-style) ────────────────────────
    damage_geojson = ""
    damage_summary = {}
    try:
        from damage_model.building_exposure import load_buildings_for_extent
        from damage_model.depth_damage import estimate_damage_from_raster

        storm_geometry = event.get("storm_geometry")
        compound_path = event.get("compound_depth_path", "")

        if storm_geometry and compound_path and os.path.exists(compound_path):
            buildings_dir = os.path.join(tile_dir, "damage")
            os.makedirs(buildings_dir, exist_ok=True)

            # Load or generate building inventory
            buildings_path = os.path.join(buildings_dir, "buildings.geojson")
            building_data_path = os.getenv("BUILDING_DATA_PATH", "")
            inventory = load_buildings_for_extent(
                storm_geometry=storm_geometry,
                data_path=building_data_path,
                output_path=buildings_path,
            )

            if inventory.building_count > 0:
                # Run damage estimation against compound depth raster
                damage_geojson = os.path.join(buildings_dir, "damage.geojson")
                damage_est = estimate_damage_from_raster(
                    depth_raster_path=compound_path,
                    buildings_geojson_path=buildings_path,
                    output_path=damage_geojson,
                )
                damage_summary = {
                    "buildings_assessed": damage_est.buildings_assessed,
                    "buildings_damaged": damage_est.buildings_damaged,
                    "total_loss_usd": damage_est.total_loss_usd,
                    "avg_damage_pct": damage_est.avg_damage_pct,
                    "max_damage_pct": damage_est.max_damage_pct,
                    "damage_by_category": damage_est.damage_by_category,
                }
                logger.info(
                    f"Damage estimate: {damage_est.buildings_damaged}/"
                    f"{damage_est.buildings_assessed} damaged, "
                    f"${damage_est.total_loss_usd:,.0f} total loss"
                )
    except Exception as e:
        logger.warning(f"Damage estimation failed (non-fatal): {e}")

    # Collect overlay metadata for the publish step
    overlay_layers = {}
    if overlay_result:
        for name, layer in overlay_result.layers.items():
            overlay_layers[name] = {
                "geojson_path": layer.geojson_path,
                "pmtiles_path": layer.pmtiles_path,
                "feature_count": layer.feature_count,
                "size_bytes": layer.size_bytes,
            }

    # Add damage overlay if available
    if damage_geojson and os.path.exists(damage_geojson):
        try:
            from tile_gen.vector_overlays import OverlayResult as OvResult
            from tile_gen.pmtiles_builder import build_vector_pmtiles

            damage_pmtiles = os.path.join(tile_dir, "overlays", "damage.pmtiles")
            os.makedirs(os.path.dirname(damage_pmtiles), exist_ok=True)

            import json
            with open(damage_geojson) as f:
                dg = json.load(f)
            feat_count = len(dg.get("features", []))

            try:
                pm_result = build_vector_pmtiles(
                    damage_geojson, damage_pmtiles,
                    min_zoom=6, max_zoom=14,
                    layer_name="damage",
                )
                overlay_layers["damage"] = {
                    "geojson_path": damage_geojson,
                    "pmtiles_path": pm_result.path,
                    "feature_count": feat_count,
                    "size_bytes": pm_result.size_bytes,
                }
            except Exception as e:
                logger.warning(f"Damage PMTiles failed: {e}")
                overlay_layers["damage"] = {
                    "geojson_path": damage_geojson,
                    "pmtiles_path": "",
                    "feature_count": feat_count,
                    "size_bytes": 0,
                }
        except Exception as e:
            logger.warning(f"Damage overlay packaging failed: {e}")

    return {
        **event,
        "stage": "tilegen_complete",
        "tile_dir": tile_dir,
        "layers": layers_output,
        "hecras_timesteps": hecras_timesteps,
        "overlay_layers": overlay_layers,
        "damage_geojson": damage_geojson,
        "damage_summary": damage_summary,
    }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Stage: PUBLISH
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def handle_publish(event: dict, context: Any) -> dict:
    """
    Stage 4: Upload tiles to S3, write manifest, invalidate CDN.
    """
    from pipeline.publisher import (
        OutputPublisher, StormManifest, ManifestLayer, OverlayLayer,
    )

    storm_id = event["storm_id"]
    advisory_num = event["advisory_number"]
    tile_dir = event["tile_dir"]

    logger.info(f"PUBLISH stage: {storm_id} advisory {advisory_num}")

    bucket = os.getenv("DATA_BUCKET", "surgedps-data")
    cf_dist = os.getenv("CLOUDFRONT_DISTRIBUTION_ID", "")
    tile_url = os.getenv("TILE_BASE_URL", f"https://{bucket}.s3.amazonaws.com")
    dry_run = os.getenv("DRY_RUN", "false").lower() == "true"

    publisher = OutputPublisher(
        bucket=bucket,
        cloudfront_distribution_id=cf_dist,
        tile_base_url=tile_url,
        dry_run=dry_run,
    )

    # Upload tiles
    uploaded = publisher.publish_tiles(tile_dir, storm_id, advisory_num)

    # Build manifest
    manifest = StormManifest(
        storm_id=storm_id,
        storm_name=event.get("storm_name", "Unknown"),
        storm_type=event.get("storm_type", "Unknown"),
        advisory_number=advisory_num,
        center=[
            event.get("center", {}).get("lon", 0),
            event.get("center", {}).get("lat", 0),
        ],
        wind_mph=event.get("wind_mph"),
        pressure_mb=event.get("pressure_mb"),
        bounds=event.get("dem_bounds"),
    )

    # Add layer metadata
    layer_configs = {
        "surge": ("Storm Surge", "cyan"),
        "rainfall": ("Rainfall Flooding", "magenta"),
        "compound": ("Compound Flooding", "violet"),
    }

    for name, (display, color) in layer_configs.items():
        layer_data = event.get("layers", {}).get(name, {})
        if not layer_data:
            continue

        manifest.layers.append(ManifestLayer(
            name=name,
            display_name=display,
            color_ramp=color,
            timesteps=[0],  # Tier 1/2: single snapshot
            max_depth_m=event.get(f"{name}_max_depth_m", 0),
            max_depth_ft=event.get(f"{name}_max_depth_m", 0) * 3.28084,
        ))

    # Add HEC-RAS multi-timestep layers (Tier 3, premium)
    hecras_timesteps = event.get("hecras_timesteps", [])
    if hecras_timesteps:
        manifest.layers.append(ManifestLayer(
            name="hecras_max",
            display_name="HEC-RAS Max Depth (Premium)",
            color_ramp="inferno",
            timesteps=hecras_timesteps,
            max_depth_m=event.get("hecras_max_depth_m", 0),
            max_depth_ft=event.get("hecras_max_depth_m", 0) * 3.28084,
        ))

    # Add vector overlay layers to manifest
    overlay_configs = {
        "storm_cone": ("Forecast Cone", "fill", {
            "fill-color": "rgba(255, 165, 0, 0.15)",
            "fill-outline-color": "rgba(255, 140, 0, 0.8)",
        }),
        "storm_track": ("Forecast Track", "line", {
            "line-color": "#FF4500",
            "line-width": 3,
            "line-dasharray": [2, 1],
        }),
        "reaches": ("River Reaches", "line", {
            "line-color": "#4FC3F7",
            "line-width": ["interpolate", ["linear"], ["get", "stream_order"], 1, 0.5, 3, 1.5, 5, 3],
            "line-opacity": 0.7,
        }),
        "tide_gauges": ("Tide Gauges", "circle", {
            "circle-radius": 7,
            "circle-color": ["interpolate", ["linear"], ["get", "water_level_m"],
                -0.5, "#2196F3", 0, "#4CAF50", 0.5, "#FF9800", 1.0, "#F44336"],
            "circle-stroke-color": "#FFFFFF",
            "circle-stroke-width": 2,
        }),
        "damage": ("Building Damage", "circle", {
            "circle-radius": ["interpolate", ["linear"], ["zoom"], 8, 2, 12, 5, 14, 8],
            "circle-color": ["match", ["get", "damage_category"],
                "severe", "#D32F2F", "major", "#FF5722",
                "moderate", "#FF9800", "minor", "#FFC107",
                "#4CAF50"],
            "circle-opacity": 0.8,
            "circle-stroke-color": "#FFFFFF",
            "circle-stroke-width": 0.5,
        }),
    }

    overlay_layer_data = event.get("overlay_layers", {})
    for name, (display, layer_type, style) in overlay_configs.items():
        layer_info = overlay_layer_data.get(name, {})
        if not layer_info:
            continue

        # Build the tiles URL relative to the storm's S3 prefix
        tiles_url = ""
        pmtiles_path = layer_info.get("pmtiles_path", "")
        if pmtiles_path:
            # Relative path from tile_dir to the pmtiles file
            import os.path
            rel = os.path.relpath(pmtiles_path, tile_dir)
            tiles_url = f"storms/{storm_id}/advisory_{advisory_num}/{rel}"

        manifest.overlays.append(OverlayLayer(
            name=name,
            display_name=display,
            layer_type=layer_type,
            tiles_url=tiles_url,
            feature_count=layer_info.get("feature_count", 0),
            style=style,
        ))

    # Add damage summary to manifest if available
    damage_summary = event.get("damage_summary", {})
    if damage_summary:
        manifest.damage_summary = damage_summary

    # Publish manifest
    manifest_url = publisher.publish_manifest(
        manifest, storm_id, advisory_num
    )

    # Invalidate CDN
    invalidation_id = publisher.invalidate_cdn(storm_id)

    return {
        **event,
        "stage": "publish_complete",
        "manifest_url": manifest_url,
        "files_uploaded": len(uploaded),
        "cdn_invalidation_id": invalidation_id,
        "completed_at": datetime.utcnow().isoformat() + "Z",
    }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Helpers
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def _find_nhdplus_reaches(storm_geometry: dict, config) -> list:
    """
    Find NHDPlus reach IDs that intersect the storm processing extent.

    Strategy:
        1. Try reading from a pre-built NHDPlus spatial index (GeoPackage)
        2. Fall back to USGS NWIS API query for gauged reaches in the bbox
        3. Last resort: generate synthetic reach IDs from bounding box grid

    Returns:
        List of NHDPlus COMID reach identifiers (integers)
    """
    # Extract bounding box from storm geometry
    coords = storm_geometry.get("coordinates", [[]])
    flat_coords = coords[0] if coords else []
    if not flat_coords:
        logger.warning("No coordinates in storm geometry — using default reaches")
        return list(range(1000, 1100))

    lons = [c[0] for c in flat_coords]
    lats = [c[1] for c in flat_coords]
    bbox = (min(lons), min(lats), max(lons), max(lats))

    # Strategy 1: Pre-built NHDPlus index (GeoPackage or shapefile)
    nhdplus_index = os.path.join(config.scratch_dir, "base_data", "nhdplus_flowlines.gpkg")
    if os.path.exists(nhdplus_index):
        try:
            import fiona
            from shapely.geometry import shape, box

            storm_box = box(*bbox)
            reach_ids = []

            with fiona.open(nhdplus_index) as src:
                for feature in src.filter(bbox=bbox):
                    geom = shape(feature["geometry"])
                    if geom.intersects(storm_box):
                        comid = feature["properties"].get(
                            "COMID", feature["properties"].get("comid", 0)
                        )
                        if comid:
                            reach_ids.append(int(comid))

            if reach_ids:
                logger.info(
                    f"Found {len(reach_ids)} NHDPlus reaches from spatial index"
                )
                return reach_ids

        except (ImportError, Exception) as e:
            logger.warning(f"NHDPlus spatial index query failed: {e}")

    # Strategy 2: Query USGS NWIS for gauged sites in the bbox
    try:
        from data_ingest.noaa_fetchers import USGSGaugeFetcher
        gauge_fetcher = USGSGaugeFetcher(config)
        gauges = gauge_fetcher.fetch_gauges_in_bbox(bbox)
        if gauges:
            reach_ids = [g.site_id for g in gauges if g.site_id]
            if reach_ids:
                logger.info(
                    f"Found {len(reach_ids)} gauged reaches from USGS NWIS"
                )
                return reach_ids
    except Exception as e:
        logger.warning(f"USGS gauge query failed: {e}")

    # Strategy 3: Generate synthetic reach IDs from grid
    # Produces a deterministic set based on bounding box so the
    # pipeline can proceed with synthetic NWM data
    west, south, east, north = bbox
    reach_ids = []
    grid_step = 0.1  # ~11km grid
    lat = south
    reach_counter = 10000
    while lat < north:
        lon = west
        while lon < east:
            reach_ids.append(reach_counter)
            reach_counter += 1
            lon += grid_step
        lat += grid_step

    logger.info(
        f"Generated {len(reach_ids)} synthetic reach IDs from "
        f"{grid_step:.1f}° grid over bbox ({west:.1f},{south:.1f})-({east:.1f},{north:.1f})"
    )
    return reach_ids


def _build_storm_geometry(event: dict, config) -> dict:
    """
    Build a GeoJSON geometry for the storm processing extent.

    Priority:
        1. Read forecast cone from downloaded NHC shapefile (most accurate)
        2. Fall back to circular buffer around storm center
    """
    import math

    # Try to read the cone from NHC shapefile downloaded by storm detector
    gis_data = event.get("gis_data", {})
    cone_dir = gis_data.get("cone_shapefile_dir", "")

    if cone_dir and os.path.isdir(cone_dir):
        try:
            from storm_detector.gis_downloader import extract_cone_geometry

            cone_geom = extract_cone_geometry(cone_dir)
            if cone_geom and cone_geom.get("coordinates"):
                logger.info("Using NHC forecast cone shapefile for storm extent")

                # Buffer the cone by config.cone_buffer_km for processing margin
                try:
                    from shapely.geometry import shape
                    from shapely.ops import transform
                    import pyproj

                    cone_shape = shape(cone_geom)
                    # Project to meters (EPSG:5070 CONUS Albers) for buffering
                    to_albers = pyproj.Transformer.from_crs(
                        "EPSG:4326", "EPSG:5070", always_xy=True
                    ).transform
                    to_wgs84 = pyproj.Transformer.from_crs(
                        "EPSG:5070", "EPSG:4326", always_xy=True
                    ).transform

                    projected = transform(to_albers, cone_shape)
                    buffered = projected.buffer(config.cone_buffer_km * 1000)
                    result = transform(to_wgs84, buffered)

                    logger.info(
                        f"Buffered cone by {config.cone_buffer_km}km: "
                        f"area={result.area:.4f} sq deg"
                    )
                    return json.loads(json.dumps(result.__geo_interface__))

                except ImportError:
                    # No shapely — return raw cone without buffer
                    logger.warning("shapely not available — using unbuffered cone")
                    return cone_geom

        except Exception as e:
            logger.warning(f"Failed to read cone shapefile: {e}")

    # Fallback: circular buffer around storm center
    center = event.get("center", {})
    lon = center.get("lon", -85.0)
    lat = center.get("lat", 26.0)

    logger.info(
        f"Using circular buffer fallback for storm extent "
        f"(center={lon:.1f},{lat:.1f})"
    )

    radius_deg = config.cone_buffer_km / 111.0 * 3  # ~3x buffer for cone

    points = []
    for i in range(33):
        angle = (i / 32) * 2 * math.pi
        px = lon + radius_deg * math.cos(angle) / math.cos(math.radians(lat))
        py = lat + radius_deg * math.sin(angle)
        points.append([px, py])
    points.append(points[0])  # close ring

    return {
        "type": "Polygon",
        "coordinates": [points],
    }


def _generate_rainfall_depth(
    qpf_path: str,
    dem_path: str,
    output_dir: str,
    storm_id: str,
    advisory_num: str,
) -> str:
    """
    Convert QPF rainfall accumulation to flood depth.

    Simplified approach: rainfall excess / cell area = depth.
    In production, use the full HAND model with catchment routing.
    """
    import rasterio
    from rasterio.warp import reproject, Resampling
    import numpy as np

    output_path = os.path.join(
        output_dir, f"depth_rainfall_{storm_id}_{advisory_num}.tif"
    )

    with rasterio.open(dem_path) as dem_src:
        dem = dem_src.read(1)
        profile = dem_src.profile.copy()
        dem_nodata = dem_src.nodata or -9999

    with rasterio.open(qpf_path) as qpf_src:
        # Resample QPF to DEM grid
        qpf = np.empty_like(dem)
        reproject(
            source=rasterio.band(qpf_src, 1),
            destination=qpf,
            src_transform=qpf_src.transform,
            src_crs=qpf_src.crs,
            dst_transform=dem_src.transform,
            dst_crs=dem_src.crs,
            resampling=Resampling.bilinear,
            dst_nodata=-9999,
        )

    # Simplified: rainfall depth = excess_mm / 1000 (convert mm to m)
    # with a runoff coefficient of ~0.4 for mixed land use
    valid = (dem != dem_nodata) & (qpf != -9999)
    runoff_coeff = 0.4
    depth = np.full_like(dem, -9999, dtype=np.float32)
    depth[valid] = (qpf[valid] * runoff_coeff) / 1000.0

    # Only keep cells where water would accumulate (lower elevations)
    # Simple: scale depth by inverse relative elevation
    if np.any(valid):
        elev_min = np.min(dem[valid])
        elev_range = np.max(dem[valid]) - elev_min
        if elev_range > 0:
            elev_factor = 1.0 - (dem[valid] - elev_min) / elev_range
            depth[valid] *= elev_factor * 3  # amplify low areas

    depth[valid & (depth < 0.05)] = 0

    profile.update(dtype="float32", nodata=-9999, compress="deflate")
    with rasterio.open(output_path, "w", **profile) as dst:
        dst.write(depth, 1)

    return output_path


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Lambda Entry Points
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# Each stage is a separate Lambda handler so Step Functions can
# invoke them independently with proper error handling and retries.

def lambda_ingest(event, context):
    """Lambda handler for the INGEST stage."""
    return handle_ingest(event, context)

def lambda_model(event, context):
    """Lambda handler for the MODEL stage."""
    return handle_model(event, context)

def lambda_tilegen(event, context):
    """Lambda handler for the TILEGEN stage."""
    return handle_tilegen(event, context)

def lambda_publish(event, context):
    """Lambda handler for the PUBLISH stage."""
    return handle_publish(event, context)
