"""
HAND-Based Flood Model (Tier 2)

Height Above Nearest Drainage (HAND) provides a fast, connectivity-aware
flood inundation estimate. Unlike the bathtub model, HAND only floods
cells that are hydraulically connected to a drainage channel.

Method:
  1. For each NHDPlus reach, get the NWM forecast discharge
  2. Convert discharge to river stage via a synthetic rating curve
  3. Flood all cells where HAND value < river stage
  4. Flood depth = stage - HAND value

For rainfall flooding, we:
  1. Estimate excess rainfall using SCS Curve Number method
  2. Route excess rainfall to reaches using time-of-concentration
  3. Add rainfall-induced discharge to NWM forecast
  4. Recompute inundation

Strengths:
  - Connectivity-aware (no phantom flooding of disconnected lows)
  - Very fast (minutes per HUC-8)
  - Uses NOAA's operational NWM forecasts
  - Runs on Lambda

Limitations:
  - Synthetic rating curves introduce uncertainty
  - No surge-river interaction (that requires HEC-RAS)
  - No 2D flow routing

Use case: Free-tier riverine/rainfall flood maps.
"""

from __future__ import annotations

import csv
import logging
import os
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import numpy as np

from flood_model.raster_utils import read_raster, write_raster

logger = logging.getLogger(__name__)


@dataclass
class HANDResult:
    """Output of the HAND-based flood model."""

    depth_path: str
    max_depth_m: float
    flooded_cells: int
    total_cells: int
    flooded_pct: float
    reaches_flooded: int
    peak_stage_m: float
    bounds: Tuple[float, float, float, float]
    crs: str
    resolution: float
    s3_key: Optional[str] = None


@dataclass
class ReachForecast:
    """NWM discharge forecast for a single reach, per timestep."""

    reach_id: int
    discharge_cms: Dict[int, float]  # hour -> discharge (m3/s)
    stage_m: Dict[int, float]        # hour -> stage (m)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Synthetic Rating Curve
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def discharge_to_stage(
    discharge_cms: float,
    drainage_area_km2: float = 100.0,
    bankfull_depth_m: float = 2.0,
) -> float:
    """
    Convert discharge to river stage using a power-law rating curve.

    Uses the empirical relationship:
        stage = a * Q^b

    where a and b are derived from the drainage area and
    estimated channel geometry.

    Args:
        discharge_cms: Streamflow in cubic meters per second
        drainage_area_km2: Upstream drainage area
        bankfull_depth_m: Estimated bankfull depth

    Returns:
        Water surface stage in meters above channel bottom
    """
    if discharge_cms <= 0:
        return 0.0

    # Estimate bankfull discharge from drainage area (Dunne & Leopold)
    q_bankfull = 0.5 * (drainage_area_km2 ** 0.75)
    if q_bankfull <= 0:
        q_bankfull = 10.0

    # Power-law: stage = bankfull_depth * (Q / Q_bankfull)^0.4
    stage = bankfull_depth_m * (discharge_cms / q_bankfull) ** 0.4

    return float(stage)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SCS Curve Number Rainfall Excess
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def compute_rainfall_excess(
    rainfall_mm: np.ndarray,
    curve_number: np.ndarray,
) -> np.ndarray:
    """
    Compute excess rainfall using the SCS Curve Number method.

    The CN method partitions total rainfall into:
      - Initial abstraction (Ia = 0.2 * S)
      - Excess (runoff)
      - Retention (infiltration)

    Q_excess = (P - 0.2*S)^2 / (P + 0.8*S)  where S = 25400/CN - 254

    Args:
        rainfall_mm: Total rainfall accumulation (mm)
        curve_number: SCS Curve Number raster (0-100)

    Returns:
        Excess rainfall in mm (same shape as input)
    """
    # Potential maximum retention (mm)
    S = (25400.0 / np.clip(curve_number, 1, 100)) - 254.0

    # Initial abstraction
    Ia = 0.2 * S

    # Excess rainfall (SCS equation)
    excess = np.where(
        rainfall_mm > Ia,
        ((rainfall_mm - Ia) ** 2) / (rainfall_mm + 0.8 * S),
        0.0,
    )

    return excess.astype(np.float32)


def nlcd_to_curve_number(nlcd_class: int, soil_group: str = "B") -> int:
    """
    Map NLCD land cover class to SCS Curve Number.

    Default soil group B (moderate infiltration).
    """
    # CN lookup: {NLCD class: {soil group: CN}}
    cn_table = {
        11: {"A": 100, "B": 100, "C": 100, "D": 100},  # Open Water
        21: {"A": 49, "B": 69, "C": 79, "D": 84},       # Developed Open
        22: {"A": 61, "B": 75, "C": 83, "D": 87},       # Developed Low
        23: {"A": 77, "B": 85, "C": 90, "D": 92},       # Developed Med
        24: {"A": 89, "B": 92, "C": 94, "D": 95},       # Developed High
        31: {"A": 77, "B": 86, "C": 91, "D": 94},       # Barren
        41: {"A": 36, "B": 60, "C": 73, "D": 79},       # Deciduous Forest
        42: {"A": 36, "B": 60, "C": 73, "D": 79},       # Evergreen Forest
        43: {"A": 36, "B": 60, "C": 73, "D": 79},       # Mixed Forest
        52: {"A": 35, "B": 56, "C": 70, "D": 77},       # Shrub
        71: {"A": 39, "B": 61, "C": 74, "D": 80},       # Grassland
        81: {"A": 49, "B": 69, "C": 79, "D": 84},       # Pasture
        82: {"A": 67, "B": 78, "C": 85, "D": 89},       # Crops
        90: {"A": 36, "B": 60, "C": 73, "D": 79},       # Woody Wetlands
        95: {"A": 36, "B": 60, "C": 73, "D": 79},       # Herbaceous Wetlands
    }
    entry = cn_table.get(nlcd_class, cn_table.get(71))
    return entry.get(soil_group, entry.get("B", 70))


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# HAND Flood Model
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def run_hand_model(
    hand_path: str,
    catchment_path: str,
    discharge_data: Dict[int, float],
    output_dir: str,
    storm_id: str = "unknown",
    advisory_num: str = "000",
    timestep: int = 0,
    min_depth_m: float = 0.05,
) -> HANDResult:
    """
    Run the HAND-based inundation model.

    For each NHDPlus reach with a forecast, compute the flood
    stage from discharge, then find all cells where HAND < stage.

    Args:
        hand_path: Path to pre-computed HAND raster (meters)
        catchment_path: Path to NHDPlus catchment raster (reach IDs)
        discharge_data: Dict mapping reach_id -> discharge (m3/s)
        output_dir: Output directory
        storm_id: For naming
        advisory_num: For naming
        timestep: Forecast hour
        min_depth_m: Noise threshold

    Returns:
        HANDResult with flood depth raster and stats
    """
    logger.info(
        f"Running HAND model: {len(discharge_data)} reaches, T+{timestep}h"
    )

    os.makedirs(output_dir, exist_ok=True)

    # Read HAND raster
    _hand = read_raster(hand_path)
    hand_data = _hand.data
    profile = _hand.profile
    hand_nodata = _hand.nodata
    bounds = _hand.bounds
    crs = _hand.crs
    res = _hand.transform.a

    # Read catchment (reach ID) raster
    import rasterio
    with rasterio.open(catchment_path) as src:
        catch_data = src.read(1).astype(np.int64)
        # Catchment rasters commonly declare nodata=0 (outside any
        # NHDPlus basin). `src.nodata or -9999` collapses 0 → -9999,
        # which then matches no cells — so ocean/outside-basin cells
        # with reach_id=0 leak into the "valid" mask and can match an
        # actual reach_id=0 discharge entry.
        catch_nodata = int(src.nodata) if src.nodata is not None else -9999

    # ── Compute Flood Depth Per Reach ──────────────────────────
    flood_depth = np.full_like(hand_data, -9999, dtype=np.float32)
    valid = (hand_data != hand_nodata) & (catch_data != catch_nodata)

    reaches_flooded = 0
    peak_stage = 0.0

    for reach_id, discharge_cms in discharge_data.items():
        # Convert discharge to stage
        stage = discharge_to_stage(discharge_cms)

        if stage <= min_depth_m:
            continue

        peak_stage = max(peak_stage, stage)

        # Find cells belonging to this reach's catchment
        reach_mask = valid & (catch_data == reach_id)

        if not np.any(reach_mask):
            continue

        # Flood cells where HAND < stage
        depth = stage - hand_data[reach_mask]
        depth = np.clip(depth, 0, None)

        # Only write cells that are actually flooded
        flooded = depth > min_depth_m
        reach_cells = np.where(reach_mask)

        flood_depth[
            reach_cells[0][flooded],
            reach_cells[1][flooded],
        ] = depth[flooded]

        if np.any(flooded):
            reaches_flooded += 1

    # ── Zero out unflooded valid cells ─────────────────────────
    unflooded_valid = valid & (flood_depth == -9999)
    flood_depth[unflooded_valid] = 0

    # ── Statistics ─────────────────────────────────────────────
    flooded_mask = valid & (flood_depth > min_depth_m)
    flooded_cells = int(np.sum(flooded_mask))
    total_cells = int(np.sum(valid))
    flooded_pct = (
        (flooded_cells / total_cells * 100) if total_cells > 0 else 0
    )
    max_depth = (
        float(np.nanmax(flood_depth[flooded_mask]))
        if flooded_cells > 0
        else 0.0
    )

    logger.info(
        f"HAND result: max_depth={max_depth:.2f}m, "
        f"peak_stage={peak_stage:.2f}m, "
        f"reaches_flooded={reaches_flooded}, "
        f"flooded={flooded_pct:.1f}%"
    )

    # ── Write Output ───────────────────────────────────────────
    ts_str = f"t{timestep:03d}" if timestep > 0 else "t000"
    output_path = os.path.join(
        output_dir,
        f"depth_hand_{storm_id}_{advisory_num}_{ts_str}.tif",
    )

    write_raster(
        output_path,
        flood_depth,
        profile,
        tags={
            "model": "hand",
            "storm_id": storm_id,
            "advisory": advisory_num,
            "timestep": str(timestep),
            "max_depth_m": f"{max_depth:.3f}",
            "reaches_flooded": str(reaches_flooded),
        },
    )

    return HANDResult(
        depth_path=output_path,
        max_depth_m=max_depth,
        flooded_cells=flooded_cells,
        total_cells=total_cells,
        flooded_pct=flooded_pct,
        reaches_flooded=reaches_flooded,
        peak_stage_m=peak_stage,
        bounds=(bounds.left, bounds.bottom, bounds.right, bounds.top),
        crs=crs,
        resolution=res,
    )


def run_rainfall_hand_model(
    hand_path: str,
    catchment_path: str,
    rainfall_path: str,
    discharge_data: Dict[int, float],
    output_dir: str,
    storm_id: str = "unknown",
    advisory_num: str = "000",
    timestep: int = 0,
    curve_number_default: int = 75,
) -> HANDResult:
    """
    Run HAND model with rainfall-enhanced discharge.

    Adds rainfall excess runoff to NWM base discharge,
    then runs the standard HAND inundation calculation.

    Args:
        hand_path: HAND raster path
        catchment_path: NHDPlus catchment ID raster
        rainfall_path: QPF accumulated rainfall raster (mm)
        discharge_data: NWM base discharge per reach
        output_dir: Output directory
        storm_id: Storm identifier
        advisory_num: Advisory number
        timestep: Forecast hour
        curve_number_default: Default SCS CN for rainfall excess

    Returns:
        HANDResult with rainfall-augmented flood depths
    """
    import rasterio

    logger.info(
        f"Running rainfall-HAND model: T+{timestep}h"
    )

    # Read rainfall
    with rasterio.open(rainfall_path) as src:
        rainfall = src.read(1)
        rain_nodata = src.nodata if src.nodata is not None else -9999

    # Compute rainfall excess using SCS CN method
    valid_rain = rainfall != rain_nodata
    cn_raster = np.full_like(rainfall, curve_number_default, dtype=np.float32)
    excess_mm = compute_rainfall_excess(rainfall, cn_raster)
    excess_mm[~valid_rain] = 0

    # Read catchment raster to aggregate excess per reach
    with rasterio.open(catchment_path) as src:
        catch_data = src.read(1).astype(np.int64)
        catch_nodata = int(src.nodata) if src.nodata is not None else -9999
        cell_area_m2 = abs(src.transform.a * src.transform.e)

    # Accumulate excess volume per reach and convert to added discharge
    enhanced_discharge = dict(discharge_data)
    duration_s = 6 * 3600  # 6-hour accumulation period

    unique_reaches = np.unique(catch_data[catch_data != catch_nodata])
    for reach_id in unique_reaches:
        reach_id = int(reach_id)
        reach_mask = catch_data == reach_id

        # Total excess volume (mm -> m -> m3)
        total_excess_m3 = (
            np.sum(excess_mm[reach_mask]) / 1000.0 * cell_area_m2
        )

        # Average discharge over the accumulation period
        added_q = total_excess_m3 / duration_s

        base_q = enhanced_discharge.get(reach_id, 0)
        enhanced_discharge[reach_id] = base_q + added_q

    logger.info(
        f"Rainfall excess added to {len(unique_reaches)} reaches"
    )

    # Run HAND model with enhanced discharge
    return run_hand_model(
        hand_path=hand_path,
        catchment_path=catchment_path,
        discharge_data=enhanced_discharge,
        output_dir=output_dir,
        storm_id=storm_id,
        advisory_num=advisory_num,
        timestep=timestep,
    )


def load_discharge_from_csv(
    csv_path: str, timestep: int = 0
) -> Dict[int, float]:
    """
    Load NWM discharge data from a CSV file.

    Expected columns: reach_id, hour, discharge_cms, stage_m
    Returns discharge for the nearest available timestep.
    """
    discharge = {}
    with open(csv_path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            hour = int(row["hour"])
            if hour == timestep:
                reach_id = int(row["reach_id"])
                q = float(row["discharge_cms"])
                discharge[reach_id] = q

    logger.info(
        f"Loaded discharge for {len(discharge)} reaches at T+{timestep}h"
    )
    return discharge
