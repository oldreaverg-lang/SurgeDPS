"""
Model Run Ledger

Captures the outputs of every storm activation so they can be compared
against ground truth.  Each run records the key model outputs:

  - modeled_loss:    Total loss from HAZUS depth-damage model ($)
  - building_count:  Number of buildings assessed
  - nsi_pct:         Fraction of buildings sourced from NSI (vs OSM)
  - avg_data_quality: Mean data quality score across all buildings
  - eli:             Expected Loss Index
  - validated_dps:   Adjusted DPS score
  - cells_loaded:    Number of grid cells included in the run

The ledger is persisted to the Railway volume so it accumulates
across deploys.  Each storm can have multiple runs (e.g., as the
model improves); the ledger tracks them all with timestamps.
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass, asdict, field
from typing import Dict, List, Optional

# Persistent storage on Railway volume
_BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
_PERSISTENT_DIR = os.environ.get('PERSISTENT_DATA_DIR', os.path.join(_BASE_DIR, 'tmp_integration'))
_LEDGER_DIR = os.path.join(_PERSISTENT_DIR, 'validation')
_LEDGER_PATH = os.path.join(_LEDGER_DIR, 'run_ledger.json')
os.makedirs(_LEDGER_DIR, exist_ok=True)


@dataclass
class ModelRun:
    """A single model activation run for one storm."""

    storm_id: str
    timestamp: float               # Unix timestamp of the run
    modeled_loss: float             # Total modeled loss in USD
    building_count: int             # Total buildings assessed
    buildings_damaged: int          # Buildings with damage > 0%
    cells_loaded: int               # Number of grid cells loaded
    nsi_count: int = 0              # Buildings from NSI source
    osm_count: int = 0              # Buildings from OSM source
    avg_data_quality: float = 0.0   # Mean data_quality across buildings
    eli: float = 0.0                # Expected Loss Index
    validated_dps: float = 0.0      # Adjusted DPS score
    dps_score: float = 0.0          # Raw DPS score
    max_depth_m: float = 0.0        # Maximum surge depth in any cell
    avg_damage_pct: float = 0.0     # Average damage % across damaged buildings
    population: Optional[int] = None  # Census county population

    # Forecast track metadata (for advisory-by-advisory timeline)
    advisory_num: Optional[str] = None     # NHC advisory number
    hours_to_landfall: Optional[int] = None  # Forecast hours to landfall
    forecast_lat: Optional[float] = None   # Predicted landfall latitude
    forecast_lon: Optional[float] = None   # Predicted landfall longitude
    used_forecast: bool = False            # True if ran at forecast point vs current

    # Model version tag — bump when the damage model changes
    model_version: str = "1.0"

    def to_dict(self) -> dict:
        return asdict(self)


def _load_ledger() -> List[dict]:
    """Load the full ledger from disk."""
    if os.path.exists(_LEDGER_PATH):
        try:
            with open(_LEDGER_PATH) as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return []
    return []


def _save_ledger(entries: List[dict]):
    """Write the full ledger to disk."""
    with open(_LEDGER_PATH, 'w') as f:
        json.dump(entries, f, indent=2)


def record_run(run: ModelRun):
    """Append a model run to the persistent ledger."""
    entries = _load_ledger()
    entries.append(run.to_dict())
    _save_ledger(entries)


def get_runs(storm_id: Optional[str] = None) -> List[dict]:
    """
    Get all runs, optionally filtered to a specific storm.
    Returns most recent first.
    """
    entries = _load_ledger()
    if storm_id:
        entries = [e for e in entries if e.get('storm_id') == storm_id]
    return sorted(entries, key=lambda e: e.get('timestamp', 0), reverse=True)


def get_latest_run(storm_id: str) -> Optional[dict]:
    """Get the most recent run for a storm, or None."""
    runs = get_runs(storm_id)
    return runs[0] if runs else None


def record_from_activation(
    storm_id: str,
    grid_cells: dict,
    storm_data: dict,
) -> ModelRun:
    """
    Build and record a ModelRun from an activation response.

    This is the main integration point — called from api_server.py
    after all cells are loaded.

    Args:
        storm_id: Storm identifier
        grid_cells: Dict of cell_key → {"buildings": FC, "flood": FC}
        storm_data: Storm metadata dict (includes eli, validated_dps, etc.)

    Returns:
        The recorded ModelRun
    """
    total_loss = 0.0
    total_buildings = 0
    total_damaged = 0
    nsi_count = 0
    osm_count = 0
    dq_sum = 0.0
    max_depth = 0.0
    damage_pcts = []

    for cell_key, cell_data in grid_cells.items():
        buildings_fc = cell_data.get("buildings", {})
        features = buildings_fc.get("features", [])

        for feat in features:
            props = feat.get("properties", {})
            total_buildings += 1

            loss = props.get("estimated_loss_usd", 0) or 0
            total_loss += loss

            dmg_pct = props.get("total_damage_pct", 0) or 0
            if dmg_pct > 0:
                total_damaged += 1
                damage_pcts.append(dmg_pct)

            depth = props.get("depth_m", 0) or 0
            if depth > max_depth:
                max_depth = depth

            source = props.get("source", "")
            if source == "NSI":
                nsi_count += 1
            elif source == "OSM":
                osm_count += 1

            dq_sum += props.get("data_quality", 0.3)

    avg_dq = dq_sum / total_buildings if total_buildings > 0 else 0
    avg_dmg = sum(damage_pcts) / len(damage_pcts) if damage_pcts else 0

    pop_data = storm_data.get("population", {})

    run = ModelRun(
        storm_id=storm_id,
        timestamp=time.time(),
        modeled_loss=round(total_loss, 2),
        building_count=total_buildings,
        buildings_damaged=total_damaged,
        cells_loaded=len(grid_cells),
        nsi_count=nsi_count,
        osm_count=osm_count,
        avg_data_quality=round(avg_dq, 3),
        eli=storm_data.get("eli", 0),
        validated_dps=storm_data.get("validated_dps", 0),
        dps_score=storm_data.get("dps_score", 0),
        max_depth_m=round(max_depth, 2),
        avg_damage_pct=round(avg_dmg, 1),
        population=pop_data.get("population") if pop_data else None,
    )

    record_run(run)
    return run
