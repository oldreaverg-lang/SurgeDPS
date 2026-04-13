"""
Centralized persistent storage manager for SurgeDPS.

Every module that reads/writes to the Railway persistent volume should
import paths from here instead of constructing its own.  This gives us:

  1. One place to change the base directory
  2. Automatic directory creation on import
  3. Disk-usage helpers for the /health endpoint
  4. Cache eviction utilities

Environment variable:
    PERSISTENT_DATA_DIR  — absolute path to the Railway volume mount.
                           Falls back to <repo_root>/tmp_integration for
                           local dev (backward-compatible).

Directory layout on the persistent volume:

    $PERSISTENT_DATA_DIR/
    ├── cells/                     # Per-storm grid cell data
    │   └── <storm_id>/
    │       ├── cell_C_R_depth.tif     # Surge depth raster (GeoTIFF)
    │       ├── cell_C_R_flood.geojson # Flood polygons
    │       ├── cell_C_R_damage.geojson# Building damage estimates
    │       └── building_index.json    # Quick cell→building_count lookup
    ├── validation/
    │   └── run_ledger.json            # Model activation history
    ├── census/                        # Cached county population data
    ├── forecasts/                     # NHC forecast track JSON cache
    ├── geocode/                       # Reverse geocoding cache
    └── monitor_state.json             # Storm monitor poll state
"""

from __future__ import annotations

import logging
import os
import shutil
from pathlib import Path

logger = logging.getLogger(__name__)

# ── Base directory ──────────────────────────────────────────────────────────
# Railway volume convention: both SurgeDPS and StormDPS mount their
# persistent volume at /app/persistent. Set this in Railway env vars:
#
#   PERSISTENT_DATA_DIR = /app/persistent
#
# The fallback (tmp_integration) is used for local dev only.
_BASE_DIR = Path(__file__).resolve().parent.parent  # repo root
_FALLBACK = str(_BASE_DIR / "tmp_integration")
PERSISTENT_DATA_DIR = Path(os.environ.get("PERSISTENT_DATA_DIR", _FALLBACK))

# ── Subdirectory definitions ────────────────────────────────────────────────
CELLS_DIR      = PERSISTENT_DATA_DIR / "cells"
VALIDATION_DIR = PERSISTENT_DATA_DIR / "validation"
CENSUS_DIR     = PERSISTENT_DATA_DIR / "census"
FORECASTS_DIR  = PERSISTENT_DATA_DIR / "forecasts"
GEOCODE_DIR    = PERSISTENT_DATA_DIR / "geocode"
MRMS_DIR       = PERSISTENT_DATA_DIR / "mrms"       # MRMS QPE GeoTIFF cache
# ── HAND/NWM (fluvial layer) ─────────────────────────────────────────────────
# HAND rasters are downloaded once per HUC8 from NOAA OWP FIM and kept
# permanently — they don't change between storms.  NWM discharge is
# storm-specific and evicted with the cell cache.
#
#   hand_fim/
#     {huc8}/
#       hand_{huc8}.tif          ← HAND raster (m above nearest drainage)
#       catchments_{huc8}.tif    ← NHDPlus catchment ID raster (reach → cell)
#   nwm/
#     {storm_id}/
#       discharge_{col}_{row}.json  ← reach_id → discharge_cms dict
HAND_DIR       = PERSISTENT_DATA_DIR / "hand_fim"
NWM_CACHE_DIR  = PERSISTENT_DATA_DIR / "nwm"
QPF_DIR        = PERSISTENT_DATA_DIR / "qpf"        # WPC QPF GeoTIFF + metadata cache
ATLAS14_DIR    = PERSISTENT_DATA_DIR / "atlas14"    # NOAA PFDS frequency tables (permanent)

MONITOR_STATE_FILE = PERSISTENT_DATA_DIR / "monitor_state.json"
LEDGER_FILE        = VALIDATION_DIR / "run_ledger.json"

# ── Create all directories on import ────────────────────────────────────────
for _d in (CELLS_DIR, VALIDATION_DIR, CENSUS_DIR, FORECASTS_DIR,
           GEOCODE_DIR, MRMS_DIR, HAND_DIR, NWM_CACHE_DIR, QPF_DIR, ATLAS14_DIR):
    _d.mkdir(parents=True, exist_ok=True)

logger.info("SurgeDPS storage root: %s", PERSISTENT_DATA_DIR)


# ── Storm cell helpers ──────────────────────────────────────────────────────

def storm_dir(storm_id: str) -> Path:
    """Return (and create) the per-storm cell cache directory."""
    d = CELLS_DIR / storm_id
    d.mkdir(parents=True, exist_ok=True)
    return d


def cell_path(storm_id: str, col: int, row: int, suffix: str) -> Path:
    """Return the path for a specific cell artifact.

    *suffix* is one of: ``depth.tif``, ``flood.geojson``,
    ``damage.geojson``, ``buildings.json``.
    """
    return storm_dir(storm_id) / f"cell_{col}_{row}_{suffix}"


def building_index_path(storm_id: str) -> Path:
    return storm_dir(storm_id) / "building_index.json"


# ── Disk usage helpers ──────────────────────────────────────────────────────

def _dir_size(path: Path) -> int:
    total = 0
    try:
        for f in path.rglob("*"):
            if f.is_file():
                total += f.stat().st_size
    except OSError:
        pass
    return total


def _file_count(path: Path) -> int:
    try:
        return sum(1 for f in path.rglob("*") if f.is_file())
    except OSError:
        return 0


def storage_summary() -> dict:
    """Return a JSON-serialisable summary of persistent storage usage."""
    sections = {
        "cells": CELLS_DIR,
        "validation": VALIDATION_DIR,
        "census": CENSUS_DIR,
        "forecasts": FORECASTS_DIR,
        "geocode": GEOCODE_DIR,
    }
    result: dict = {"root": str(PERSISTENT_DATA_DIR)}
    total_bytes = 0

    for key, path in sections.items():
        sz = _dir_size(path)
        total_bytes += sz
        result[key] = {
            "path": str(path),
            "size_mb": round(sz / 1_048_576, 2),
            "files": _file_count(path),
        }

    # Monitor state file
    if MONITOR_STATE_FILE.exists():
        sz = MONITOR_STATE_FILE.stat().st_size
        total_bytes += sz

    result["total_mb"] = round(total_bytes / 1_048_576, 2)

    # Count storms with cached cells
    try:
        storm_dirs = [d for d in CELLS_DIR.iterdir() if d.is_dir()]
        result["storms_cached"] = len(storm_dirs)
    except OSError:
        result["storms_cached"] = 0

    # Disk free
    try:
        usage = shutil.disk_usage(str(PERSISTENT_DATA_DIR))
        result["volume_total_mb"] = round(usage.total / 1_048_576, 2)
        result["volume_free_mb"] = round(usage.free / 1_048_576, 2)
        result["volume_used_pct"] = round(
            (usage.used / usage.total) * 100, 1
        )
    except OSError:
        pass

    return result


# ── Cell cache eviction ─────────────────────────────────────────────────────

CELLS_MAX_BYTES = 2 * 1_073_741_824  # 2 GB default


def evict_oldest_storms(max_bytes: int = CELLS_MAX_BYTES) -> int:
    """Remove oldest storm directories when the cell cache exceeds *max_bytes*.

    Evicts full storm directories (not individual cells) sorted by oldest
    modification time.  Returns number of storms removed.
    """
    if _dir_size(CELLS_DIR) <= max_bytes:
        return 0

    storm_dirs = sorted(
        [d for d in CELLS_DIR.iterdir() if d.is_dir()],
        key=lambda d: d.stat().st_mtime,
    )
    removed = 0
    for sd in storm_dirs:
        if _dir_size(CELLS_DIR) <= max_bytes * 0.75:  # shrink to 75%
            break
        try:
            shutil.rmtree(sd)
            removed += 1
            logger.info("Evicted cell cache for storm %s", sd.name)
        except OSError as e:
            logger.warning("Failed to evict %s: %s", sd.name, e)
    return removed
