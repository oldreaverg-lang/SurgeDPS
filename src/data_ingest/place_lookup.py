"""
Point-in-polygon lookup against Census TIGER/Line Place boundaries.

Provides a single function — `lookup_place(lat, lon)` — that returns the
(geoid, name, state) of the city / CDP whose boundary contains the point,
or `None` if the point is outside every loaded Place polygon.

Source data: scripts/build_tiger_place_index.py downloads + simplifies
TIGER 2024 PLACE shapefiles, emits one pickle per state under
data/places/places_<state_fips>.pkl. This module loads every pickle in
that directory at import and builds a single shapely STRtree spanning
all states. Pickles are bundled into the Docker image; no runtime
downloads.

Usage:
    from data_ingest.place_lookup import lookup_place
    hit = lookup_place(29.85, -89.94)
    # hit == {"geoid": "2204480", "name": "Belle Chasse", "state": "LA",
    #          "classfp": "U6"} or None
"""

from __future__ import annotations

import logging
import os
import pickle
from threading import Lock
from typing import Optional

from shapely.geometry import Point, shape
from shapely.strtree import STRtree

logger = logging.getLogger(__name__)

# Resolve data dir relative to repo root so the module works whether the
# server is launched from scripts/ or from the repo root.
_BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
_PLACES_DIR = os.path.join(_BASE_DIR, "data", "places")

# Lazy-loaded index — populated on first lookup, then reused for the
# lifetime of the process.
_INDEX_LOCK = Lock()
_TREE: Optional[STRtree] = None
_POLYGONS: list = []   # parallel to _RECORDS, indexed by STRtree result
_RECORDS: list[dict] = []  # {geoid, name, state, classfp}


def _ensure_loaded() -> None:
    """Build the spatial index on first use. Called by lookup_place()."""
    global _TREE, _POLYGONS, _RECORDS
    if _TREE is not None:
        return
    with _INDEX_LOCK:
        if _TREE is not None:
            return  # another thread won the race
        polys: list = []
        recs: list[dict] = []
        if not os.path.isdir(_PLACES_DIR):
            logger.warning("place_lookup: %s not found — no city limits loaded; "
                           "all lookups will return None.", _PLACES_DIR)
            _TREE = STRtree([])
            _POLYGONS = []
            _RECORDS = []
            return
        for fn in sorted(os.listdir(_PLACES_DIR)):
            if not fn.endswith(".pkl"):
                continue
            try:
                with open(os.path.join(_PLACES_DIR, fn), "rb") as f:
                    data = pickle.load(f)
            except Exception as e:
                logger.warning("place_lookup: skipping %s: %s", fn, e)
                continue
            state_abbr = data.get("state_abbr", "??")
            for r in data.get("records", []):
                try:
                    poly = shape(r["geometry"])
                except Exception:
                    continue
                if poly.is_empty:
                    continue
                polys.append(poly)
                recs.append({
                    "geoid": r["geoid"],
                    "name": r["name"],
                    "state": r.get("state", state_abbr),
                    "classfp": r.get("classfp", ""),
                })
        _TREE = STRtree(polys)
        _POLYGONS = polys
        _RECORDS = recs
        logger.info("place_lookup: loaded %d Place polygons across "
                    "%d state pickle(s) from %s",
                    len(polys), len([f for f in os.listdir(_PLACES_DIR)
                                     if f.endswith(".pkl")]), _PLACES_DIR)


def lookup_place(lat: float, lon: float) -> Optional[dict]:
    """Return the Place record containing (lat, lon), or None.

    For points outside every loaded polygon (oceans, unincorporated
    rural areas not covered by a CDP), returns None — callers should
    fall back to county-level or "Unincorporated" bucketing.

    Thread-safe. First call triggers index build (slow); subsequent
    calls are O(log n) tree query + O(k) exact tests on the typically
    1–3 candidate polygons that overlap the query bbox.
    """
    _ensure_loaded()
    if not _RECORDS:
        return None
    pt = Point(lon, lat)
    # STRtree.query() returns the integer indices of polygons whose
    # bounding boxes intersect the query geometry's bbox (which for a
    # Point is just the point). For each candidate we do the exact
    # contains() check.
    candidate_idxs = _TREE.query(pt)
    for i in candidate_idxs:
        if _POLYGONS[i].contains(pt):
            return _RECORDS[i]
    return None


def stats() -> dict:
    """Diagnostic — total polygons loaded and per-state counts."""
    _ensure_loaded()
    per_state: dict[str, int] = {}
    for r in _RECORDS:
        per_state[r["state"]] = per_state.get(r["state"], 0) + 1
    return {
        "total_places": len(_RECORDS),
        "per_state": per_state,
        "data_dir": _PLACES_DIR,
    }
