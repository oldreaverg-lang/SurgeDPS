"""
FEMA National Structure Inventory (NSI) Fetcher

Queries the USACE NSI API for actual, tabulated building valuations.

Unlike generic HAZUS defaults (flat $/sqft × average sqft), the NSI contains
per-building structure and content replacement values derived from:
  - Census Bureau housing unit values
  - Dun & Bradstreet commercial establishment data
  - NFIP underwriting records
  - BEA regional price deflators

Each structure record includes:
  - val_struct: structure replacement cost in USD (the real number)
  - val_cont: contents replacement cost in USD
  - sqft: actual building footprint area
  - found_ht: first-floor elevation above grade in feet
  - num_story: number of stories
  - med_yr_blt: median year built
  - occtype: HAZUS-compatible occupancy type code

API: https://nsi.sec.usace.army.mil/nsiapi/structures
bbox: closed lon/lat polygon, e.g. "lon1,lat1,lon2,lat2,...,lon1,lat1"

Docs: https://www.hec.usace.army.mil/confluence/nsi/technicalreferences/latest/api-reference-guide
"""

from __future__ import annotations

import json
import logging
import os
from typing import Dict, Optional, Tuple

try:
    from data_ingest.duckdb_cache import building_cache as _building_cache
except ImportError:
    _building_cache = None  # graceful no-op if cache not available

import requests

logger = logging.getLogger(__name__)

NSI_ENDPOINT = "https://nsi.sec.usace.army.mil/nsiapi/structures"
NSI_TIMEOUT = 30  # seconds

# Reusable session for connection pooling (avoids per-request TCP handshake)
_nsi_session = requests.Session()
_nsi_session.headers.update({
    "Accept": "application/json",
    "User-Agent": "SurgeDPS/1.0 (flood-damage-model)",
})

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# NSI occtype → full HAZUS code
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# Foundation types that indicate a basement or crawlspace
_BASEMENT_FOUND_TYPES = {"basement", "crawl", "crawlspace", "crawl space"}

def _nsi_to_hazus(occtype: str, num_story: int, found_type: str) -> str:
    """
    Map NSI occupancy type + structural attributes to a full HAZUS code
    compatible with our depth-damage curves.
    """
    base = (occtype or "RES1").upper().split("-")[0]

    if base == "RES1":
        stories = "1S" if num_story <= 1 else "2S"
        has_basement = (found_type or "").lower().strip() in _BASEMENT_FOUND_TYPES
        basement = "WB" if has_basement else "NB"
        return f"RES1-{stories}{basement}"

    if base in ("RES2", "RES3", "RES4", "RES5", "RES6"):
        # Map other residential types to closest RES1 proxy
        stories = "1S" if num_story <= 1 else "2S"
        return f"RES1-{stories}NB"

    if base.startswith("COM"):
        return "COM"

    if base.startswith("IND") or base.startswith("AGR"):
        return "IND"

    # GOV, EDU, REL → COM proxy
    if base.startswith(("GOV", "EDU", "REL")):
        return "COM"

    return "RES1-1SNB"  # safe default


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# NSI fetcher
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def fetch_buildings_nsi(
    lon_min: float,
    lat_min: float,
    lon_max: float,
    lat_max: float,
    output_path: str,
    cache: bool = True,
) -> Optional[str]:
    """
    Fetch per-building valuations from the FEMA National Structure Inventory.

    Returns the path to a written GeoJSON file on success, or None if the
    NSI API is unavailable or returns no structures (caller should fall back
    to OSM).

    GeoJSON feature properties include:
        id          - NSI fd_id
        type        - HAZUS occupancy code (e.g. "RES1-1SNB")
        area_sqft   - actual building footprint in sqft
        val_struct  - structure replacement value (USD)
        val_cont    - contents replacement value (USD)
        found_ht    - first-floor elevation above grade (ft)
        num_story   - number of stories
        med_yr_blt  - median year built
        occtype     - raw NSI occupancy type
    """
    if cache and os.path.exists(output_path):
        with open(output_path) as f:
            data = json.load(f)
        n = len(data.get("features", []))
        # Check it's an NSI file (has val_struct)
        if n > 0 and "val_struct" in (data["features"][0].get("properties") or {}):
            logger.info("[NSI] Cache hit: %d structures from %s", n, output_path)
            return output_path

    # NSI bbox = closed polygon of lon/lat pairs (counterclockwise rectangle)
    bbox = (
        f"{lon_min},{lat_min},"
        f"{lon_max},{lat_min},"
        f"{lon_max},{lat_max},"
        f"{lon_min},{lat_max},"
        f"{lon_min},{lat_min}"
    )

    logger.info("[NSI] Querying structures in [%.3f,%.3f → %.3f,%.3f]",
                lon_min, lat_min, lon_max, lat_max)

    try:
        resp = _nsi_session.get(
            NSI_ENDPOINT,
            params={"bbox": bbox},
            timeout=NSI_TIMEOUT,
        )
        resp.raise_for_status()
        raw = resp.json()
    except Exception as exc:
        logger.warning("[NSI] API request failed: %s", exc)
        return None

    nsi_features = raw.get("features", [])
    if not nsi_features:
        logger.info("[NSI] API returned 0 features for this bbox")
        return None

    logger.info("[NSI] %d structures received", len(nsi_features))

    # Convert NSI features to our GeoJSON schema with validation
    out_features = []
    _skipped_invalid = 0
    _validation_notes: list = []

    for feat in nsi_features:
        props = feat.get("properties", {})
        geom = feat.get("geometry", {})

        # NSI geometry is a Point with [lon, lat]
        coords = geom.get("coordinates", [])
        if not coords or len(coords) < 2:
            # Fall back to x/y properties
            lon = props.get("x")
            lat = props.get("y")
            if lon is None or lat is None:
                _skipped_invalid += 1
                continue
        else:
            lon, lat = coords[0], coords[1]

        # ── Input validation ────────────────────────────────────────
        # Reject records with clearly invalid data before they reach
        # the damage model and produce garbage outputs.

        # Coords outside the requested bbox (+ small tolerance for edge cases)
        tol = 0.05
        if not (lon_min - tol <= lon <= lon_max + tol and
                lat_min - tol <= lat <= lat_max + tol):
            _skipped_invalid += 1
            continue

        val_struct = props.get("val_struct")
        val_cont   = props.get("val_cont")
        sqft       = props.get("sqft")
        found_ht   = props.get("found_ht")
        med_yr_blt = props.get("med_yr_blt")

        # val_struct = 0 or negative is a data entry error
        if val_struct is not None and float(val_struct) <= 0:
            _skipped_invalid += 1
            _validation_notes.append(f"val_struct<=0: {props.get('fd_id')}")
            continue

        # Negative sqft is nonsensical
        if sqft is not None and float(sqft) <= 0:
            _skipped_invalid += 1
            continue

        # found_ht > 30 ft is likely a sensor error (stilted V-zone buildings
        # rarely exceed 15 ft, most coastal properties are 1-4 ft)
        if found_ht is not None and float(found_ht) > 30:
            _validation_notes.append(
                f"found_ht={found_ht}ft (capped to 30): {props.get('fd_id')}"
            )
            found_ht = 30.0  # cap rather than skip — building is still real

        # med_yr_blt in the future or < 1700 is noise
        if med_yr_blt is not None:
            yr = int(med_yr_blt)
            if yr < 1700 or yr > 2030:
                med_yr_blt = None  # drop bad year rather than skip the building

        # ── End validation ──────────────────────────────────────────

        num_story = int(props.get("num_story") or 1)
        found_type = str(props.get("found_type") or "")
        occtype = str(props.get("occtype") or "RES1")
        hazus_code = _nsi_to_hazus(occtype, num_story, found_type)

        # ── Data quality score (0.0–1.0) ─────────────────────────
        # Based on attribute completeness.  Each key NSI field adds
        # weight; more attributes = more reliable damage estimate.
        # NSI baseline is 0.4 (having real val_struct alone is worth
        # more than OSM/MSFT which start at 0.1–0.2).
        _dq = 0.4  # base: we have an NSI record at all
        if val_struct is not None: _dq += 0.2
        if found_ht   is not None: _dq += 0.15
        if med_yr_blt is not None: _dq += 0.1
        if sqft       is not None: _dq += 0.1
        if val_cont   is not None: _dq += 0.05

        out_props: Dict = {
            "id":       str(props.get("fd_id", f"nsi_{len(out_features)}")),
            "type":     hazus_code,
            "occtype":  occtype,
            "source":   "NSI",
            "data_quality": round(min(_dq, 1.0), 2),
        }
        if sqft       is not None: out_props["area_sqft"]  = round(float(sqft), 1)
        if val_struct is not None: out_props["val_struct"]  = round(float(val_struct), 2)
        if val_cont   is not None: out_props["val_cont"]    = round(float(val_cont), 2)
        if found_ht   is not None: out_props["found_ht"]    = round(float(found_ht), 2)
        if med_yr_blt is not None: out_props["med_yr_blt"]  = int(med_yr_blt)

        out_features.append({
            "type": "Feature",
            "properties": out_props,
            "geometry": {"type": "Point", "coordinates": [lon, lat]},
        })

    if _skipped_invalid:
        logger.warning("[NSI] Skipped %d invalid records (of %d received)",
                       _skipped_invalid, len(nsi_features))
    if _validation_notes:
        logger.info("[NSI] Validation notes: %s", "; ".join(_validation_notes[:10]))

    if not out_features:
        logger.info("[NSI] No valid features after conversion, falling back to OSM")
        return None

    # Cache in DuckDB for fast in-session lookups (cell_key passed by caller via output_path)
    if _building_cache is not None:
        cell_key = os.path.splitext(os.path.basename(output_path))[0]  # e.g. "ian_2_3_buildings"
        _building_cache.store(cell_key, out_features)

    geojson = {"type": "FeatureCollection", "features": out_features}
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(geojson, f)

    # Quick value stats
    structs = [f["properties"].get("val_struct", 0) for f in out_features if f["properties"].get("val_struct")]
    if structs:
        avg = sum(structs) / len(structs)
        logger.info("[NSI] Wrote %d structures; avg val_struct=$%,.0f "
                    "(min=$%,.0f max=$%,.0f)",
                    len(out_features), avg, min(structs), max(structs))
    else:
        logger.info("[NSI] Wrote %d structures to %s", len(out_features), output_path)

    return output_path
