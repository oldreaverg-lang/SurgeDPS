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

import requests

logger = logging.getLogger(__name__)

NSI_ENDPOINT = "https://nsi.sec.usace.army.mil/nsiapi/structures"
NSI_TIMEOUT = 30  # seconds

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
            logger.info(f"[NSI] Cache hit: {n} structures from {output_path}")
            print(f"  [NSI cache hit] {n} structures loaded from {output_path}")
            return output_path

    # NSI bbox = closed polygon of lon/lat pairs (counterclockwise rectangle)
    bbox = (
        f"{lon_min},{lat_min},"
        f"{lon_max},{lat_min},"
        f"{lon_max},{lat_max},"
        f"{lon_min},{lat_max},"
        f"{lon_min},{lat_min}"
    )

    print(f"  [NSI] Querying structures in "
          f"[{lon_min:.3f},{lat_min:.3f} → {lon_max:.3f},{lat_max:.3f}] ...")

    try:
        resp = requests.get(
            NSI_ENDPOINT,
            params={"bbox": bbox},
            timeout=NSI_TIMEOUT,
            headers={"Accept": "application/json",
                     "User-Agent": "SurgeDPS/1.0 (flood-damage-model)"},
        )
        resp.raise_for_status()
        raw = resp.json()
    except Exception as exc:
        logger.warning(f"[NSI] API request failed: {exc}")
        print(f"  [NSI] Request failed ({exc.__class__.__name__}), will fall back to OSM")
        return None

    nsi_features = raw.get("features", [])
    if not nsi_features:
        logger.info("[NSI] API returned 0 features for this bbox")
        print("  [NSI] No structures returned for this area, will fall back to OSM")
        return None

    print(f"  [NSI] {len(nsi_features)} structures received")

    # Convert NSI features to our GeoJSON schema
    out_features = []
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
                continue
        else:
            lon, lat = coords[0], coords[1]

        num_story = int(props.get("num_story") or 1)
        found_type = str(props.get("found_type") or "")
        occtype = str(props.get("occtype") or "RES1")
        hazus_code = _nsi_to_hazus(occtype, num_story, found_type)

        val_struct = props.get("val_struct")
        val_cont   = props.get("val_cont")
        sqft       = props.get("sqft")
        found_ht   = props.get("found_ht")
        med_yr_blt = props.get("med_yr_blt")

        out_props: Dict = {
            "id":       str(props.get("fd_id", f"nsi_{len(out_features)}")),
            "type":     hazus_code,
            "occtype":  occtype,
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

    if not out_features:
        print("  [NSI] No valid features after conversion, falling back to OSM")
        return None

    geojson = {"type": "FeatureCollection", "features": out_features}
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(geojson, f)

    # Quick value stats
    structs = [f["properties"].get("val_struct", 0) for f in out_features if f["properties"].get("val_struct")]
    if structs:
        avg = sum(structs) / len(structs)
        print(f"  [NSI] Wrote {len(out_features)} structures; "
              f"avg val_struct=${avg:,.0f} "
              f"(min=${min(structs):,.0f} max=${max(structs):,.0f})")
    else:
        print(f"  [NSI] Wrote {len(out_features)} structures to {output_path}")

    return output_path
