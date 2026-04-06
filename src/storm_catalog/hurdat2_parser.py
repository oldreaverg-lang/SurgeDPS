"""
HURDAT2 Parser

Parses the NHC HURDAT2 best-track database into StormEntry objects
for every Atlantic tropical cyclone from 1851 to present.

HURDAT2 format (per NHC documentation):
  Header line:  ATCF_ID, NAME, NUM_ENTRIES
  Data lines:   DATE, TIME, RECORD_ID, STATUS, LAT, LON, MAX_WIND, MIN_PRESSURE, ...

Record IDs: L = landfall, blank = routine, others = intensity change
Status codes: TD, TS, HU, EX, SS, SD, LO, WV, DB
"""

from __future__ import annotations

import math
import os
import re
from typing import Dict, List, Optional, Tuple

from .catalog import StormEntry, CELL_WIDTH, CELL_HEIGHT


def _parse_latlon(s: str) -> float:
    """Parse '28.0N' or '94.8W' into a signed float."""
    s = s.strip()
    m = re.match(r'([\d.]+)([NSEW])', s)
    if not m:
        return 0.0
    val = float(m.group(1))
    if m.group(2) in ('S', 'W'):
        val = -val
    return val


from common.saffir_simpson import wind_to_category as _saffir_simpson  # noqa: E402


def parse_hurdat2(filepath: str) -> List[StormEntry]:
    """
    Parse the full HURDAT2 file into a list of StormEntry objects.

    For each storm, determines:
      - Landfall point (from 'L' record) or peak intensity point
      - Peak wind and minimum pressure
      - Approximate heading and forward speed at the key point
      - Saffir-Simpson category from peak wind
    """
    storms: List[StormEntry] = []

    with open(filepath, 'r') as f:
        lines = f.readlines()

    i = 0
    while i < len(lines):
        line = lines[i].strip()
        if not line:
            i += 1
            continue

        # Header line: "AL011851,            UNNAMED,     14,"
        parts = line.split(',')
        if len(parts) < 3:
            i += 1
            continue

        atcf_id = parts[0].strip()
        # Check if this looks like an ATCF ID (2 letters + digits)
        if not re.match(r'^[A-Z]{2}\d+', atcf_id):
            i += 1
            continue

        name = parts[1].strip()
        try:
            num_entries = int(parts[2].strip())
        except ValueError:
            i += 1
            continue

        # Parse all data lines for this storm
        track_points = []
        landfall_points = []

        for j in range(i + 1, min(i + 1 + num_entries, len(lines))):
            dline = lines[j].strip()
            if not dline:
                continue
            dparts = dline.split(',')
            if len(dparts) < 8:
                continue

            date_str = dparts[0].strip()
            time_str = dparts[1].strip()
            record_id = dparts[2].strip()
            status = dparts[3].strip()
            lat = _parse_latlon(dparts[4].strip())
            lon = _parse_latlon(dparts[5].strip())

            try:
                wind = int(dparts[6].strip())
            except ValueError:
                wind = 0

            try:
                pres = int(dparts[7].strip())
            except ValueError:
                pres = -999

            if wind < 0:
                wind = 0
            if pres < 0 or pres > 1100:
                pres = 1013

            point = {
                'date': date_str, 'time': time_str,
                'record_id': record_id, 'status': status,
                'lat': lat, 'lon': lon,
                'wind': wind, 'pressure': pres,
            }
            track_points.append(point)

            if record_id == 'L':
                landfall_points.append(point)

        i += 1 + num_entries

        if not track_points:
            continue

        # ── Determine the key point: prefer US landfall, then any landfall, then peak ──
        peak_point = max(track_points, key=lambda p: p['wind'])

        # Filter for US-mainland landfalls (rough CONUS bbox)
        us_landfalls = [
            p for p in landfall_points
            if 24.0 <= p['lat'] <= 50.0 and -100.0 <= p['lon'] <= -65.0
        ]

        if us_landfalls:
            # Strongest US landfall
            key_point = max(us_landfalls, key=lambda p: p['wind'])
        elif landfall_points:
            # Strongest landfall anywhere
            key_point = max(landfall_points, key=lambda p: p['wind'])
        else:
            key_point = peak_point

        # ── Compute heading and speed from adjacent points ──
        heading = 0.0
        speed = 10.0
        key_idx = track_points.index(key_point)
        if key_idx > 0:
            prev = track_points[key_idx - 1]
            dlat = key_point['lat'] - prev['lat']
            dlon = key_point['lon'] - prev['lon']
            heading = math.degrees(math.atan2(dlon, dlat)) % 360
            # Rough speed: 6 hours between fixes, ~60nm per degree
            dist_deg = math.sqrt(dlat**2 + (dlon * math.cos(math.radians(key_point['lat'])))**2)
            speed = max(dist_deg * 60 / 6, 3)  # nm in 6 hours → kt

        # ── Extract year ──
        year = int(track_points[0]['date'][:4])

        # ── Build display name ──
        if name == 'UNNAMED':
            display_name = f"Unnamed ({atcf_id})"
        else:
            cat = _saffir_simpson(peak_point['wind'])
            if cat >= 1:
                display_name = f"Hurricane {name}"
            elif peak_point['wind'] >= 34:
                display_name = f"Tropical Storm {name}"
            else:
                display_name = f"Tropical Depression {name}"

        # ── Determine peak category from peak wind (not landfall wind) ──
        category = _saffir_simpson(peak_point['wind'])

        # Use best available pressure
        pres = key_point['pressure']
        if pres >= 1013 or pres <= 0:
            # Try peak point
            pres = peak_point['pressure']
        if pres >= 1013 or pres <= 0:
            pres = 1013 - category * 15  # rough estimate

        storms.append(StormEntry(
            storm_id=atcf_id.lower(),
            name=display_name,
            year=year,
            category=category,
            status="historical",
            landfall_lon=key_point['lon'],
            landfall_lat=key_point['lat'],
            max_wind_kt=peak_point['wind'],
            min_pressure_mb=pres,
            heading_deg=heading,
            speed_kt=round(speed, 1),
            basin="AL",
            advisory="best-track",
            has_us_landfall=len(us_landfalls) > 0,
        ))

    return storms


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Pre-loaded database
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

_ALL_STORMS: Optional[List[StormEntry]] = None
_STORMS_BY_YEAR: Optional[Dict[int, List[StormEntry]]] = None
_STORMS_BY_ID: Optional[Dict[str, StormEntry]] = None


def _ensure_loaded():
    """Parse HURDAT2 on first access, cache the result."""
    global _ALL_STORMS, _STORMS_BY_YEAR, _STORMS_BY_ID

    if _ALL_STORMS is not None:
        return

    data_dir = os.path.join(os.path.dirname(__file__), '..', '..', 'data')
    filepath = os.path.join(data_dir, 'hurdat2.txt')

    if not os.path.exists(filepath):
        print(f"WARNING: HURDAT2 file not found at {filepath}")
        _ALL_STORMS = []
        _STORMS_BY_YEAR = {}
        _STORMS_BY_ID = {}
        return

    print(f"Parsing HURDAT2 database: {filepath}")
    _ALL_STORMS = parse_hurdat2(filepath)
    print(f"  Loaded {len(_ALL_STORMS)} storms ({_ALL_STORMS[0].year}-{_ALL_STORMS[-1].year})")

    _STORMS_BY_YEAR = {}
    _STORMS_BY_ID = {}
    for s in _ALL_STORMS:
        _STORMS_BY_YEAR.setdefault(s.year, []).append(s)
        _STORMS_BY_ID[s.storm_id] = s


def get_all_hurdat2_storms() -> List[StormEntry]:
    _ensure_loaded()
    return _ALL_STORMS or []


def get_seasons(us_landfall_only: bool = True) -> List[dict]:
    """Return list of {year, count} for the storm browser, newest first.
    If us_landfall_only=True, only count storms that made US mainland landfall."""
    _ensure_loaded()
    if not _STORMS_BY_YEAR:
        return []
    result = []
    for y, ss in _STORMS_BY_YEAR.items():
        filtered = [s for s in ss if s.has_us_landfall] if us_landfall_only else ss
        if filtered:
            result.append({"year": y, "count": len(filtered)})
    return sorted(result, key=lambda x: x["year"], reverse=True)


def get_storms_for_year(year: int, us_landfall_only: bool = True) -> List[StormEntry]:
    """Return storms for a given season/year.
    If us_landfall_only=True, only return storms that made US mainland landfall."""
    _ensure_loaded()
    storms = (_STORMS_BY_YEAR or {}).get(year, [])
    if us_landfall_only:
        storms = [s for s in storms if s.has_us_landfall]
    return storms


def search_storms(query: str, limit: int = 20, us_landfall_only: bool = True) -> List[StormEntry]:
    """Search storms by name or ATCF ID.
    If us_landfall_only=True, only return storms that made US mainland landfall."""
    _ensure_loaded()
    if not _ALL_STORMS:
        return []

    q = query.lower().strip()
    results = []
    for s in _ALL_STORMS:
        if q in s.name.lower() or q in s.storm_id.lower():
            if us_landfall_only and not s.has_us_landfall:
                continue
            results.append(s)
            if len(results) >= limit:
                break
    return results


def get_storm_by_id(storm_id: str) -> Optional[StormEntry]:
    """Look up a single storm by its ATCF ID (lowercase)."""
    _ensure_loaded()
    return (_STORMS_BY_ID or {}).get(storm_id.lower())
