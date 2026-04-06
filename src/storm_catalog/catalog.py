"""
Storm Catalog

Provides a unified interface to browse historical landfalling hurricanes
and fetch currently active tropical cyclones from NHC RSS feeds.

Each storm entry includes the metadata the pipeline needs:
  - Landfall (or current) coordinates for map centering
  - Wind speed and central pressure for the parametric surge model
  - Grid origin so the cell system centers on the eye

Historical storms are curated from HURDAT2 best-track data for major
US landfalls. Active storms are pulled live from NHC RSS.
"""

from __future__ import annotations

import logging
import math
import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional

import requests

logger = logging.getLogger(__name__)


@dataclass
class StormEntry:
    """A storm available for analysis."""

    storm_id: str           # e.g. "AL092008" or "ike_2008"
    name: str               # e.g. "Hurricane Ike"
    year: int
    category: int           # Saffir-Simpson (0 = TS)
    status: str             # "historical" or "active"
    landfall_lon: float     # Eye longitude at landfall / current position
    landfall_lat: float     # Eye latitude at landfall / current position
    max_wind_kt: int        # Max sustained wind (knots) at landfall
    min_pressure_mb: int    # Minimum central pressure (mb)
    heading_deg: float      # Storm heading in degrees (0=N, 90=E)
    speed_kt: float         # Forward speed (knots)
    basin: str              # "AL" (Atlantic) or "EP" (East Pacific)
    advisory: str           # Advisory number or "best-track"
    has_us_landfall: bool = False  # True if storm made US mainland landfall
    dps_score: float = 0.0  # Cumulative DPS from StormDPS (0-100)

    # Grid system: these define the cell (0,0) origin for this storm
    @property
    def grid_origin_lon(self) -> float:
        """SW corner longitude of the center cell."""
        return round(self.landfall_lon - CELL_WIDTH / 2, 4)

    @property
    def grid_origin_lat(self) -> float:
        """SW corner latitude of the center cell."""
        return round(self.landfall_lat - CELL_HEIGHT / 2, 4)

    def to_dict(self) -> dict:
        d = asdict(self)
        d["grid_origin_lon"] = self.grid_origin_lon
        d["grid_origin_lat"] = self.grid_origin_lat
        return d


# Grid cell dimensions (shared with api_server.py and App.tsx)
CELL_WIDTH = 0.4    # degrees longitude
CELL_HEIGHT = 0.3   # degrees latitude


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Historical Storm Database
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#
# Curated from HURDAT2 best-track data.  Landfall coords are the
# 6-hourly fix nearest to (or at) the primary US landfall.
# Wind / pressure are the values at that fix.
#
# Sources:
#   - NHC Tropical Cyclone Reports
#   - HURDAT2 (nhc.noaa.gov/data/hurdat/)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

HISTORICAL_STORMS: List[StormEntry] = [
    # ── Category 5 ───────────────────────────────────────
    StormEntry(
        storm_id="michael_2018", name="Hurricane Michael", year=2018,
        category=5, status="historical",
        landfall_lon=-85.5, landfall_lat=30.2,
        max_wind_kt=140, min_pressure_mb=919,
        heading_deg=25, speed_kt=14,
        basin="AL", advisory="best-track",
        dps_score=87.8,
    ),
    # ── Category 4 ───────────────────────────────────────
    StormEntry(
        storm_id="katrina_2005", name="Hurricane Katrina", year=2005,
        category=4, status="historical",
        landfall_lon=-89.6, landfall_lat=29.3,
        max_wind_kt=110, min_pressure_mb=920,
        heading_deg=0, speed_kt=11,
        basin="AL", advisory="best-track",
        dps_score=89.0,
    ),
    StormEntry(
        storm_id="ike_2008", name="Hurricane Ike", year=2008,
        category=4, status="historical",
        landfall_lon=-94.7, landfall_lat=29.3,
        max_wind_kt=95, min_pressure_mb=950,
        heading_deg=315, speed_kt=13,
        basin="AL", advisory="best-track",
        dps_score=88.5,
    ),
    StormEntry(
        storm_id="harvey_2017", name="Hurricane Harvey", year=2017,
        category=4, status="historical",
        landfall_lon=-96.8, landfall_lat=28.0,
        max_wind_kt=115, min_pressure_mb=938,
        heading_deg=315, speed_kt=10,
        basin="AL", advisory="best-track",
        dps_score=88.4,
    ),
    StormEntry(
        storm_id="ian_2022", name="Hurricane Ian", year=2022,
        category=4, status="historical",
        landfall_lon=-82.2, landfall_lat=26.6,
        max_wind_kt=130, min_pressure_mb=937,
        heading_deg=35, speed_kt=9,
        basin="AL", advisory="best-track",
        dps_score=90.1,
    ),
    StormEntry(
        storm_id="laura_2020", name="Hurricane Laura", year=2020,
        category=4, status="historical",
        landfall_lon=-93.3, landfall_lat=30.0,
        max_wind_kt=130, min_pressure_mb=937,
        heading_deg=350, speed_kt=15,
        basin="AL", advisory="best-track",
        dps_score=86.9,
    ),
    StormEntry(
        storm_id="ida_2021", name="Hurricane Ida", year=2021,
        category=4, status="historical",
        landfall_lon=-90.1, landfall_lat=29.2,
        max_wind_kt=130, min_pressure_mb=930,
        heading_deg=330, speed_kt=13,
        basin="AL", advisory="best-track",
        dps_score=88.7,
    ),
    # ── Category 3 ───────────────────────────────────────
    StormEntry(
        storm_id="irma_2017", name="Hurricane Irma", year=2017,
        category=3, status="historical",
        landfall_lon=-81.8, landfall_lat=25.9,
        max_wind_kt=115, min_pressure_mb=929,
        heading_deg=350, speed_kt=8,
        basin="AL", advisory="best-track",
        dps_score=87.6,
    ),
    StormEntry(
        storm_id="florence_2018", name="Hurricane Florence", year=2018,
        category=1, status="historical",
        landfall_lon=-77.9, landfall_lat=34.2,
        max_wind_kt=75, min_pressure_mb=958,
        heading_deg=315, speed_kt=5,
        basin="AL", advisory="best-track",
        dps_score=85.4,
    ),
    # ── Category 2 ───────────────────────────────────────
    StormEntry(
        storm_id="sandy_2012", name="Hurricane Sandy", year=2012,
        category=2, status="historical",
        landfall_lon=-74.5, landfall_lat=39.4,
        max_wind_kt=70, min_pressure_mb=940,
        heading_deg=315, speed_kt=28,
        basin="AL", advisory="best-track",
        dps_score=89.5,
    ),
    StormEntry(
        storm_id="delta_2020", name="Hurricane Delta", year=2020,
        category=2, status="historical",
        landfall_lon=-93.1, landfall_lat=29.8,
        max_wind_kt=85, min_pressure_mb=970,
        heading_deg=350, speed_kt=14,
        basin="AL", advisory="best-track",
        dps_score=77.7,
    ),
    # ── Category 1 ───────────────────────────────────────
    StormEntry(
        storm_id="nicholas_2021", name="Hurricane Nicholas", year=2021,
        category=1, status="historical",
        landfall_lon=-95.2, landfall_lat=28.8,
        max_wind_kt=65, min_pressure_mb=995,
        heading_deg=320, speed_kt=9,
        basin="AL", advisory="best-track",
        dps_score=61.6,  # Nicholas not in compiled bundle; estimated
    ),
    StormEntry(
        storm_id="nate_2017", name="Hurricane Nate", year=2017,
        category=1, status="historical",
        landfall_lon=-88.8, landfall_lat=30.3,
        max_wind_kt=75, min_pressure_mb=981,
        heading_deg=5, speed_kt=22,
        basin="AL", advisory="best-track",
        dps_score=45.5,  # Nate not in compiled bundle; estimated
    ),
    StormEntry(
        storm_id="helene_2024", name="Hurricane Helene", year=2024,
        category=4, status="historical",
        landfall_lon=-83.8, landfall_lat=29.6,
        max_wind_kt=120, min_pressure_mb=938,
        heading_deg=15, speed_kt=20,
        basin="AL", advisory="best-track",
        dps_score=84.1,
    ),
    StormEntry(
        storm_id="milton_2024", name="Hurricane Milton", year=2024,
        category=3, status="historical",
        landfall_lon=-82.6, landfall_lat=27.6,
        max_wind_kt=120, min_pressure_mb=949,
        heading_deg=60, speed_kt=16,
        basin="AL", advisory="best-track",
        dps_score=87.5,
    ),
]

# Index by storm_id for fast lookup
_HISTORICAL_INDEX: Dict[str, StormEntry] = {s.storm_id: s for s in HISTORICAL_STORMS}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Active Storm Fetcher (NHC RSS)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

NHC_RSS_FEEDS = {
    "AL": "https://www.nhc.noaa.gov/index-at.xml",
    "EP": "https://www.nhc.noaa.gov/index-ep.xml",
    "CP": "https://www.nhc.noaa.gov/index-cp.xml",
}

NHC_NS = {"nhc": "https://www.nhc.noaa.gov"}


def _parse_coord(text: str) -> Optional[float]:
    """Parse NHC coordinate strings like '25.4N' or '80.2W'."""
    if not text:
        return None
    text = text.strip()
    m = re.match(r'([\d.]+)\s*([NSEW])', text, re.IGNORECASE)
    if m:
        val = float(m.group(1))
        d = m.group(2).upper()
        if d in ('S', 'W'):
            val = -val
        return val
    try:
        return float(text)
    except ValueError:
        return None


def _parse_int(text: str) -> int:
    """Extract first integer from a string like '150 mph' or '940 mb'."""
    if not text:
        return 0
    m = re.search(r'(\d+)', text)
    return int(m.group(1)) if m else 0


from common.saffir_simpson import wind_to_category as _saffir_simpson  # noqa: E402


def fetch_active_storms() -> List[StormEntry]:
    """
    Fetch currently active tropical cyclones from NHC RSS feeds.

    Returns a StormEntry for each active system with enough data
    to drive the surge model and map centering.
    """
    storms = []

    for basin, url in NHC_RSS_FEEDS.items():
        try:
            resp = requests.get(url, timeout=15,
                                headers={"User-Agent": "SurgeDPS/1.0"})
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.warning(f"Failed to fetch NHC {basin} feed: {e}")
            continue

        try:
            root = ET.fromstring(resp.text)
        except ET.ParseError as e:
            logger.warning(f"Failed to parse NHC {basin} XML: {e}")
            continue

        for item in root.findall(".//item"):
            cyclone = item.find("nhc:Cyclone", NHC_NS)
            if cyclone is None:
                continue

            atcf = (cyclone.findtext("nhc:atcf", "", NHC_NS) or "").strip()
            name = (cyclone.findtext("nhc:name", "", NHC_NS) or "").strip()
            stype = (cyclone.findtext("nhc:type", "", NHC_NS) or "").strip()
            center = (cyclone.findtext("nhc:center", "", NHC_NS) or "").strip()
            wind_str = cyclone.findtext("nhc:wind", "", NHC_NS) or ""
            pres_str = cyclone.findtext("nhc:pressure", "", NHC_NS) or ""
            movement = cyclone.findtext("nhc:movement", "", NHC_NS) or ""

            if not atcf or not center:
                continue

            # Parse center — could be "25.4, -80.2" or "25.4N 80.2W"
            parts = re.split(r'[,\s]+', center.strip())
            lat = _parse_coord(parts[0]) if len(parts) >= 1 else None
            lon = _parse_coord(parts[1]) if len(parts) >= 2 else None

            if lat is None or lon is None:
                continue

            wind_kt = _parse_int(wind_str)
            # NHC sometimes reports mph — convert if > 200 (no storm has 200kt)
            if "mph" in wind_str.lower() and wind_kt > 0:
                wind_kt = int(wind_kt * 0.868976)

            pressure = _parse_int(pres_str)
            cat = _saffir_simpson(wind_kt)

            # Parse movement for heading/speed (best effort)
            heading = 0.0
            speed = 10.0
            move_match = re.search(
                r'(N|NE|E|SE|S|SW|W|NW)\s+.*?(\d+)\s*(mph|kt)',
                movement, re.IGNORECASE
            )
            if move_match:
                compass = {
                    'N': 0, 'NE': 45, 'E': 90, 'SE': 135,
                    'S': 180, 'SW': 225, 'W': 270, 'NW': 315,
                }
                heading = compass.get(move_match.group(1).upper(), 0)
                spd = int(move_match.group(2))
                if 'mph' in move_match.group(3).lower():
                    spd = int(spd * 0.868976)
                speed = spd

            display_name = f"{stype} {name}" if name else stype
            storm_id = f"active_{atcf.lower()}"

            # Deduplicate (NHC sends multiple items per storm)
            if any(s.storm_id == storm_id for s in storms):
                continue

            storms.append(StormEntry(
                storm_id=storm_id,
                name=display_name,
                year=2026,  # current year
                category=cat,
                status="active",
                landfall_lon=lon,
                landfall_lat=lat,
                max_wind_kt=wind_kt,
                min_pressure_mb=pressure,
                heading_deg=heading,
                speed_kt=speed,
                basin=basin,
                advisory=atcf,
            ))

    return storms


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Public API
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def get_all_storms() -> List[StormEntry]:
    """Return historical + active storms, active first."""
    active = fetch_active_storms()
    return active + HISTORICAL_STORMS


def get_storm(storm_id: str) -> Optional[StormEntry]:
    """Look up a storm by ID. Checks historical first, then active."""
    if storm_id in _HISTORICAL_INDEX:
        return _HISTORICAL_INDEX[storm_id]

    # Check active storms
    for s in fetch_active_storms():
        if s.storm_id == storm_id:
            return s

    return None
