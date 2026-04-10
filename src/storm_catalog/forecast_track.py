"""
NHC Forecast Track Fetcher

Queries NOAA's ArcGIS MapServer for active tropical cyclone forecast
tracks, providing predicted positions, wind speeds, and pressures at
12/24/36/48/72/96/120 hour intervals.

This data drives two critical pipeline features:

  1. Predicted landfall targeting — the storm monitor runs the surge
     model at the forecast landfall point rather than the current
     position, producing meaningful damage estimates days in advance.

  2. Advisory timeline — each advisory's prediction is logged to the
     validation ledger, creating a timeline that shows how accuracy
     improves as the storm approaches landfall.

Data source:
  NOAA NWS MapServices — NHC Tropical Weather
  https://mapservices.weather.noaa.gov/tropical/rest/services/tropical/NHC_tropical_weather/MapServer

  Layer pattern (per storm slot):
    AT{n} Forecast Points: layer IDs 6, 32, 58, 84, 110 (AT1-AT5)
    AT{n} Forecast Track:  layer IDs 7, 33, 59, 85, 111
    AT{n} Forecast Cone:   layer IDs 8, 34, 60, 86, 112

  Fields: lat, lon, maxwind, mslp, tau (forecast hour), stormname,
          ssnum (category), tcdir (direction), tcspd (speed),
          advisnum (advisory number)
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Tuple

import requests

logger = logging.getLogger(__name__)

# ── ArcGIS MapServer endpoint ──
_BASE_URL = (
    "https://mapservices.weather.noaa.gov/tropical/rest/services"
    "/tropical/NHC_tropical_weather/MapServer"
)

# Forecast Points layer IDs for Atlantic slots AT1-AT5
# Each active storm occupies a numbered slot
_AT_FORECAST_POINT_LAYERS = [6, 32, 58, 84, 110]

# Eastern Pacific slots EP1-EP5
_EP_FORECAST_POINT_LAYERS = [136, 162, 188, 214, 240]

_SESSION = requests.Session()
_SESSION.headers.update({"User-Agent": "SurgeDPS/1.0 (surgedps.com)"})

# Cache on Railway volume — paths centralised in storage.py
from persistent_paths import FORECASTS_DIR
_FORECAST_CACHE_DIR = str(FORECASTS_DIR)


@dataclass
class ForecastPoint:
    """A single forecast position along the predicted track."""

    tau: int                # Hours from current advisory (0, 12, 24, 36, 48, 72, 96, 120)
    lat: float              # Forecast latitude
    lon: float              # Forecast longitude
    max_wind_kt: int        # Forecast max sustained wind (knots)
    gust_kt: int            # Forecast gust (knots)
    pressure_mb: int        # Forecast central pressure (mb)
    category: int           # Saffir-Simpson category at this point
    storm_type: str         # e.g., "Hurricane", "Tropical Storm"
    direction_deg: float    # Forecast heading (degrees)
    speed_kt: float         # Forecast forward speed (knots)
    date_label: str         # Human-readable date/time label

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class ForecastTrack:
    """Complete forecast track for one active storm."""

    storm_name: str
    advisory_num: str
    basin: str
    points: List[ForecastPoint]
    fetched_at: float       # Unix timestamp

    @property
    def current_position(self) -> Optional[ForecastPoint]:
        """The tau=0 point (current position)."""
        for p in self.points:
            if p.tau == 0:
                return p
        return self.points[0] if self.points else None

    @property
    def predicted_landfall(self) -> Optional[ForecastPoint]:
        """
        Estimate the landfall point from the forecast track.

        Finds the first forecast point that is over land (within CONUS
        coastal bbox) with maximum wind intensity. If no point crosses
        land, returns the point of closest approach to the US coast.
        """
        if not self.points:
            return None

        # Simple coastal detection: point is "near coast" if within
        # the US Gulf/Atlantic coastal zone
        coastal_candidates = []
        for p in self.points:
            if p.tau == 0:
                continue  # Skip current position
            # US coastal zone (generous bbox)
            if (24.0 <= p.lat <= 45.0 and -100.0 <= p.lon <= -65.0):
                coastal_candidates.append(p)

        if coastal_candidates:
            # Return the highest-wind point near the coast
            # (typically the landfall point or closest approach)
            return max(coastal_candidates, key=lambda p: p.max_wind_kt)

        # No coastal points — storm may not make US landfall
        # Return the closest approach (minimum distance to US coastline center)
        if len(self.points) > 1:
            us_coast_lat, us_coast_lon = 30.0, -85.0  # Gulf Coast center
            return min(self.points[1:], key=lambda p:
                       (p.lat - us_coast_lat)**2 + (p.lon - us_coast_lon)**2)

        return None

    @property
    def max_forecast_wind(self) -> int:
        """Peak wind speed anywhere along the forecast track."""
        return max((p.max_wind_kt for p in self.points), default=0)

    @property
    def hours_to_landfall(self) -> Optional[int]:
        """Estimated hours until landfall, or None if no landfall predicted."""
        lf = self.predicted_landfall
        return lf.tau if lf else None

    def to_dict(self) -> dict:
        return {
            "storm_name": self.storm_name,
            "advisory_num": self.advisory_num,
            "basin": self.basin,
            "points": [p.to_dict() for p in self.points],
            "predicted_landfall": self.predicted_landfall.to_dict() if self.predicted_landfall else None,
            "hours_to_landfall": self.hours_to_landfall,
            "max_forecast_wind": self.max_forecast_wind,
            "fetched_at": self.fetched_at,
        }


def _query_layer(layer_id: int) -> List[dict]:
    """
    Query an ArcGIS MapServer layer for all features.
    Returns list of feature dicts with attributes and geometry.
    """
    url = f"{_BASE_URL}/{layer_id}/query"
    try:
        resp = _SESSION.get(url, params={
            "where": "1=1",
            "outFields": "*",
            "f": "json",
            "returnGeometry": "true",
        }, timeout=15)
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        logger.warning("[Forecast] ArcGIS query failed for layer %d: %s", layer_id, exc)
        return []

    return data.get("features", [])


def fetch_forecast_track(storm_name: Optional[str] = None) -> List[ForecastTrack]:
    """
    Fetch forecast tracks for all active Atlantic storms.

    If storm_name is provided, only return the track for that storm.

    Returns a list of ForecastTrack objects (one per active storm).
    """
    import time
    tracks: List[ForecastTrack] = []

    for layer_id in _AT_FORECAST_POINT_LAYERS:
        features = _query_layer(layer_id)
        if not features:
            continue

        # Group by storm name (all features in a layer should be same storm)
        storm_points: Dict[str, List[ForecastPoint]] = {}
        storm_meta: Dict[str, dict] = {}

        for feat in features:
            attrs = feat.get("attributes", {})
            name = (attrs.get("stormname") or "").strip()
            if not name:
                continue
            if storm_name and name.upper() != storm_name.upper():
                continue

            geom = feat.get("geometry", {})
            lon = geom.get("x") or attrs.get("lon", 0)
            lat = geom.get("y") or attrs.get("lat", 0)

            point = ForecastPoint(
                tau=int(attrs.get("tau") or attrs.get("fcstprd") or 0),
                lat=float(lat),
                lon=float(lon),
                max_wind_kt=int(attrs.get("maxwind") or 0),
                gust_kt=int(attrs.get("gust") or 0),
                pressure_mb=int(attrs.get("mslp") or 0),
                category=int(attrs.get("ssnum") or 0),
                storm_type=(attrs.get("tcdvlp") or attrs.get("stormtype") or ""),
                direction_deg=float(attrs.get("tcdir") or 0),
                speed_kt=float(attrs.get("tcspd") or 0),
                date_label=(attrs.get("fldatelbl") or attrs.get("datelbl") or ""),
            )

            if name not in storm_points:
                storm_points[name] = []
                storm_meta[name] = {
                    "advisory": attrs.get("advisnum", ""),
                    "basin": attrs.get("basin", "AL"),
                }
            storm_points[name].append(point)

        for name, points in storm_points.items():
            # Sort by forecast hour
            points.sort(key=lambda p: p.tau)
            meta = storm_meta[name]

            track = ForecastTrack(
                storm_name=name,
                advisory_num=meta["advisory"],
                basin=meta["basin"],
                points=points,
                fetched_at=time.time(),
            )
            tracks.append(track)

            # Cache to disk
            _cache_track(track)

    return tracks


def _cache_track(track: ForecastTrack):
    """Save a forecast track to disk for historical review."""
    safe_name = track.storm_name.replace(" ", "_").lower()
    adv = track.advisory_num.replace("/", "_")
    path = os.path.join(_FORECAST_CACHE_DIR, f"{safe_name}_adv{adv}.json")
    with open(path, 'w') as f:
        json.dump(track.to_dict(), f, indent=2)


def fetch_forecast_cone() -> Dict[str, dict]:
    """
    Fetch the NHC forecast cone polygons for all active Atlantic storms.

    Returns a dict of storm_name → GeoJSON Polygon geometry representing
    the cone of uncertainty. This is the area the user can drag the
    landfall marker within.

    Cone layers: AT1=8, AT2=34, AT3=60, AT4=86, AT5=112
    """
    _AT_CONE_LAYERS = [8, 34, 60, 86, 112]
    cones: Dict[str, dict] = {}

    for layer_id in _AT_CONE_LAYERS:
        features = _query_layer(layer_id)
        if not features:
            continue

        for feat in features:
            attrs = feat.get("attributes", {})
            name = (attrs.get("stormname") or "").strip()
            if not name:
                continue

            geom = feat.get("geometry", {})
            rings = geom.get("rings", [])
            if not rings:
                continue

            # Convert ArcGIS rings to GeoJSON polygon
            geojson_coords = []
            for ring in rings:
                geojson_coords.append([[pt[0], pt[1]] for pt in ring])

            cones[name.upper()] = {
                "type": "Polygon",
                "coordinates": geojson_coords,
                "properties": {
                    "storm_name": name,
                    "advisory_num": attrs.get("advisnum", ""),
                },
            }

    return cones


def get_cached_tracks(storm_name: str) -> List[dict]:
    """
    Load all cached forecast tracks for a storm (advisory timeline).

    Returns list of track dicts sorted by advisory number, enabling
    post-event analysis of how the forecast evolved.
    """
    safe_name = storm_name.replace(" ", "_").lower()
    tracks = []
    for fname in os.listdir(_FORECAST_CACHE_DIR):
        if fname.startswith(safe_name) and fname.endswith(".json"):
            path = os.path.join(_FORECAST_CACHE_DIR, fname)
            with open(path) as f:
                tracks.append(json.load(f))
    return sorted(tracks, key=lambda t: t.get("advisory_num", ""))
