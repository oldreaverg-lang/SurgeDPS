"""
NOAA CO-OPS Tide Gauge Integration

Fetches real-time and predicted water levels from NOAA's Center for
Operational Oceanographic Products and Services (CO-OPS) tide stations.

Used for:
  1. Tide bias correction — add current tide level to modeled surge height
  2. Gauge overlay — display active stations on the map as a vector layer
  3. Validation — compare modeled surge against observed water levels

API Reference:
  Data:     https://api.tidesandcurrents.noaa.gov/api/prod/datagetter
  Metadata: https://api.tidesandcurrents.noaa.gov/mdapi/prod/webapi/stations.json

Station IDs are 7-character codes (e.g., "8729108" for Panama City, FL).
Datum used: NAVD88 (consistent with DEM elevations) or MLLW (tidal reference).
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests

logger = logging.getLogger(__name__)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Data Classes
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


@dataclass
class TideStation:
    """Metadata for a single CO-OPS tide station."""

    station_id: str
    name: str
    lat: float
    lon: float
    state: str = ""
    station_type: str = ""  # "waterlevels", "currents", etc.
    has_water_level: bool = True

    @property
    def coordinates(self) -> Tuple[float, float]:
        """Return [lon, lat] for GeoJSON compatibility."""
        return (self.lon, self.lat)


@dataclass
class TideReading:
    """A single water level observation or prediction."""

    time: str               # ISO timestamp
    water_level_m: float    # Water level in meters (relative to datum)
    sigma: float = 0.0      # Standard deviation (observations only)
    flags: str = ""         # Quality flags
    quality: str = ""       # Quality code


@dataclass
class TideGaugeData:
    """Complete tide data for a station."""

    station: TideStation
    datum: str                              # "NAVD" or "MLLW"
    readings: List[TideReading] = field(default_factory=list)
    predictions: List[TideReading] = field(default_factory=list)

    @property
    def latest_level_m(self) -> Optional[float]:
        """Most recent observed water level in meters."""
        if self.readings:
            return self.readings[-1].water_level_m
        return None

    @property
    def latest_prediction_m(self) -> Optional[float]:
        """Most recent predicted tide level in meters."""
        if self.predictions:
            return self.predictions[-1].water_level_m
        return None

    @property
    def tide_bias_m(self) -> float:
        """
        Current tide bias in meters (above/below datum).

        Uses the latest observation if available, otherwise the latest
        prediction. Returns 0 if no data is available.
        """
        if self.latest_level_m is not None:
            return self.latest_level_m
        if self.latest_prediction_m is not None:
            return self.latest_prediction_m
        return 0.0

    @property
    def max_predicted_m(self) -> float:
        """Maximum predicted tide level over the forecast window."""
        if not self.predictions:
            return 0.0
        return max(p.water_level_m for p in self.predictions)


@dataclass
class TideGaugeResult:
    """Combined result from all gauges in the storm area."""

    stations: List[TideGaugeData] = field(default_factory=list)

    @property
    def station_count(self) -> int:
        return len(self.stations)

    @property
    def mean_tide_bias_m(self) -> float:
        """Average tide bias across all stations."""
        biases = [s.tide_bias_m for s in self.stations if s.tide_bias_m != 0]
        if not biases:
            return 0.0
        return sum(biases) / len(biases)

    @property
    def max_tide_bias_m(self) -> float:
        """Maximum tide bias across all stations (worst case)."""
        biases = [s.tide_bias_m for s in self.stations]
        return max(biases) if biases else 0.0

    def to_geojson(self) -> dict:
        """
        Export all station locations and readings as a GeoJSON
        FeatureCollection for vector tile generation.
        """
        features = []
        for gauge in self.stations:
            station = gauge.station
            features.append({
                "type": "Feature",
                "properties": {
                    "layer": "tide_gauge",
                    "station_id": station.station_id,
                    "name": station.name,
                    "state": station.state,
                    "water_level_m": round(gauge.tide_bias_m, 3),
                    "water_level_ft": round(gauge.tide_bias_m * 3.28084, 2),
                    "datum": gauge.datum,
                    "max_predicted_m": round(gauge.max_predicted_m, 3),
                    "max_predicted_ft": round(gauge.max_predicted_m * 3.28084, 2),
                    "has_observation": gauge.latest_level_m is not None,
                    "reading_count": len(gauge.readings),
                },
                "geometry": {
                    "type": "Point",
                    "coordinates": list(station.coordinates),
                },
            })

        return {
            "type": "FeatureCollection",
            "features": features,
        }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Gulf Coast Station Registry
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# Pre-built list of key Gulf Coast CO-OPS stations.
# Avoids needing a metadata API call for the common case.
# These cover the primary storm surge impact zone for Gulf hurricanes.

GULF_COAST_STATIONS: List[TideStation] = [
    # Texas
    TideStation("8770570", "Sabine Pass North", 29.7283, -93.8700, "TX"),
    TideStation("8771013", "Eagle Point", 29.4810, -94.9180, "TX"),
    TideStation("8771450", "Galveston Pier 21", 29.3100, -94.7933, "TX"),
    TideStation("8772447", "USCG Freeport", 28.9433, -95.3083, "TX"),
    TideStation("8773146", "Matagorda City", 28.7100, -95.9133, "TX"),
    TideStation("8774770", "Rockport", 28.0217, -97.0467, "TX"),
    TideStation("8775870", "Corpus Christi", 27.5800, -97.2167, "TX"),
    TideStation("8779770", "Port Isabel", 26.0617, -97.2150, "TX"),
    # Louisiana
    TideStation("8760922", "Pilots Station East", 28.9322, -89.4075, "LA"),
    TideStation("8761724", "Grand Isle", 29.2633, -89.9567, "LA"),
    TideStation("8761305", "Shell Beach", 29.8683, -89.6733, "LA"),
    TideStation("8764044", "Berwick", 29.6717, -91.2383, "LA"),
    TideStation("8764227", "Lawma, Amerada Pass", 29.4500, -91.3383, "LA"),
    TideStation("8767816", "Lake Charles", 30.2233, -93.2217, "LA"),
    # Mississippi / Alabama
    TideStation("8741533", "Pascagoula NOAA Lab", 30.3683, -88.5633, "MS"),
    TideStation("8735180", "Dauphin Island", 30.2500, -88.0750, "AL"),
    TideStation("8737048", "Mobile State Docks", 30.7083, -88.0433, "AL"),
    # Florida Panhandle
    TideStation("8729108", "Panama City", 30.1522, -85.6669, "FL"),
    TideStation("8729840", "Pensacola", 30.4044, -87.2112, "FL"),
    TideStation("8726724", "Clearwater Beach", 27.9783, -82.8317, "FL"),
    TideStation("8726520", "St. Petersburg", 27.7606, -82.6269, "FL"),
    TideStation("8725110", "Naples", 26.1317, -81.8075, "FL"),
    TideStation("8723214", "Virginia Key", 25.7314, -80.1618, "FL"),
    TideStation("8724580", "Key West", 24.5508, -81.8081, "FL"),
    # Florida Atlantic
    TideStation("8721604", "Trident Pier", 28.4158, -80.5931, "FL"),
    TideStation("8720218", "Mayport", 30.3967, -81.4300, "FL"),
]


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Tide Gauge Fetcher
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TideGaugeFetcher:
    """
    Fetches real-time and predicted water levels from NOAA CO-OPS.

    Usage:
        fetcher = TideGaugeFetcher()
        result = fetcher.fetch_for_storm(storm_geometry)
        bias = result.mean_tide_bias_m
    """

    DATA_API = "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter"
    METADATA_API = "https://api.tidesandcurrents.noaa.gov/mdapi/prod/webapi/stations.json"

    def __init__(
        self,
        config=None,
        session: Optional[requests.Session] = None,
        datum: str = "NAVD",
        hours_back: int = 6,
        hours_forward: int = 48,
        timeout: int = 15,
    ):
        """
        Args:
            config: IngestConfig (optional, for scratch_dir)
            session: Requests session (for connection pooling)
            datum: Vertical datum — "NAVD" for NAVD88, "MLLW" for tidal
            hours_back: How many hours of recent observations to fetch
            hours_forward: How many hours of tide predictions to fetch
            timeout: HTTP request timeout in seconds
        """
        self.config = config
        self.session = session or requests.Session()
        self.session.headers.setdefault(
            "User-Agent", "SurgeDPS/1.0 (storm surge research)"
        )
        self.datum = datum
        self.hours_back = hours_back
        self.hours_forward = hours_forward
        self.timeout = timeout

    def fetch_for_storm(
        self,
        storm_geometry: dict,
        buffer_deg: float = 0.5,
    ) -> TideGaugeResult:
        """
        Fetch tide data for all stations within the storm extent.

        Args:
            storm_geometry: GeoJSON Polygon geometry of the storm area
            buffer_deg: Extra buffer in degrees around the bounding box

        Returns:
            TideGaugeResult with readings for all matched stations
        """
        # Find stations within storm extent
        stations = self._find_stations_in_extent(storm_geometry, buffer_deg)

        if not stations:
            logger.info("No tide stations found in storm extent")
            return TideGaugeResult()

        logger.info(f"Found {len(stations)} tide stations in storm extent")

        # Fetch data for each station
        result = TideGaugeResult()
        now = datetime.now(timezone.utc)
        begin = now - timedelta(hours=self.hours_back)
        end = now + timedelta(hours=self.hours_forward)

        for station in stations:
            try:
                gauge_data = self._fetch_station_data(station, begin, end)
                result.stations.append(gauge_data)
            except Exception as e:
                logger.warning(
                    f"Failed to fetch tide data for {station.station_id} "
                    f"({station.name}): {e}"
                )

        logger.info(
            f"Tide gauge result: {result.station_count} stations, "
            f"mean bias={result.mean_tide_bias_m:.3f}m, "
            f"max bias={result.max_tide_bias_m:.3f}m"
        )
        return result

    def _find_stations_in_extent(
        self,
        storm_geometry: dict,
        buffer_deg: float,
    ) -> List[TideStation]:
        """
        Find CO-OPS stations within the storm bounding box.

        Uses the pre-built Gulf Coast registry first, then optionally
        queries the metadata API for additional stations.
        """
        coords = storm_geometry.get("coordinates", [[]])
        flat = coords[0] if coords else []
        if not flat:
            return []

        lons = [c[0] for c in flat]
        lats = [c[1] for c in flat]
        west = min(lons) - buffer_deg
        south = min(lats) - buffer_deg
        east = max(lons) + buffer_deg
        north = max(lats) + buffer_deg

        # Check pre-built registry
        matched = [
            s for s in GULF_COAST_STATIONS
            if west <= s.lon <= east and south <= s.lat <= north
        ]

        if matched:
            logger.info(
                f"Matched {len(matched)} stations from Gulf Coast registry"
            )
            return matched

        # Fallback: query metadata API
        return self._query_metadata_api(west, south, east, north)

    def _query_metadata_api(
        self,
        west: float,
        south: float,
        east: float,
        north: float,
    ) -> List[TideStation]:
        """
        Query the CO-OPS metadata API for water level stations
        in the given bounding box.
        """
        try:
            resp = self.session.get(
                self.METADATA_API,
                params={"type": "waterlevels"},
                timeout=self.timeout,
            )
            resp.raise_for_status()
            data = resp.json()

            stations = []
            for s in data.get("stations", []):
                lat = float(s.get("lat", 0))
                lon = float(s.get("lng", 0))

                if west <= lon <= east and south <= lat <= north:
                    stations.append(TideStation(
                        station_id=str(s.get("id", "")),
                        name=s.get("name", ""),
                        lat=lat,
                        lon=lon,
                        state=s.get("state", ""),
                        station_type="waterlevels",
                    ))

            logger.info(
                f"Metadata API returned {len(stations)} stations in bbox"
            )
            return stations

        except Exception as e:
            logger.warning(f"Metadata API query failed: {e}")
            return []

    def _fetch_station_data(
        self,
        station: TideStation,
        begin: datetime,
        end: datetime,
    ) -> TideGaugeData:
        """
        Fetch recent observations and predictions for a single station.
        """
        gauge = TideGaugeData(station=station, datum=self.datum)

        # Fetch recent observations (water_level)
        now = datetime.now(timezone.utc)
        obs_begin = begin.strftime("%Y%m%d %H:%M")
        obs_end = min(now, end).strftime("%Y%m%d %H:%M")

        try:
            obs_data = self._api_request(
                station.station_id,
                product="water_level",
                begin_date=obs_begin,
                end_date=obs_end,
            )
            gauge.readings = self._parse_readings(obs_data)
        except Exception as e:
            logger.debug(f"No observations for {station.station_id}: {e}")

        # Fetch predictions (next hours_forward hours)
        pred_begin = now.strftime("%Y%m%d %H:%M")
        pred_end = end.strftime("%Y%m%d %H:%M")

        try:
            pred_data = self._api_request(
                station.station_id,
                product="predictions",
                begin_date=pred_begin,
                end_date=pred_end,
            )
            gauge.predictions = self._parse_predictions(pred_data)
        except Exception as e:
            logger.debug(f"No predictions for {station.station_id}: {e}")

        return gauge

    def _api_request(
        self,
        station_id: str,
        product: str,
        begin_date: str,
        end_date: str,
    ) -> dict:
        """
        Make a single request to the CO-OPS data API.

        Returns:
            Parsed JSON response dict
        """
        params = {
            "station": station_id,
            "begin_date": begin_date,
            "end_date": end_date,
            "product": product,
            "datum": self.datum,
            "units": "metric",
            "time_zone": "gmt",
            "format": "json",
            "application": "SurgeDPS",
        }

        resp = self.session.get(
            self.DATA_API, params=params, timeout=self.timeout
        )
        resp.raise_for_status()
        data = resp.json()

        # CO-OPS returns errors inside a valid JSON response
        if "error" in data:
            raise ValueError(data["error"].get("message", "Unknown API error"))

        return data

    @staticmethod
    def _parse_readings(data: dict) -> List[TideReading]:
        """Parse water_level response into TideReading objects."""
        readings = []
        for item in data.get("data", []):
            try:
                level = float(item.get("v", 0))
                readings.append(TideReading(
                    time=item.get("t", ""),
                    water_level_m=level,
                    sigma=float(item.get("s", 0)),
                    flags=item.get("f", ""),
                    quality=item.get("q", ""),
                ))
            except (ValueError, TypeError):
                continue
        return readings

    @staticmethod
    def _parse_predictions(data: dict) -> List[TideReading]:
        """Parse predictions response into TideReading objects."""
        predictions = []
        for item in data.get("predictions", []):
            try:
                level = float(item.get("v", 0))
                predictions.append(TideReading(
                    time=item.get("t", ""),
                    water_level_m=level,
                ))
            except (ValueError, TypeError):
                continue
        return predictions

    def write_geojson(
        self,
        result: TideGaugeResult,
        output_path: str,
    ) -> str:
        """
        Write tide gauge data as a GeoJSON file for vector tile generation.

        Args:
            result: TideGaugeResult from fetch_for_storm()
            output_path: Path to write the GeoJSON file

        Returns:
            Path to the written file
        """
        geojson = result.to_geojson()

        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, "w") as f:
            json.dump(geojson, f)

        logger.info(
            f"Tide gauge GeoJSON: {len(geojson['features'])} stations "
            f"-> {output_path}"
        )
        return output_path
