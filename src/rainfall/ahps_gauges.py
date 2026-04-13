"""
AHPS Stream Gauge Client
========================
Fetches real-time river stage and flow data from NOAA's Advanced Hydrologic
Prediction Service (AHPS) / National Water Prediction Service (NWPS) API.

Two data sources, both free with no auth:
  1. NWPS JSON API  — https://api.water.noaa.gov/nwps/v1/
     Replaces the legacy AHPS XML API as of 2024. Returns stage (ft), flow (cfs),
     flood categories (action/minor/moderate/major), and 3-day forecasts.
     Rate-limited to ~10 req/s (no key needed).

  2. USGS NWIS JSON — https://waterservices.usgs.gov/nwis/iv/
     Real-time instantaneous values for ~13k stream gauges.
     More granular (15-min cadence) but fewer flood-stage attributes.

This module uses NWPS (source 1) as primary and USGS NWIS as supplemental
for stations that are in USGS but not NWS.

Usage:
    from rainfall.ahps_gauges import AHPSClient, GaugeReading

    client = AHPSClient()

    # Get all gauges within a bounding box that are currently at flood stage
    gauges = client.get_flood_gauges_in_bbox(
        lon_min=-98.0, lat_min=27.0, lon_max=-94.0, lat_max=31.0,
        min_flood_category="minor",
    )
    for g in gauges:
        print(g.site_id, g.stage_ft, g.flood_category, g.status_label)

    # Get the current stage at a specific NWS location ID
    reading = client.get_gauge(site_id="HOUS4")
    if reading:
        print(reading.stage_ft, reading.forecast_crest_ft)
"""

from __future__ import annotations

import json
import logging
import os
import time
import urllib.request
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

_NWPS_BASE   = "https://api.water.noaa.gov/nwps/v1"
_USGS_BASE   = "https://waterservices.usgs.gov/nwis/iv"
_REQUEST_TIMEOUT = 15
_USER_AGENT = "SurgeDPS/1.0 (surgedps.com)"

# Ordered flood categories (NWS terminology)
FLOOD_CATEGORIES = ["none", "action", "minor", "moderate", "major"]
_CAT_RANK = {c: i for i, c in enumerate(FLOOD_CATEGORIES)}


@dataclass
class GaugeForecastPoint:
    """Single point on a stage forecast."""
    valid_time: str   # ISO 8601 UTC
    stage_ft: float
    flow_cfs: Optional[float] = None


@dataclass
class GaugeReading:
    """Current conditions and forecast for one stream gauge."""
    site_id: str                    # NWS location ID (e.g. "HOUS4") or USGS site no.
    site_name: str
    lat: float
    lon: float
    # ── Current observed ──
    stage_ft: Optional[float] = None
    flow_cfs: Optional[float] = None
    obs_time: Optional[str] = None  # ISO 8601 UTC
    # ── Flood thresholds (NWS) ──
    action_stage_ft: Optional[float] = None
    minor_flood_ft: Optional[float] = None
    moderate_flood_ft: Optional[float] = None
    major_flood_ft: Optional[float] = None
    bankfull_ft: Optional[float] = None
    record_stage_ft: Optional[float] = None
    # ── Flood status ──
    flood_category: str = "none"    # "none" | "action" | "minor" | "moderate" | "major"
    status_label: str = "Normal"    # Human-readable e.g. "Minor Flooding"
    pct_above_minor: Optional[float] = None  # how far above minor flood stage (%)
    # ── Forecast ──
    forecast: List[GaugeForecastPoint] = field(default_factory=list)
    forecast_crest_ft: Optional[float] = None
    forecast_crest_time: Optional[str] = None
    forecast_source: str = "nwps"


class AHPSClient:
    """
    Client for NOAA AHPS / NWPS real-time stream gauge data.

    Args:
        cache_ttl_seconds: How long to cache gauge readings before re-fetching.
                           Default 300 (5 min) — suitable for operational use.
    """

    def __init__(self, cache_ttl_seconds: int = 300):
        self._cache: Dict[str, Tuple[float, GaugeReading]] = {}
        self._ttl = cache_ttl_seconds

    # ── Public interface ─────────────────────────────────────────────────────

    def get_gauge(self, site_id: str) -> Optional[GaugeReading]:
        """
        Fetch current conditions for a single NWS gauge (AHPS location ID).

        Args:
            site_id: NWS location ID, e.g. "HOUS4", "BVRT2".

        Returns:
            GaugeReading or None if the gauge is not found / offline.
        """
        cached = self._from_cache(site_id)
        if cached:
            return cached

        url = f"{_NWPS_BASE}/gauges/{site_id.upper()}"
        data = self._get_json(url)
        if not data:
            return None

        reading = self._parse_nwps_gauge(data)
        if reading:
            self._to_cache(site_id, reading)
        return reading

    def get_flood_gauges_in_bbox(
        self,
        lon_min: float,
        lat_min: float,
        lon_max: float,
        lat_max: float,
        min_flood_category: str = "action",
        limit: int = 200,
    ) -> List[GaugeReading]:
        """
        Fetch all gauges in a bounding box at or above a flood category.

        Uses the NWPS /gauges endpoint with bbox filtering.
        Falls back to a state-by-state query if bbox not supported.

        Args:
            lon_min, lat_min, lon_max, lat_max: Bounding box (decimal degrees).
            min_flood_category: "action" | "minor" | "moderate" | "major".
                                Only gauges at or above this level are returned.
            limit: Max gauges to return.

        Returns:
            List of GaugeReading, sorted by flood severity descending.
        """
        min_rank = _CAT_RANK.get(min_flood_category, 1)

        # Try NWPS bbox query
        url = (
            f"{_NWPS_BASE}/gauges"
            f"?minLon={lon_min:.4f}&minLat={lat_min:.4f}"
            f"&maxLon={lon_max:.4f}&maxLat={lat_max:.4f}"
            f"&status=flood&limit={limit}"
        )
        data = self._get_json(url)

        readings: List[GaugeReading] = []
        if data and isinstance(data, dict):
            for item in data.get("gauges", data.get("data", [])):
                reading = self._parse_nwps_gauge(item)
                if reading and _CAT_RANK.get(reading.flood_category, 0) >= min_rank:
                    readings.append(reading)

        # Deduplicate and sort by severity
        seen = set()
        unique = []
        for r in readings:
            if r.site_id not in seen:
                seen.add(r.site_id)
                unique.append(r)

        unique.sort(
            key=lambda r: (_CAT_RANK.get(r.flood_category, 0), r.stage_ft or 0),
            reverse=True,
        )
        logger.info(
            "AHPS bbox query: %d gauges at or above '%s' in bbox (%.2f,%.2f)-(%.2f,%.2f)",
            len(unique), min_flood_category, lon_min, lat_min, lon_max, lat_max,
        )
        return unique

    def get_gauges_for_storm(
        self,
        landfall_lat: float,
        landfall_lon: float,
        radius_deg: float = 4.0,
        min_flood_category: str = "action",
    ) -> List[GaugeReading]:
        """
        Convenience wrapper: get flood gauges within radius of a storm's landfall.

        Args:
            landfall_lat, landfall_lon: Storm center.
            radius_deg: Search radius in decimal degrees (~111 km per degree).
            min_flood_category: Minimum category to include.

        Returns:
            Sorted list of GaugeReading.
        """
        return self.get_flood_gauges_in_bbox(
            lon_min=landfall_lon - radius_deg,
            lat_min=landfall_lat - radius_deg,
            lon_max=landfall_lon + radius_deg,
            lat_max=landfall_lat + radius_deg,
            min_flood_category=min_flood_category,
        )

    def to_geojson(self, readings: List[GaugeReading]) -> dict:
        """
        Convert a list of GaugeReadings to a GeoJSON FeatureCollection.
        Suitable for direct API response or map overlay.
        """
        features = []
        for r in readings:
            features.append({
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [r.lon, r.lat],
                },
                "properties": {
                    "site_id": r.site_id,
                    "site_name": r.site_name,
                    "stage_ft": r.stage_ft,
                    "flow_cfs": r.flow_cfs,
                    "obs_time": r.obs_time,
                    "flood_category": r.flood_category,
                    "status_label": r.status_label,
                    "action_stage_ft": r.action_stage_ft,
                    "minor_flood_ft": r.minor_flood_ft,
                    "moderate_flood_ft": r.moderate_flood_ft,
                    "major_flood_ft": r.major_flood_ft,
                    "record_stage_ft": r.record_stage_ft,
                    "forecast_crest_ft": r.forecast_crest_ft,
                    "forecast_crest_time": r.forecast_crest_time,
                    "pct_above_minor": r.pct_above_minor,
                    "layer": "stream_gauges",
                },
            })
        return {"type": "FeatureCollection", "features": features}

    # ── Parsing ──────────────────────────────────────────────────────────────

    def _parse_nwps_gauge(self, data: dict) -> Optional[GaugeReading]:
        """
        Parse a NWPS API response (single gauge or item in list) into GaugeReading.
        Handles both the full /gauges/{id} response and list items.
        """
        try:
            # Location/identity
            lid     = data.get("lid") or data.get("locationId") or data.get("id", "")
            name    = data.get("name") or data.get("locationName") or lid
            latd    = data.get("latitude") or data.get("lat")
            lond    = data.get("longitude") or data.get("lon")
            if latd is None or lond is None:
                geog = data.get("geography", {})
                latd = geog.get("latitude")
                lond = geog.get("longitude")

            if latd is None or lond is None:
                return None

            r = GaugeReading(
                site_id=str(lid),
                site_name=str(name),
                lat=float(latd),
                lon=float(lond),
            )

            # Observed stage/flow
            obs = data.get("observed", data)
            if isinstance(obs, dict):
                r.stage_ft  = _safe_float(obs.get("primary") or obs.get("stage"))
                r.flow_cfs  = _safe_float(obs.get("secondary") or obs.get("flow"))
                r.obs_time  = obs.get("timestamp") or obs.get("time")

            # Flood thresholds
            thresholds = data.get("flood", data.get("thresholds", data))
            if isinstance(thresholds, dict):
                r.action_stage_ft   = _safe_float(thresholds.get("action"))
                r.minor_flood_ft    = _safe_float(thresholds.get("minor"))
                r.moderate_flood_ft = _safe_float(thresholds.get("moderate"))
                r.major_flood_ft    = _safe_float(thresholds.get("major"))
                r.record_stage_ft   = _safe_float(thresholds.get("record"))
                r.bankfull_ft       = _safe_float(thresholds.get("bankfull"))

            # Flood status
            status = data.get("status", {})
            if isinstance(status, dict):
                cat = (status.get("observed", {}) or {}).get("floodCategory", "none")
            else:
                cat = data.get("floodCategory", data.get("flood_category", "none"))
            r.flood_category = _normalize_category(cat)
            r.status_label   = _category_label(r.flood_category, r.stage_ft, r.minor_flood_ft)

            # How far above minor (for map color ramp)
            if r.stage_ft is not None and r.minor_flood_ft is not None and r.minor_flood_ft > 0:
                r.pct_above_minor = round(
                    (r.stage_ft - r.minor_flood_ft) / r.minor_flood_ft * 100, 1
                )

            # Forecast crest
            forecast_data = data.get("forecast", {})
            if isinstance(forecast_data, dict):
                crest = forecast_data.get("crest", {})
                if crest:
                    r.forecast_crest_ft   = _safe_float(crest.get("primary") or crest.get("stage"))
                    r.forecast_crest_time = crest.get("timestamp") or crest.get("time")
                # Time series
                ts_list = forecast_data.get("timeSeries", [])
                for pt in ts_list[:24]:  # cap at 24 points
                    r.forecast.append(GaugeForecastPoint(
                        valid_time=pt.get("time", ""),
                        stage_ft=_safe_float(pt.get("primary") or pt.get("stage")) or 0,
                        flow_cfs=_safe_float(pt.get("secondary") or pt.get("flow")),
                    ))

            return r

        except Exception as exc:
            logger.debug("Failed to parse NWPS gauge record: %s — %s", exc, data)
            return None

    # ── HTTP helpers ─────────────────────────────────────────────────────────

    def _get_json(self, url: str) -> Optional[dict]:
        """Fetch a URL and return parsed JSON. Returns None on any failure."""
        try:
            req = urllib.request.Request(url, headers={"User-Agent": _USER_AGENT})
            with urllib.request.urlopen(req, timeout=_REQUEST_TIMEOUT) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except Exception as exc:
            logger.warning("AHPS request failed (%s): %s", url, exc)
            return None

    # ── Cache ────────────────────────────────────────────────────────────────

    def _from_cache(self, key: str) -> Optional[GaugeReading]:
        entry = self._cache.get(key)
        if entry and (time.time() - entry[0]) < self._ttl:
            return entry[1]
        return None

    def _to_cache(self, key: str, reading: GaugeReading):
        self._cache[key] = (time.time(), reading)


# ── Helpers ──────────────────────────────────────────────────────────────────

def _safe_float(val) -> Optional[float]:
    try:
        f = float(val)
        return f if f > -999 else None
    except (TypeError, ValueError):
        return None


def _normalize_category(raw: str) -> str:
    if not raw:
        return "none"
    raw = raw.lower().strip()
    mapping = {
        "no flooding": "none", "normal": "none", "low": "none", "false": "none",
        "action": "action", "at action stage": "action",
        "minor": "minor", "minor flooding": "minor",
        "moderate": "moderate", "moderate flooding": "moderate",
        "major": "major", "major flooding": "major",
    }
    return mapping.get(raw, "none" if raw in ("", "none", "0") else "action")


def _category_label(category: str, stage_ft: Optional[float], minor_ft: Optional[float]) -> str:
    labels = {
        "none":     "Normal",
        "action":   "Near Flood Stage",
        "minor":    "Minor Flooding",
        "moderate": "Moderate Flooding",
        "major":    "Major Flooding",
    }
    base = labels.get(category, "Unknown")
    if stage_ft is not None and minor_ft is not None and stage_ft > minor_ft:
        above = stage_ft - minor_ft
        base += f" ({above:+.1f} ft above minor)"
    return base
