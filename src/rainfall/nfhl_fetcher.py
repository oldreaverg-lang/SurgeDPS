"""
FEMA NFHL (National Flood Hazard Layer) Fetcher
================================================
Fetches flood zone designations from the FEMA NFHL REST API and uses them
to classify surge vs. flood losses for insurance routing.

Why this matters (the legal split):
  - NFIP (National Flood Insurance Program) covers riverine and coastal flooding
    for properties in Special Flood Hazard Areas (Zone AE, VE, A, etc.)
  - Standard homeowners covers wind-driven rain and some water damage
  - Surge in coastal VE/AE zones → NFIP claim
  - Rainfall accumulation in Zone X (outside SFHA) → may be homeowners
  - The classification rule used here: building's FIRM flood zone determines
    which policy line "owns" the loss for insurance routing purposes.

API: FEMA NFHL Feature Service (ArcGIS REST)
  https://services.arcgis.com/P3ePLMYs2RVChkJx/arcgis/rest/services/USA_Flood_Hazard_Reduced_Set_gdb/FeatureServer/0/query
  Free, no auth, geometryType=esriGeometryPoint, spatial reference 4326.

Zones returned:
  - VE, V   → Coastal high-hazard (wave action) — NFIP
  - AE, AO, AH, A → Riverine SFHA — NFIP
  - X500    → 0.2% annual chance (500-yr) — limited coverage
  - X       → Outside SFHA — no NFIP requirement
  - D       → Undetermined

Usage:
    from rainfall.nfhl_fetcher import NFHLClient, classify_loss_mechanism

    client = NFHLClient()
    zone = client.get_zone(lat=29.76, lon=-95.37)   # → "AE"
    mechanism = classify_loss_mechanism(zone, surge_m=0.3, rainfall_m=0.5)
    # → "compound_nfip"   (in SFHA, both surge and rainfall present)
"""

from __future__ import annotations

import json
import logging
import time
import urllib.request
import urllib.parse
from typing import Optional, Tuple

logger = logging.getLogger(__name__)

_NFHL_BASE = (
    "https://services.arcgis.com/P3ePLMYs2RVChkJx/arcgis/rest/services"
    "/USA_Flood_Hazard_Reduced_Set_gdb/FeatureServer/0/query"
)
_REQUEST_TIMEOUT = 10
_USER_AGENT = "SurgeDPS/1.0 (surgedps.com)"

# Zone → risk tier mapping
# "sfha" = Special Flood Hazard Area (1% annual chance, NFIP mandatory purchase)
# "moderate" = 0.2% annual chance flood zone (500-yr)
# "minimal" = Zone X, outside SFHA
# "coastal" = Coastal high-hazard (wave action, NFIP)
_ZONE_TIER: dict[str, str] = {
    "VE": "coastal",    # Coastal, base flood elevations determined
    "V":  "coastal",    # Coastal, no base flood elevations
    "AE": "sfha",       # Riverine, base flood elevations on FIRM
    "AO": "sfha",       # Riverine, flood depths 1–3 feet
    "AH": "sfha",       # Ponding, flood depths 1–3 feet
    "A":  "sfha",       # Riverine, no base flood elevations
    "A99":"sfha",       # Protected by levee (federal project)
    "AR": "sfha",       # Flood insurance rate restored
    "X500": "moderate", # 500-year flood zone (0.2% annual chance)
    "X":  "minimal",    # Outside SFHA
    "D":  "unknown",    # Undetermined
}


class NFHLClient:
    """
    Client for FEMA NFHL flood zone lookups.

    Args:
        cache_ttl_seconds: Time-to-live for cached lookups. NFHL changes very
                           infrequently (FIRM revisions are years apart), so
                           a multi-hour TTL is reasonable. Default: 3600 (1 hr).
                           In production, use a persistent disk cache.
    """

    def __init__(self, cache_ttl_seconds: int = 3600):
        self._cache: dict[str, Tuple[float, Optional[str]]] = {}
        self._ttl = cache_ttl_seconds
        self._last_request = 0.0

    def get_zone(self, lat: float, lon: float) -> Optional[str]:
        """
        Return the FIRM flood zone designation at a given point.

        Args:
            lat: Latitude (decimal degrees, WGS84)
            lon: Longitude (decimal degrees, WGS84)

        Returns:
            Zone string like "AE", "VE", "X", or None if not found / API error.
        """
        cache_key = f"{lat:.5f},{lon:.5f}"
        cached = self._from_cache(cache_key)
        if cached is not None:
            return cached if cached != "__NONE__" else None

        zone = self._fetch_zone(lat, lon)
        self._to_cache(cache_key, zone if zone is not None else "__NONE__")
        return zone

    def get_zone_tier(self, lat: float, lon: float) -> str:
        """
        Return the risk tier for a point: "coastal", "sfha", "moderate", "minimal", "unknown".
        Falls back to "unknown" on API failure.
        """
        zone = self.get_zone(lat, lon)
        if zone is None:
            return "unknown"
        return _ZONE_TIER.get(zone.upper(), "minimal")

    def batch_zones(
        self, points: list[Tuple[float, float]]
    ) -> list[Optional[str]]:
        """
        Look up NFHL zones for a batch of (lat, lon) points.
        Rate-limits to 1 request per second to respect FEMA's servers.

        Args:
            points: List of (lat, lon) tuples.

        Returns:
            List of zone strings (parallel to input), None where unavailable.
        """
        results = []
        for lat, lon in points:
            results.append(self.get_zone(lat, lon))
        return results

    # ── Internal ─────────────────────────────────────────────────────────────

    def _fetch_zone(self, lat: float, lon: float) -> Optional[str]:
        """Single NFHL point query."""
        # Rate limit: 1 req/s to avoid hammering FEMA's ArcGIS servers
        elapsed = time.time() - self._last_request
        if elapsed < 1.0:
            time.sleep(1.0 - elapsed)
        self._last_request = time.time()

        params = urllib.parse.urlencode({
            "geometry": f"{lon},{lat}",
            "geometryType": "esriGeometryPoint",
            "inSR": "4326",
            "spatialRel": "esriSpatialRelIntersects",
            "outFields": "FLD_ZONE,ZONE_SUBTY,SFHA_TF",
            "returnGeometry": "false",
            "f": "json",
        })
        url = f"{_NFHL_BASE}?{params}"

        try:
            req = urllib.request.Request(url, headers={"User-Agent": _USER_AGENT})
            with urllib.request.urlopen(req, timeout=_REQUEST_TIMEOUT) as resp:
                data = json.loads(resp.read().decode("utf-8"))

            features = data.get("features", [])
            if not features:
                return "X"  # No FIRM coverage → treat as Zone X (outside SFHA)

            attrs = features[0].get("attributes", {})
            zone = attrs.get("FLD_ZONE", "X") or "X"
            subtype = attrs.get("ZONE_SUBTY", "")
            # Handle X500 (0.2% annual chance) stored as "0.2 PCT ANNUAL CHANCE"
            if zone == "X" and subtype and "0.2" in str(subtype):
                zone = "X500"
            return zone.upper().strip()

        except Exception as exc:
            logger.warning("NFHL lookup failed for (%.4f, %.4f): %s", lat, lon, exc)
            return None

    def _from_cache(self, key: str) -> Optional[str]:
        entry = self._cache.get(key)
        if entry and (time.time() - entry[0]) < self._ttl:
            return entry[1]
        return None

    def _to_cache(self, key: str, value: str):
        self._cache[key] = (time.time(), value)


# ── Loss classification ───────────────────────────────────────────────────────

def classify_loss_mechanism(
    flood_zone: Optional[str],
    surge_m: float = 0.0,
    rainfall_m: float = 0.0,
    wind_damage_pct: float = 0.0,
) -> str:
    """
    Classify what policy line "owns" the loss at a building.

    This is the rule used for CAT claim routing — determines whether a
    building's damage should be reported as a surge/flood claim (NFIP)
    or a wind/water claim (homeowners) or compound.

    Classification rules (in priority order):
      1. Coastal zone (VE/V) + any water → "surge_nfip"
      2. SFHA (AE/AO etc.) + surge depth → "surge_nfip"
      3. SFHA + rainfall only → "flood_nfip"
      4. Outside SFHA + rainfall → "pluvial_homeowners"
      5. Wind only → "wind_homeowners"
      6. Both SFHA flood and rainfall present → "compound_nfip"

    Args:
        flood_zone: NFHL zone string (e.g. "AE", "VE", "X"). None = unknown.
        surge_m: Surge depth at building (meters).
        rainfall_m: Rainfall-induced flood depth at building (meters).
        wind_damage_pct: Wind damage percentage (0-100).

    Returns:
        One of: "surge_nfip", "flood_nfip", "compound_nfip",
                "pluvial_homeowners", "wind_homeowners", "minimal", "unknown"
    """
    zone = (flood_zone or "X").upper().strip()
    tier = _ZONE_TIER.get(zone, "minimal")

    has_surge    = surge_m   > 0.05
    has_rainfall = rainfall_m > 0.05
    has_wind     = wind_damage_pct > 5.0

    if tier == "coastal":
        if has_surge and has_rainfall:
            return "compound_nfip"
        return "surge_nfip"

    if tier == "sfha":
        if has_surge and has_rainfall:
            return "compound_nfip"
        if has_surge:
            return "surge_nfip"
        if has_rainfall:
            return "flood_nfip"

    if tier in ("minimal", "moderate", "unknown"):
        if has_rainfall:
            return "pluvial_homeowners"
        if has_surge:
            # Surge reaching an X-zone building is unusual — flag it
            return "surge_nfip"

    if has_wind:
        return "wind_homeowners"

    return "minimal"
