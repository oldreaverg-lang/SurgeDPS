"""
NWM Discharge Fetcher (HTTPS, no boto3)

Fetches National Water Model streamflow for NHDPlus reaches near a storm
without downloading multi-gigabyte CONUS NetCDF files.

Strategy
--------
1.  Use the AHPS gauge network (which we already query for flood stage) to
    identify the gaged reaches in the storm area.  Each gauge has a lat/lon
    we can resolve to an NHDPlus COMID via the USGS NLDI API.

2.  Pull the current observed flow (cfs) for each gauge from the NOAA NWPS
    v1 API.  This is the same API used by AHPSClient but we query the full
    stageflow timeseries to get the 6-hour-average discharge.

3.  Return a Dict[int, float] of {comid: discharge_cms} that run_hand_model()
    can consume directly.

Why not the S3 NWM files?
The noaa-nwm-pds bucket files cover all ~2.7M CONUS reaches per timestep
(~70 MB each).  We need data for ~20-100 reaches.  The gauge-based path
here is 5-10 lightweight JSON calls instead of a 70 MB download.

Limitation: discharge is only available where AHPS gauges exist.  Smaller
ungaged streams won't flood in the HAND model, which is correct conservatism
for v1.  A future enhancement (Atlas 14 + synthetic rating curves on all
NWM reaches) can fill the gaps.

APIs used (all free, no auth):
  USGS NLDI:  https://labs.waterdata.usgs.gov/api/nldi/linked-data/
  NOAA NWPS:  https://api.water.noaa.gov/nwps/v1/
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

# ── API base URLs ────────────────────────────────────────────────────────────
_NLDI_BASE  = "https://labs.waterdata.usgs.gov/api/nldi/linked-data"
_NWPS_BASE  = "https://api.water.noaa.gov/nwps/v1"
_TIMEOUT_S  = 10   # per-request timeout (seconds)
_RETRY_MAX  = 2
_RATE_DELAY = 0.1  # seconds between NLDI calls to be polite


# ── Data types ───────────────────────────────────────────────────────────────

@dataclass
class ReachDischarge:
    """NWM discharge for one NHDPlus reach."""
    comid: int
    discharge_cms: float      # m³/s
    source: str               # "ahps_observed" | "nwm_analysis" | "synthetic"
    site_id: str = ""         # AHPS site_id that provided this reading
    site_name: str = ""


@dataclass
class NWMResult:
    """Collection of reach-level discharge values for a storm area."""
    reaches: List[ReachDischarge]
    peak_discharge_cms: float
    reach_count: int
    notes: str = ""

    def as_discharge_dict(self) -> Dict[int, float]:
        """Return {comid: discharge_cms} for run_hand_model()."""
        return {r.comid: r.discharge_cms for r in self.reaches}


# ── NLDI helpers ─────────────────────────────────────────────────────────────

def _http_get_json(url: str, timeout: int = _TIMEOUT_S) -> Optional[dict]:
    """Fetch JSON from a URL; returns None on failure."""
    for attempt in range(_RETRY_MAX):
        try:
            req = urllib.request.Request(
                url,
                headers={"User-Agent": "SurgeDPS/1.0 (storm flood model)"},
            )
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode())
        except Exception as exc:
            if attempt < _RETRY_MAX - 1:
                time.sleep(0.5)
            else:
                logger.debug("HTTP GET failed %s: %s", url, exc)
    return None


def resolve_comid_for_point(lat: float, lon: float) -> Optional[int]:
    """
    Use USGS NLDI to find the NHDPlus COMID for a lat/lon point.

    Returns the COMID of the NHDPlus flowline closest to the point,
    or None if the lookup fails.
    """
    # NLDI position query — returns the nearest NHDPlus COMID
    url = f"{_NLDI_BASE}/comid/position?coords=POINT({lon}%20{lat})"
    data = _http_get_json(url)
    if not data:
        return None

    # Response is GeoJSON FeatureCollection
    features = data.get("features", [])
    if not features:
        return None

    props = features[0].get("properties", {})
    comid = props.get("identifier") or props.get("comid")
    if comid is None:
        return None

    try:
        return int(comid)
    except (ValueError, TypeError):
        return None


def resolve_comids_for_gauges(
    gauges: List[dict],  # list of {site_id, lat, lon, flow_cfs, ...}
    cache_path: Optional[str] = None,
) -> Dict[str, int]:
    """
    Resolve NHDPlus COMIDs for a list of AHPS gauge dicts.

    Results are cached to disk so each gauge is only resolved once
    per deployment (COMIDs don't change).

    Args:
        gauges: list of gauge dicts with at least {site_id, lat, lon}
        cache_path: JSON file to cache site_id → comid mappings

    Returns:
        Dict mapping site_id → comid
    """
    # Load existing cache
    cache: Dict[str, int] = {}
    if cache_path and os.path.exists(cache_path):
        try:
            with open(cache_path) as f:
                cache = json.load(f)
        except Exception:
            pass

    new_lookups = 0
    for g in gauges:
        sid = g.get("site_id", "")
        if not sid or sid in cache:
            continue

        lat = g.get("lat")
        lon = g.get("lon")
        if lat is None or lon is None:
            continue

        comid = resolve_comid_for_point(lat, lon)
        if comid:
            cache[sid] = comid
            new_lookups += 1

        time.sleep(_RATE_DELAY)  # be polite to NLDI

    # Persist updated cache
    if new_lookups > 0 and cache_path:
        try:
            os.makedirs(os.path.dirname(cache_path), exist_ok=True)
            with open(cache_path, "w") as f:
                json.dump(cache, f)
        except Exception as exc:
            logger.warning("Failed to save COMID cache: %s", exc)

    return cache


# ── NWPS stageflow ───────────────────────────────────────────────────────────

def _fetch_gauge_flow_cfs(site_id: str) -> Optional[float]:
    """
    Fetch the most recent observed discharge (cfs) for an AHPS gauge
    from the NOAA NWPS v1 API stageflow endpoint.

    Falls back to None if the API doesn't have flow data for this gauge.
    """
    url = f"{_NWPS_BASE}/gauges/{site_id}/stageflow"
    data = _http_get_json(url)
    if not data:
        return None

    # Response: {"observed": {"data": [{"stage": ..., "flow": ...}, ...]}, ...}
    observed = data.get("observed", {}) or {}
    series = observed.get("data", []) or []

    for point in reversed(series):  # most-recent last
        flow = point.get("flow")
        if flow is not None:
            try:
                return float(flow)
            except (ValueError, TypeError):
                pass

    return None


# ── Main entry point ─────────────────────────────────────────────────────────

def fetch_nwm_discharge(
    gauges: List[dict],
    comid_cache_path: Optional[str] = None,
    min_discharge_cms: float = 1.0,
) -> NWMResult:
    """
    Fetch NWM-equivalent discharge for reaches near a storm.

    Uses AHPS observed flow as a proxy for NWM discharge at gaged
    reaches.  For each AHPS gauge we:
      1. Resolve the NHDPlus COMID for the gauge location (NLDI)
      2. Pull the latest observed flow (NWPS stageflow API)
      3. Convert cfs → m³/s

    Args:
        gauges: List of gauge dicts from AHPSClient.to_geojson() features,
                each with {site_id, lat, lon, flow_cfs, ...}.
                Also accepts GaugeReading objects with those attributes.
        comid_cache_path: Path to persist site_id → comid mappings.
        min_discharge_cms: Ignore reaches with less than this flow.

    Returns:
        NWMResult with discharge per COMID ready for run_hand_model().
    """
    if not gauges:
        return NWMResult(reaches=[], peak_discharge_cms=0.0,
                         reach_count=0, notes="No gauges provided")

    # Normalise: accept dicts or GaugeReading-like objects
    gauge_list: List[dict] = []
    for g in gauges:
        if isinstance(g, dict):
            gauge_list.append(g)
        else:
            gauge_list.append({
                "site_id":  getattr(g, "site_id", ""),
                "site_name": getattr(g, "site_name", ""),
                "lat":      getattr(g, "lat", None),
                "lon":      getattr(g, "lon", None),
                "flow_cfs": getattr(g, "flow_cfs", None),
            })

    logger.info("[NWM] Resolving COMIDs for %d gauges", len(gauge_list))
    site_to_comid = resolve_comids_for_gauges(gauge_list, comid_cache_path)

    if not site_to_comid:
        return NWMResult(reaches=[], peak_discharge_cms=0.0,
                         reach_count=0, notes="NLDI COMID lookup returned nothing")

    reaches: List[ReachDischarge] = []
    comid_seen: set = set()

    for g in gauge_list:
        sid = g.get("site_id", "")
        comid = site_to_comid.get(sid)
        if comid is None or comid in comid_seen:
            continue

        # Use preloaded flow_cfs if available, else re-query NWPS
        flow_cfs = g.get("flow_cfs")
        source = "ahps_observed"

        if flow_cfs is None:
            flow_cfs = _fetch_gauge_flow_cfs(sid)
            time.sleep(_RATE_DELAY)

        if flow_cfs is None:
            logger.debug("[NWM] No flow for gauge %s — skipping", sid)
            continue

        discharge_cms = float(flow_cfs) * 0.0283168  # cfs → m³/s
        if discharge_cms < min_discharge_cms:
            continue

        comid_seen.add(comid)
        reaches.append(ReachDischarge(
            comid=comid,
            discharge_cms=discharge_cms,
            source=source,
            site_id=sid,
            site_name=g.get("site_name", ""),
        ))

    peak = max((r.discharge_cms for r in reaches), default=0.0)

    logger.info(
        "[NWM] %d reaches with discharge data, peak=%.1f m³/s",
        len(reaches), peak,
    )

    notes = (
        f"{len(reaches)} gaged reaches; {len(gauge_list) - len(reaches)} "
        f"gauges skipped (no COMID or no flow data)"
    )

    return NWMResult(
        reaches=reaches,
        peak_discharge_cms=peak,
        reach_count=len(reaches),
        notes=notes,
    )


# ── Storm-area convenience wrapper ───────────────────────────────────────────

def fetch_nwm_for_cell(
    lon_min: float,
    lat_min: float,
    lon_max: float,
    lat_max: float,
    landfall_lat: float,
    landfall_lon: float,
    nwm_cache_dir: str,
    storm_id: str,
    col: int,
    row: int,
    radius_deg: float = 4.0,
    min_flood_category: str = "action",
    cache_ttl_seconds: int = 1800,
) -> Optional[NWMResult]:
    """
    High-level wrapper: get NWM discharge for a cell bounding box.

    1. Checks disk cache (JSON) — returns cached result if fresh enough.
    2. Calls AHPSClient to get active flood gauges near the storm.
    3. Fetches discharge for gaged reaches via NLDI + NWPS.
    4. Writes result to cache.

    The result is None if no gauges are found or NLDI/NWPS are unreachable.
    """
    import time

    cell_cache_dir = os.path.join(nwm_cache_dir, storm_id)
    os.makedirs(cell_cache_dir, exist_ok=True)
    cache_file = os.path.join(cell_cache_dir, f"discharge_{col}_{row}.json")
    comid_cache = os.path.join(nwm_cache_dir, "site_comid_cache.json")

    # Check disk cache
    if os.path.exists(cache_file):
        mtime = os.path.getmtime(cache_file)
        if time.time() - mtime < cache_ttl_seconds:
            try:
                with open(cache_file) as f:
                    data = json.load(f)
                reaches = [ReachDischarge(**r) for r in data["reaches"]]
                peak = data.get("peak_discharge_cms", 0.0)
                logger.info(
                    "[NWM] Cache hit for cell (%d,%d) — %d reaches",
                    col, row, len(reaches),
                )
                return NWMResult(
                    reaches=reaches,
                    peak_discharge_cms=peak,
                    reach_count=len(reaches),
                    notes=data.get("notes", "from cache"),
                )
            except Exception as exc:
                logger.warning("[NWM] Cache read failed: %s", exc)

    # Fetch from APIs
    try:
        from rainfall.ahps_gauges import AHPSClient
        client = AHPSClient(cache_ttl_seconds=300)
        gauges = client.get_gauges_for_storm(
            landfall_lat=landfall_lat,
            landfall_lon=landfall_lon,
            radius_deg=radius_deg,
            min_flood_category=min_flood_category,
        )
    except Exception as exc:
        logger.warning("[NWM] AHPS gauge fetch failed: %s", exc)
        return None

    if not gauges:
        logger.info("[NWM] No active flood gauges in storm area")
        return None

    # Convert GaugeReading objects to dicts
    gauge_dicts = [
        {
            "site_id":  g.site_id,
            "site_name": g.site_name,
            "lat":       g.lat,
            "lon":       g.lon,
            "flow_cfs":  g.flow_cfs,
        }
        for g in gauges
    ]

    result = fetch_nwm_discharge(gauge_dicts, comid_cache_path=comid_cache)

    # Persist to cache
    if result.reaches:
        try:
            payload = {
                "reaches": [
                    {
                        "comid": r.comid,
                        "discharge_cms": r.discharge_cms,
                        "source": r.source,
                        "site_id": r.site_id,
                        "site_name": r.site_name,
                    }
                    for r in result.reaches
                ],
                "peak_discharge_cms": result.peak_discharge_cms,
                "notes": result.notes,
            }
            with open(cache_file, "w") as f:
                json.dump(payload, f)
        except Exception as exc:
            logger.warning("[NWM] Cache write failed: %s", exc)

    return result
