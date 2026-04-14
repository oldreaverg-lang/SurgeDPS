"""
AHPS Historical Gauge Fetcher
=============================
For historical storms, NWPS /gauges returns only *current* flood status — so
every archived event shows zero flooded gauges. This module reconstructs the
peak gauge reading per site *during the storm window* by pulling archived
15-min values from USGS NWIS IV, then classifies each peak against the
NWS flood thresholds for that site.

Output: GeoJSON FeatureCollection in the same shape `ahps_gauges.to_geojson`
produces, so the frontend doesn't need to change.

Auto-population:
    Called as a background job from the activation handler. The result is
    cached permanently on the Railway volume at
        {PERSISTENT_DIR}/cache/gauges_historical/{storm_id}.json
    and served directly on subsequent requests.

Data sources
------------
  * USGS NWIS Instantaneous Values  — archive back to ~2007 for most sites,
    earlier at a subset. Param `00065` = gauge height (ft).
        https://waterservices.usgs.gov/nwis/iv/?format=json&bBox=W,S,E,N
        &startDT=YYYY-MM-DD&endDT=YYYY-MM-DD&parameterCd=00065
  * NWPS /gauges/{lid}  — flood thresholds (action/minor/moderate/major).
    Cached in a shared per-site JSON on the volume so we don't re-fetch
    thresholds for the same station across storms.
"""

from __future__ import annotations

import json
import logging
import os
import time
import threading
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

_NWIS_BASE = "https://waterservices.usgs.gov/nwis/iv"
_NWPS_BASE = "https://api.water.noaa.gov/nwps/v1"
_REQUEST_TIMEOUT = 25
_USER_AGENT = "SurgeDPS/1.0 (surgedps.com) historical"

_CAT_RANK = {"none": 0, "action": 1, "minor": 2, "moderate": 3, "major": 4}

# Per-storm window overrides (days after landfall). Defaults to 3.
# Widened for slow/stalling storms where flooding persisted well after landfall.
_STORM_WINDOW_OVERRIDES: Dict[str, int] = {
    "harvey_2017":  7,   # stalled over Houston for ~5 days
    "florence_2018": 6,  # extreme inland rainfall, slow exit
    "helene_2024":   5,  # major Appalachian flooding days after landfall
    "ida_2021":      5,  # NE flash flooding days later
}

# In-process lock around the shared USGS threshold cache file — otherwise two
# threads populating different storms could race and corrupt the file.
_THRESHOLDS_LOCK = threading.Lock()


@dataclass
class _SitePeak:
    site_no: str
    site_name: str
    lat: float
    lon: float
    peak_stage_ft: Optional[float]
    peak_time: Optional[str]    # ISO UTC


# ─── Public API ──────────────────────────────────────────────────────────


def fetch_historical_gauges(
    storm_id: str,
    landfall_lat: float,
    landfall_lon: float,
    landfall_date: str,     # "YYYY-MM-DD"
    radius_deg: float = 4.0,
    persistent_dir: str = "",
) -> dict:
    """
    Fetch and cache historical peak-stage gauges for one storm.

    Idempotent: if the cache file already exists *and* represents a
    successful fetch (no ``_fetch_error`` flag), returns its content.
    Error-flagged caches are treated as misses so the next request
    automatically retries the upstream USGS / NWPS calls.

    Args:
        storm_id:        e.g. "harvey_2017"
        landfall_lat/lon: storm center at landfall
        landfall_date:   "YYYY-MM-DD" (UTC)
        radius_deg:      search bbox half-width in decimal degrees
        persistent_dir:  Railway volume root. Defaults to env PERSISTENT_DIR
                         or "./data".

    Returns:
        Dict with keys {storm_id, gauge_count, at_or_above_major,
        at_or_above_moderate, at_or_above_minor, gauges: GeoJSON FC,
        _cached_at}.  Same shape as the live /api/gauges response.
    """
    persistent_dir = persistent_dir or os.environ.get("PERSISTENT_DIR", "./data")
    cache_dir = os.path.join(persistent_dir, "cache", "gauges_historical")
    os.makedirs(cache_dir, exist_ok=True)
    cache_path = os.path.join(cache_dir, f"{storm_id}.json")

    # Serve cached copy if present — but skip caches that represent a failed
    # or incomplete fetch.  Two cases require a retry:
    #
    # 1. New caches written with ``_fetch_error: true`` — upstream NWIS call
    #    failed during warm_cache; these must not be served as "no gauges".
    #
    # 2. Legacy caches written before the ``_fetch_error`` field was added
    #    (i.e., the field is absent) that have gauge_count == 0.  These were
    #    written when NWIS returned zero timeSeries for the bbox (most likely
    #    because the old URL lacked ``siteType=ST`` causing an oversize
    #    request to be silently truncated or rate-limited).  Re-fetching with
    #    the corrected URL will recover actual gauge data.
    if os.path.exists(cache_path):
        try:
            with open(cache_path) as f:
                cached = json.load(f)
            has_error_field = "_fetch_error" in cached
            is_error = cached.get("_fetch_error", False)
            is_legacy_empty = not has_error_field and cached.get("gauge_count", -1) == 0
            if not is_error and not is_legacy_empty:
                return cached
            reason = "_fetch_error flag set" if is_error else "legacy 0-gauge cache (pre-siteType=ST fix)"
            logger.info("historical gauges cache for %s needs refresh (%s) — refetching",
                        storm_id, reason)
        except Exception as e:
            logger.warning("historical gauges cache corrupt for %s: %s — refetching", storm_id, e)

    # Compute event window
    window_days = _STORM_WINDOW_OVERRIDES.get(storm_id, 3)
    try:
        landfall_dt = datetime.strptime(landfall_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError:
        logger.error("Bad landfall_date %r for %s", landfall_date, storm_id)
        return _empty_response(storm_id)
    start_dt = landfall_dt - timedelta(days=1)          # 1 day pre-landfall
    end_dt   = landfall_dt + timedelta(days=window_days)

    # 1) Fetch all NWIS IV site-peaks in the bbox during the window
    site_peaks, nwis_fetch_ok = _fetch_nwis_iv_peaks(
        lon_min=landfall_lon - radius_deg,
        lat_min=landfall_lat - radius_deg,
        lon_max=landfall_lon + radius_deg,
        lat_max=landfall_lat + radius_deg,
        start=start_dt,
        end=end_dt,
    )
    logger.info("USGS NWIS: %d site-peaks for %s in window %s/%s (fetch_ok=%s)",
                len(site_peaks), storm_id,
                start_dt.date(), end_dt.date(), nwis_fetch_ok)

    # 2) For each site, look up NWS flood thresholds (shared cache on volume)
    thresholds_cache = _load_thresholds_cache(persistent_dir)
    features = []
    counts = {"major": 0, "moderate": 0, "minor": 0}
    for sp in site_peaks:
        if sp.peak_stage_ft is None:
            continue
        thr = _get_site_thresholds(sp.site_no, thresholds_cache, persistent_dir)
        category, pct_above_minor = _classify_peak(sp.peak_stage_ft, thr)
        if category in counts:
            counts[category] += 1
        features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [sp.lon, sp.lat]},
            "properties": {
                "site_id": sp.site_no,
                "site_name": sp.site_name,
                "stage_ft": round(sp.peak_stage_ft, 2),
                "obs_time": sp.peak_time,
                "flood_category": category,
                "status_label": _status_label(category, sp.peak_stage_ft, thr.get("minor")),
                "action_stage_ft":   thr.get("action"),
                "minor_flood_ft":    thr.get("minor"),
                "moderate_flood_ft": thr.get("moderate"),
                "major_flood_ft":    thr.get("major"),
                "record_stage_ft":   thr.get("record"),
                "pct_above_minor":   pct_above_minor,
                "historical":        True,
                "storm_id":          storm_id,
                "window_start":      start_dt.isoformat(),
                "window_end":        end_dt.isoformat(),
                "layer":             "stream_gauges",
            },
        })

    # Mark the response with _fetch_error if NWIS returned nothing useful.
    # This lets the cache-serve logic at the top retry on the next request
    # rather than permanently serving zeros.
    fetch_error = not nwis_fetch_ok and len(features) == 0

    response = {
        "storm_id": storm_id,
        "gauge_count": len(features),
        "at_or_above_major":    counts["major"],
        "at_or_above_moderate": counts["major"] + counts["moderate"],
        "at_or_above_minor":    counts["major"] + counts["moderate"] + counts["minor"],
        "gauges": {"type": "FeatureCollection", "features": features},
        "source": "usgs_nwis_iv_historical",
        "window_days": window_days,
        "_cached_at": datetime.now(timezone.utc).isoformat(),
        "_fetch_error": fetch_error,
    }

    # 3) Atomic write to volume
    try:
        tmp = f"{cache_path}.tmp.{os.getpid()}.{threading.get_ident()}"
        with open(tmp, "w") as f:
            json.dump(response, f)
        os.replace(tmp, cache_path)
        logger.info("Wrote historical gauge cache → %s (%d gauges)",
                    cache_path, len(features))
    except Exception as e:
        logger.error("Failed to write gauge cache %s: %s", cache_path, e)

    return response


def cache_exists(storm_id: str, persistent_dir: str = "") -> bool:
    """True if a *successful* historical gauge cache for this storm is on disk.

    Returns False if the file is absent, unreadable, flagged with
    ``_fetch_error``, or is a legacy 0-gauge cache written before the
    ``siteType=ST`` URL fix (identified by a missing ``_fetch_error`` field
    combined with gauge_count == 0).  Both cases require a re-fetch.
    """
    persistent_dir = persistent_dir or os.environ.get("PERSISTENT_DIR", "./data")
    path = os.path.join(persistent_dir, "cache", "gauges_historical", f"{storm_id}.json")
    if not os.path.exists(path):
        return False
    try:
        with open(path) as f:
            data = json.load(f)
        has_error_field = "_fetch_error" in data
        if data.get("_fetch_error", False):
            return False
        if not has_error_field and data.get("gauge_count", -1) == 0:
            return False  # legacy empty cache — needs re-fetch with fixed URL
        return True
    except Exception:
        return False


# ─── USGS NWIS fetcher ───────────────────────────────────────────────────


def _fetch_nwis_iv_peaks(
    lon_min: float, lat_min: float, lon_max: float, lat_max: float,
    start: datetime, end: datetime,
) -> Tuple[List[_SitePeak], bool]:
    """
    Pull gauge-height (param 00065) time series for every NWIS stream-gauge
    site in the bbox over [start, end] and return (per-site peaks, fetch_ok).

    Returns:
        peaks    — list of _SitePeak (may be empty on success with no sites)
        fetch_ok — True if NWIS returned a parseable response (even if empty);
                   False if the HTTP request failed or returned an error body.

    Notes:
        • ``siteType=ST`` restricts results to stream gauges only (omitting
          groundwater wells, atmospheric, etc.) which prevents the NWIS API
          from silently truncating responses when the bbox contains thousands
          of non-stream sites.
        • NWIS IV bbox must be west,south,east,north (decimal degrees).
        • Window must be ≤ 120 days; ours is at most ~8 days.
    """
    url = (
        f"{_NWIS_BASE}/?format=json"
        f"&bBox={lon_min:.4f},{lat_min:.4f},{lon_max:.4f},{lat_max:.4f}"
        f"&startDT={start.strftime('%Y-%m-%dT%H:%MZ')}"
        f"&endDT={end.strftime('%Y-%m-%dT%H:%MZ')}"
        f"&parameterCd=00065"
        f"&siteType=ST"       # stream gauges only — avoids 413/truncation on large bboxes
        f"&siteStatus=all"
    )
    data = _get_json(url)
    if data is None:
        # HTTP failure or unparseable body — caller will mark _fetch_error
        return [], False

    peaks: List[_SitePeak] = []
    ts_list = (data.get("value") or {}).get("timeSeries") or []
    logger.info("NWIS IV returned %d timeSeries entries for bbox %.4f,%.4f → %.4f,%.4f",
                len(ts_list), lon_min, lat_min, lon_max, lat_max)

    for ts in ts_list:
        try:
            src_info = ts.get("sourceInfo", {})
            site_no = (src_info.get("siteCode", [{}])[0] or {}).get("value", "")
            name = src_info.get("siteName", site_no)
            geo = src_info.get("geoLocation", {}).get("geogLocation", {})
            lat = float(geo.get("latitude"))
            lon = float(geo.get("longitude"))

            values = ((ts.get("values") or [{}])[0] or {}).get("value") or []
            peak_v: Optional[float] = None
            peak_t: Optional[str] = None
            for rec in values:
                try:
                    v = float(rec.get("value"))
                except (TypeError, ValueError):
                    continue
                if v < -900:      # NWIS sentinel for missing
                    continue
                if peak_v is None or v > peak_v:
                    peak_v = v
                    peak_t = rec.get("dateTime")

            peaks.append(_SitePeak(
                site_no=site_no, site_name=name, lat=lat, lon=lon,
                peak_stage_ft=peak_v, peak_time=peak_t,
            ))
        except Exception as e:
            logger.debug("Skipping malformed NWIS timeSeries entry: %s", e)

    return peaks, True  # fetch succeeded (even if 0 stream gauges in bbox)


# ─── NWPS threshold cache ────────────────────────────────────────────────


def _load_thresholds_cache(persistent_dir: str) -> Dict[str, Dict[str, float]]:
    path = os.path.join(persistent_dir, "cache", "gauges_historical",
                        "_nwps_thresholds.json")
    if not os.path.exists(path):
        return {}
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return {}


def _save_thresholds_cache(persistent_dir: str, cache: Dict[str, Dict[str, float]]):
    path = os.path.join(persistent_dir, "cache", "gauges_historical",
                        "_nwps_thresholds.json")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f"{path}.tmp.{os.getpid()}.{threading.get_ident()}"
    try:
        with open(tmp, "w") as f:
            json.dump(cache, f)
        os.replace(tmp, path)
    except Exception as e:
        logger.warning("thresholds cache write failed: %s", e)


def _get_site_thresholds(
    site_no: str,
    cache: Dict[str, Dict[str, float]],
    persistent_dir: str,
) -> Dict[str, Optional[float]]:
    """
    Resolve flood thresholds for a USGS site number.

    Strategy: NWPS stations are keyed by NWS location ID (lid), not USGS
    site_no. We use the NWPS metadata lookup which accepts USGS site numbers
    as an alias. Results are persisted in a shared _nwps_thresholds.json on
    the volume so every subsequent storm reuses them.
    """
    if site_no in cache:
        return cache[site_no]

    thr: Dict[str, Optional[float]] = {}
    url = f"{_NWPS_BASE}/gauges/{urllib.parse.quote(site_no)}"
    data = _get_json(url)
    if data and isinstance(data, dict):
        flood = data.get("flood", data.get("thresholds", {})) or {}
        for k in ("action", "minor", "moderate", "major", "record", "bankfull"):
            v = flood.get(k)
            try:
                thr[k] = float(v) if v is not None and float(v) > -900 else None
            except (TypeError, ValueError):
                thr[k] = None

    # Persist under the in-process lock — even a short NWPS outage where this
    # returns {} should be cached as "tried" so we don't re-hit per render.
    with _THRESHOLDS_LOCK:
        cache[site_no] = thr
        _save_thresholds_cache(persistent_dir, cache)

    return thr


# ─── Classification helpers ──────────────────────────────────────────────


def _classify_peak(
    stage_ft: float,
    thr: Dict[str, Optional[float]],
) -> Tuple[str, Optional[float]]:
    """Assign a flood category from a peak stage + threshold set."""
    major    = thr.get("major")
    moderate = thr.get("moderate")
    minor    = thr.get("minor")
    action   = thr.get("action")

    if major is not None and stage_ft >= major:
        cat = "major"
    elif moderate is not None and stage_ft >= moderate:
        cat = "moderate"
    elif minor is not None and stage_ft >= minor:
        cat = "minor"
    elif action is not None and stage_ft >= action:
        cat = "action"
    else:
        cat = "none"

    pct: Optional[float] = None
    if minor is not None and minor > 0:
        pct = round((stage_ft - minor) / minor * 100, 1)
    return cat, pct


def _status_label(category: str, stage_ft: float, minor: Optional[float]) -> str:
    labels = {
        "none":     "Normal",
        "action":   "Near Flood Stage",
        "minor":    "Minor Flooding",
        "moderate": "Moderate Flooding",
        "major":    "Major Flooding",
    }
    label = labels.get(category, "Unknown")
    if minor is not None and stage_ft > minor:
        label += f" ({stage_ft - minor:+.1f} ft above minor)"
    return label


def _empty_response(storm_id: str) -> dict:
    return {
        "storm_id": storm_id,
        "gauge_count": 0,
        "at_or_above_major": 0,
        "at_or_above_moderate": 0,
        "at_or_above_minor": 0,
        "gauges": {"type": "FeatureCollection", "features": []},
        "source": "usgs_nwis_iv_historical",
        "_cached_at": datetime.now(timezone.utc).isoformat(),
    }


# ─── HTTP ────────────────────────────────────────────────────────────────


def _get_json(url: str) -> Optional[dict]:
    """HTTP GET → parsed JSON, or None on any failure.

    Logs HTTP status code and truncated response body on non-200 so that
    operators can distinguish NWIS rate-limiting (429), server errors (5xx),
    and bad-request failures (400 / 414 URL too long) from true network
    timeouts.
    """
    import urllib.error as _ue
    try:
        req = urllib.request.Request(url, headers={"User-Agent": _USER_AGENT})
        with urllib.request.urlopen(req, timeout=_REQUEST_TIMEOUT) as resp:
            raw = resp.read()
            logger.debug("NWIS/NWPS %s → HTTP 200, %d bytes", url.split('?')[0], len(raw))
            return json.loads(raw.decode("utf-8"))
    except _ue.HTTPError as exc:
        body_preview = ""
        try:
            body_preview = exc.read(256).decode("utf-8", errors="replace")
        except Exception:
            pass
        logger.warning(
            "NWIS/NWPS HTTP %d for %s — %s — body: %s",
            exc.code, url.split('?')[0], exc.reason, body_preview,
        )
        return None
    except Exception as exc:
        logger.warning("NWIS/NWPS request failed (%s): %s",
                       url.split('?')[0], exc)
        return None
