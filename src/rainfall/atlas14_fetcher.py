"""
Atlas 14 / NOAA PFDS Return-Period Precipitation Lookup

NOAA Precipitation Frequency Data Server (PFDS) provides pre-computed
return-period precipitation estimates for CONUS from historical gauge
records and GEV frequency analysis (NOAA Atlas 14, Volumes 1–11).

The API is a free, unauthenticated HTTP endpoint.  One call per storm
centroid returns a table covering all standard durations (5-min …
60-day) × return periods (2-yr … 1000-yr).

API endpoint (text format):
  https://hdsc.nws.noaa.gov/cgi-bin/hdsc/new/fe_text_mean.sh
  ?lat={lat}&lon={lon}&data=depth&units=english&series=pds

Response: plain-text CSV with durations as rows and return periods as
columns.  Values are in inches (English units).

For tropical cyclones we compare the storm's estimated 24-hr and
72-hr total accumulation (from the Lonfat model) to the frequency
table to label the event as e.g. "~100-year rainfall".  This label
appears in the CAT Deployment Report and SitRep.

Caching: one JSON file per (lat°, lon°) degree cell, kept permanently.
Atlas 14 values don't change between storms.
"""

from __future__ import annotations

import json
import logging
import math
import os
import time
import urllib.request
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

_PFDS_BASE = "https://hdsc.nws.noaa.gov/cgi-bin/hdsc/new/fe_text_mean.sh"
_TIMEOUT_S = 15
_RETRY_MAX  = 2

# Return periods from the PFDS table (years), in ascending order
_RETURN_PERIODS = [2, 5, 10, 25, 50, 100, 200, 500, 1000]

# Duration labels as they appear in the PFDS text output
# Maps our canonical key → possible PFDS label(s)
_DURATION_ALIASES: Dict[str, List[str]] = {
    "24hr":  ["24-hr", "24hr", "24 hr"],
    "48hr":  ["48-hr", "48hr", "48 hr", "2-day", "2day", "2 day"],
    "72hr":  ["72-hr", "72hr", "72 hr", "3-day", "3day", "3 day"],
    "7day":  ["7-day", "7day", "7 day"],
}


# ── Output types ──────────────────────────────────────────────────────────────

@dataclass
class Atlas14Point:
    """Return-period precipitation depths for a lat/lon point."""
    lat: float
    lon: float
    # {duration_key: {return_period_str: depth_inches}}
    table: Dict[str, Dict[str, float]]
    source: str = "noaa_pfds"

    def depth_in(self, duration_key: str, return_period_yr: int) -> Optional[float]:
        """Return depth (inches) for a duration and return period."""
        rp_str = str(return_period_yr)
        row = self.table.get(duration_key, {})
        return row.get(rp_str)

    def depth_mm(self, duration_key: str, return_period_yr: int) -> Optional[float]:
        """Return depth (mm) for a duration and return period."""
        d = self.depth_in(duration_key, return_period_yr)
        return d * 25.4 if d is not None else None


@dataclass
class RainfallReturnPeriod:
    """Classified return period for a storm's observed rainfall."""
    label: str               # e.g. "~100-year", ">1000-year"
    return_period_yr: int    # Best-estimate return period
    duration_key: str        # Which duration window was matched ("24hr", "72hr")
    observed_mm: float       # Observed / modeled accumulation (mm)
    threshold_mm: float      # Atlas 14 threshold for the classified return period
    atlas14: Optional[Atlas14Point] = None
    notes: str = ""


# ── HTTP helper ───────────────────────────────────────────────────────────────

def _http_get(url: str, timeout: int = _TIMEOUT_S) -> Optional[str]:
    """Fetch plain-text URL; returns None on failure."""
    for attempt in range(_RETRY_MAX):
        try:
            req = urllib.request.Request(
                url,
                headers={"User-Agent": "SurgeDPS/1.0 (storm flood model)"},
            )
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.read().decode("utf-8", errors="replace")
        except Exception as exc:
            if attempt < _RETRY_MAX - 1:
                time.sleep(1.0)
            else:
                logger.debug("Atlas14 HTTP GET failed %s: %s", url, exc)
    return None


# ── PFDS text parser ──────────────────────────────────────────────────────────

def _parse_pfds_text(text: str) -> Optional[Dict[str, Dict[str, float]]]:
    """
    Parse the NOAA PFDS plain-text CSV into a nested dict.

    The PFDS output format (English units, PDS series):
        Recurrence Intervals (years)
        Duration,2,5,10,25,50,100,200,500,1000
        5-min,0.34,0.43,0.49,...
        ...
        24-hr,3.07,4.29,5.29,...
        2-day,3.69,5.07,...
        3-day,4.06,5.54,...

    Returns {duration_label: {return_period_str: depth_inches}} or None.
    """
    lines = [l.strip() for l in text.splitlines() if l.strip()]

    # Locate header row (contains "2" and "1000" as column headers)
    rp_labels: List[str] = []
    data_lines: List[str] = []
    header_found = False

    for i, line in enumerate(lines):
        parts = [p.strip() for p in line.split(",")]
        if not header_found:
            # Header row starts with "Duration" or similar and has numeric columns
            if len(parts) >= 5:
                numeric = sum(1 for p in parts[1:] if p.isdigit())
                if numeric >= 4:
                    rp_labels = parts[1:]
                    header_found = True
                    continue
        else:
            data_lines.append(line)

    if not rp_labels or not data_lines:
        return None

    result: Dict[str, Dict[str, float]] = {}
    for line in data_lines:
        parts = [p.strip() for p in line.split(",")]
        if len(parts) < len(rp_labels) + 1:
            continue
        dur_label = parts[0]
        row: Dict[str, float] = {}
        for j, rp in enumerate(rp_labels):
            try:
                row[rp] = float(parts[j + 1])
            except (ValueError, IndexError):
                pass
        if row:
            result[dur_label] = row

    return result if result else None


def _normalize_table(raw: Dict[str, Dict[str, float]]) -> Dict[str, Dict[str, float]]:
    """
    Re-key the raw PFDS table to our canonical duration keys.

    Input keys look like '24-hr', '3-day'; we want '24hr', '72hr', etc.
    Return periods are already strings like '2', '5', '10' → keep as-is.
    """
    canonical: Dict[str, Dict[str, float]] = {}
    for canon_key, aliases in _DURATION_ALIASES.items():
        for alias in aliases:
            for raw_key, row in raw.items():
                if raw_key.lower() == alias.lower():
                    canonical[canon_key] = row
                    break
            if canon_key in canonical:
                break
    return canonical


# ── Main fetch function ───────────────────────────────────────────────────────

def fetch_atlas14(
    lat: float,
    lon: float,
    cache_dir: Optional[str] = None,
) -> Optional[Atlas14Point]:
    """
    Fetch Atlas 14 precipitation frequency data for a lat/lon point.

    Results are cached at 1° resolution (Atlas 14 values are nearly
    constant across small areas and don't change between storms).

    Args:
        lat: Latitude of the storm centroid or point of interest.
        lon: Longitude (negative for western hemisphere).
        cache_dir: Directory to cache JSON results.

    Returns:
        Atlas14Point with the full frequency table, or None on failure.
    """
    # Cache key: round to nearest degree
    lat1 = round(lat, 0)
    lon1 = round(lon, 0)
    cache_file: Optional[str] = None

    if cache_dir:
        os.makedirs(cache_dir, exist_ok=True)
        cache_file = os.path.join(cache_dir, f"atlas14_{lat1:+.0f}_{lon1:+.0f}.json")
        if os.path.exists(cache_file):
            try:
                with open(cache_file) as f:
                    data = json.load(f)
                return Atlas14Point(
                    lat=data["lat"],
                    lon=data["lon"],
                    table=data["table"],
                    source="noaa_pfds_cache",
                )
            except Exception as exc:
                logger.warning("Atlas14 cache read failed: %s", exc)

    # Build PFDS URL
    url = (
        f"{_PFDS_BASE}"
        f"?lat={lat:.4f}&lon={lon:.4f}"
        f"&data=depth&units=english&series=pds"
    )
    logger.info("[Atlas14] Fetching %s", url)

    text = _http_get(url)
    if not text:
        logger.warning("[Atlas14] HTTP fetch failed for (%.2f, %.2f)", lat, lon)
        return None

    raw = _parse_pfds_text(text)
    if not raw:
        logger.warning("[Atlas14] Parse failed — response:\n%s", text[:400])
        return None

    table = _normalize_table(raw)
    if not table:
        # Still save the raw table under original keys
        table = {k: v for k, v in raw.items()}

    point = Atlas14Point(lat=lat, lon=lon, table=table)

    # Persist cache
    if cache_file:
        try:
            with open(cache_file, "w") as f:
                json.dump({"lat": lat, "lon": lon, "table": table}, f)
        except Exception as exc:
            logger.warning("Atlas14 cache write failed: %s", exc)

    return point


# ── Return-period classifier ──────────────────────────────────────────────────

def classify_storm_rainfall(
    observed_mm: float,
    duration_hr: float,
    atlas14: Optional[Atlas14Point],
    prefer_duration_key: str = "auto",
) -> RainfallReturnPeriod:
    """
    Classify a storm's total rainfall against Atlas 14 return periods.

    Args:
        observed_mm:       Storm total accumulation (mm) over `duration_hr`.
        duration_hr:       Duration of accumulation window (hours).
        atlas14:           Atlas14Point from fetch_atlas14(), or None.
        prefer_duration_key: Which duration row to use for comparison.
                           "auto" selects the best match (24hr, 48hr, 72hr).

    Returns:
        RainfallReturnPeriod with a human-readable label like "~100-year".
    """
    observed_in = observed_mm / 25.4  # mm → inches

    # Select duration key to compare against
    if prefer_duration_key == "auto":
        if duration_hr <= 30:
            dur_key = "24hr"
        elif duration_hr <= 60:
            dur_key = "48hr"
        else:
            dur_key = "72hr"
    else:
        dur_key = prefer_duration_key

    if atlas14 is None or not atlas14.table:
        return RainfallReturnPeriod(
            label="unknown (Atlas 14 unavailable)",
            return_period_yr=0,
            duration_key=dur_key,
            observed_mm=observed_mm,
            threshold_mm=0.0,
            notes="Atlas 14 data not available",
        )

    row = atlas14.table.get(dur_key) or atlas14.table.get("72hr") or {}
    if not row:
        # Fallback: try any available row
        for fallback_key in ["24hr", "48hr", "72hr", "7day"]:
            row = atlas14.table.get(fallback_key, {})
            if row:
                dur_key = fallback_key
                break

    if not row:
        return RainfallReturnPeriod(
            label="unknown (no matching duration)",
            return_period_yr=0,
            duration_key=dur_key,
            observed_mm=observed_mm,
            threshold_mm=0.0,
            notes=f"Duration key '{dur_key}' not in Atlas 14 table",
        )

    # Find bracketing return periods
    # Row may be keyed by string ("2", "5", "10", ...) or int
    sorted_rp = sorted(
        [(int(k), v) for k, v in row.items() if k.isdigit() and float(v) > 0],
        key=lambda x: x[0],
    )

    if not sorted_rp:
        return RainfallReturnPeriod(
            label="unknown",
            return_period_yr=0,
            duration_key=dur_key,
            observed_mm=observed_mm,
            threshold_mm=0.0,
            atlas14=atlas14,
        )

    # Check if observed exceeds the maximum return period in the table
    max_rp, max_val_in = sorted_rp[-1]
    if observed_in >= max_val_in:
        return RainfallReturnPeriod(
            label=f">{max_rp}-year",
            return_period_yr=max_rp,
            duration_key=dur_key,
            observed_mm=observed_mm,
            threshold_mm=max_val_in * 25.4,
            atlas14=atlas14,
            notes=f"Exceeds {max_rp}-year threshold ({max_val_in:.2f} in)",
        )

    # Check if below minimum return period
    min_rp, min_val_in = sorted_rp[0]
    if observed_in < min_val_in:
        return RainfallReturnPeriod(
            label=f"<{min_rp}-year",
            return_period_yr=min_rp,
            duration_key=dur_key,
            observed_mm=observed_mm,
            threshold_mm=min_val_in * 25.4,
            atlas14=atlas14,
            notes=f"Below {min_rp}-year threshold",
        )

    # Find the bracket
    best_rp = min_rp
    best_threshold_in = min_val_in
    for rp, val_in in sorted_rp:
        if observed_in >= val_in:
            best_rp = rp
            best_threshold_in = val_in
        else:
            break

    # Snap to nearest standard return period label
    label = f"~{best_rp}-year"

    return RainfallReturnPeriod(
        label=label,
        return_period_yr=best_rp,
        duration_key=dur_key,
        observed_mm=observed_mm,
        threshold_mm=best_threshold_in * 25.4,
        atlas14=atlas14,
        notes=(
            f"Observed {observed_in:.1f} in ≥ {best_rp}-yr threshold "
            f"({best_threshold_in:.1f} in) for {dur_key}"
        ),
    )


# ── Storm-area wrapper ────────────────────────────────────────────────────────

def get_return_period_for_storm(
    storm_lat: float,
    storm_lon: float,
    total_precip_mm: float,
    storm_speed_kt: float,
    cache_dir: Optional[str] = None,
) -> RainfallReturnPeriod:
    """
    High-level wrapper: classify a storm's rainfall as a return period event.

    Computes the effective accumulation duration from storm speed,
    fetches Atlas 14 for the storm centroid, and classifies.

    Args:
        storm_lat, storm_lon: Storm centroid coordinates.
        total_precip_mm: Peak total precipitation (mm) from Lonfat model.
        storm_speed_kt:  Storm forward speed (knots).
        cache_dir:       Directory for Atlas 14 cache JSON files.

    Returns:
        RainfallReturnPeriod ready for inclusion in CAT reports.
    """
    from flood_model.rainfall import estimate_storm_duration_hr

    duration_hr = estimate_storm_duration_hr(storm_speed_kt)

    atlas14 = fetch_atlas14(storm_lat, storm_lon, cache_dir=cache_dir)

    rp = classify_storm_rainfall(
        observed_mm=total_precip_mm,
        duration_hr=duration_hr,
        atlas14=atlas14,
    )
    logger.info(
        "[Atlas14] Storm (%.2f, %.2f): %.0f mm / %.0f hr → %s",
        storm_lat, storm_lon, total_precip_mm, duration_hr, rp.label,
    )
    return rp
