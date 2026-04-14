"""
USGS Short-Term Network (STN) High Water Mark Fetcher

High Water Marks (HWMs) are the single best ground-truth source for
storm surge validation. USGS field crews deploy after every major
coastal event and survey debris lines, mud lines, and seed lines with
RTK GPS. Each mark records:

  - Latitude/longitude (sub-meter accuracy)
  - Water surface elevation (ft, NAVD88)
  - Height above ground (ft)
  - Quality rating (Excellent / Good / Fair / Poor)
  - HWM type (debris, mud line, seed, mud splash, etc.)

For Hurricane Michael (2018), USGS collected ~220 HWMs along the FL
Panhandle — enough for robust spatial validation of the surge model.

API Reference:
  Base: https://stn.wim.usgs.gov/STNServices/
  Events: /Events.json
  HWMs by event: /Events/{event_id}/HWMs.json

Known Event IDs:
  Michael 2018:   193
  Florence 2018:  190
  Harvey 2017:    180
  Irma 2017:      182
  Ian 2022:       330
  Ida 2021:       303
  Laura 2020:     260
  Sally 2020:     261
  Matthew 2016:   153
  Helene 2024:    >= 400  (verify via /Events.json)
  Milton 2024:    >= 400

Output is cached to parquet for fast re-reads across validation runs.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional

import requests

logger = logging.getLogger(__name__)


STN_BASE = "https://stn.wim.usgs.gov/STNServices"

# Curated event IDs for known storms. Discovered via the /Events.json
# endpoint and frozen here so validation runs are reproducible.
# Verified against STN /Events.json 2026-04-14.
STORM_EVENT_IDS: Dict[str, int] = {
    "michael_2018": 287,
    # The following are placeholders — verify via /Events.json before use.
    # To discover: curl .../Events.json | grep -i <name>
    # "matthew_2016": 0,
    # "harvey_2017": 0,
    # "irma_2017": 0,
    # "florence_2018": 0,
    # "laura_2020": 0,
    # "sally_2020": 0,
    # "ida_2021": 0,
    # "ian_2022": 0,
}

# STN quality ID → name (from /HWMQualities.json, frozen 2026-04-14)
QUALITY_NAME_BY_ID: Dict[int, str] = {
    1: "Excellent",
    2: "Good",
    3: "Fair",
    4: "Poor",
    5: "VeryPoor",
    6: "Unknown",
}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Data Classes
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


@dataclass
class HighWaterMark:
    """A single USGS high water mark."""

    hwm_id: int
    storm_id: str
    latitude: float
    longitude: float

    # Water surface elevation in feet above NAVD88
    # (USGS's primary measurement — survey-grade)
    elev_ft: Optional[float] = None

    # Depth above ground in feet (when collected)
    height_above_gnd_ft: Optional[float] = None

    # Quality rating: "Excellent" (~0.1 ft), "Good" (~0.2 ft),
    # "Fair" (~0.4 ft), "Poor" (> 0.4 ft)
    quality: str = "Unknown"

    # Type of mark: "Debris", "Mud", "Seed line", "Mud splash", etc.
    hwm_type: str = "Unknown"

    # Environment: "Coastal", "Riverine", "Lacustrine"
    environment: str = "Unknown"

    # Locale / municipality (for reporting)
    locale: str = ""

    # STN site/sensor id (for cross-reference to deployed sensors)
    site_id: Optional[int] = None

    # Date/time of survey (ISO string)
    survey_date: str = ""

    # Notes from the field crew
    notes: str = ""

    def to_dict(self) -> dict:
        return asdict(self)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Fetcher
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def _safe_float(value) -> Optional[float]:
    """Coerce to float; return None on null/empty."""
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_hwm_record(storm_id: str, record: dict) -> HighWaterMark:
    """Normalize a raw STN HWM record into a HighWaterMark."""
    q_id = record.get("hwm_quality_id")
    quality_name = QUALITY_NAME_BY_ID.get(q_id, "Unknown") if q_id else "Unknown"

    return HighWaterMark(
        hwm_id=int(record.get("hwm_id") or 0),
        storm_id=storm_id,
        latitude=float(record.get("latitude_dd") or 0.0),
        longitude=float(record.get("longitude_dd") or 0.0),
        elev_ft=_safe_float(record.get("elev_ft")),
        height_above_gnd_ft=_safe_float(record.get("height_above_gnd")),
        quality=quality_name,
        hwm_type=str(record.get("hwm_type_id") or "Unknown"),
        environment=record.get("hwm_environment") or "Unknown",
        locale=record.get("hwm_locationdescription") or "",
        site_id=record.get("site_id"),
        survey_date=record.get("survey_date") or "",
        notes=(record.get("hwm_notes") or "").strip()[:500],
    )


def fetch_hwms(
    storm_id: str,
    event_id: Optional[int] = None,
    timeout: int = 60,
) -> List[HighWaterMark]:
    """
    Fetch all USGS HWMs for a storm from the STN API.

    Args:
        storm_id: Our canonical storm key (e.g. "michael_2018")
        event_id: STN event ID. If None, looked up from STORM_EVENT_IDS.
        timeout: HTTP timeout in seconds.

    Returns:
        List of HighWaterMark records. May be empty on API failure.
    """
    if event_id is None:
        event_id = STORM_EVENT_IDS.get(storm_id)
        if event_id is None:
            logger.warning(
                f"No STN event_id known for {storm_id}; "
                f"add to STORM_EVENT_IDS or pass event_id explicitly"
            )
            return []

    url = f"{STN_BASE}/Events/{event_id}/HWMs.json"
    logger.info(f"Fetching HWMs for {storm_id} from {url}")

    try:
        resp = requests.get(url, timeout=timeout)
        resp.raise_for_status()
        payload = resp.json()
    except Exception as exc:
        logger.error(f"STN HWM fetch failed for {storm_id}: {exc}")
        return []

    if not isinstance(payload, list):
        logger.error(f"Unexpected STN response type for {storm_id}: {type(payload)}")
        return []

    hwms = [_parse_hwm_record(storm_id, rec) for rec in payload]
    logger.info(f"Fetched {len(hwms)} HWMs for {storm_id}")
    return hwms


def filter_quality(
    hwms: List[HighWaterMark],
    min_quality: str = "Fair",
) -> List[HighWaterMark]:
    """
    Drop HWMs below a quality threshold.

    Default "Fair" keeps Excellent + Good + Fair (excludes Poor and
    Unknown). Use "Good" for tighter validation, "Poor" to keep
    everything.
    """
    rank = {
        "Excellent": 4, "Good": 3, "Fair": 2, "Poor": 1,
        "VeryPoor": 0, "Unknown": -1,
    }
    threshold = rank.get(min_quality, 2)
    return [h for h in hwms if rank.get(h.quality, -1) >= threshold]


def filter_coastal(hwms: List[HighWaterMark]) -> List[HighWaterMark]:
    """Keep only coastal HWMs (exclude riverine, lacustrine)."""
    return [h for h in hwms if "Coastal" in (h.environment or "")]


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Caching
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def cache_path(storm_id: str, cache_dir: str = "data/validation/hwms") -> str:
    os.makedirs(cache_dir, exist_ok=True)
    return os.path.join(cache_dir, f"{storm_id}_hwms.csv")


def save_hwms(
    hwms: List[HighWaterMark],
    storm_id: str,
    cache_dir: str = "data/validation/hwms",
) -> str:
    """Persist HWMs to CSV. Returns the output path."""
    import csv

    path = cache_path(storm_id, cache_dir)
    if not hwms:
        # Still write an empty file with headers so downstream code
        # can distinguish "fetched 0" from "never fetched".
        fields = [f for f in HighWaterMark.__dataclass_fields__]
    else:
        fields = list(hwms[0].to_dict().keys())

    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for h in hwms:
            w.writerow(h.to_dict())
    logger.info(f"Saved {len(hwms)} HWMs → {path}")
    return path


def load_hwms(
    storm_id: str,
    cache_dir: str = "data/validation/hwms",
) -> List[HighWaterMark]:
    """Load HWMs from CSV cache. Returns [] if not cached."""
    import csv

    path = cache_path(storm_id, cache_dir)
    if not os.path.exists(path):
        return []

    # Figure out which fields are numeric vs string from the dataclass
    numeric_float = {"latitude", "longitude", "elev_ft", "height_above_gnd_ft"}
    numeric_int = {"hwm_id", "site_id"}

    out: List[HighWaterMark] = []
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            kw = {}
            for k, v in row.items():
                if v == "" or v == "None":
                    kw[k] = None
                elif k in numeric_float:
                    try:
                        kw[k] = float(v)
                    except ValueError:
                        kw[k] = None
                elif k in numeric_int:
                    try:
                        kw[k] = int(v)
                    except ValueError:
                        kw[k] = None
                else:
                    kw[k] = v
            # Required fields
            if kw.get("latitude") is None or kw.get("longitude") is None:
                continue
            out.append(HighWaterMark(**kw))
    return out


def fetch_or_load(
    storm_id: str,
    cache_dir: str = "data/validation/hwms",
    force: bool = False,
) -> List[HighWaterMark]:
    """Load from cache if present; otherwise fetch + cache."""
    if not force:
        cached = load_hwms(storm_id, cache_dir)
        if cached:
            logger.info(f"Loaded {len(cached)} HWMs from cache for {storm_id}")
            return cached

    hwms = fetch_hwms(storm_id)
    if hwms:
        save_hwms(hwms, storm_id, cache_dir)
    return hwms
