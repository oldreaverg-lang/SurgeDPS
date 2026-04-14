"""
Pointwise Bathtub Model

A storage-free variant of the bathtub flood model that computes modeled
depth at a discrete list of (lat, lon) points rather than on a raster
grid. Useful for:

  - Validation against observation networks (USGS HWMs, tide gauges)
    when a full-extent DEM is not yet staged.
  - Rapid sensitivity testing — iterate on a surge profile and re-score
    in seconds without rebuilding a raster.
  - Comparing the baseline bathtub to more sophisticated models on
    the exact same observation set.

Inputs per point:
  - Ground elevation at the point (NAVD88 ft)
  - Modeled water surface elevation at the point (NAVD88 ft)

Depth:  max(0, WSE - ground_elev)

Ground elevation is fetched from USGS Elevation Point Query Service
(EPQS), which is free, accurate (~1m 3DEP-derived), and requires no
local raster storage. Results are cached per-point to data/cache/epqs
so repeat runs are instant.
"""

from __future__ import annotations

import csv
import hashlib
import logging
import math
import os
import time
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Tuple

import requests

from validation.spatial_sampler import SampledObservation, _classify_contingency

logger = logging.getLogger(__name__)


EPQS_URL = "https://epqs.nationalmap.gov/v1/json"
EPQS_CACHE = "data/cache/epqs_elevations.csv"
M_TO_FT = 3.280839895


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# EPQS Ground Elevation Lookup (with CSV cache)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def _cache_key(lat: float, lon: float) -> str:
    """Stable hash for a coordinate (6-decimal precision ~10cm)."""
    return hashlib.sha1(f"{lat:.6f},{lon:.6f}".encode()).hexdigest()[:16]


def _load_epqs_cache() -> Dict[str, float]:
    if not os.path.exists(EPQS_CACHE):
        return {}
    cache: Dict[str, float] = {}
    with open(EPQS_CACHE, newline="") as f:
        for row in csv.DictReader(f):
            try:
                cache[row["key"]] = float(row["elev_ft"])
            except (ValueError, KeyError):
                pass
    return cache


def _save_epqs_cache(cache: Dict[str, float]) -> None:
    os.makedirs(os.path.dirname(EPQS_CACHE), exist_ok=True)
    with open(EPQS_CACHE, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["key", "elev_ft"])
        w.writeheader()
        for k, v in cache.items():
            w.writerow({"key": k, "elev_ft": v})


def fetch_ground_elevation_ft(
    lat: float,
    lon: float,
    cache: Optional[Dict[str, float]] = None,
    retries: int = 2,
    timeout: int = 15,
) -> Optional[float]:
    """
    Return ground elevation at (lat, lon) in feet NAVD88, via USGS EPQS.

    Uses an in-process cache dict plus a CSV-backed persistent cache.
    Returns None if the service fails or returns no data (e.g. over
    ocean, where EPQS returns no value).
    """
    key = _cache_key(lat, lon)
    if cache is not None and key in cache:
        return cache[key]

    params = {
        "x": lon,
        "y": lat,
        "wkid": 4326,
        "units": "Feet",
        "includeDate": "false",
    }

    for attempt in range(retries + 1):
        try:
            resp = requests.get(EPQS_URL, params=params, timeout=timeout)
            resp.raise_for_status()
            data = resp.json()
            val = data.get("value")
            if val is None:
                return None
            elev = float(val)
            # EPQS sometimes returns large sentinels for water; clip
            if elev < -100 or elev > 15000:
                return None
            if cache is not None:
                cache[key] = elev
            return elev
        except Exception as exc:
            if attempt == retries:
                logger.warning(f"EPQS failed for ({lat},{lon}): {exc}")
                return None
            time.sleep(0.5 * (attempt + 1))
    return None


def fetch_ground_elevations_batch(
    coords: List[Tuple[float, float]],
    use_cache: bool = True,
    throttle_s: float = 0.05,
) -> List[Optional[float]]:
    """Fetch ground elevations for many points, with persistent cache."""
    cache = _load_epqs_cache() if use_cache else {}
    starting_size = len(cache)

    out: List[Optional[float]] = []
    for i, (lat, lon) in enumerate(coords):
        elev = fetch_ground_elevation_ft(lat, lon, cache=cache)
        out.append(elev)
        if throttle_s and elev is not None and i % 20 == 19:
            time.sleep(throttle_s)

    if use_cache and len(cache) > starting_size:
        _save_epqs_cache(cache)
        logger.info(
            f"EPQS cache grew {starting_size} → {len(cache)} "
            f"(hits={starting_size} new={len(cache) - starting_size})"
        )

    return out


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Pointwise Bathtub
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


SurgeFieldFn = Callable[[float, float], float]
"""A surge field function: (lat, lon) -> modeled water surface
elevation in feet NAVD88."""


@dataclass
class PointwiseBathtubResult:
    n_points: int
    n_with_ground_elev: int
    n_flooded_modeled: int
    mean_wse_ft: float


def run_pointwise_bathtub_on_hwms(
    hwms: list,
    surge_field_fn: SurgeFieldFn,
    storm_id: str,
    flood_threshold_ft: float = 0.5,
    use_epqs_cache: bool = True,
) -> Tuple[List[SampledObservation], PointwiseBathtubResult]:
    """
    Apply a pointwise bathtub model to each HWM and produce
    SampledObservation records ready for compute_metrics.

    Args:
        hwms: List of HighWaterMark records
        surge_field_fn: (lat, lon) -> modeled WSE in ft NAVD88
        storm_id: for tagging
        flood_threshold_ft: threshold for flooded classification

    Returns:
        (samples, summary)
    """
    usable = [h for h in hwms if h.height_above_gnd_ft is not None]
    if not usable:
        return [], PointwiseBathtubResult(0, 0, 0, 0.0)

    coords = [(h.latitude, h.longitude) for h in usable]
    ground_elevs = fetch_ground_elevations_batch(coords, use_cache=use_epqs_cache)

    samples: List[SampledObservation] = []
    n_with_ground = 0
    n_flooded = 0
    wse_sum = 0.0

    for h, ground_ft in zip(usable, ground_elevs):
        wse_ft = surge_field_fn(h.latitude, h.longitude)
        wse_sum += wse_ft

        observed = float(h.height_above_gnd_ft)
        observed_flooded = observed >= flood_threshold_ft

        if ground_ft is None:
            modeled_depth = None
            residual = None
            rel_err = None
            modeled_flooded = False
            note = (h.locale or "") + " [no EPQS]"
        else:
            n_with_ground += 1
            modeled_depth = max(0.0, wse_ft - ground_ft)
            residual = modeled_depth - observed
            rel_err = (residual / observed) if observed > 0 else None
            modeled_flooded = modeled_depth >= flood_threshold_ft
            if modeled_flooded:
                n_flooded += 1
            note = h.locale or ""

        samples.append(
            SampledObservation(
                obs_id=f"hwm-{h.hwm_id}",
                storm_id=storm_id,
                source="usgs_hwm",
                latitude=h.latitude,
                longitude=h.longitude,
                observed_ft=observed,
                modeled_ft=modeled_depth,
                residual_ft=residual,
                rel_error=rel_err,
                observed_flooded=observed_flooded,
                modeled_flooded=modeled_flooded,
                contingency=_classify_contingency(
                    observed_flooded, modeled_flooded
                ),
                quality=h.quality,
                notes=note,
            )
        )

    summary = PointwiseBathtubResult(
        n_points=len(samples),
        n_with_ground_elev=n_with_ground,
        n_flooded_modeled=n_flooded,
        mean_wse_ft=(wse_sum / len(samples)) if samples else 0.0,
    )
    logger.info(
        f"Pointwise bathtub: {n_with_ground}/{len(samples)} points with "
        f"ground elev; {n_flooded} modeled flooded; "
        f"mean WSE = {summary.mean_wse_ft:.2f} ft"
    )
    return samples, summary


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Surge Field Builders
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371.0
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = (math.sin(dp / 2) ** 2
         + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2)
    return 2 * R * math.asin(math.sqrt(a))


def exponential_surge_field(
    landfall_lat: float,
    landfall_lon: float,
    peak_surge_ft: float,
    decay_km: float = 40.0,
    min_surge_ft: float = 0.5,
) -> SurgeFieldFn:
    """
    Build an idealized radially-symmetric surge field that decays
    exponentially from the landfall point.

        WSE(lat, lon) = peak * exp(-dist_km / decay_km)

    Good first approximation when only a peak magnitude + landfall
    location is known. Underestimates asymmetry (surge is usually
    biased to the right of the storm track in the northern hemisphere).
    """
    def fn(lat: float, lon: float) -> float:
        d = _haversine_km(landfall_lat, landfall_lon, lat, lon)
        val = peak_surge_ft * math.exp(-d / decay_km)
        return max(min_surge_ft, val)
    return fn


def interpolated_surge_field(
    anchor_points: List[Tuple[float, float, float]],
    power: float = 2.0,
    search_radius_km: float = 150.0,
    floor_ft: float = 0.5,
) -> SurgeFieldFn:
    """
    Build a surge field by inverse-distance-weighted interpolation
    between observed or digitized peak-surge anchor points.

    Args:
        anchor_points: list of (lat, lon, peak_ft) tuples
        power: IDW exponent (2 = classic inverse-distance-squared)
        search_radius_km: ignore anchors beyond this distance
        floor_ft: minimum returned value

    Returns:
        (lat, lon) -> WSE in ft
    """
    def fn(lat: float, lon: float) -> float:
        num = 0.0
        den = 0.0
        for a_lat, a_lon, a_val in anchor_points:
            d = _haversine_km(a_lat, a_lon, lat, lon)
            if d > search_radius_km:
                continue
            if d < 0.01:
                return a_val  # exact match
            w = 1.0 / (d ** power)
            num += w * a_val
            den += w
        if den == 0:
            return floor_ft
        return max(floor_ft, num / den)
    return fn
