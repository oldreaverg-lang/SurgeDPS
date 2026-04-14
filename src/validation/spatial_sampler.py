"""
Spatial Sampler

Given a modeled flood depth raster and a set of point observations
(HWMs, tide gauges), sample the raster at each observation coordinate
and compute residuals. This is the core building block of spatial
validation.

Units convention:
  - Rasters produced by bathtub/HAND/compound models are in FEET
    (the depth pipeline converts meters → feet for display).
  - USGS HWMs use FEET for height_above_gnd and elev_ft.
  - Tide-gauge observations are stored in METERS internally but we
    convert here.

Residual sign convention:
  residual = modeled - observed
    positive → model overestimates
    negative → model underestimates

Flooded/unflooded classification uses a configurable threshold
(default 0.5 ft) so that contingency metrics (POD/FAR/CSI) can be
computed alongside depth residuals.
"""

from __future__ import annotations

import logging
import math
import os
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

M_TO_FT = 3.280839895


@dataclass
class SampledObservation:
    """One observation paired with its modeled value."""

    obs_id: str                      # stable identifier (e.g. "hwm-12345")
    storm_id: str
    source: str                      # "usgs_hwm", "noaa_gauge", ...
    latitude: float
    longitude: float

    observed_ft: float               # ground-truth depth above ground (ft)
    modeled_ft: Optional[float]      # modeled depth at this location (ft)

    residual_ft: Optional[float]     # modeled - observed
    rel_error: Optional[float]       # residual / observed (when observed > 0)

    observed_flooded: bool
    modeled_flooded: bool
    contingency: str                 # "hit" | "miss" | "false_alarm" | "correct_neg"

    quality: str = "Unknown"
    notes: str = ""

    def to_dict(self) -> dict:
        return asdict(self)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Core Sampler
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def sample_raster_at_points(
    raster_path: str,
    coords: List[Tuple[float, float]],
    nodata_fallback: float = 0.0,
) -> List[Optional[float]]:
    """
    Sample a GeoTIFF raster at (lon, lat) points.

    Reprojects the points into the raster's CRS automatically.

    Args:
        raster_path: Path to GeoTIFF
        coords: list of (lon, lat) tuples in WGS84
        nodata_fallback: value to substitute for nodata cells
                         (None returned instead if the point is
                         completely outside the raster extent)

    Returns:
        List of floats (same length as coords). None for points
        outside the raster bounds.
    """
    import rasterio
    from rasterio.warp import transform as warp_transform

    if not os.path.exists(raster_path):
        raise FileNotFoundError(raster_path)
    if not coords:
        return []

    with rasterio.open(raster_path) as src:
        lons = [c[0] for c in coords]
        lats = [c[1] for c in coords]

        # Reproject WGS84 → raster CRS (no-op if already EPSG:4326)
        if str(src.crs).upper() not in ("EPSG:4326", "OGC:CRS84"):
            xs, ys = warp_transform(
                {"init": "EPSG:4326"}, src.crs, lons, lats
            )
        else:
            xs, ys = lons, lats

        # Use rasterio.sample for efficient point extraction
        samples = list(src.sample(zip(xs, ys), indexes=1))

        nodata = src.nodata
        left, bottom, right, top = src.bounds

    results: List[Optional[float]] = []
    for (x, y), sample in zip(zip(xs, ys), samples):
        if not (left <= x <= right and bottom <= y <= top):
            results.append(None)  # outside raster extent
            continue
        val = float(sample[0])
        if nodata is not None and (val == nodata or math.isnan(val)):
            results.append(nodata_fallback)
        else:
            results.append(val)
    return results


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Observation Adapters
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def _classify_contingency(
    observed_flooded: bool, modeled_flooded: bool
) -> str:
    if observed_flooded and modeled_flooded:
        return "hit"
    if observed_flooded and not modeled_flooded:
        return "miss"
    if not observed_flooded and modeled_flooded:
        return "false_alarm"
    return "correct_neg"


def sample_hwms(
    hwms: list,
    depth_raster_path: str,
    storm_id: str,
    flood_threshold_ft: float = 0.5,
) -> List[SampledObservation]:
    """
    Sample a depth raster at USGS HWM coordinates.

    Only HWMs with height_above_gnd_ft populated are used (those are
    the ones directly comparable to modeled depth).

    Args:
        hwms: List of HighWaterMark objects
              (from data_ingest.usgs_hwm)
        depth_raster_path: modeled flood depth GeoTIFF (feet)
        storm_id: for tagging
        flood_threshold_ft: min depth to call a cell "flooded"

    Returns:
        List of SampledObservation, one per usable HWM.
    """
    usable = [h for h in hwms if h.height_above_gnd_ft is not None]
    if not usable:
        logger.warning(
            f"No HWMs with height_above_gnd_ft for {storm_id}; "
            f"try filter_quality() less strictly or check fetcher"
        )
        return []

    coords = [(h.longitude, h.latitude) for h in usable]
    modeled_vals = sample_raster_at_points(depth_raster_path, coords)

    results: List[SampledObservation] = []
    for h, modeled in zip(usable, modeled_vals):
        observed = float(h.height_above_gnd_ft)
        observed_flooded = observed >= flood_threshold_ft

        if modeled is None:
            # outside raster extent → treat as unflooded
            residual = None
            rel_err = None
            modeled_flooded = False
        else:
            residual = modeled - observed
            rel_err = (residual / observed) if observed > 0 else None
            modeled_flooded = modeled >= flood_threshold_ft

        results.append(
            SampledObservation(
                obs_id=f"hwm-{h.hwm_id}",
                storm_id=storm_id,
                source="usgs_hwm",
                latitude=h.latitude,
                longitude=h.longitude,
                observed_ft=observed,
                modeled_ft=modeled,
                residual_ft=residual,
                rel_error=rel_err,
                observed_flooded=observed_flooded,
                modeled_flooded=modeled_flooded,
                contingency=_classify_contingency(
                    observed_flooded, modeled_flooded
                ),
                quality=h.quality,
                notes=h.locale,
            )
        )

    logger.info(
        f"Sampled {len(results)} HWMs against {os.path.basename(depth_raster_path)}"
    )
    return results


def sample_tide_gauges(
    gauge_peaks_m: Dict[str, Dict],
    depth_raster_path: str,
    storm_id: str,
    flood_threshold_ft: float = 0.5,
) -> List[SampledObservation]:
    """
    Sample a depth raster at NOAA tide-gauge locations.

    Args:
        gauge_peaks_m: dict keyed by station_id with values
                       {"name": str, "lat": float, "lon": float,
                        "peak_m": float}
                       Peak is storm peak water level above MHHW
                       (or whatever datum you calibrated to) in METERS.
        depth_raster_path: modeled flood depth GeoTIFF (feet)
        storm_id: for tagging
        flood_threshold_ft: flooded threshold
    """
    if not gauge_peaks_m:
        return []

    stations = list(gauge_peaks_m.items())
    coords = [(s["lon"], s["lat"]) for _, s in stations]
    modeled_vals = sample_raster_at_points(depth_raster_path, coords)

    results: List[SampledObservation] = []
    for (sid, s), modeled in zip(stations, modeled_vals):
        observed = float(s.get("peak_m", 0.0)) * M_TO_FT
        observed_flooded = observed >= flood_threshold_ft

        if modeled is None:
            residual = None
            rel_err = None
            modeled_flooded = False
        else:
            residual = modeled - observed
            rel_err = (residual / observed) if observed > 0 else None
            modeled_flooded = modeled >= flood_threshold_ft

        results.append(
            SampledObservation(
                obs_id=f"gauge-{sid}",
                storm_id=storm_id,
                source="noaa_gauge",
                latitude=float(s["lat"]),
                longitude=float(s["lon"]),
                observed_ft=observed,
                modeled_ft=modeled,
                residual_ft=residual,
                rel_error=rel_err,
                observed_flooded=observed_flooded,
                modeled_flooded=modeled_flooded,
                contingency=_classify_contingency(
                    observed_flooded, modeled_flooded
                ),
                quality="Gauge",
                notes=s.get("name", ""),
            )
        )
    return results


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Persistence
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def save_samples(
    samples: List[SampledObservation],
    storm_id: str,
    cache_dir: str = "data/validation/samples",
) -> str:
    """Persist sampled observations to CSV."""
    import csv

    os.makedirs(cache_dir, exist_ok=True)
    path = os.path.join(cache_dir, f"{storm_id}_samples.csv")

    if samples:
        fields = list(samples[0].to_dict().keys())
    else:
        fields = [f for f in SampledObservation.__dataclass_fields__]

    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for s in samples:
            w.writerow(s.to_dict())
    logger.info(f"Saved {len(samples)} samples → {path}")
    return path
