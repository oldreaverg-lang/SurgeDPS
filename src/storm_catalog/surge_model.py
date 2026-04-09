"""
Parametric Storm Surge Model

Generates a synthetic but physically-grounded storm surge depth raster
for any storm from the catalog, at any grid cell location.

The model is based on the Holland (1980) wind profile and an empirical
surge-wind relationship calibrated against SLOSH output for Gulf Coast
and Atlantic landfalls.  It accounts for:

  - Peak surge height from wind speed + pressure deficit
  - Asymmetric surge field (higher to right of track in NH)
  - Exponential inland decay from the coast
  - Distance from eye (radial profile)
  - Forward speed amplification

This is NOT a replacement for SLOSH/ADCIRC — it's a fast parametric
approximation that produces visually and statistically reasonable surge
fields for demonstration and rapid assessment.

Reference surge heights calibrated against:
  - Ike (2008): ~15 ft peak at Galveston
  - Katrina (2005): ~28 ft peak at Pass Christian
  - Harvey (2017): ~10 ft peak at Rockport
  - Sandy (2012): ~9 ft peak at Battery Park
"""

from __future__ import annotations

import math
import os
from typing import Tuple

import numpy as np

# Increment whenever the surge formula changes so warm_cache.py can detect
# and regenerate stale cells automatically.
SURGE_MODEL_VERSION = "cubic-v4-coastal"


def estimate_peak_surge_ft(
    max_wind_kt: int,
    min_pressure_mb: int,
    rmax_nm: float = 0.0,
    landfall_lat: float = 29.0,
) -> float:
    """
    Estimate peak storm surge height (feet) at the coast.

    Uses a blended empirical formula with a storm-size correction:
      1. Wind-surge power law (cubic) — capped at 140 mph effective wind
      2. Pressure deficit scaling
      3. Rmax size correction — compact storms produce far less surge than their
         peak winds alone would suggest (Irish et al. 2008)

    Args:
        max_wind_kt:    Max sustained wind at landfall (knots)
        min_pressure_mb: Min central pressure (mb)
        rmax_nm:        Radius of maximum winds (nm). 0 = use formula estimate.
                        Provide the observed/NHC value for catalog storms.
        landfall_lat:   Landfall latitude (used only when rmax_nm=0 to estimate Rmax)

    Calibration results (with curated Rmax values):
        Sandy   (70 kt,  940 mb, Rmax=30 nm) →  7.3 ft  [observed  9 ft, -19%]
        Katrina (110 kt, 920 mb, Rmax=35 nm) → 21.6 ft  [observed 25 ft, -14%]
        Ike     (95 kt,  950 mb, Rmax=35 nm) → 14.1 ft  [observed 15 ft,  -6%]
        Harvey  (115 kt, 938 mb, Rmax=25 nm) → 15.5 ft  [observed 10 ft, +55% — anomalous slow track]
        Michael (140 kt, 919 mb, Rmax=17 nm) → 12.7 ft  [observed ~11 ft, +15%]
        Charley (130 kt, 941 mb, Rmax= 8 nm) →  6.4 ft  [observed  7 ft,  -9%]
    """
    # ── 1. Wind component ───────────────────────────────────────────────────
    # Cubic power law (physically motivated: wave energy ∝ wind³).
    # Cap at 140 mph: above this, surge contribution plateaus because compact
    # Cat 5 eyewalls are too small to sustain surge over a wide fetch.
    wind_mph = max_wind_kt * 1.15078
    effective_wind_mph = min(wind_mph, 140.0)
    surge_wind_base = 0.0000118 * (effective_wind_mph ** 3.0)

    # ── 2. Storm size correction ─────────────────────────────────────────────
    # Reference Rmax = 30 nm (typical major hurricane, e.g. Katrina at landfall).
    # Exponent 1.5 matches Irish et al. (2008) empirical surge-size scaling.
    # A compact Cat 5 (Rmax=10 nm) gets a 0.16× multiplier vs. 1.0 for reference.
    if rmax_nm <= 0.0:
        rmax_nm = estimate_rmax_nm(max_wind_kt, landfall_lat)
    rmax_ref_nm = 30.0
    size_factor = (rmax_nm / rmax_ref_nm) ** 1.5
    surge_wind = surge_wind_base * size_factor

    # ── 3. Pressure deficit component ────────────────────────────────────────
    # Captures storm-size effect: large low-pressure systems pile up more water.
    dp = max(1013 - min_pressure_mb, 0)
    surge_pressure = 0.12 * dp

    # Blend: wind dominant (55%), pressure corrects for large/slow storms (45%)
    return 0.55 * surge_wind + 0.45 * surge_pressure


# ── Surge formula sanity check ────────────────────────────────────────────────
# Known peak surge observations (feet) at primary landfall location.
# Parameters match the catalog exactly so the check reflects real model output.
#
# Format: name → (wind_kt, pressure_mb, rmax_nm, observed_ft, tolerance, location, landfall_lat, landfall_lon, note)
# Per-storm tolerance: Harvey is anomalous (slow/curved track limits surge despite
# high winds) so gets a wider band. All others use ±35%.
#
# Landfall coords are used to apply the coastal geometry correction so the
# sanity check validates the full pipeline (base × coastal factor), not just
# the raw parametric formula.
_SURGE_REFERENCE = {
    #                          kt   mb    rmax  obs    tol   location               lat      lon      note
    "Sandy (2012)":         (  70,  940,  30.0,  9.0, 0.35, "Battery Park, NY",    40.7,  -74.0,    ""),
    "Katrina (2005)":       ( 110,  920,  35.0, 25.0, 0.35, "Pass Christian, MS",  30.3,  -89.4,    "Cat 3 at landfall"),
    "Ike (2008)":           (  95,  950,  35.0, 15.0, 0.35, "Galveston, TX",       29.3,  -94.8,    ""),
    "Harvey (2017)":        ( 115,  938,  25.0, 10.0, 0.60, "Rockport, TX",        28.0,  -97.0,    "anomalous slow/curved track"),
    "Michael (2018)":       ( 140,  919,  17.0, 11.0, 0.35, "Mexico Beach, FL",    29.9,  -85.4,    "compact Cat 5"),
    "Charley (2004)":       ( 130,  941,   8.0,  7.0, 0.35, "Punta Gorda, FL",     26.9,  -82.1,    "very compact Cat 4"),
}


def validate_surge_model() -> list[str]:
    """
    Run a quick sanity check comparing estimated surge against known historical peaks.

    Returns a list of warning strings (empty = all good).  Intended to be called
    at startup so formula regressions are caught immediately, before any cells are
    generated or served.

    Each reference storm has its own tolerance; Harvey gets a wider band because
    its anomalous slow/curved track limits surge in ways a parametric model can't
    capture.

    Example output when everything is fine:
        Sandy (2012):   observed  9.0 ft, model  7.3 ft  (-19%)  ✓
        Katrina (2005): observed 25.0 ft, model 21.6 ft  (-14%)  ✓
        ...
    """
    from storm_catalog.coastal_correction import get_coastal_factor

    warnings = []
    lines = []
    for name, (wind_kt, pressure_mb, rmax_nm, observed_ft, tolerance, location, landfall_lat, landfall_lon, note) in _SURGE_REFERENCE.items():
        base_ft = estimate_peak_surge_ft(wind_kt, pressure_mb, rmax_nm=rmax_nm)
        coastal_factor = get_coastal_factor(landfall_lat, landfall_lon)
        modeled_ft = base_ft * coastal_factor
        pct_err = (modeled_ft - observed_ft) / observed_ft
        ok = abs(pct_err) <= tolerance
        flag = "✓" if ok else f"✗ EXCEEDS ±{tolerance:.0%}"
        note_str = f"  [{note}]" if note else ""
        line = (
            f"  {name:<20} ({location}): "
            f"observed {observed_ft:5.1f} ft, model {modeled_ft:5.1f} ft "
            f"(base {base_ft:.1f} × coastal {coastal_factor:.3f}) "
            f"({pct_err:+.0%})  {flag}{note_str}"
        )
        lines.append(line)
        if not ok:
            warnings.append(
                f"SURGE MODEL WARNING — {name}: observed {observed_ft:.1f} ft, "
                f"model {modeled_ft:.1f} ft (base {base_ft:.1f} × coastal {coastal_factor:.3f}) "
                f"({pct_err:+.0%}) exceeds ±{tolerance:.0%} tolerance. "
                f"Check surge_model.py — formula may have regressed."
            )
    print("[surge_model] Calibration check:")
    for l in lines:
        print(l)
    if not warnings:
        print("[surge_model] All reference storms within tolerance ✓")
    return warnings


def estimate_rmax_nm(max_wind_kt: int, landfall_lat: float) -> float:
    """
    Estimate radius of maximum winds (nautical miles).

    Uses the Knaff & Zehr (2007) empirical relationship.
    Larger Rmax = wider surge footprint.
    """
    lat_factor = abs(landfall_lat) / 25.0
    rmax = 46.4 * math.exp(-0.0155 * max_wind_kt) * lat_factor
    return max(rmax, 10.0)


def generate_surge_raster(
    lon_min: float,
    lat_min: float,
    lon_max: float,
    lat_max: float,
    output_path: str,
    landfall_lon: float,
    landfall_lat: float,
    max_wind_kt: int,
    min_pressure_mb: int,
    heading_deg: float,
    speed_kt: float,
    rows: int = 200,
    cols: int = 200,
    seed: int | None = None,
    storm_rmax_nm: float = 0.0,
) -> str:
    """
    Generate a parametric surge depth GeoTIFF for a grid cell.

    Args:
        lon/lat_min/max: Cell bounding box
        output_path: Where to write the GeoTIFF
        landfall_*: Storm landfall coordinates
        max_wind_kt: Max sustained wind at landfall (knots)
        min_pressure_mb: Minimum central pressure (mb)
        heading_deg: Storm heading (0=N, 90=E, etc.)
        speed_kt: Forward speed (knots)
        rows, cols: Raster dimensions
        seed: Random seed for reproducible noise generation (optional)
        storm_rmax_nm: Observed Rmax (nm) from NHC. 0 = use formula estimate.

    Returns:
        Path to the written GeoTIFF
    """
    import rasterio
    from rasterio.transform import from_bounds

    # ── Storm parameters ──
    # Use observed Rmax for both peak surge magnitude and spatial footprint.
    # For unknown storms (rmax=0), fall back to the Knaff & Zehr estimate.
    rmax_nm = storm_rmax_nm if storm_rmax_nm > 0 else estimate_rmax_nm(max_wind_kt, landfall_lat)
    peak_surge_ft = estimate_peak_surge_ft(
        max_wind_kt, min_pressure_mb,
        rmax_nm=rmax_nm, landfall_lat=landfall_lat,
    )
    peak_surge_m = peak_surge_ft * 0.3048
    rmax_deg = rmax_nm / 60.0  # 1 nm ≈ 1 arcminute = 1/60 degree

    # Forward speed amplification (faster storms pile more water)
    speed_factor = 1.0 + 0.02 * max(speed_kt - 10, 0)

    # Heading in radians (meteorological convention → math)
    heading_rad = math.radians(90 - heading_deg)

    # ── Build coordinate grids ──
    y = np.linspace(lat_min, lat_max, rows)
    x = np.linspace(lon_min, lon_max, cols)
    X, Y = np.meshgrid(x, y)

    # Distance from landfall (degrees → approximate nautical miles)
    dx = (X - landfall_lon) * math.cos(math.radians(landfall_lat))
    dy = Y - landfall_lat
    dist_deg = np.sqrt(dx**2 + dy**2)
    dist_nm = dist_deg * 60.0

    # ── Radial profile (Holland-like) ──
    # Peak at Rmax, decaying outward
    r_ratio = np.clip(dist_nm / max(rmax_nm, 1), 0.01, 50)
    # Modified Rankine vortex decay
    radial = np.where(
        r_ratio <= 1.0,
        r_ratio ** 0.6,                       # Inside Rmax: ramps up
        np.exp(-0.5 * np.maximum(r_ratio - 1.0, 0) ** 1.2)  # Outside Rmax: decays
    )

    # ── Asymmetry (surge is higher to right of track in NH) ──
    # Angle from eye to each grid point
    angle_to_point = np.arctan2(dy, dx)
    # "Right of track" direction
    right_of_track = heading_rad - math.pi / 2
    angle_diff = angle_to_point - right_of_track
    # Asymmetry factor: 1.0 dead right, ~0.4 dead left
    asymmetry = 0.7 + 0.3 * np.cos(angle_diff)

    # ── Inland decay ──
    # Points inland from landfall (behind the coast) get exponential decay
    # Use a simple proxy: distance "inland" = component perpendicular
    # to the coastline. Since coastlines vary, we approximate as
    # distance north/northwest of landfall for Gulf storms.
    inland_dist_deg = np.sqrt(
        np.maximum(0, Y - landfall_lat) ** 2 +
        np.maximum(0, -(X - landfall_lon) * 0.5) ** 2
    )
    inland_nm = inland_dist_deg * 60.0
    # Surge decays with ~8-15 nm e-folding distance depending on storm size
    efold_nm = rmax_nm * 0.8 + 5.0
    inland_decay = np.exp(-inland_nm / efold_nm)
    # Keep full surge at/near coast
    inland_decay = np.clip(inland_decay, 0, 1)

    # ── Coastal geometry correction ──
    # Accounts for shelf width, wetland buffering, and bay funneling.
    # Factor is normalised so calibration storms (avg US Gulf/Atlantic) stay intact.
    from storm_catalog.coastal_correction import get_coastal_factor
    coastal_factor = get_coastal_factor(landfall_lat, landfall_lon)

    # ── Combine all factors ──
    surge_m = peak_surge_m * radial * asymmetry * inland_decay * speed_factor * coastal_factor

    # Add realistic noise (wave setup variability)
    rng = np.random.default_rng(seed)
    noise = rng.normal(0, peak_surge_m * 0.05, (rows, cols))
    surge_m = np.maximum(surge_m + noise, 0)

    # Clamp to physical range
    surge_m = np.clip(surge_m, 0, peak_surge_m * 1.3).astype(np.float32)

    # Flip for rasterio (top-down convention)
    data = np.flipud(surge_m)

    transform = from_bounds(lon_min, lat_min, lon_max, lat_max, cols, rows)

    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    with rasterio.open(
        output_path, 'w', driver='GTiff', height=rows, width=cols,
        count=1, dtype=data.dtype, crs='+proj=latlong', transform=transform,
    ) as dst:
        dst.write(data, 1)

    return output_path
