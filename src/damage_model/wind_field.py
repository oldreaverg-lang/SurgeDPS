"""
Asymmetric Holland Wind Field Model for SurgeDPS

Generates spatially-resolved wind speed estimates at arbitrary (lat, lon)
points using the Holland (1980) parametric vortex with NHC quadrant radii
to model asymmetry.

Data source: IBTrACS best-track archive (USA agency columns) stored in
the StormDPS data cache.  For each storm the module selects the snapshot
closest to the cataloged landfall location and reads:

  - USA_WIND / USA_PRES  – max sustained wind and central pressure
  - USA_RMW              – radius of maximum winds (nm)
  - USA_R34/R50/R64 × 4  – 34/50/64-kt wind radii per quadrant (nm)
  - STORM_SPEED / DIR    – translational motion

The Holland profile is solved in each of the four quadrants independently
using the observed wind radii to back-solve the B parameter, then
smoothly interpolated across azimuth.  A translational asymmetry
correction adds forward speed on the right side and subtracts on the
left (Northern Hemisphere).

Wind speeds are returned in mph at 10 m height (standard meteorological
reference).  The model matches StormDPS core/ike.py's
synthesize_asymmetric_wind_field() methodology but is implemented as
a lightweight point-query function (no numpy grid required).

References:
  Holland (1980) "An Analytic Model of the Wind and Pressure Profiles
    in Hurricanes", Monthly Weather Review, 108:1212–1218
  Knaff & Zehr (2007) RMW estimation formula
  Emanuel (2005) translational asymmetry correction
"""

from __future__ import annotations

import csv
import logging
import math
import os
from dataclasses import dataclass, field
from functools import lru_cache
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# ── Constants ────────────────────────────────────────────────────────
NM_TO_M = 1852.0          # nautical miles → meters
KT_TO_MS = 0.514444       # knots → m/s
MS_TO_MPH = 2.23694       # m/s → mph
DEG_TO_RAD = math.pi / 180.0
EARTH_RADIUS_M = 6_371_000.0

# IBTrACS CSV path — relative to StormDPS data cache
_IBTRACS_CSV: Optional[str] = None


def _find_ibtracs_csv() -> str:
    """Locate the IBTrACS CSV in the StormDPS data cache."""
    global _IBTRACS_CSV
    if _IBTRACS_CSV and os.path.exists(_IBTRACS_CSV):
        return _IBTRACS_CSV

    # Try common relative locations from SurgeDPS project root
    candidates = [
        os.path.join(os.path.dirname(__file__), "..", "..", "..", "StormDPS", "data", "cache", "ibtracs_all.csv"),
        os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", "StormDPS", "data", "cache", "ibtracs_all.csv"),
        os.environ.get("IBTRACS_CSV_PATH", ""),
    ]
    # Also check mnt paths (Cowork environment)
    for base in ["/sessions", os.path.expanduser("~")]:
        for root, dirs, files in os.walk(base):
            if "ibtracs_all.csv" in files:
                candidates.append(os.path.join(root, "ibtracs_all.csv"))
            if root.count(os.sep) > 6:
                break

    for c in candidates:
        if c and os.path.exists(c):
            _IBTRACS_CSV = os.path.abspath(c)
            logger.info("[WindField] IBTrACS CSV: %s", _IBTRACS_CSV)
            return _IBTRACS_CSV

    raise FileNotFoundError(
        "Cannot locate ibtracs_all.csv. Set IBTRACS_CSV_PATH env var or "
        "ensure StormDPS/data/cache/ibtracs_all.csv exists."
    )


# ── Data structures ──────────────────────────────────────────────────

@dataclass
class QuadrantRadii:
    """Wind radii in meters for NE/SE/SW/NW quadrants at a given threshold."""
    ne: float = 0.0
    se: float = 0.0
    sw: float = 0.0
    nw: float = 0.0

    def max_radius(self) -> float:
        return max(self.ne, self.se, self.sw, self.nw)

    def at_azimuth(self, azimuth_deg: float) -> float:
        """Interpolate radius at a given meteorological azimuth (0=N, 90=E)."""
        az = azimuth_deg % 360
        # Quadrant centers: NE=45, SE=135, SW=225, NW=315
        quadrants = [
            (45.0, self.ne),
            (135.0, self.se),
            (225.0, self.sw),
            (315.0, self.nw),
        ]
        # Find the two bounding quadrant centers and interpolate
        for i in range(4):
            c1_az, c1_r = quadrants[i]
            c2_az, c2_r = quadrants[(i + 1) % 4]

            # Normalize angles for wrapping
            lo = c1_az - 45  # start of this quadrant's influence
            hi = c1_az + 45  # end → next quadrant starts

            if lo < 0:
                if az >= lo + 360 or az < hi:
                    # We're in the NW→NE transition zone
                    if az >= lo + 360:
                        frac = (az - (lo + 360)) / 90.0
                    else:
                        frac = (az + 360 - (lo + 360)) / 90.0
                    return c1_r + frac * (c2_r - c1_r)
            elif lo <= az < hi:
                frac = (az - lo) / 90.0
                return c1_r + frac * (c2_r - c1_r)

        # Fallback: nearest quadrant
        if 0 <= az < 90:
            return self.ne
        elif 90 <= az < 180:
            return self.se
        elif 180 <= az < 270:
            return self.sw
        else:
            return self.nw


@dataclass
class LandfallSnapshot:
    """Landfall-nearest IBTrACS observation with all wind structure data."""
    storm_name: str
    iso_time: str
    lat: float
    lon: float
    max_wind_kt: float
    max_wind_ms: float
    min_pressure_mb: float
    rmw_m: float
    r34: QuadrantRadii
    r50: QuadrantRadii
    r64: QuadrantRadii
    storm_speed_ms: float = 0.0
    storm_dir_deg: float = 0.0  # meteorological: 0=N, 90=E

    @property
    def max_wind_mph(self) -> float:
        return self.max_wind_ms * MS_TO_MPH


# ── IBTrACS Snapshot Loader ──────────────────────────────────────────

# Map SurgeDPS storm_id patterns to IBTrACS NAME + year
_STORM_NAME_MAP = {
    "michael_2018": ("MICHAEL", 2018),
    "katrina_2005": ("KATRINA", 2005),
    "ike_2008": ("IKE", 2008),
    "harvey_2017": ("HARVEY", 2017),
    "ian_2022": ("IAN", 2022),
    "laura_2020": ("LAURA", 2020),
    "ida_2021": ("IDA", 2021),
    "irma_2017": ("IRMA", 2017),
    "florence_2018": ("FLORENCE", 2018),
    "sandy_2012": ("SANDY", 2012),
    "delta_2020": ("DELTA", 2020),
}


def _safe_float(val: str, default: float = 0.0) -> float:
    """Parse a float from IBTrACS CSV, handling blanks and spaces."""
    v = val.strip() if val else ""
    if not v or v == " ":
        return default
    try:
        return float(v)
    except ValueError:
        return default


@lru_cache(maxsize=32)
def load_landfall_snapshot(
    storm_id: str,
    landfall_lat: float,
    landfall_lon: float,
) -> Optional[LandfallSnapshot]:
    """
    Load the IBTrACS observation nearest to the cataloged landfall point.

    Searches for the storm by name+year, then picks the snapshot that:
      1. Has USA_R34_NE data (quadrant radii available)
      2. Is closest to (landfall_lat, landfall_lon)

    Returns None if the storm isn't found or has no quadrant data.
    """
    csv_path = _find_ibtracs_csv()

    # Resolve storm name
    sid_lower = storm_id.lower()
    if sid_lower in _STORM_NAME_MAP:
        target_name, target_year = _STORM_NAME_MAP[sid_lower]
    else:
        # Try parsing "name_year" format
        parts = storm_id.rsplit("_", 1)
        if len(parts) == 2 and parts[1].isdigit():
            target_name = parts[0].upper()
            target_year = int(parts[1])
        else:
            logger.warning("[WindField] Cannot resolve storm_id=%s to IBTrACS name", storm_id)
            return None

    best_snapshot: Optional[LandfallSnapshot] = None
    best_dist = float("inf")

    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = row.get("NAME", "").strip()
            iso_time = row.get("ISO_TIME", "")
            year_str = iso_time[:4] if iso_time else ""

            if name != target_name or year_str != str(target_year):
                continue

            # Must have USA quadrant data
            r34_ne = _safe_float(row.get("USA_R34_NE", ""))
            if r34_ne <= 0:
                continue

            usa_lat = _safe_float(row.get("USA_LAT", ""))
            usa_lon = _safe_float(row.get("USA_LON", ""))
            if usa_lat == 0 and usa_lon == 0:
                continue

            # Distance to cataloged landfall
            dist = _haversine_m(usa_lat, usa_lon, landfall_lat, landfall_lon)
            if dist >= best_dist:
                continue

            # Parse all fields
            max_wind_kt = _safe_float(row.get("USA_WIND", ""))
            if max_wind_kt <= 0:
                continue

            rmw_nm = _safe_float(row.get("USA_RMW", ""))
            if rmw_nm <= 0:
                # Knaff & Zehr (2007) estimate
                vmax_ms = max_wind_kt * KT_TO_MS
                rmw_nm = 46.4 * math.exp(-0.0155 * vmax_ms * 1.94384 + 0.0169 * abs(usa_lat))

            snap = LandfallSnapshot(
                storm_name=name,
                iso_time=iso_time,
                lat=usa_lat,
                lon=usa_lon,
                max_wind_kt=max_wind_kt,
                max_wind_ms=max_wind_kt * KT_TO_MS,
                min_pressure_mb=_safe_float(row.get("USA_PRES", ""), 1013),
                rmw_m=rmw_nm * NM_TO_M,
                r34=QuadrantRadii(
                    ne=r34_ne * NM_TO_M,
                    se=_safe_float(row.get("USA_R34_SE", "")) * NM_TO_M,
                    sw=_safe_float(row.get("USA_R34_SW", "")) * NM_TO_M,
                    nw=_safe_float(row.get("USA_R34_NW", "")) * NM_TO_M,
                ),
                r50=QuadrantRadii(
                    ne=_safe_float(row.get("USA_R50_NE", "")) * NM_TO_M,
                    se=_safe_float(row.get("USA_R50_SE", "")) * NM_TO_M,
                    sw=_safe_float(row.get("USA_R50_SW", "")) * NM_TO_M,
                    nw=_safe_float(row.get("USA_R50_NW", "")) * NM_TO_M,
                ),
                r64=QuadrantRadii(
                    ne=_safe_float(row.get("USA_R64_NE", "")) * NM_TO_M,
                    se=_safe_float(row.get("USA_R64_SE", "")) * NM_TO_M,
                    sw=_safe_float(row.get("USA_R64_SW", "")) * NM_TO_M,
                    nw=_safe_float(row.get("USA_R64_NW", "")) * NM_TO_M,
                ),
                storm_speed_ms=_safe_float(row.get("STORM_SPEED", "")) * KT_TO_MS,
                storm_dir_deg=_safe_float(row.get("STORM_DIR", "")),
            )
            best_snapshot = snap
            best_dist = dist

    if best_snapshot:
        logger.info(
            "[WindField] Loaded %s landfall snapshot: %s | Vmax=%d kt | "
            "RMW=%.0f nm | R34 max=%.0f nm | dist=%.0f km from catalog",
            target_name, best_snapshot.iso_time,
            best_snapshot.max_wind_kt,
            best_snapshot.rmw_m / NM_TO_M,
            best_snapshot.r34.max_radius() / NM_TO_M,
            best_dist / 1000,
        )
    else:
        logger.warning("[WindField] No IBTrACS snapshot found for %s (%d)", target_name, target_year)

    return best_snapshot


# ── Holland Parametric Wind Model ────────────────────────────────────

def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in meters."""
    rlat1, rlat2 = lat1 * DEG_TO_RAD, lat2 * DEG_TO_RAD
    dlat = (lat2 - lat1) * DEG_TO_RAD
    dlon = (lon2 - lon1) * DEG_TO_RAD
    a = math.sin(dlat / 2) ** 2 + math.cos(rlat1) * math.cos(rlat2) * math.sin(dlon / 2) ** 2
    return EARTH_RADIUS_M * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _bearing_deg(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Bearing from point 1 to point 2 in degrees (0=N, 90=E, clockwise)."""
    rlat1, rlat2 = lat1 * DEG_TO_RAD, lat2 * DEG_TO_RAD
    dlon = (lon2 - lon1) * DEG_TO_RAD
    x = math.sin(dlon) * math.cos(rlat2)
    y = math.cos(rlat1) * math.sin(rlat2) - math.sin(rlat1) * math.cos(rlat2) * math.cos(dlon)
    return (math.atan2(x, y) * 180 / math.pi) % 360


def _estimate_holland_b(
    vmax_ms: float,
    rmw_m: float,
    r_ref_m: float = 0.0,
    v_ref_ms: float = 0.0,
) -> float:
    """
    Estimate the Holland B parameter.

    If a reference radius (r_ref_m) and wind speed (v_ref_ms) are given,
    back-solve B from the Holland profile.  Otherwise use the empirical
    formula from Vickery & Wadhera (2008).
    """
    if r_ref_m > rmw_m and v_ref_ms > 0 and vmax_ms > 0:
        # Back-solve: v/vmax = sqrt((rmw/r)^B * exp(1 - (rmw/r)^B))
        # Let x = (rmw/r)^B.  Then (v/vmax)^2 = x * exp(1 - x)
        # Solve numerically via Newton iteration
        ratio = (v_ref_ms / vmax_ms) ** 2
        x_ratio = rmw_m / r_ref_m  # < 1 since r_ref > rmw

        # Initial guess from log relationship
        b_guess = max(0.5, min(2.5,
            math.log(ratio / math.e) / math.log(x_ratio) if x_ratio > 0.01 else 1.0
        ))

        # Newton iteration
        for _ in range(20):
            xb = x_ratio ** b_guess
            f_val = xb * math.exp(1 - xb) - ratio
            # df/dB = xb * ln(x_ratio) * exp(1 - xb) * (1 - xb)
            ln_xr = math.log(x_ratio) if x_ratio > 1e-10 else -10
            df = xb * ln_xr * math.exp(1 - xb) * (1 - xb)
            if abs(df) < 1e-12:
                break
            b_new = b_guess - f_val / df
            b_new = max(0.3, min(3.0, b_new))
            if abs(b_new - b_guess) < 1e-6:
                b_guess = b_new
                break
            b_guess = b_new

        return max(0.5, min(2.5, b_guess))

    # Empirical fallback (Vickery & Wadhera 2008)
    vmax_kt = vmax_ms / KT_TO_MS
    b = 1.0 + 0.5 * (vmax_kt - 75) / 75  # rough fit
    return max(0.5, min(2.5, b))


def _holland_wind_at_radius(
    r_m: float,
    rmw_m: float,
    vmax_ms: float,
    b_param: float,
) -> float:
    """
    Holland (1980) gradient-level wind speed at radius r.

    V(r) = Vmax * sqrt( (rmw/r)^B * exp(1 - (rmw/r)^B) )
    """
    if r_m <= 0:
        return vmax_ms
    if r_m <= rmw_m:
        # Inside the eye wall — linear ramp up to Vmax at RMW
        return vmax_ms * (r_m / rmw_m)

    x = (rmw_m / r_m) ** b_param
    v = vmax_ms * math.sqrt(x * math.exp(1.0 - x))

    # Outer-region decay: ensure wind drops to ~0 well beyond the last
    # observed radius.  Apply a gentle taper beyond 1.5× the R34 extent.
    return max(0.0, v)


def _translational_asymmetry(
    azimuth_from_center: float,
    storm_speed_ms: float,
    storm_dir_deg: float,
) -> float:
    """
    Translational velocity correction (Emanuel 2005).

    In the Northern Hemisphere, the right-front quadrant gets enhanced
    winds.  The correction factor is ~0.5 × Vt × cos(θ - θ_storm).
    """
    if storm_speed_ms <= 0:
        return 0.0

    # Relative angle: azimuth minus storm heading
    # Maximum enhancement 90° to the right of motion (right-front quadrant)
    rel_angle = (azimuth_from_center - storm_dir_deg) * DEG_TO_RAD
    # Factor of 0.5 recommended by Emanuel (2005) for surface winds
    return 0.5 * storm_speed_ms * math.cos(rel_angle)


# ── Public API ───────────────────────────────────────────────────────

def get_wind_speed_at_point(
    snapshot: LandfallSnapshot,
    target_lat: float,
    target_lon: float,
) -> float:
    """
    Compute wind speed (mph) at an arbitrary point using the asymmetric
    Holland parametric model fitted to IBTrACS quadrant radii.

    The model:
    1. Computes distance and bearing from storm center to target
    2. Interpolates the observed R34/R50/R64 radii at the target's azimuth
    3. Back-solves Holland B using the best-available reference radius
    4. Evaluates the Holland profile at the target's distance
    5. Adds translational asymmetry correction (Northern Hemisphere)

    Returns wind speed in mph (10 m height, 1-minute sustained).
    """
    dist_m = _haversine_m(snapshot.lat, snapshot.lon, target_lat, target_lon)
    azimuth = _bearing_deg(snapshot.lat, snapshot.lon, target_lat, target_lon)

    # Get azimuth-interpolated radii
    r34_m = snapshot.r34.at_azimuth(azimuth)
    r50_m = snapshot.r50.at_azimuth(azimuth)
    r64_m = snapshot.r64.at_azimuth(azimuth)

    # Back-solve Holland B using a blended multi-radius approach.
    #
    # IBTrACS reports SURFACE wind speeds (10 m, 1-min sustained), so the
    # Holland profile is fitted directly to surface observations — no
    # gradient-to-surface reduction is needed.
    #
    # Using R34 as the primary constraint gives the best outer-profile fit.
    # R64/R50 constrain the inner profile but can produce very steep B
    # values for compact storms, leading to unrealistic rapid decay at
    # larger radii where most buildings are located.
    vmax = snapshot.max_wind_ms
    rmw = snapshot.rmw_m

    b_candidates = []
    if r34_m > rmw and r34_m > 0:
        b_candidates.append((_estimate_holland_b(vmax, rmw, r34_m, 34 * KT_TO_MS), 0.50))
    if r50_m > rmw and r50_m > 0:
        b_candidates.append((_estimate_holland_b(vmax, rmw, r50_m, 50 * KT_TO_MS), 0.30))
    if r64_m > rmw and r64_m > 0:
        b_candidates.append((_estimate_holland_b(vmax, rmw, r64_m, 64 * KT_TO_MS), 0.20))

    if b_candidates:
        # Weighted average of B estimates (R34 weighted highest)
        total_w = sum(w for _, w in b_candidates)
        b_param = sum(b * w for b, w in b_candidates) / total_w
    else:
        b_param = _estimate_holland_b(vmax, rmw)

    b_param = max(0.5, min(2.5, b_param))

    # Evaluate Holland profile (fitted to surface winds directly)
    wind_ms = _holland_wind_at_radius(dist_m, rmw, vmax, b_param)

    # Apply translational asymmetry (Emanuel 2005)
    wind_ms += _translational_asymmetry(azimuth, snapshot.storm_speed_ms, snapshot.storm_dir_deg)
    wind_ms = max(0.0, wind_ms)

    # Overland reduction: buildings are on land where surface friction
    # reduces winds compared to the over-water IBTrACS reference.
    # Standard factor: 0.85 (WMO/NHC guideline for near-coast terrain).
    wind_ms *= 0.85

    # Outer taper: beyond the R34 extent, linearly decay to 0 over
    # another R34-width distance.  This prevents unrealistic wind speeds
    # at extreme range where the Holland profile becomes unphysical.
    if r34_m > 0 and dist_m > r34_m:
        taper_end = r34_m * 2.0
        if dist_m >= taper_end:
            wind_ms = 0.0
        else:
            taper_frac = 1.0 - (dist_m - r34_m) / (taper_end - r34_m)
            wind_ms *= taper_frac

    return round(wind_ms * MS_TO_MPH, 1)


def get_wind_speed_for_building(
    storm_id: str,
    landfall_lat: float,
    landfall_lon: float,
    building_lat: float,
    building_lon: float,
) -> Optional[float]:
    """
    Convenience wrapper: resolve storm → snapshot → wind speed at building.

    Returns wind speed in mph, or None if the storm lacks IBTrACS data.
    """
    snap = load_landfall_snapshot(storm_id, landfall_lat, landfall_lon)
    if snap is None:
        return None
    return get_wind_speed_at_point(snap, building_lat, building_lon)
