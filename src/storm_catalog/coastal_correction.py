"""
Regional Coastal Surge Amplification Correction.

The parametric surge formula in surge_model.py produces a baseline surge height
calibrated against historical US landfalls.  That baseline does not account for
the dramatic variation in surge amplification between coastal regions:

  - Louisiana coast (180km shelf, 0.0002 slope): surge amplified 1.5–2×
  - Puerto Rico (8km shelf, steep drop-off):     surge damped to ~0.4×
  - NY Bight (wide shelf + bay funneling):       surge amplified ~1.5×
  - Lesser Antilles (4km shelf, steep):          surge damped to ~0.35×

This module provides a per-region correction factor that, when applied to the
surge raster output, produces regionally accurate surge depths without requiring
a full hydrodynamic model.

Data source: Extracted from StormDPS/core/storm_surge.py COASTAL_PROFILES,
calibrated against NOAA tide gauge records and SLOSH model outputs.

Reference: Irish et al. (2008); Weisberg & Zheng (2006); Dietrich et al. (2011)

Usage:
    factor = get_coastal_factor(landfall_lat, landfall_lon)
    surge_raster *= factor          # apply in generate_surge_raster()
"""

from __future__ import annotations
from dataclasses import dataclass


@dataclass(frozen=True)
class RegionProfile:
    name: str
    surge_amplification: float  # Shelf geometry multiplier vs. neutral coast
    wetland_buffer: float       # Fraction of surge absorbed by coastal wetlands (0–1)
    bay_funneling: float        # Bay/estuary amplification (1.0 = open coast)

    @property
    def effective_factor(self) -> float:
        """Net surge multiplier: amplification × (1 - wetland absorption) × funneling."""
        return self.surge_amplification * (1.0 - self.wetland_buffer) * self.bay_funneling


# ── Regional profiles ──────────────────────────────────────────────────────────
# Source: StormDPS/core/storm_surge.py COASTAL_PROFILES (surge_amplification,
# wetland_buffer, bay_funneling).  Rainfall and economic fields omitted.
REGION_PROFILES: dict[str, RegionProfile] = {

    # ── US Gulf Coast (highest surge vulnerability) ──────────────────────────
    "gulf_west_tx":       RegionProfile("W Texas Coast",          1.35, 0.10, 1.15),
    "gulf_central_tx":    RegionProfile("Central Texas Coast",    1.40, 0.08, 1.15),
    "gulf_la":            RegionProfile("Louisiana Coast",        1.70, 0.12, 1.25),
    "gulf_ms_al":         RegionProfile("Mississippi/Alabama",    1.40, 0.12, 1.25),
    "gulf_fl_panhandle":  RegionProfile("FL Panhandle",           1.25, 0.05, 1.10),
    "gulf_fl_west":       RegionProfile("FL West Coast",          1.35, 0.10, 1.20),

    # ── US Atlantic Coast ────────────────────────────────────────────────────
    "atl_fl_east":        RegionProfile("FL East Coast",          1.05, 0.05, 1.20),
    "atl_ga_sc":          RegionProfile("GA/SC Coast",            1.20, 0.20, 1.15),
    "atl_nc":             RegionProfile("NC Coast",               1.15, 0.15, 1.20),
    "atl_mid":            RegionProfile("Mid-Atlantic (VA–NJ)",   1.30, 0.08, 1.25),
    "atl_ne":             RegionProfile("NE (NY–New England)",    1.35, 0.05, 1.35),

    # ── Caribbean ────────────────────────────────────────────────────────────
    "carib_pr":           RegionProfile("Puerto Rico",            0.70, 0.02, 1.05),
    "carib_usvi":         RegionProfile("US Virgin Islands",      0.60, 0.01, 1.00),
    "carib_bahamas":      RegionProfile("Bahamas",                1.30, 0.02, 1.05),
    "carib_jamaica":      RegionProfile("Jamaica",                0.75, 0.03, 1.05),
    "carib_cuba_n":       RegionProfile("Northern Cuba",          1.00, 0.10, 1.10),
    "carib_hispaniola":   RegionProfile("Hispaniola (DR/Haiti)",  0.72, 0.02, 1.05),
    "carib_lesser_antilles": RegionProfile("Lesser Antilles",    0.55, 0.01, 1.00),
    "carib_cayman":       RegionProfile("Cayman Islands",         0.50, 0.01, 1.00),

    # ── Mexico / Central America ─────────────────────────────────────────────
    "mex_yucatan":        RegionProfile("Yucatan Peninsula",      1.55, 0.15, 1.05),
    "mex_gulf":           RegionProfile("Mexico Gulf Coast",      1.10, 0.10, 1.05),
    "central_am":         RegionProfile("Central America",        0.80, 0.08, 1.05),

    # ── Fallback ─────────────────────────────────────────────────────────────
    "open_ocean":         RegionProfile("Open Ocean",             1.00, 0.00, 1.00),
}


# ── Reference factor ───────────────────────────────────────────────────────────
# The base surge formula is calibrated against US landfalls whose coastal
# effective factors average to this value.  Dividing by it normalises the
# correction so calibration storms stay accurate.
#
# Derived from six reference storms (geometric-mean approach):
#   Sandy   → atl_ne          effective = 1.729
#   Katrina → gulf_la         effective = 1.870
#   Ike     → gulf_central_tx effective = 1.482
#   Harvey  → gulf_central_tx effective = 1.482
#   Michael → gulf_fl_panhandle effective = 1.306
#   Charley → gulf_fl_west    effective = 1.458
#   mean ≈ 1.555
REFERENCE_COASTAL_FACTOR: float = 1.555


def get_region_key(lat: float, lon: float) -> str:
    """
    Return the coastal region key for a landfall lat/lon.

    Checks bounding boxes in priority order (most specific first).
    Matches the legacy fallback logic in StormDPS/core/storm_surge.py so
    both tools assign the same region to the same coordinates.

    Returns "open_ocean" when no coastal region matches.
    """
    # ── Caribbean ─────────────────────────────────────────────────────────────
    if lat < 21 and -88 < lon < -59:
        # Puerto Rico
        if 17.8 < lat < 18.6 and -67.3 < lon < -65.5:
            return "carib_pr"
        # USVI
        if 17.6 < lat < 18.4 and -65.1 < lon < -64.5:
            return "carib_usvi"
        # Bahamas
        if 20 < lat < 27 and -80 < lon < -73:
            return "carib_bahamas"
        # Cayman Islands
        if 19.2 < lat < 19.8 and -81.6 < lon < -79.7:
            return "carib_cayman"
        # Jamaica
        if 17.5 < lat < 18.6 and -78.5 < lon < -76:
            return "carib_jamaica"
        # Cuba (north coast)
        if 19.5 < lat < 23.5 and -85 < lon < -74:
            return "carib_cuba_n"
        # Hispaniola
        if 17.5 < lat < 20.5 and -75 < lon < -68:
            return "carib_hispaniola"
        # Lesser Antilles
        if 12 < lat < 18.5 and -63 < lon < -59:
            return "carib_lesser_antilles"
        return "carib_lesser_antilles"  # default for unknown Caribbean

    # ── Yucatan Peninsula ──────────────────────────────────────────────────────
    if 18 < lat < 22 and -92 < lon < -86:
        return "mex_yucatan"

    # ── Mexico Gulf Coast ──────────────────────────────────────────────────────
    # Cap at 26.5°N — US-Mexico border (Rio Grande mouth) is at ~26°N;
    # above that, the lat/lon belongs to the Texas Gulf Coast, not Mexico.
    if 18 < lat < 26.5 and -100 < lon < -93.5:
        return "mex_gulf"

    # ── Central America (Caribbean side) ──────────────────────────────────────
    if 8 < lat < 18 and -92 < lon < -76:
        return "central_am"

    # ── US Gulf Coast — west to east ──────────────────────────────────────────
    # Western TX (Corpus Christi–Brownsville)
    if 25.5 < lat < 28.5 and -98 < lon < -96:
        return "gulf_west_tx"
    # Central TX (Galveston–Corpus Christi)
    if 27 < lat < 30.5 and -97.5 < lon < -93.5:
        return "gulf_central_tx"
    # Louisiana coast (extends a bit east to capture MS-border landfalls)
    if 28 < lat < 31 and -93.5 < lon < -89.3:
        return "gulf_la"
    # Mississippi / Alabama coast
    if 29 < lat < 31 and -89.5 < lon < -87.3:
        return "gulf_ms_al"
    # FL Panhandle (including Big Bend transition zone)
    if 29 < lat < 31 and -87.5 < lon < -83.5:
        return "gulf_fl_panhandle"
    # FL West Coast (Tampa Bay down to Naples)
    if 24 < lat < 30 and -83.5 < lon < -79.5:
        return "gulf_fl_west"

    # ── US Atlantic Coast — south to north ────────────────────────────────────
    # FL East Coast (Miami to Jacksonville)
    if 24 < lat < 31 and -81.5 < lon < -79.5:
        return "atl_fl_east"
    # GA / SC Coast
    if 30 < lat < 34 and -82 < lon < -78:
        return "atl_ga_sc"
    # NC Coast
    if 33.5 < lat < 37 and -79 < lon < -74:
        return "atl_nc"
    # Mid-Atlantic (VA to NJ)
    if 36 < lat < 40 and -77 < lon < -73:
        return "atl_mid"
    # Northeast (NY to New England)
    if 39 < lat < 45 and -76 < lon < -68:
        return "atl_ne"

    return "open_ocean"


def get_coastal_factor(landfall_lat: float, landfall_lon: float) -> float:
    """
    Return the normalised coastal surge correction factor for a landfall location.

    A factor of 1.0 means the coast behaves like the reference conditions the
    base surge formula was calibrated against (average US Gulf/Atlantic landfall).
    Factor > 1.0 = more amplification (wide shallow shelf, bay funneling).
    Factor < 1.0 = less amplification (steep drop-off, small island, wetland buffer).

    Typical values:
        Louisiana coast:      ~1.20  (very wide shallow shelf)
        NY Bight / NE:        ~1.11  (wide shelf + strong bay funneling)
        FL Panhandle:         ~0.84  (moderate shelf, less funneling)
        Puerto Rico:          ~0.46  (steep, narrow shelf)
        Lesser Antilles:      ~0.35  (extremely steep drop-off)

    Returns:
        float ≥ 0, multiplicative factor to apply to the surge raster.
    """
    key = get_region_key(landfall_lat, landfall_lon)
    if key == "open_ocean":
        return 1.0  # No coastal correction for open-ocean positions

    profile = REGION_PROFILES.get(key)
    if profile is None:
        return 1.0

    return profile.effective_factor / REFERENCE_COASTAL_FACTOR
