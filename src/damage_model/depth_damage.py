"""
FEMA Depth-Damage Functions

Implements the FIA (Federal Insurance Administration) credibility-weighted
depth-damage curves used by FEMA's HAZUS flood model.

These functions map flood depth (feet above first finished floor) to a
damage percentage for both building structure and contents.

Source: FEMA HAZUS Flood Model Technical Manual (Chapter 5: Direct Physical
Damage — General Building Stock). Curves derived from FIA/NFIP claims data,
interpolated to 1-foot increments from -4 to +24 feet.

Building categories follow HAZUS occupancy codes:
  RES1-1SNB: 1-story, no basement
  RES1-2SNB: 2-story, no basement
  RES1-1SWB: 1-story, with basement
  RES1-2SWB: 2-story, with basement
  RES1-SL:   Split-level

Each curve has structure and contents variants.
"""

from __future__ import annotations

import hashlib
import logging
import math
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from damage_model.building_adjuster import adjust_damage_pct

import numpy as np

logger = logging.getLogger(__name__)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Wind Damage Model (ported from StormDPS economic_vulnerability.py)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#
# Based on:
#   - Emanuel (2011) power dissipation index relationship
#   - Florida Building Code loss reduction studies
#   - HAZUS-MH wind damage curves
#
# The function uses a modified power law: d = 1 - exp(-3 × normalized^2.5)
# with a building-code-dependent onset threshold.


def estimate_wind_damage_pct(
    wind_speed_mph: float,
    building_resilience: float = 0.60,
    med_yr_blt: Optional[int] = None,
) -> float:
    """
    Estimate wind damage as a percentage of structure replacement value.

    Based on StormDPS core/economic_vulnerability.py wind_damage_function(),
    recalibrated for 1-minute sustained surface winds (IBTrACS reference).

    The original StormDPS function used gradient-level winds (~1.25× surface);
    thresholds here are adjusted downward accordingly.  Onset thresholds
    align with FEMA HAZUS-MH wind damage initiation for wood-frame
    residential:  ~55 mph for older construction, ~70 mph for modern code.

    Args:
        wind_speed_mph: Sustained surface wind speed at the building (mph)
        building_resilience: Building code quality factor (0-1, 1 = best).
                             Default 0.60 = typical Gulf Coast.
        med_yr_blt: Year built — post-2002 Florida Building Code gets
                    a resilience boost; pre-1995 gets a penalty.

    Returns:
        Structure damage percentage (0-100) from wind only.
    """
    vmax_ms = wind_speed_mph * 0.44704  # mph → m/s

    # Era adjustment to building resilience
    if med_yr_blt is not None:
        if med_yr_blt >= 2002:
            building_resilience = min(1.0, building_resilience + 0.15)
        elif med_yr_blt < 1995:
            building_resilience = max(0.20, building_resilience * 0.85)

    # Damage onset threshold (calibrated for 1-min sustained surface winds)
    #   resilience=0.40 (poor code):  23 m/s ≈  51 mph (damage at strong TS)
    #   resilience=0.60 (typical):    27 m/s ≈  60 mph (damage at weak Cat 1)
    #   resilience=0.80 (modern):     31 m/s ≈  69 mph (damage at Cat 1)
    #   resilience=1.00 (best):       35 m/s ≈  78 mph (damage at Cat 1+)
    v_threshold = 15.0 + 20.0 * building_resilience

    if vmax_ms < v_threshold:
        return 0.0

    v_excess = vmax_ms - v_threshold
    # Cat 5 surface sustained ≈ 70 m/s (157 mph)
    v_max_excess = 70.0 - v_threshold

    normalized = min(1.0, v_excess / v_max_excess)

    # Modified power law: rapid onset, then saturation
    # d = 1 - exp(-3 × n^2.5)
    damage_frac = 1.0 - math.exp(-3.0 * normalized ** 2.5)
    damage_frac = max(0.0, min(1.0, damage_frac))

    return round(damage_frac * 100, 1)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# FIA Depth-Damage Tables
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# Depths in feet relative to first finished floor.
# Negative = water below floor level; 0 = at floor; positive = above.
# HAZUS uses 29 points from -4 to +24 feet at 1-foot intervals.
# We use the key inflection points for interpolation.

DEPTHS_FT = [-4, -3, -2, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8,
             10, 12, 14, 16, 18, 20, 22, 24]

# Structure damage (% of replacement cost)
# Source: FIA credibility-weighted damage functions, HAZUS Tech Manual Ch.5
STRUCTURE_DAMAGE: Dict[str, List[float]] = {
    # 1-story, no basement (most common coastal residential)
    "RES1-1SNB": [
        0, 0, 0, 0, 10, 18, 25, 31, 37, 42, 46, 50, 53,
        58, 62, 65, 67, 69, 70, 71, 72,
    ],
    # 2-story, no basement
    "RES1-2SNB": [
        0, 0, 0, 0, 8, 14, 20, 25, 29, 33, 36, 39, 42,
        47, 52, 56, 59, 62, 64, 66, 68,
    ],
    # 1-story, with basement
    "RES1-1SWB": [
        2, 5, 8, 12, 16, 23, 29, 35, 40, 45, 49, 52, 55,
        60, 64, 67, 69, 71, 72, 73, 74,
    ],
    # 2-story, with basement
    "RES1-2SWB": [
        2, 4, 7, 10, 13, 18, 24, 29, 33, 37, 40, 43, 46,
        51, 55, 59, 62, 64, 66, 68, 70,
    ],
    # Split-level
    "RES1-SL": [
        1, 3, 5, 8, 12, 17, 22, 27, 32, 36, 40, 43, 46,
        51, 55, 59, 62, 64, 66, 68, 70,
    ],
    # Commercial (simplified: average of COM1-COM10)
    "COM": [
        0, 0, 0, 0, 5, 10, 15, 20, 25, 30, 35, 39, 43,
        50, 55, 59, 62, 65, 67, 69, 70,
    ],
    # Industrial
    "IND": [
        0, 0, 0, 0, 4, 8, 13, 18, 23, 28, 32, 36, 40,
        47, 52, 56, 60, 63, 65, 67, 69,
    ],
}

# Contents damage (% of contents value)
# Contents value defaults to 50% of building replacement cost (HAZUS default)
CONTENTS_DAMAGE: Dict[str, List[float]] = {
    "RES1-1SNB": [
        0, 0, 1, 5, 12, 22, 32, 40, 47, 53, 58, 62, 65,
        70, 74, 77, 79, 81, 82, 83, 84,
    ],
    "RES1-2SNB": [
        0, 0, 1, 4, 10, 18, 26, 33, 39, 44, 49, 53, 56,
        62, 67, 71, 74, 76, 78, 80, 82,
    ],
    "RES1-1SWB": [
        5, 8, 12, 16, 20, 28, 36, 43, 49, 55, 60, 64, 67,
        72, 76, 79, 81, 83, 84, 85, 86,
    ],
    "RES1-2SWB": [
        4, 6, 10, 14, 17, 24, 31, 37, 43, 48, 52, 56, 59,
        65, 70, 74, 77, 79, 81, 83, 84,
    ],
    "RES1-SL": [
        3, 5, 8, 12, 15, 21, 28, 35, 41, 46, 50, 54, 57,
        63, 68, 72, 75, 78, 80, 82, 83,
    ],
    "COM": [
        0, 0, 0, 1, 8, 16, 24, 32, 39, 46, 52, 57, 61,
        68, 73, 77, 80, 82, 84, 86, 87,
    ],
    "IND": [
        0, 0, 0, 1, 6, 13, 20, 27, 34, 40, 46, 51, 55,
        63, 69, 73, 77, 80, 82, 84, 85,
    ],
}

# Default building type when not specified
DEFAULT_BUILDING_TYPE = "RES1-1SNB"

# Default replacement cost per sq ft (USD, 2024 Gulf Coast average)
# Source: RSMeans residential cost data, adjusted for Gulf Coast region
DEFAULT_COST_PER_SQFT: Dict[str, float] = {
    "RES1-1SNB": 150.0,
    "RES1-2SNB": 145.0,
    "RES1-1SWB": 160.0,
    "RES1-2SWB": 155.0,
    "RES1-SL": 155.0,
    "COM": 175.0,
    "IND": 120.0,
}

# Default building sizes (sq ft) for damage estimation when area is unknown
DEFAULT_SQFT: Dict[str, float] = {
    "RES1-1SNB": 1400.0,
    "RES1-2SNB": 2200.0,
    "RES1-1SWB": 1600.0,
    "RES1-2SWB": 2400.0,
    "RES1-SL": 1800.0,
    "COM": 5000.0,
    "IND": 10000.0,
}

# ── Contents-to-Structure Value Ratios ────────────────────────────────────
# Source: FEMA HAZUS Flood Technical Manual, Table 5.7
# "Ratio of Contents to Structure Value by Occupancy"
#
# The flat 0.50 ratio is HAZUS's default for single-family residential.
# Commercial and industrial occupancy types have dramatically different
# ratios — a retail store's inventory can equal its building value, and a
# heavy-industrial facility's equipment can be worth 1.5× the structure.
#
# Keyed by NSI occtype prefix so that "COM1" and "COM1-1S" both match "COM1".
# Fallback: 0.50 for any unrecognized code.
CONTENTS_TO_STRUCTURE_RATIO_TABLE: Dict[str, float] = {
    # ── Residential ──
    "RES1":  0.50,   # Single-family dwelling
    "RES2":  0.50,   # Manufactured housing
    "RES3":  0.50,   # Multi-family (2-4 units)
    "RES4":  0.50,   # Temporary lodging (hotel/motel)
    "RES5":  0.50,   # Institutional dormitory
    "RES6":  0.50,   # Nursing home
    # ── Commercial ──
    "COM1":  1.00,   # Retail trade (inventory-heavy)
    "COM2":  1.00,   # Wholesale trade (inventory-heavy)
    "COM3":  0.50,   # Personal/repair services
    "COM4":  1.00,   # Professional/technical services (IT equipment)
    "COM5":  0.50,   # Banks/financial institutions
    "COM6":  0.50,   # Hospital/medical office
    "COM7":  0.50,   # Medical office/clinic
    "COM8":  0.75,   # Restaurant/bar (equipment + perishables)
    "COM9":  1.00,   # Entertainment/recreation
    "COM10": 0.50,   # Parking garage
    # ── Industrial ──
    "IND1":  1.50,   # Heavy industrial (machinery/equipment dominant)
    "IND2":  1.50,   # Light industrial
    "IND3":  1.50,   # Food/drugs/chemicals manufacturing
    "IND4":  1.50,   # Metals/minerals processing
    "IND5":  1.50,   # High-technology manufacturing
    "IND6":  1.00,   # Construction
    # ── Agriculture ──
    "AGR1":  1.00,   # Agriculture
    # ── Religious / Government / Education ──
    "REL1":  1.00,   # Church/non-profit
    "GOV1":  0.50,   # General government
    "GOV2":  0.50,   # Emergency response
    "EDU1":  0.50,   # Schools
    "EDU2":  0.50,   # Colleges/universities
}

# Default fallback ratio (HAZUS residential default)
CONTENTS_TO_STRUCTURE_RATIO = 0.50


def _get_contents_ratio(occtype: Optional[str] = None) -> float:
    """
    Return the contents-to-structure value ratio for a given occupancy type.

    Matches on the longest prefix: "COM1" matches "COM1", "RES1-1SNB" matches "RES1".
    Falls back to the flat 0.50 default for unrecognized codes.
    """
    if not occtype:
        return CONTENTS_TO_STRUCTURE_RATIO
    occ = occtype.upper().strip()
    # Try exact match first, then progressively shorter prefixes
    for length in range(len(occ), 2, -1):
        prefix = occ[:length]
        if prefix in CONTENTS_TO_STRUCTURE_RATIO_TABLE:
            return CONTENTS_TO_STRUCTURE_RATIO_TABLE[prefix]
    return CONTENTS_TO_STRUCTURE_RATIO

# First finished floor height above grade (feet)
# Slab-on-grade (no basement) = ~1 foot; with basement = ~1 foot;
# elevated (V-zone) = varies. This shifts the depth relative to ground.
DEFAULT_FFH_FT: Dict[str, float] = {
    "RES1-1SNB": 1.0,
    "RES1-2SNB": 1.0,
    "RES1-1SWB": 1.0,  # Basement floor is below, first floor at grade+1
    "RES1-2SWB": 1.0,
    "RES1-SL": 0.5,
    "COM": 0.0,
    "IND": 0.0,
}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Interpolation Engine
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def get_damage_pct(
    depth_ft: float,
    building_type: str = DEFAULT_BUILDING_TYPE,
    component: str = "structure",
) -> float:
    """
    Look up the damage percentage for a given flood depth and building type.

    Uses linear interpolation between the tabulated depth-damage curve points.

    Args:
        depth_ft: Flood depth in feet relative to first finished floor.
                  Negative values = water below floor level.
        building_type: HAZUS occupancy code (e.g., "RES1-1SNB")
        component: "structure" or "contents"

    Returns:
        Damage as a percentage (0-100) of replacement value
    """
    table = STRUCTURE_DAMAGE if component == "structure" else CONTENTS_DAMAGE
    curve = table.get(building_type, table[DEFAULT_BUILDING_TYPE])

    # Clamp to curve bounds
    if depth_ft <= DEPTHS_FT[0]:
        return float(curve[0])
    if depth_ft >= DEPTHS_FT[-1]:
        return float(curve[-1])

    # Linear interpolation
    for i in range(len(DEPTHS_FT) - 1):
        if DEPTHS_FT[i] <= depth_ft <= DEPTHS_FT[i + 1]:
            frac = (depth_ft - DEPTHS_FT[i]) / (DEPTHS_FT[i + 1] - DEPTHS_FT[i])
            return curve[i] + frac * (curve[i + 1] - curve[i])

    return 0.0


def get_total_damage_pct(
    depth_ft: float,
    building_type: str = DEFAULT_BUILDING_TYPE,
    occtype: Optional[str] = None,
) -> float:
    """
    Combined structure + contents damage as percentage of total value.

    Total value = structure replacement + contents (ratio depends on occupancy type).
    Combined damage = (struct_dmg * struct_val + content_dmg * content_val) / total_val
    """
    struct_pct = get_damage_pct(depth_ft, building_type, "structure")
    content_pct = get_damage_pct(depth_ft, building_type, "contents")

    # Use occupancy-specific ratio if available, else HAZUS residential default
    r = _get_contents_ratio(occtype)
    return (struct_pct + r * content_pct) / (1 + r)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Damage Estimation
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


@dataclass
class BuildingDamage:
    """Estimated damage for a single building."""

    building_id: str
    lon: float
    lat: float
    depth_m: float
    depth_ft: float
    building_type: str
    structure_damage_pct: float
    contents_damage_pct: float
    total_damage_pct: float
    estimated_loss_usd: float
    replacement_value_usd: float
    found_ht: Optional[float] = None      # foundation height (ft above grade)
    val_struct: Optional[float] = None     # structure replacement value (USD)
    val_cont: Optional[float] = None       # contents replacement value (USD)
    # ── Wind damage (separate peril, from StormDPS wind model) ──
    wind_damage_pct: Optional[float] = None        # structure damage from wind (%)
    wind_loss_usd: Optional[float] = None          # wind-only loss (USD)
    wind_speed_mph: Optional[float] = None         # wind speed at building location
    combined_loss_usd: Optional[float] = None      # surge + wind combined loss
    # ── Depth uncertainty confidence interval ──
    # Parametric surge models have ~±30% depth uncertainty.  We report
    # low/high loss bounds so adjusters can bracket their estimates.
    loss_low_usd: Optional[float] = None   # loss at 0.7× depth (optimistic)
    loss_high_usd: Optional[float] = None  # loss at 1.3× depth (conservative)
    structure_dmg_pct_low: Optional[float] = None
    structure_dmg_pct_high: Optional[float] = None
    contents_dmg_pct_low: Optional[float] = None
    contents_dmg_pct_high: Optional[float] = None
    # ── FEMA IHP eligibility estimate ──
    ihp_eligible: Optional[bool] = None    # estimated IHP eligibility
    ihp_category: Optional[str] = None     # "minor"/"moderate"/"major"/"severe"
    ihp_est_amount: Optional[float] = None # estimated IHP payout (USD)
    # ── Rainfall flood damage (separate peril) ──
    rainfall_depth_m: Optional[float] = None    # rainfall-induced flood depth
    rainfall_loss_usd: Optional[float] = None   # rainfall-only loss
    # ── Confidence & Uncertainty ──
    confidence: Optional[str] = None             # "high" | "medium" | "low"
    confidence_score: Optional[float] = None     # 0.0–1.0 composite
    value_confidence: Optional[str] = None       # "high" | "medium" | "low"
    value_method: Optional[str] = None           # how replacement value was estimated
    value_low_usd: Optional[float] = None        # 20th percentile replacement
    value_high_usd: Optional[float] = None       # 80th percentile replacement
    foundation_confidence: Optional[str] = None  # "high" | "medium" | "low"
    foundation_type: Optional[str] = None        # "slab" | "crawlspace" | "elevated"
    prob_elevated: Optional[float] = None        # P(foundation > 2ft)
    # ── Pass-through metadata from building source (NSI/OSM) ──
    # These fields survive the damage pipeline so the frontend CSV export
    # can include them for insurance adjusters and emergency managers.
    source: Optional[str] = None           # "NSI" | "OSM" | "MSFT"
    data_quality: Optional[float] = None   # 0.0–1.0 reliability score
    occtype: Optional[str] = None          # raw NSI occupancy code (e.g. "RES1", "COM3")
    med_yr_blt: Optional[int] = None       # median year built
    num_story: Optional[int] = None        # number of stories
    area_sqft: Optional[float] = None      # building footprint in sqft


@dataclass
class DamageEstimate:
    """Aggregated damage estimate for a storm area."""

    buildings_assessed: int
    buildings_damaged: int
    total_loss_usd: float
    total_replacement_usd: float
    avg_damage_pct: float
    max_damage_pct: float
    damage_by_category: Dict[str, int]  # e.g., {"minor": 50, "major": 20, ...}
    buildings: List[BuildingDamage]

    def damage_category(self, pct: float) -> str:
        """Classify damage percentage into FEMA categories."""
        return _damage_category(pct)


def _damage_category(pct: float) -> str:
    """Classify damage percentage into FEMA categories (standalone version)."""
    if pct <= 0:
        return "none"
    elif pct < 10:
        return "minor"
    elif pct < 30:
        return "moderate"
    elif pct < 50:
        return "major"
    else:
        return "severe"


def _cost_multiplier(building_id: str) -> float:
    """
    Return a deterministic per-building replacement-cost multiplier in [0.60, 1.40].

    Seeded by the building ID so the same building always gets the same value,
    but neighbouring buildings with different IDs get meaningfully different
    estimates — reflecting real-world variation in age, condition, finish level,
    and local market value that OSM tags don't capture.
    """
    if not building_id:
        return 1.0
    digest = int(hashlib.md5(building_id.encode()).hexdigest()[:8], 16)
    return 0.60 + (digest / 0xFFFF_FFFF) * 0.80   # uniform in [0.60, 1.40]


def estimate_building_damage(
    depth_m: float,
    lon: float = 0.0,
    lat: float = 0.0,
    building_type: str = DEFAULT_BUILDING_TYPE,
    building_id: str = "",
    sqft: Optional[float] = None,
    first_floor_ht_ft: Optional[float] = None,
    val_struct: Optional[float] = None,
    val_cont: Optional[float] = None,
    med_yr_blt: Optional[int] = None,
    num_story: Optional[int] = None,
    occtype: Optional[str] = None,
    use_nsi_adjustments: bool = True,
    county_cost_per_sqft: Optional[float] = None,
    wind_speed_mph: Optional[float] = None,
    building_resilience: float = 0.60,
    state_fips: Optional[str] = None,
    flood_zone: Optional[str] = None,
    rainfall_depth_m: Optional[float] = None,
) -> BuildingDamage:
    """
    Estimate flood damage for a single building.

    When val_struct / val_cont are provided (sourced from FEMA NSI), they are
    used directly as the replacement values so the damage calculation is:
        loss = struct_pct% × val_struct + content_pct% × val_cont

    Without NSI data the model falls back to:
        replacement = (sqft or type_default) × cost_per_sqft × id_multiplier

    When use_nsi_adjustments=True (default), per-building NSI attributes are
    used to refine the HAZUS base damage percentages:
        - Foundation height (found_ht): elevated buildings sustain less damage
        - Construction era (med_yr_blt): pre-1970 buildings are more vulnerable
        - Multi-story (num_story): upper-floor contents survive partial flooding

    Args:
        depth_m: Flood depth at the building location (meters, above ground)
        lon, lat: Building coordinates
        building_type: HAZUS occupancy code
        building_id: Unique building identifier
        sqft: Building area in square feet (defaults per type if not provided)
        first_floor_ht_ft: First-floor elevation above grade (ft); from NSI found_ht
        val_struct: Structure replacement value in USD (from FEMA NSI)
        val_cont:   Contents replacement value in USD (from FEMA NSI)
        med_yr_blt: Median year built (from FEMA NSI)
        num_story:  Number of stories (from FEMA NSI)
        occtype:    NSI occupancy type code (e.g. "RES1")
        use_nsi_adjustments: Apply per-building refinements (default True)
        county_cost_per_sqft: Census ACS-derived $/sqft for this county (optional).
                              Overrides the flat DEFAULT_COST_PER_SQFT when no
                              NSI val_struct is available.

    Returns:
        BuildingDamage with loss estimates
    """
    depth_ft = depth_m * 3.28084

    # Adjust for first finished floor height
    # NSI found_ht is the elevation above grade — use it when available
    _fdn_confidence = "low"
    _fdn_type = "unknown"
    _prob_elevated = None
    if first_floor_ht_ft is not None:
        ffh = first_floor_ht_ft
        _fdn_confidence = "high"  # NSI data
        _fdn_type = "elevated" if ffh >= 2.0 else "slab"
    else:
        # Enhanced fallback: probabilistic foundation estimation
        try:
            from damage_model.foundation_estimator import estimate_foundation_height
            fe = estimate_foundation_height(
                building_type=building_type,
                state_fips=state_fips,
                flood_zone=flood_zone,
                med_yr_blt=med_yr_blt,
                lat=lat, lon=lon,
            )
            ffh = fe.best_estimate_ft
            _fdn_confidence = fe.confidence
            _fdn_type = fe.foundation_type
            _prob_elevated = fe.prob_elevated
        except Exception:
            ffh = DEFAULT_FFH_FT.get(building_type, 1.0)
    depth_above_floor_ft = depth_ft - ffh

    # Look up damage percentages from HAZUS depth-damage curves
    struct_pct = get_damage_pct(depth_above_floor_ft, building_type, "structure")
    content_pct = get_damage_pct(depth_above_floor_ft, building_type, "contents")

    # Apply NSI-based per-building adjustments when attributes are available
    if use_nsi_adjustments and any(v is not None for v in (first_floor_ht_ft, med_yr_blt, num_story)):
        struct_pct, content_pct = adjust_damage_pct(
            structure_pct=struct_pct,
            contents_pct=content_pct,
            found_ht=first_floor_ht_ft,
            med_yr_blt=med_yr_blt,
            num_story=num_story,
            occtype=occtype,
            depth_above_grade_ft=depth_ft,
        )

    # Recompute weighted total from (possibly adjusted) struct/content pcts
    # Use occupancy-specific contents-to-structure ratio (HAZUS Table 5.7)
    r = _get_contents_ratio(occtype)
    total_pct = (struct_pct + r * content_pct) / (1 + r)

    # ── Replacement value ───────────────────────────────────────────
    _val_confidence = "low"
    _val_method = "default"
    _val_low = _val_high = None
    if val_struct is not None:
        # NSI path: use actual tabulated replacement costs
        struct_value = float(val_struct)
        content_value = float(val_cont) if val_cont is not None else struct_value * r
        _val_confidence = "high"
        _val_method = "NSI tabulated"
        _val_low = struct_value * 0.90   # NSI has ~±10% uncertainty
        _val_high = struct_value * 1.10
    else:
        # Enhanced fallback: use PropertyValuation for layered estimate
        try:
            from damage_model.property_estimator import estimate_replacement_value
            pv = estimate_replacement_value(
                building_type=building_type,
                sqft=sqft,
                county_cost_per_sqft=county_cost_per_sqft,
                med_yr_blt=med_yr_blt,
                state_fips=state_fips,
                building_id=building_id,
            )
            struct_value = pv.mid_usd
            content_value = struct_value * r
            _val_confidence = pv.confidence
            _val_method = pv.method
            _val_low = pv.low_usd
            _val_high = pv.high_usd
        except Exception:
            # Final fallback: original simple estimation
            area = sqft or DEFAULT_SQFT.get(building_type, 1400)
            if county_cost_per_sqft is not None:
                base_cost = county_cost_per_sqft
            else:
                base_cost = DEFAULT_COST_PER_SQFT.get(building_type, 150)
            cost_per_sqft = base_cost * _cost_multiplier(building_id)
            struct_value = area * cost_per_sqft
            content_value = struct_value * r
            _val_method = "flat $/sqft fallback"

    replacement = struct_value + content_value
    loss = (struct_pct / 100 * struct_value) + (content_pct / 100 * content_value)

    # ── Depth uncertainty bounds (±30%) ─────────────────────────────
    # Parametric surge models carry ~30% depth uncertainty (FEMA 2015,
    # "Guidelines for Flood Risk Analysis").  We bracket loss at 0.7×
    # and 1.3× the best-estimate depth to give adjusters a range.
    loss_low = loss_high = None
    s_low = s_high = c_low = c_high = None
    for depth_mult, tag in [(0.7, "low"), (1.3, "high")]:
        d_ft = depth_m * depth_mult * 3.28084
        daf = d_ft - ffh
        sp = get_damage_pct(daf, building_type, "structure")
        cp = get_damage_pct(daf, building_type, "contents")
        if use_nsi_adjustments and any(v is not None for v in (first_floor_ht_ft, med_yr_blt, num_story)):
            sp, cp = adjust_damage_pct(
                structure_pct=sp, contents_pct=cp,
                found_ht=first_floor_ht_ft, med_yr_blt=med_yr_blt,
                num_story=num_story, occtype=occtype,
                depth_above_grade_ft=d_ft,
            )
        bound_loss = (sp / 100 * struct_value) + (cp / 100 * content_value)
        if tag == "low":
            loss_low = round(bound_loss, 0)
            s_low, c_low = round(sp, 1), round(cp, 1)
        else:
            loss_high = round(bound_loss, 0)
            s_high, c_high = round(sp, 1), round(cp, 1)

    # ── FEMA IHP eligibility estimate ───────────────────────────────
    # IHP provides assistance to owner-occupied primary residences.
    # Thresholds based on FEMA IHP guidance (simplified):
    #   - Real property damage > $0 → eligible for some assistance
    #   - Up to $41,000 (FY2024 max, adjusted periodically)
    # Categories mirror FEMA preliminary damage assessment levels.
    ihp_eligible = None
    ihp_category = None
    ihp_est_amount = None
    occ_upper = (occtype or "").upper()
    is_residential = occ_upper.startswith("RES") or building_type.startswith("RES")
    if is_residential and loss > 0:
        ihp_eligible = True
        IHP_MAX = 42_500.0  # FY2025 maximum IHP award
        struct_loss = struct_pct / 100 * struct_value
        if struct_loss < 5_000:
            ihp_category = "minor"
            ihp_est_amount = min(struct_loss * 0.8, IHP_MAX)
        elif struct_loss < 20_000:
            ihp_category = "moderate"
            ihp_est_amount = min(struct_loss * 0.7, IHP_MAX)
        elif struct_loss < 50_000:
            ihp_category = "major"
            ihp_est_amount = min(struct_loss * 0.6, IHP_MAX)
        else:
            ihp_category = "severe"
            ihp_est_amount = IHP_MAX
        ihp_est_amount = round(ihp_est_amount, 0)
    elif is_residential:
        ihp_eligible = False

    # ── Wind damage (separate peril) ───────────────────────────────
    # Uses the asymmetric Holland model (wind_field.py) to provide
    # per-building sustained surface wind speed.  Structural failures
    # are driven by 3-second gust loading (ASCE 7), not 1-minute
    # sustained speeds, so we apply a gust factor to the damage
    # calculation.  Calibrated via parameter sweep across 7 benchmark
    # storms: 1.15 balances accuracy across surge-dominant and
    # wind-dominant events (v4 calibration, MAPE ~18%).
    #
    # The combined loss formula uses "max + 30% secondary" to account
    # for synergistic wind-water interaction: wind breaches roofs →
    # rain intrusion amplifies flood damage.  0.30 was optimal in the
    # sensitivity sweep (tested 0.20–0.50); higher values over-counted
    # wind in surge-dominant storms like Katrina.
    GUST_FACTOR = 1.15  # 3-sec gust / 1-min sustained (calibrated v4)
    INTERACTION = 0.30  # secondary peril contribution (calibrated v4)
    wind_dmg_pct = None
    wind_loss = None
    combined_loss = None
    if wind_speed_mph is not None and wind_speed_mph > 0:
        effective_wind = wind_speed_mph * GUST_FACTOR
        wind_dmg_pct = estimate_wind_damage_pct(
            effective_wind, building_resilience, med_yr_blt,
        )
        wind_loss = round(wind_dmg_pct / 100 * struct_value, 0)
        # Combined: primary peril + 30% of secondary (interaction factor)
        if wind_loss > loss:
            combined_loss = round(wind_loss + INTERACTION * loss, 0)
        else:
            combined_loss = round(loss + INTERACTION * wind_loss, 0)

    # ── Rainfall flood damage ────────────────────────────────────
    # Adds rainfall-induced flooding as a separate damage layer.
    # This captures inland rain flooding missed by the surge model
    # (critical for storms like Harvey where 80%+ of damage was rain).
    _rainfall_loss = None
    if rainfall_depth_m is not None and rainfall_depth_m > 0.01:
        rain_depth_ft = rainfall_depth_m * 3.28084
        rain_above_floor = rain_depth_ft - ffh
        rain_struct_pct = get_damage_pct(rain_above_floor, building_type, "structure")
        rain_content_pct = get_damage_pct(rain_above_floor, building_type, "contents")
        _rainfall_loss = round(
            (rain_struct_pct / 100 * struct_value)
            + (rain_content_pct / 100 * content_value), 0
        )
        # Add to surge loss if rainfall depth exceeds surge depth
        # (compound flooding handled by taking max at each building)
        if _rainfall_loss > loss:
            loss = _rainfall_loss

    # ── Composite confidence score ─────────────────────────────
    # Combines data quality signals from all inputs into a single
    # adjuster-facing confidence label: "high" | "medium" | "low"
    _conf_score = 0.0
    # Value data quality (0.35 weight)
    _vq = {"high": 0.35, "medium": 0.20, "low": 0.05}.get(_val_confidence, 0.05)
    _conf_score += _vq
    # Foundation data quality (0.30 weight)
    _fq = {"high": 0.30, "medium": 0.15, "low": 0.05}.get(_fdn_confidence, 0.05)
    _conf_score += _fq
    # Depth source quality (0.20 weight — surge depth always present)
    _conf_score += 0.15  # parametric surge = medium
    # Wind data (0.15 weight)
    if wind_speed_mph is not None:
        _conf_score += 0.10
    # Classify
    if _conf_score >= 0.65:
        _confidence = "high"
    elif _conf_score >= 0.35:
        _confidence = "medium"
    else:
        _confidence = "low"

    return BuildingDamage(
        building_id=building_id,
        lon=lon,
        lat=lat,
        depth_m=depth_m,
        depth_ft=depth_ft,
        building_type=building_type,
        structure_damage_pct=round(struct_pct, 1),
        contents_damage_pct=round(content_pct, 1),
        total_damage_pct=round(total_pct, 1),
        estimated_loss_usd=round(loss, 0),
        replacement_value_usd=round(replacement, 0),
        found_ht=first_floor_ht_ft,
        val_struct=round(val_struct, 0) if val_struct is not None else None,
        val_cont=round(val_cont, 0) if val_cont is not None else None,
        wind_damage_pct=wind_dmg_pct,
        wind_loss_usd=wind_loss,
        wind_speed_mph=round(wind_speed_mph, 0) if wind_speed_mph is not None else None,
        combined_loss_usd=combined_loss,
        loss_low_usd=loss_low,
        loss_high_usd=loss_high,
        structure_dmg_pct_low=s_low,
        structure_dmg_pct_high=s_high,
        contents_dmg_pct_low=c_low,
        contents_dmg_pct_high=c_high,
        ihp_eligible=ihp_eligible,
        ihp_category=ihp_category,
        ihp_est_amount=ihp_est_amount,
        # ── Rainfall ──
        rainfall_depth_m=rainfall_depth_m,
        rainfall_loss_usd=_rainfall_loss,
        # ── Confidence & Uncertainty ──
        confidence=_confidence,
        confidence_score=round(_conf_score, 2),
        value_confidence=_val_confidence,
        value_method=_val_method,
        value_low_usd=round(_val_low, 0) if _val_low else None,
        value_high_usd=round(_val_high, 0) if _val_high else None,
        foundation_confidence=_fdn_confidence,
        foundation_type=_fdn_type,
        prob_elevated=round(_prob_elevated, 2) if _prob_elevated is not None else None,
    )


def estimate_damage_from_raster(
    depth_raster_path: str,
    buildings_geojson_path: str,
    output_path: str = "",
    building_type: str = DEFAULT_BUILDING_TYPE,
    storm_id: Optional[str] = None,
    landfall_lat: Optional[float] = None,
    landfall_lon: Optional[float] = None,
    max_wind_kt: Optional[float] = None,
    storm_speed_kt: Optional[float] = None,
    storm_heading_deg: Optional[float] = None,
) -> DamageEstimate:
    """
    Estimate damage for all buildings by sampling flood depth at each location.

    Args:
        depth_raster_path: Path to flood depth GeoTIFF (meters)
        buildings_geojson_path: GeoJSON with building point/polygon features
        output_path: Optional path to write damage results as GeoJSON
        building_type: Default building type (used when feature has no type)
        storm_id: Storm identifier for IBTrACS wind field lookup (e.g. "michael_2018")
        landfall_lat: Landfall latitude for wind field snapshot selection
        landfall_lon: Landfall longitude for wind field snapshot selection
        max_wind_kt: Max sustained wind (knots) for rainfall estimation
        storm_speed_kt: Forward speed (knots) for rainfall estimation
        storm_heading_deg: Storm heading (degrees from N) for rainfall estimation

    Returns:
        DamageEstimate with per-building and aggregated loss data
    """
    import json
    import rasterio

    logger.info(
        f"Estimating damage: raster={depth_raster_path}, "
        f"buildings={buildings_geojson_path}"
    )

    # Load buildings
    with open(buildings_geojson_path) as f:
        buildings_data = json.load(f)

    features = buildings_data.get("features", [])
    if not features:
        logger.warning("No buildings in input GeoJSON")
        return DamageEstimate(
            buildings_assessed=0, buildings_damaged=0,
            total_loss_usd=0, total_replacement_usd=0,
            avg_damage_pct=0, max_damage_pct=0,
            damage_by_category={}, buildings=[],
        )

    # ── Census ACS county-level cost (one lookup per cell) ──
    # Use the cell center as the location for the county lookup.
    # This replaces the flat $150/sqft fallback for non-NSI buildings.
    county_cost_per_sqft = None
    try:
        from data_ingest.census_fetcher import get_county_home_value
        # Estimate cell center from the first few building coordinates
        sample_coords = []
        for feat in features[:10]:
            geom = feat.get("geometry", {})
            c = _get_centroid(geom)
            if c[0] != 0 and c[1] != 0:
                sample_coords.append(c)
        if sample_coords:
            avg_lon = sum(c[0] for c in sample_coords) / len(sample_coords)
            avg_lat = sum(c[1] for c in sample_coords) / len(sample_coords)
            home_val = get_county_home_value(avg_lat, avg_lon)
            if home_val:
                county_cost_per_sqft = home_val["cost_per_sqft_est"]
                logger.info(
                    "[Census] Using county-level cost: $%.0f/sqft for %s, %s",
                    county_cost_per_sqft, home_val["county_name"], home_val["state_code"],
                )
    except Exception as exc:
        logger.info("[Census] County cost lookup failed (using defaults): %s", exc)

    # ── State FIPS for property/foundation estimation ──
    cell_state_fips = None
    try:
        from damage_model.property_estimator import get_state_fips_from_coords
        if sample_coords:
            cell_state_fips = get_state_fips_from_coords(avg_lat, avg_lon)
            if cell_state_fips:
                logger.info("[StateFIPS] Cell in state FIPS %s", cell_state_fips)
    except Exception:
        pass

    # ── Wind field from IBTrACS quadrant radii (Phase 2) ──
    # Loads the landfall-nearest IBTrACS snapshot and creates an asymmetric
    # Holland parametric wind model.  Wind speed at each building is queried
    # individually using the per-quadrant fitted profile.
    wind_snapshot = None
    if storm_id and landfall_lat is not None and landfall_lon is not None:
        try:
            from damage_model.wind_field import load_landfall_snapshot, get_wind_speed_at_point
            wind_snapshot = load_landfall_snapshot(storm_id, landfall_lat, landfall_lon)
            if wind_snapshot:
                logger.info(
                    "[WindField] Asymmetric Holland model active — %s | Vmax=%d kt | "
                    "RMW=%.0f nm | R34 max=%.0f nm",
                    wind_snapshot.storm_name,
                    wind_snapshot.max_wind_kt,
                    wind_snapshot.rmw_m / 1852,
                    wind_snapshot.r34.max_radius() / 1852,
                )
        except Exception as exc:
            logger.info("[WindField] Wind field init failed (wind damage disabled): %s", exc)

    # ── Rainfall estimation parameters ──
    # Extract storm parameters from wind snapshot if available, else from args
    _rain_center_lat = landfall_lat
    _rain_center_lon = landfall_lon
    _rain_max_wind_kt = max_wind_kt
    _rain_speed_kt = storm_speed_kt
    _rain_heading_deg = storm_heading_deg or 0.0

    if wind_snapshot is not None:
        _rain_center_lat = _rain_center_lat or wind_snapshot.lat
        _rain_center_lon = _rain_center_lon or wind_snapshot.lon
        _rain_max_wind_kt = _rain_max_wind_kt or wind_snapshot.max_wind_kt
        if _rain_speed_kt is None and wind_snapshot.storm_speed_ms > 0:
            _rain_speed_kt = wind_snapshot.storm_speed_ms / 0.514444  # m/s → kt
        _rain_heading_deg = _rain_heading_deg or wind_snapshot.storm_dir_deg

    _rainfall_available = False
    _estimate_rain = None
    if (_rain_center_lat is not None and _rain_center_lon is not None
            and _rain_max_wind_kt is not None and _rain_max_wind_kt > 0
            and _rain_speed_kt is not None and _rain_speed_kt > 0):
        try:
            from flood_model.rainfall import estimate_rainfall_at_point
            _estimate_rain = estimate_rainfall_at_point
            _rainfall_available = True
            logger.info(
                "[Rainfall] Parametric rainfall active — Vmax=%.0f kt, "
                "speed=%.1f kt, heading=%.0f°",
                _rain_max_wind_kt, _rain_speed_kt, _rain_heading_deg,
            )
        except ImportError:
            logger.info("[Rainfall] rainfall module not available")
    else:
        logger.info("[Rainfall] Insufficient storm params for rainfall estimation")

    # Open depth raster
    with rasterio.open(depth_raster_path) as src:
        depth_band = src.read(1)
        nodata = src.nodata or -9999
        transform = src.transform

        buildings = []
        for i, feat in enumerate(features):
            geom = feat.get("geometry", {})
            props = feat.get("properties", {})

            # Get building centroid
            lon, lat = _get_centroid(geom)
            if lon == 0 and lat == 0:
                continue

            # Sample depth at building location
            try:
                row, col = rasterio.transform.rowcol(transform, lon, lat)
                if 0 <= row < depth_band.shape[0] and 0 <= col < depth_band.shape[1]:
                    depth_m = float(depth_band[row, col])
                else:
                    depth_m = 0.0
            except Exception:
                depth_m = 0.0

            if depth_m == nodata or depth_m <= 0.0:
                depth_m = 0.0

            # Get building type from properties or use default
            btype = props.get("building_type", props.get("type", building_type))
            if btype not in STRUCTURE_DAMAGE:
                btype = building_type

            bid = props.get("id", props.get("building_id", str(i)))
            sqft      = props.get("area_sqft")
            val_struct = props.get("val_struct")   # NSI: actual structure value
            val_cont   = props.get("val_cont")     # NSI: actual contents value
            found_ht   = props.get("found_ht")     # NSI: first-floor elevation

            # ── Inundation mask ──────────────────────────────────────────────
            # Only assess HAZUS damage for buildings where surge actually
            # reaches the first finished floor.  The parametric raster assigns
            # small non-zero depths to every grid point (noise floor ~0.05 m)
            # so without this check all buildings in the cell get some loss.
            # We resolve foundation height here using the same priority order
            # as estimate_building_damage(), then skip buildings that are dry
            # (unless rainfall adds enough depth to matter).
            depth_ft = depth_m * 3.28084
            if found_ht is not None:
                ffh = float(found_ht)
            else:
                try:
                    from damage_model.foundation_estimator import estimate_foundation_height
                    _fe = estimate_foundation_height(
                        building_type=btype, state_fips=cell_state_fips,
                        flood_zone=props.get("flood_zone"),
                        med_yr_blt=int(props["med_yr_blt"]) if props.get("med_yr_blt") is not None else None,
                        lat=lat, lon=lon,
                    )
                    ffh = _fe.best_estimate_ft
                except Exception:
                    ffh = DEFAULT_FFH_FT.get(btype, 1.0)

            # ── Per-building rainfall depth ─────────────────────────────
            bldg_rain_depth_m = None
            if _rainfall_available and _estimate_rain is not None:
                try:
                    bldg_rain_depth_m = _estimate_rain(
                        point_lat=lat, point_lon=lon,
                        center_lat=_rain_center_lat, center_lon=_rain_center_lon,
                        max_wind_kt=_rain_max_wind_kt,
                        storm_speed_kt=_rain_speed_kt,
                        heading_deg=_rain_heading_deg,
                    )
                    if bldg_rain_depth_m is not None and bldg_rain_depth_m < 0.01:
                        bldg_rain_depth_m = None  # below noise threshold
                except Exception:
                    bldg_rain_depth_m = None

            # Combined check: surge OR rainfall must reach the floor
            rain_depth_ft = (bldg_rain_depth_m or 0.0) * 3.28084
            effective_depth_ft = max(depth_ft, rain_depth_ft)
            if effective_depth_ft < ffh - 0.1:
                # Neither surge nor rainfall reaches the first floor.
                continue

            # ── Wind speed from asymmetric Holland model ──
            bldg_wind_mph = None
            if wind_snapshot is not None:
                try:
                    bldg_wind_mph = get_wind_speed_at_point(wind_snapshot, lat, lon)
                except Exception:
                    pass  # wind model failure is non-fatal

            damage = estimate_building_damage(
                depth_m=depth_m, lon=lon, lat=lat,
                building_type=btype, building_id=str(bid),
                sqft=float(sqft) if sqft else None,
                first_floor_ht_ft=float(found_ht) if found_ht is not None else None,
                val_struct=float(val_struct) if val_struct is not None else None,
                val_cont=float(val_cont) if val_cont is not None else None,
                med_yr_blt=int(props["med_yr_blt"]) if props.get("med_yr_blt") is not None else None,
                num_story=int(props["num_story"]) if props.get("num_story") is not None else None,
                occtype=str(props["occtype"]) if props.get("occtype") else None,
                county_cost_per_sqft=county_cost_per_sqft,
                wind_speed_mph=bldg_wind_mph,
                state_fips=cell_state_fips,
                flood_zone=props.get("flood_zone"),
                rainfall_depth_m=bldg_rain_depth_m,
            )
            # Carry source metadata through so the frontend CSV export can use it
            damage.source       = props.get("source")
            damage.data_quality = props.get("data_quality")
            damage.occtype      = props.get("occtype")
            damage.med_yr_blt   = props.get("med_yr_blt")
            damage.num_story    = props.get("num_story")
            damage.area_sqft    = props.get("area_sqft")
            buildings.append(damage)

    # Aggregate
    damaged = [b for b in buildings if b.total_damage_pct > 0]
    total_loss = sum(b.estimated_loss_usd for b in buildings)
    total_replacement = sum(b.replacement_value_usd for b in buildings)
    max_pct = max((b.total_damage_pct for b in buildings), default=0)
    avg_pct = (
        sum(b.total_damage_pct for b in damaged) / len(damaged)
        if damaged else 0
    )

    # Categorize
    categories = {"none": 0, "minor": 0, "moderate": 0, "major": 0, "severe": 0}
    est = DamageEstimate(
        buildings_assessed=len(buildings),
        buildings_damaged=len(damaged),
        total_loss_usd=total_loss,
        total_replacement_usd=total_replacement,
        avg_damage_pct=round(avg_pct, 1),
        max_damage_pct=round(max_pct, 1),
        damage_by_category=categories,
        buildings=buildings,
    )
    for b in buildings:
        cat = est.damage_category(b.total_damage_pct)
        categories[cat] = categories.get(cat, 0) + 1

    logger.info(
        f"Damage estimate: {est.buildings_assessed} assessed, "
        f"{est.buildings_damaged} damaged, "
        f"${est.total_loss_usd:,.0f} total loss"
    )

    # Write output GeoJSON
    if output_path:
        _write_damage_geojson(buildings, output_path)

    return est


def _get_centroid(geom: dict) -> Tuple[float, float]:
    """Extract centroid coordinates from a GeoJSON geometry."""
    gtype = geom.get("type", "")
    coords = geom.get("coordinates", [])

    if gtype == "Point":
        return (coords[0], coords[1]) if len(coords) >= 2 else (0, 0)

    elif gtype == "Polygon":
        ring = coords[0] if coords else []
        if not ring:
            return (0, 0)
        lons = [c[0] for c in ring]
        lats = [c[1] for c in ring]
        return (sum(lons) / len(lons), sum(lats) / len(lats))

    elif gtype == "MultiPolygon":
        # Use first polygon's centroid
        if coords and coords[0]:
            ring = coords[0][0]
            lons = [c[0] for c in ring]
            lats = [c[1] for c in ring]
            return (sum(lons) / len(lons), sum(lats) / len(lats))

    return (0, 0)


def _write_damage_geojson(
    buildings: List[BuildingDamage],
    output_path: str,
) -> str:
    """Write building damage results as a GeoJSON FeatureCollection."""
    import json
    import os

    features = []
    for b in buildings:
        features.append({
            "type": "Feature",
            "properties": {
                "layer": "damage",
                "building_id": b.building_id,
                "depth_m": round(b.depth_m, 2),
                "depth_ft": round(b.depth_ft, 1),
                "building_type": b.building_type,
                "structure_damage_pct": b.structure_damage_pct,
                "contents_damage_pct": b.contents_damage_pct,
                "total_damage_pct": b.total_damage_pct,
                "estimated_loss_usd": b.estimated_loss_usd,
                "damage_category": _damage_category(b.total_damage_pct),
                "replacement_value_usd": b.replacement_value_usd,
                "found_ht": b.found_ht,
                "val_struct": b.val_struct,
                "val_cont": b.val_cont,
                "loss_low_usd": b.loss_low_usd,
                "loss_high_usd": b.loss_high_usd,
                "structure_dmg_pct_low": b.structure_dmg_pct_low,
                "structure_dmg_pct_high": b.structure_dmg_pct_high,
                "contents_dmg_pct_low": b.contents_dmg_pct_low,
                "contents_dmg_pct_high": b.contents_dmg_pct_high,
                "ihp_eligible": b.ihp_eligible,
                "ihp_category": b.ihp_category,
                "ihp_est_amount": b.ihp_est_amount,
                "wind_damage_pct": b.wind_damage_pct,
                "wind_loss_usd": b.wind_loss_usd,
                "wind_speed_mph": b.wind_speed_mph,
                "combined_loss_usd": b.combined_loss_usd,
                "source": b.source,
                "data_quality": b.data_quality,
                "occtype": b.occtype,
                "med_yr_blt": b.med_yr_blt,
                "num_story": b.num_story,
                "area_sqft": b.area_sqft,
            },
            "geometry": {
                "type": "Point",
                "coordinates": [b.lon, b.lat],
            },
        })

    geojson = {"type": "FeatureCollection", "features": features}

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(geojson, f)

    logger.info(f"Damage GeoJSON: {len(features)} buildings -> {output_path}")
    return output_path
