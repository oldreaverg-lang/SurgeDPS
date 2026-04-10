"""
Property Value Estimator

Replaces the flat $/sqft fallback with a layered estimation system
that uses multiple data proxies to produce replacement cost RANGES.

This module handles property value estimation when NSI tabulated values
are unavailable (the common case for OSM-sourced buildings). It produces
replacement cost ranges (low, mid, high) rather than point estimates.

Data layers (highest to lowest priority):
  1. NSI tabulated values (handled upstream — this module is the fallback)
  2. Census ACS county median home value (via county_cost_per_sqft parameter)
  3. RS Means regional cost indices by state
  4. Structure type and year-built adjustments
  5. Condition/age depreciation curves

Output: PropertyValuation dataclass with (low, mid, high) replacement cost
tuple, confidence label, method description, and derived cost per sqft.

Sources
-------
- RS Means 2024 location factors for regional construction cost variation
- HAZUS building type cost baselines (2024 dollars, national average)
- FIA/NFIP claims depreciation curves for year-built adjustment
- U.S. Census Bureau ACS median home value (county-level proxy for value density)
"""

from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# RS Means Regional Cost Indices
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#
# Maps state FIPS codes to regional construction cost multipliers
# relative to national average (100 = baseline). Hurricane-prone
# coastal states use actual RS Means 2024 data; interior states
# use reasonable estimates based on regional labor/material costs.

RS_MEANS_COST_INDEX = {
    # Hurricane/coastal high-risk states (actual RS Means 2024)
    "12": 0.89,  # Florida
    "48": 0.85,  # Texas
    "22": 0.86,  # Louisiana
    "28": 0.80,  # Mississippi
    "01": 0.82,  # Alabama
    "13": 0.85,  # Georgia
    "45": 0.82,  # South Carolina
    "37": 0.84,  # North Carolina
    "51": 0.90,  # Virginia
    "36": 1.15,  # New York
    "34": 1.12,  # New Jersey
    "09": 1.10,  # Connecticut
    "25": 1.15,  # Massachusetts
    "44": 1.05,  # Rhode Island

    # Other coastal/high-cost states
    "06": 1.25,  # California
    "02": 1.20,  # Alaska
    "15": 1.18,  # Hawaii
    "10": 1.08,  # Delaware
    "24": 1.10,  # Maryland
    "42": 1.08,  # Pennsylvania
    "41": 1.02,  # Oregon
    "53": 1.05,  # Washington

    # Interior states (estimated based on regional factors)
    "04": 0.95,  # Arizona
    "05": 0.88,  # Arkansas
    "06": 1.25,  # California
    "08": 0.98,  # Colorado
    "09": 1.10,  # Connecticut
    "11": 1.20,  # DC
    "12": 0.89,  # Florida
    "13": 0.85,  # Georgia
    "16": 0.92,  # Idaho
    "17": 1.05,  # Illinois
    "18": 0.98,  # Indiana
    "19": 0.95,  # Iowa
    "20": 0.93,  # Kansas
    "21": 0.92,  # Kentucky
    "22": 0.86,  # Louisiana
    "23": 1.08,  # Maine
    "24": 1.10,  # Maryland
    "25": 1.15,  # Massachusetts
    "26": 1.03,  # Michigan
    "27": 1.02,  # Minnesota
    "28": 0.80,  # Mississippi
    "29": 0.96,  # Missouri
    "30": 0.91,  # Montana
    "31": 0.90,  # Nebraska
    "32": 0.99,  # Nevada
    "33": 1.12,  # New Hampshire
    "34": 1.12,  # New Jersey
    "35": 0.90,  # New Mexico
    "36": 1.15,  # New York
    "37": 0.84,  # North Carolina
    "38": 0.88,  # North Dakota
    "39": 1.00,  # Ohio
    "40": 0.87,  # Oklahoma
    "41": 1.02,  # Oregon
    "42": 1.08,  # Pennsylvania
    "44": 1.05,  # Rhode Island
    "45": 0.82,  # South Carolina
    "46": 0.89,  # South Dakota
    "47": 0.88,  # Tennessee
    "48": 0.85,  # Texas
    "49": 0.98,  # Utah
    "50": 1.06,  # Vermont
    "51": 0.90,  # Virginia
    "53": 1.05,  # Washington
    "54": 0.89,  # West Virginia
    "55": 1.00,  # Wisconsin
    "56": 0.89,  # Wyoming
    "72": 0.75,  # Puerto Rico
}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# National Building Type Cost Baselines
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#
# 2024 replacement cost per square foot (in USD) for HAZUS building types.
# Replaces the old flat $150/sqft. Values are national averages before
# regional multiplier and condition adjustments.

NATIONAL_COST_PER_SQFT = {
    "RES1-1SNB": 165,   # 1-story, wood frame, no basement
    "RES1-2SNB": 155,   # 2-story, wood frame, no basement (economy of scale)
    "RES1-1SWB": 180,   # 1-story, wood frame, with basement
    "RES1-2SWB": 170,   # 2-story, wood frame, with basement
    "RES1-SL":   170,   # Split-level residential
    "RES2":      85,    # Manufactured housing (trailers)
    "RES3":      145,   # Multi-family residential
    "COM":       195,   # Commercial (higher finishes, systems)
    "IND":       135,   # Industrial/warehouse (minimal finishes)
}

# Default for unknown types
_DEFAULT_COST_PER_SQFT = 150


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Year-Built Depreciation/Appreciation
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def year_built_factor(med_yr_blt: Optional[int]) -> float:
    """
    Return depreciation/appreciation multiplier based on construction year.

    Older buildings suffer greater depreciation due to material degradation,
    outdated systems, and deferred maintenance. Newer buildings reflect higher
    labor and material costs from modern codes.

    Args:
        med_yr_blt: Median (or representative) year built, or None

    Returns:
        Multiplier (< 1.0 = depreciation, > 1.0 = appreciation)

    Sources:
        - FIA/NFIP claims analysis: pre-1970 structures show ~15-20% higher
          damage rates (attributed to materials, maintenance, and standards)
        - International Building Code adoption effect (post-2000, ~15% cost increase)
        - Modern energy code material costs (2016+, ~12% premium)
    """
    if med_yr_blt is None:
        return 1.00  # Neutral if unknown

    if med_yr_blt < 1950:
        return 0.75  # Heavy depreciation: ~25% reduction
    elif med_yr_blt < 1970:
        return 0.85  # Pre-FIRM era, some depreciation
    elif med_yr_blt < 1990:
        return 0.92  # Gradual improvement toward baseline
    elif med_yr_blt < 2005:
        return 1.00  # 1990–2004: baseline era
    elif med_yr_blt < 2016:
        return 1.05  # Post-IBC adoption: 5% cost increase
    else:
        return 1.12  # 2016+: modern code materials, higher labor


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# State FIPS Lookup from Coordinates
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#
# Bounding boxes for hurricane-prone states. Used to infer state FIPS
# when only lat/lon are available. Coverage is intentionally limited
# to high-hazard coastal states; return None for interior points.

_STATE_BBOX = {
    "12": ((24.5, -87.6), (30.5, -80.0)),     # Florida
    "48": ((25.8, -106.6), (36.5, -93.5)),    # Texas
    "22": ((28.9, -94.0), (32.7, -88.8)),     # Louisiana
    "28": ((29.8, -91.7), (34.9, -88.1)),     # Mississippi
    "01": ((30.2, -88.5), (35.0, -84.9)),     # Alabama
    "13": ((30.4, -85.6), (34.9, -80.8)),     # Georgia
    "45": ((32.0, -83.4), (34.8, -78.5)),     # South Carolina
    "37": ((33.8, -84.3), (36.6, -75.4)),     # North Carolina
    "51": ((36.5, -83.7), (39.5, -75.2)),     # Virginia
    "36": ((40.5, -79.8), (45.0, -71.9)),     # New York
    "34": ((38.9, -75.6), (41.4, -73.9)),     # New Jersey
    "09": ((41.1, -73.7), (42.1, -71.8)),     # Connecticut
    "25": ((41.2, -73.5), (42.9, -69.9)),     # Massachusetts
}


def get_state_fips_from_coords(lat: float, lon: float) -> Optional[str]:
    """
    Return state FIPS code for a lat/lon pair (if in a covered state).

    Only covers hurricane-prone coastal states and northeastern high-cost
    areas. Returns None for interior or uncovered regions.

    Args:
        lat: Latitude (decimal degrees, WGS84)
        lon: Longitude (decimal degrees, WGS84, negative for Western Hemisphere)

    Returns:
        State FIPS code as string (e.g., "12" for Florida), or None if unmapped
    """
    for fips, ((lat_min, lon_min), (lat_max, lon_max)) in _STATE_BBOX.items():
        if lat_min <= lat <= lat_max and lon_min <= lon <= lon_max:
            return fips
    return None


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Per-Building Variation Multiplier
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _building_multiplier(building_id: str) -> float:
    """
    Return a hash-seeded building-specific cost multiplier.

    Accounts for unmeasured variation (lot size, location desirability,
    site-specific factors). Narrower range than old damage model:
    0.75–1.25 instead of 0.60–1.40 (reflects uncertainty in value,
    not damage susceptibility).

    Args:
        building_id: Unique building identifier (OSM ID, NSI ID, etc.)

    Returns:
        Multiplier in range [0.75, 1.25]
    """
    if not building_id:
        return 1.0

    hash_val = int(hashlib.md5(building_id.encode()).hexdigest(), 16)
    # Map hash to [0, 1) then scale to [0.75, 1.25]
    normalized = (hash_val % 1000000) / 1000000.0
    return 0.75 + normalized * 0.50


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Main Output Dataclass
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@dataclass
class PropertyValuation:
    """
    Estimated replacement cost range for a building.

    This replaces point estimates with ranges because property values
    derived from proxies (county median, building type, age) have
    inherent uncertainty.

    Attributes:
        low_usd: 20th percentile estimate (USD)
        mid_usd: Median/central estimate (USD)
        high_usd: 80th percentile estimate (USD)
        confidence: Quality of estimate ("high", "medium", or "low")
        method: Human-readable description of estimation approach
        cost_per_sqft: Mid-range cost per sqft (for verification/audit)
    """
    low_usd: float
    mid_usd: float
    high_usd: float
    confidence: str
    method: str
    cost_per_sqft: float

    def __post_init__(self):
        """Validate that low <= mid <= high."""
        if not (self.low_usd <= self.mid_usd <= self.high_usd):
            raise ValueError(
                f"Invalid range: low={self.low_usd}, mid={self.mid_usd}, "
                f"high={self.high_usd}"
            )


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Main Estimation Function
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def estimate_replacement_value(
    building_type: str = "RES1-1SNB",
    sqft: Optional[float] = None,
    county_cost_per_sqft: Optional[float] = None,
    med_yr_blt: Optional[int] = None,
    state_fips: Optional[str] = None,
    lat: Optional[float] = None,
    lon: Optional[float] = None,
    building_id: str = "",
) -> PropertyValuation:
    """
    Estimate replacement cost for a building using layered data proxies.

    When NSI tabulated values are unavailable, this function blends regional
    cost indices, county-level property value data, and building-specific
    adjustments to produce a replacement cost RANGE rather than a point estimate.

    Estimation logic:
    1. Start with NATIONAL_COST_PER_SQFT for the building_type
    2. Apply RS_MEANS_COST_INDEX if state_fips is available (or inferred from lat/lon)
    3. If county_cost_per_sqft is available, blend:
       - 60% weight to county proxy (county ACS median home value ÷ 1600 sqft)
       - 40% weight to type-adjusted national cost
       (County data is valuable but derived with uncertainty; national baseline
        is anchored to peer-reviewed HAZUS estimates.)
    4. Apply year_built_factor() depreciation/appreciation
    5. Compute per-building variation multiplier (0.75–1.25 from building_id hash)
    6. Generate range: low = mid × 0.75, high = mid × 1.35
    7. Assign confidence level based on data availability:
       - "high": county + year_built + sqft all available
       - "medium": county OR year_built available
       - "low": only building_type + defaults

    Args:
        building_type: HAZUS occupancy code (e.g., "RES1-1SNB", "COM", "IND").
                       Default: "RES1-1SNB" (most common).
        sqft: Building footprint or conditioned area in square feet, or None.
        county_cost_per_sqft: Proxy for regional property values, derived from
                              Census ACS median home value (county-level) divided
                              by 1600 sqft (typical dwelling size). Optional.
        med_yr_blt: Year built (NSI med_yr_blt) or None.
        state_fips: State FIPS code (2-digit string, e.g., "12" for FL), or None.
        lat: Latitude for inferring state_fips if not provided. Ignored if
             state_fips is given.
        lon: Longitude for inferring state_fips if not provided. Ignored if
             state_fips is given.
        building_id: Unique identifier (OSM ID, NSI ID, etc.) for per-building
                     variation. Empty string → no variation (mid = base).

    Returns:
        PropertyValuation: Dataclass with low/mid/high ranges (USD), confidence
                          label, method description, and derived $/sqft.

    Example:
        >>> v = estimate_replacement_value(
        ...     building_type="RES1-2SNB",
        ...     sqft=1800,
        ...     county_cost_per_sqft=110,
        ...     med_yr_blt=1995,
        ...     state_fips="12",
        ... )
        >>> print(f"${v.low_usd:,.0f} – ${v.high_usd:,.0f}")
    """
    # ─────────────────────────────────────────────────────────────────────
    # Step 1: Infer state FIPS from lat/lon if not provided
    # ─────────────────────────────────────────────────────────────────────
    if state_fips is None and lat is not None and lon is not None:
        state_fips = get_state_fips_from_coords(lat, lon)

    # ─────────────────────────────────────────────────────────────────────
    # Step 2: Get base cost per sqft (national average for type)
    # ─────────────────────────────────────────────────────────────────────
    base_cost_per_sqft = NATIONAL_COST_PER_SQFT.get(
        building_type, _DEFAULT_COST_PER_SQFT
    )

    # ─────────────────────────────────────────────────────────────────────
    # Step 3: Apply regional cost index
    # ─────────────────────────────────────────────────────────────────────
    regional_multiplier = 1.0
    if state_fips and state_fips in RS_MEANS_COST_INDEX:
        regional_multiplier = RS_MEANS_COST_INDEX[state_fips]

    type_adjusted_cost_per_sqft = base_cost_per_sqft * regional_multiplier

    # ─────────────────────────────────────────────────────────────────────
    # Step 4: Blend with county proxy if available
    # ─────────────────────────────────────────────────────────────────────
    if county_cost_per_sqft is not None and county_cost_per_sqft > 0:
        # County proxy is 60%, type-adjusted national is 40%
        blended_cost_per_sqft = (
            0.60 * county_cost_per_sqft + 0.40 * type_adjusted_cost_per_sqft
        )
        has_county = True
    else:
        blended_cost_per_sqft = type_adjusted_cost_per_sqft
        has_county = False

    # ─────────────────────────────────────────────────────────────────────
    # Step 5: Apply year-built depreciation/appreciation
    # ─────────────────────────────────────────────────────────────────────
    yr_blt_mult = year_built_factor(med_yr_blt)
    has_year_built = med_yr_blt is not None

    depreciated_cost_per_sqft = blended_cost_per_sqft * yr_blt_mult

    # ─────────────────────────────────────────────────────────────────────
    # Step 6: Apply per-building variation multiplier
    # ─────────────────────────────────────────────────────────────────────
    building_mult = _building_multiplier(building_id)
    final_cost_per_sqft = depreciated_cost_per_sqft * building_mult

    # ─────────────────────────────────────────────────────────────────────
    # Step 7: Compute total cost (mid estimate)
    # ─────────────────────────────────────────────────────────────────────
    if sqft is not None and sqft > 0:
        mid_usd = final_cost_per_sqft * sqft
        has_sqft = True
    else:
        # No sqft provided — use a typical dwelling size (1600 sqft for res)
        # and note this in the method description
        typical_sqft = 1600 if building_type.startswith("RES") else 3000
        mid_usd = final_cost_per_sqft * typical_sqft
        has_sqft = False

    # ─────────────────────────────────────────────────────────────────────
    # Step 8: Generate range
    # ─────────────────────────────────────────────────────────────────────
    low_usd = mid_usd * 0.75    # 20th percentile
    high_usd = mid_usd * 1.35   # 80th percentile

    # ─────────────────────────────────────────────────────────────────────
    # Step 9: Assign confidence level
    # ─────────────────────────────────────────────────────────────────────
    if has_county and has_year_built and has_sqft:
        confidence = "high"
    elif has_county or has_year_built:
        confidence = "medium"
    else:
        confidence = "low"

    # ─────────────────────────────────────────────────────────────────────
    # Step 10: Assemble method description
    # ─────────────────────────────────────────────────────────────────────
    method_parts = [f"HAZUS {building_type}"]

    if has_county:
        method_parts.append(f"county proxy")
    else:
        method_parts.append("national baseline")

    if state_fips:
        method_parts.append(f"RS Means {state_fips}")

    if has_year_built:
        method_parts.append(f"year-built {med_yr_blt}")

    if building_id:
        method_parts.append(f"per-bldg variation")

    if not has_sqft:
        method_parts.append("(est. sqft)")

    method = " + ".join(method_parts)

    # ─────────────────────────────────────────────────────────────────────
    # Step 11: Log estimation (if enabled)
    # ─────────────────────────────────────────────────────────────────────
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug(
            "Property valuation: type=%s sqft=%s county_cost=%.0f yr_blt=%s "
            "state=%s → ${low:,.0f}–${mid:,.0f}–${high:,.0f} ({conf})",
            building_type,
            sqft or "est.",
            county_cost_per_sqft or 0,
            med_yr_blt or "?",
            state_fips or "?",
            low=low_usd,
            mid=mid_usd,
            high=high_usd,
            conf=confidence,
        )

    return PropertyValuation(
        low_usd=round(low_usd, 2),
        mid_usd=round(mid_usd, 2),
        high_usd=round(high_usd, 2),
        confidence=confidence,
        method=method,
        cost_per_sqft=round(final_cost_per_sqft, 2),
    )
