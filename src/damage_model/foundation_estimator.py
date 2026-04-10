"""
Foundation Height Estimator

Provides probabilistic first-floor height (FFH) estimation using
available building attributes and regional heuristics when FEMA NSI
data (found_ht) is unavailable.

The foundation height is THE most important variable in flood damage
estimation — the difference between a slab-on-grade (1 ft) and an
elevated home (4+ ft) can mean the difference between a total loss
and zero damage.

Data layers used for inference:
  1. NSI found_ht (handled upstream — this module is the fallback)
  2. FEMA flood zone (V, AE, A, X → elevation requirements)
  3. Year built + region (post-NFIP buildings in flood zones are elevated)
  4. Building type (manufactured homes, commercial → different patterns)
  5. Regional construction patterns (Gulf Coast crawlspace vs NE slab)

Output: estimated FFH + probability distribution + confidence.

Sources
-------
- FEMA HAZUS Flood Technical Manual (2022), Section 5.3.2
  "First Floor Height" elevation requirements by flood zone.

- National Flood Insurance Program (NFIP) effectiveness (1973 onward)
  shows dramatic increase in elevated construction in high-hazard zones
  post-Katrina (2005) and post-Sandy (2012).

- FIA claims data: elevated homes (>2 ft) in 5-10 ft surge depth events
  sustain 40-60% less damage than slab-on-grade equivalents.

- Regional construction patterns: Gulf Coast 80% elevated post-2005 vs
  Northeast 40% basements + 50% slab (NSI analysis, FEMA 2024).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Regional Foundation Patterns
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

_REGIONAL_PATTERNS = {
    # Gulf Coast & High-Hazard States (post-Katrina elevated rates)
    "12": {  # Florida
        "slab_pct": 0.25,
        "crawlspace_pct": 0.15,
        "elevated_pct": 0.50,
        "basement_pct": 0.10,
        "default_slab_ft": 1.0,
        "default_crawl_ft": 2.5,
        "default_elevated_ft": 6.0,
        "default_basement_ft": 1.0,
    },
    "48": {  # Texas
        "slab_pct": 0.35,
        "crawlspace_pct": 0.25,
        "elevated_pct": 0.30,
        "basement_pct": 0.10,
        "default_slab_ft": 1.0,
        "default_crawl_ft": 2.5,
        "default_elevated_ft": 5.0,
        "default_basement_ft": 1.0,
    },
    "22": {  # Louisiana
        "slab_pct": 0.20,
        "crawlspace_pct": 0.10,
        "elevated_pct": 0.60,
        "basement_pct": 0.10,
        "default_slab_ft": 1.0,
        "default_crawl_ft": 2.5,
        "default_elevated_ft": 6.5,
        "default_basement_ft": 1.0,
    },
    "28": {  # Mississippi
        "slab_pct": 0.25,
        "crawlspace_pct": 0.15,
        "elevated_pct": 0.50,
        "basement_pct": 0.10,
        "default_slab_ft": 1.0,
        "default_crawl_ft": 2.5,
        "default_elevated_ft": 6.0,
        "default_basement_ft": 1.0,
    },
    "01": {  # Alabama
        "slab_pct": 0.30,
        "crawlspace_pct": 0.20,
        "elevated_pct": 0.40,
        "basement_pct": 0.10,
        "default_slab_ft": 1.0,
        "default_crawl_ft": 2.5,
        "default_elevated_ft": 5.5,
        "default_basement_ft": 1.0,
    },
    "13": {  # Georgia
        "slab_pct": 0.45,
        "crawlspace_pct": 0.25,
        "elevated_pct": 0.20,
        "basement_pct": 0.10,
        "default_slab_ft": 1.0,
        "default_crawl_ft": 2.5,
        "default_elevated_ft": 5.0,
        "default_basement_ft": 1.0,
    },
    "45": {  # South Carolina
        "slab_pct": 0.35,
        "crawlspace_pct": 0.25,
        "elevated_pct": 0.30,
        "basement_pct": 0.10,
        "default_slab_ft": 1.0,
        "default_crawl_ft": 2.5,
        "default_elevated_ft": 5.5,
        "default_basement_ft": 1.0,
    },
    "37": {  # North Carolina
        "slab_pct": 0.40,
        "crawlspace_pct": 0.30,
        "elevated_pct": 0.20,
        "basement_pct": 0.10,
        "default_slab_ft": 1.0,
        "default_crawl_ft": 2.5,
        "default_elevated_ft": 5.0,
        "default_basement_ft": 1.0,
    },
    "51": {  # Virginia
        "slab_pct": 0.40,
        "crawlspace_pct": 0.30,
        "elevated_pct": 0.20,
        "basement_pct": 0.10,
        "default_slab_ft": 1.0,
        "default_crawl_ft": 2.5,
        "default_elevated_ft": 5.0,
        "default_basement_ft": 1.0,
    },
    # Northeast States (basement-heavy, slab-common)
    "36": {  # New York
        "slab_pct": 0.35,
        "crawlspace_pct": 0.10,
        "elevated_pct": 0.10,
        "basement_pct": 0.45,
        "default_slab_ft": 1.0,
        "default_crawl_ft": 2.5,
        "default_elevated_ft": 5.0,
        "default_basement_ft": 0.8,
    },
    "34": {  # New Jersey
        "slab_pct": 0.30,
        "crawlspace_pct": 0.10,
        "elevated_pct": 0.15,
        "basement_pct": 0.45,
        "default_slab_ft": 1.0,
        "default_crawl_ft": 2.5,
        "default_elevated_ft": 5.0,
        "default_basement_ft": 0.8,
    },
    "09": {  # Connecticut
        "slab_pct": 0.30,
        "crawlspace_pct": 0.10,
        "elevated_pct": 0.15,
        "basement_pct": 0.45,
        "default_slab_ft": 1.0,
        "default_crawl_ft": 2.5,
        "default_elevated_ft": 5.0,
        "default_basement_ft": 0.8,
    },
    "25": {  # Massachusetts
        "slab_pct": 0.30,
        "crawlspace_pct": 0.10,
        "elevated_pct": 0.15,
        "basement_pct": 0.45,
        "default_slab_ft": 1.0,
        "default_crawl_ft": 2.5,
        "default_elevated_ft": 5.0,
        "default_basement_ft": 0.8,
    },
    # Default for unmapped regions
    "_default": {
        "slab_pct": 0.45,
        "crawlspace_pct": 0.20,
        "elevated_pct": 0.20,
        "basement_pct": 0.15,
        "default_slab_ft": 1.0,
        "default_crawl_ft": 2.5,
        "default_elevated_ft": 5.0,
        "default_basement_ft": 1.0,
    },
}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Flood Zone Elevation Requirements
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

_FLOOD_ZONE_ELEVATION = {
    "V": {
        "name": "Coastal High Hazard",
        "must_elevate": True,
        "min_ffh_ft": 5.0,
        "max_ffh_ft": 12.0,
        "default_ffh_ft": 8.0,
    },
    "AE": {
        "name": "1% Annual Chance (Riverine/Coastal)",
        "must_elevate": False,
        "min_ffh_ft": 1.0,
        "max_ffh_ft": 8.0,
        "default_ffh_ft": 3.5,
    },
    "A": {
        "name": "1% Annual Chance (Approximate)",
        "must_elevate": False,
        "min_ffh_ft": 1.0,
        "max_ffh_ft": 6.0,
        "default_ffh_ft": 2.5,
    },
    "X": {
        "name": "0.2% Annual Chance / Minimal Hazard",
        "must_elevate": False,
        "min_ffh_ft": 1.0,
        "max_ffh_ft": 4.0,
        "default_ffh_ft": 1.0,
    },
}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# NFIP Era Compliance Factors
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# Year-built breakpoints and their NFIP/elevation compliance rates in flood zones
_YEAR_BUILT_ERAS = {
    # Pre-NFIP (1968): No flood awareness
    (None, 1973): {
        "era_name": "Pre-NFIP",
        "flood_zone_elevated_rate": 0.10,  # 10% compliance
        "non_fz_slab_rate": 0.70,           # 70% slab outside flood zones
    },
    # Early NFIP (1974-1994): Poorly enforced
    (1974, 1994): {
        "era_name": "Early NFIP",
        "flood_zone_elevated_rate": 0.35,  # 35% compliance
        "non_fz_slab_rate": 0.65,
    },
    # Improving enforcement (1995-2004)
    (1995, 2004): {
        "era_name": "Maturing NFIP",
        "flood_zone_elevated_rate": 0.65,  # 65% compliance
        "non_fz_slab_rate": 0.60,
    },
    # Post-Katrina strong enforcement (2005+)
    (2005, None): {
        "era_name": "Post-Katrina",
        "flood_zone_elevated_rate": 0.85,  # 85% compliance
        "non_fz_slab_rate": 0.55,
    },
}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Foundation Estimate Dataclass
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@dataclass
class FoundationEstimate:
    """
    Probabilistic foundation height estimate.

    When NSI data (found_ht) is unavailable, this dataclass provides
    a best estimate + confidence interval + metadata about estimation
    method and foundation type.

    Attributes:
        best_estimate_ft: Most likely first-floor height (feet above grade).
                         Use this for damage calculations.
        low_ft: 10th percentile of the distribution (conservative/lower bound).
        high_ft: 90th percentile of the distribution (upper bound).
        confidence: Quality indicator ("high" | "medium" | "low").
                   High: V-zone or explicit NSI data.
                   Medium: AE/A zone + year built heuristics.
                   Low: X zone or no flood zone data.
        method: Human-readable description of the estimation strategy
               (e.g., "V-zone elevation requirement", "regional pattern").
        foundation_type: Inferred foundation type
                        ("slab" | "crawlspace" | "elevated" | "basement" | "unknown").
        prob_slab: Probability of slab-on-grade (0.0–1.0).
        prob_elevated: Probability of elevated (>2 ft) (0.0–1.0).
    """

    best_estimate_ft: float
    low_ft: float
    high_ft: float
    confidence: str
    method: str
    foundation_type: str
    prob_slab: float
    prob_elevated: float

    def __post_init__(self):
        """Validate estimate ranges."""
        if not (self.low_ft <= self.best_estimate_ft <= self.high_ft):
            raise ValueError(
                f"Invalid range: low={self.low_ft}, best={self.best_estimate_ft}, "
                f"high={self.high_ft}"
            )
        if not (0.0 <= self.confidence_score <= 1.0):
            raise ValueError(f"Confidence score must be in [0, 1], got {self.confidence_score}")
        if not self.confidence in ("high", "medium", "low"):
            raise ValueError(f"Confidence must be 'high'/'medium'/'low', got {self.confidence}")
        if not (0.0 <= self.prob_slab <= 1.0) or not (0.0 <= self.prob_elevated <= 1.0):
            raise ValueError("Probabilities must be in [0, 1]")

    @property
    def confidence_score(self) -> float:
        """Numeric confidence score (1.0=high, 0.67=medium, 0.33=low)."""
        return {"high": 1.0, "medium": 0.67, "low": 0.33}.get(self.confidence, 0.33)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# State FIPS to Coordinate Lookup (from property_estimator)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

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


def get_state_fips_for_coord(lat: float, lon: float) -> Optional[str]:
    """
    Return state FIPS code for a lat/lon pair.

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
# Helper Functions
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def _get_year_built_era(
    med_yr_blt: Optional[int],
) -> tuple[str, float, float]:
    """
    Classify year built into an era and return compliance rates.

    Args:
        med_yr_blt: Median year built (or None)

    Returns:
        (era_name, flood_zone_elevated_rate, non_fz_slab_rate)
    """
    if med_yr_blt is None:
        # Default to early NFIP for unknown
        era = _YEAR_BUILT_ERAS[(1974, 1994)]
        return era["era_name"], era["flood_zone_elevated_rate"], era["non_fz_slab_rate"]

    for (start, end), era_data in _YEAR_BUILT_ERAS.items():
        if (start is None or med_yr_blt >= start) and (end is None or med_yr_blt <= end):
            return (
                era_data["era_name"],
                era_data["flood_zone_elevated_rate"],
                era_data["non_fz_slab_rate"],
            )

    # Fallback (shouldn't reach)
    era = _YEAR_BUILT_ERAS[(2005, None)]
    return era["era_name"], era["flood_zone_elevated_rate"], era["non_fz_slab_rate"]


def _normalize_building_type(building_type: str) -> str:
    """
    Normalize building type code to major category.

    Args:
        building_type: NSI occupancy type (e.g., "RES1-1SNB", "RES2", "COM1")

    Returns:
        Normalized type: "residential", "manufactured", "commercial", or "unknown"
    """
    btype = (building_type or "").upper()

    if btype.startswith("RES1"):
        return "residential"
    if btype.startswith("RES2") or btype.startswith("MH"):
        return "manufactured"
    if btype.startswith(("COM", "IND", "REC", "REL", "GOV")):
        return "commercial"

    return "unknown"


def _get_regional_pattern(state_fips: Optional[str]) -> dict:
    """
    Retrieve regional foundation pattern for a state.

    Args:
        state_fips: State FIPS code (2-digit string) or None

    Returns:
        Dictionary with foundation type percentages and defaults
    """
    if state_fips and state_fips in _REGIONAL_PATTERNS:
        return _REGIONAL_PATTERNS[state_fips]
    return _REGIONAL_PATTERNS["_default"]


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Main Estimation Function
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def estimate_foundation_height(
    building_type: str = "RES1-1SNB",
    state_fips: Optional[str] = None,
    flood_zone: Optional[str] = None,
    med_yr_blt: Optional[int] = None,
    lat: Optional[float] = None,
    lon: Optional[float] = None,
) -> FoundationEstimate:
    """
    Estimate first-floor height (FFH) using available building attributes.

    This is the core fallback when NSI found_ht data is unavailable. The
    function layers multiple signals: flood zone requirements, construction era,
    regional patterns, and building type.

    Decision logic:
    1. If flood_zone is V → assume elevated (6-10 ft), high confidence
    2. If flood_zone is AE + year >= 2005 → likely elevated (3-6 ft), medium confidence
    3. If flood_zone is AE + year < 1974 → use regional default, low confidence
    4. If flood_zone is A or X → use regional slab/crawlspace distribution
    5. If no flood zone → use regional patterns + year built heuristics
    6. Manufactured homes (RES2) → always slab/pier, typically 2-4 ft
    7. Commercial/industrial → typically slab-on-grade (0-1 ft)

    Args:
        building_type: NSI occupancy type code (default "RES1-1SNB" for
                      standard residential single-family, no basement).
                      E.g., "RES1", "RES2", "COM1", "IND1".
        state_fips: State FIPS code (2-digit string, e.g., "12" for FL).
                   If None and lat/lon provided, will attempt to infer.
        flood_zone: FEMA flood zone ("V", "AE", "A", "X", or None).
                   V-zone triggers mandatory elevation assumptions.
        med_yr_blt: Median year built (integer), used for NFIP compliance era.
        lat: Latitude (decimal degrees, WGS84). Used to infer state_fips if needed.
        lon: Longitude (decimal degrees, WGS84, negative for Western Hemisphere).
             Used to infer state_fips if needed.

    Returns:
        FoundationEstimate dataclass with best estimate, range, confidence,
        method description, inferred foundation type, and probabilities.

    Examples:
        # Residential in V-zone (coastal high hazard): expect ~8 ft elevation
        >>> est = estimate_foundation_height(
        ...     flood_zone="V",
        ...     lat=27.5, lon=-80.0  # Miami
        ... )
        >>> est.best_estimate_ft
        8.0
        >>> est.confidence
        'high'

        # Residential in non-flood zone, Gulf Coast, post-NFIP:
        >>> est = estimate_foundation_height(
        ...     state_fips="12",  # Florida
        ...     med_yr_blt=1995,
        ...     flood_zone="X"
        ... )
        >>> est.best_estimate_ft
        1.0
        >>> est.foundation_type
        'slab'

        # Manufactured home (RES2): always elevated/pier foundation
        >>> est = estimate_foundation_height(building_type="RES2")
        >>> est.foundation_type
        'elevated'
        >>> est.best_estimate_ft >= 2.0
        True
    """

    # ────────────────────────────────────────────────────────────────────
    # Step 1: Infer state_fips from lat/lon if not provided
    # ────────────────────────────────────────────────────────────────────
    if state_fips is None and lat is not None and lon is not None:
        state_fips = get_state_fips_for_coord(lat, lon)

    # ────────────────────────────────────────────────────────────────────
    # Step 2: Normalize building type
    # ────────────────────────────────────────────────────────────────────
    norm_btype = _normalize_building_type(building_type)

    # ────────────────────────────────────────────────────────────────────
    # Step 3: Handle special building type cases
    # ────────────────────────────────────────────────────────────────────
    if norm_btype == "manufactured":
        # Manufactured homes: pier foundation, typically 2-4 ft
        return FoundationEstimate(
            best_estimate_ft=3.0,
            low_ft=2.0,
            high_ft=4.0,
            confidence="medium",
            method="Manufactured home pier foundation (RES2/MH type)",
            foundation_type="elevated",
            prob_slab=0.10,
            prob_elevated=0.85,
        )

    if norm_btype == "commercial":
        # Commercial/industrial: usually slab-on-grade with minimal elevation
        # unless in V-zone
        if flood_zone == "V":
            return FoundationEstimate(
                best_estimate_ft=5.0,
                low_ft=4.0,
                high_ft=7.0,
                confidence="medium",
                method="Commercial building in V-zone (coastal high hazard)",
                foundation_type="elevated",
                prob_slab=0.20,
                prob_elevated=0.70,
            )
        # Non-V-zone commercial defaults to slab
        return FoundationEstimate(
            best_estimate_ft=0.5,
            low_ft=0.0,
            high_ft=1.5,
            confidence="high",
            method="Commercial building (typically slab-on-grade)",
            foundation_type="slab",
            prob_slab=0.90,
            prob_elevated=0.05,
        )

    # ────────────────────────────────────────────────────────────────────
    # Step 4: Flood zone-specific logic (V-zone is strongest signal)
    # ────────────────────────────────────────────────────────────────────
    if flood_zone == "V":
        # Coastal high hazard zone: MUST be elevated
        # Typically 3-5 ft above BFE + freeboard
        return FoundationEstimate(
            best_estimate_ft=8.0,
            low_ft=6.0,
            high_ft=12.0,
            confidence="high",
            method="V-zone (coastal high hazard) elevation requirement",
            foundation_type="elevated",
            prob_slab=0.05,
            prob_elevated=0.92,
        )

    # ────────────────────────────────────────────────────────────────────
    # Step 5: AE zone + year-built logic
    # ────────────────────────────────────────────────────────────────────
    if flood_zone == "AE":
        era_name, fz_elevated_rate, _ = _get_year_built_era(med_yr_blt)

        # Higher elevation rate → more likely elevated
        if fz_elevated_rate >= 0.75:
            # High compliance era (post-Katrina): likely elevated
            return FoundationEstimate(
                best_estimate_ft=4.5,
                low_ft=3.0,
                high_ft=6.5,
                confidence="medium",
                method=f"AE-zone with {era_name} compliance (~{fz_elevated_rate*100:.0f}% elevated)",
                foundation_type="elevated",
                prob_slab=0.15,
                prob_elevated=0.70,
            )
        elif fz_elevated_rate >= 0.50:
            # Moderate compliance: mixed
            return FoundationEstimate(
                best_estimate_ft=3.0,
                low_ft=1.5,
                high_ft=5.0,
                confidence="medium",
                method=f"AE-zone with {era_name} compliance (~{fz_elevated_rate*100:.0f}% elevated)",
                foundation_type="crawlspace",
                prob_slab=0.35,
                prob_elevated=0.50,
            )
        else:
            # Low compliance (pre-1974): mostly slab/crawlspace
            regional = _get_regional_pattern(state_fips)
            avg_ffh = (
                regional["default_slab_ft"] * (regional["slab_pct"] + 0.1)
                + regional["default_crawl_ft"] * regional["crawlspace_pct"]
            ) / (regional["slab_pct"] + regional["crawlspace_pct"] + 0.1)

            return FoundationEstimate(
                best_estimate_ft=avg_ffh,
                low_ft=1.0,
                high_ft=3.5,
                confidence="low",
                method=f"AE-zone with {era_name} (pre-NFIP era, low compliance)",
                foundation_type="crawlspace",
                prob_slab=0.55,
                prob_elevated=0.25,
            )

    # ────────────────────────────────────────────────────────────────────
    # Step 6: A or X zone → use regional patterns
    # ────────────────────────────────────────────────────────────────────
    if flood_zone in ("A", "X"):
        regional = _get_regional_pattern(state_fips)

        # For A-zone, slight bias toward elevation; for X-zone, use regional defaults
        if flood_zone == "A":
            # A-zone (approximate flood hazard): some elevation encouraged
            prob_s = regional["slab_pct"] * 0.85
            prob_c = regional["crawlspace_pct"] * 1.15
            prob_e = regional["elevated_pct"] * 1.20
        else:
            # X-zone (minimal hazard): use regional distribution as-is
            prob_s = regional["slab_pct"]
            prob_c = regional["crawlspace_pct"]
            prob_e = regional["elevated_pct"]

        total = prob_s + prob_c + prob_e + regional["basement_pct"]
        prob_s_norm = prob_s / total if total > 0 else 0.45
        prob_c_norm = prob_c / total if total > 0 else 0.25
        prob_e_norm = prob_e / total if total > 0 else 0.15

        # Weighted average FFH
        avg_ffh = (
            regional["default_slab_ft"] * prob_s_norm
            + regional["default_crawl_ft"] * prob_c_norm
            + regional["default_elevated_ft"] * prob_e_norm
        )

        # Determine dominant foundation type
        if prob_s_norm >= 0.45:
            ftype = "slab"
        elif prob_c_norm >= 0.30:
            ftype = "crawlspace"
        elif prob_e_norm >= 0.20:
            ftype = "elevated"
        else:
            ftype = "unknown"

        confidence = "medium" if flood_zone == "A" else "low"
        method = (
            f"{flood_zone}-zone, {regional.get('default_slab_ft', 'N/A'):.1f} ft default"
        )

        # Ensure high >= best_estimate
        low_val = max(1.0, avg_ffh * 0.6)
        high_val = max(avg_ffh + 1.5, regional["default_elevated_ft"])

        return FoundationEstimate(
            best_estimate_ft=avg_ffh,
            low_ft=low_val,
            high_ft=high_val,
            confidence=confidence,
            method=method,
            foundation_type=ftype,
            prob_slab=prob_s_norm,
            prob_elevated=prob_e_norm,
        )

    # ────────────────────────────────────────────────────────────────────
    # Step 7: No flood zone → use regional + era heuristics
    # ────────────────────────────────────────────────────────────────────
    regional = _get_regional_pattern(state_fips)
    era_name, fz_elevated_rate, non_fz_slab_rate = _get_year_built_era(med_yr_blt)

    # Blend era heuristic with regional pattern
    # Older buildings: higher slab rate
    # Newer buildings in high-hazard states: higher elevation
    blend_slab = regional["slab_pct"] * non_fz_slab_rate
    blend_crawl = regional["crawlspace_pct"]
    blend_elev = regional["elevated_pct"] * (1.0 - non_fz_slab_rate)

    total = blend_slab + blend_crawl + blend_elev + regional["basement_pct"]
    if total > 0:
        prob_s_norm = blend_slab / total
        prob_c_norm = blend_crawl / total
        prob_e_norm = blend_elev / total
    else:
        prob_s_norm = 0.50
        prob_c_norm = 0.25
        prob_e_norm = 0.15

    avg_ffh = (
        regional["default_slab_ft"] * prob_s_norm
        + regional["default_crawl_ft"] * prob_c_norm
        + regional["default_elevated_ft"] * prob_e_norm
    )

    # Determine dominant foundation type
    if prob_s_norm >= 0.50:
        ftype = "slab"
    elif prob_c_norm >= 0.30:
        ftype = "crawlspace"
    elif prob_e_norm >= 0.20:
        ftype = "elevated"
    else:
        ftype = "unknown"

    method = f"Regional pattern ({state_fips or 'unmapped'}) + {era_name} era ({med_yr_blt or '?'})"

    # Ensure valid range: low < best < high
    low_val = max(0.5, avg_ffh * 0.6)
    high_val = max(avg_ffh + 0.5, regional["default_elevated_ft"])

    return FoundationEstimate(
        best_estimate_ft=round(avg_ffh, 2),
        low_ft=round(low_val, 1),
        high_ft=round(high_val, 1),
        confidence="low",
        method=method,
        foundation_type=ftype,
        prob_slab=round(prob_s_norm, 2),
        prob_elevated=round(prob_e_norm, 2),
    )
