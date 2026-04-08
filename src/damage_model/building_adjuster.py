"""
Building-Specific Damage Adjustments

Applies per-building modifiers to HAZUS base damage percentages using
attributes already available from the FEMA National Structure Inventory
(found_ht, med_yr_blt, num_story, occtype).

Each adjustment is additive (percentage points) and the combined result is
clamped to [0, 100].  All adjustments are intentionally conservative —
the goal is to reduce systematic bias in the flat HAZUS curves, not to
introduce large swings from a small number of attributes.

Sources
-------
- Foundation height adjustment:
    FEMA HAZUS Flood Technical Manual (2022), Section 5.3.2 — "First Floor
    Height" — explicitly notes that FFH > 2 ft above grade substantially
    reduces damage for a given surge depth.

- Construction era adjustment:
    FEMA NFIP claims data analysis (FEMA 2013, "Homeowner Flood Insurance
    Affordability Act Studies") shows pre-1970 structures have ~15-20%
    higher average claim rates than post-2000 structures at equivalent depths,
    attributed to pre-FIRM construction standards.

- Multi-story contents adjustment:
    HAZUS Technical Manual Ch. 5 — contents curves assume a single-story
    distribution. For multi-story residential, upper-floor contents are
    undamaged in partial-flooding events, so the contents damage fraction
    is reduced proportionally.
"""

from __future__ import annotations

import logging
from typing import Optional

logger = logging.getLogger(__name__)


# ── Tuning constants ──────────────────────────────────────────────────────────

# Foundation height thresholds (feet above grade)
_FFH_ELEVATED_THRESHOLD = 2.0    # > 2 ft: significant reduction
_FFH_HIGH_THRESHOLD     = 4.0    # > 4 ft: greater reduction (V-zone / stilts)

# Construction era breakpoints
_ERA_OLD   = 1970   # pre-FIRM, older building codes
_ERA_NEW   = 2000   # post-2000 International Building Code adoption

# Maximum magnitude of any single adjustment (guardrail)
_MAX_SINGLE_ADJ = 15.0  # pp


def foundation_height_adjustment(
    base_pct: float,
    found_ht: Optional[float],
    depth_above_grade_ft: float,
) -> float:
    """
    Reduce damage for buildings with elevated first floors.

    When found_ht > 2 ft, the actual depth above the first finished floor is
    less than the surge depth above grade — the HAZUS curves already account
    for a ~1 ft default FFH.  This adjustment corrects for buildings that sit
    higher than that default.

    Args:
        base_pct: HAZUS damage percentage (0-100)
        found_ht: First-floor height above grade in feet (from NSI)
        depth_above_grade_ft: Surge depth at this location, feet above grade

    Returns:
        Adjustment in percentage points (negative = damage reduction)
    """
    if found_ht is None or found_ht <= 1.0:
        return 0.0  # at or below HAZUS default — no adjustment

    # The HAZUS curves use a ~1 ft default FFH.  Extra elevation above that
    # reduces the effective flood depth hitting the structure.
    extra_elevation = found_ht - 1.0  # feet of extra protection

    # If the surge doesn't even reach the floor, full protection
    if depth_above_grade_ft <= found_ht:
        return -min(base_pct, _MAX_SINGLE_ADJ)

    if found_ht >= _FFH_HIGH_THRESHOLD:
        adj = -min(base_pct * 0.20, _MAX_SINGLE_ADJ)   # up to -20% of base damage
    elif found_ht >= _FFH_ELEVATED_THRESHOLD:
        # Scale linearly between 2 ft and 4 ft threshold
        frac = (found_ht - _FFH_ELEVATED_THRESHOLD) / (_FFH_HIGH_THRESHOLD - _FFH_ELEVATED_THRESHOLD)
        adj = -min(base_pct * (0.08 + frac * 0.12), _MAX_SINGLE_ADJ)  # -8% to -20%
    else:
        # 1–2 ft: small reduction
        frac = (found_ht - 1.0) / (_FFH_ELEVATED_THRESHOLD - 1.0)
        adj = -min(base_pct * frac * 0.08, _MAX_SINGLE_ADJ)  # up to -8%

    return round(adj, 2)


def construction_era_adjustment(med_yr_blt: Optional[int]) -> float:
    """
    Adjust damage based on construction era (building code quality).

    Pre-1970 buildings pre-date most flood-aware construction codes and have
    statistically higher damage rates.  Post-2000 buildings benefit from IBC
    flood provisions.

    Args:
        med_yr_blt: Median year built (from NSI), or None

    Returns:
        Adjustment in percentage points (+/- relative to base)
    """
    if med_yr_blt is None:
        return 0.0

    if med_yr_blt < _ERA_OLD:
        # Pre-1970: older materials, no NFIP building standards
        return +7.0
    elif med_yr_blt >= _ERA_NEW:
        # Post-2000: modern flood provisions, better materials
        return -4.0
    else:
        # 1970–1999: linear interpolation between extremes
        frac = (med_yr_blt - _ERA_OLD) / (_ERA_NEW - _ERA_OLD)
        return round(+7.0 + frac * (-4.0 - 7.0), 2)  # +7 → -4 pp


def multistory_contents_adjustment(
    contents_pct: float,
    num_story: Optional[int],
    occtype: Optional[str],
    depth_above_grade_ft: float,
) -> float:
    """
    Reduce contents damage for multi-story buildings in partial-flood events.

    For a 2-story residential building with 4 ft of surge, upper-floor contents
    are undamaged.  HAZUS contents curves assume single-story distribution, so
    they overestimate contents loss for taller buildings at moderate depths.

    Only applied to residential occupancy types (RES*).

    Args:
        contents_pct: HAZUS contents damage percentage
        num_story: Number of stories (from NSI)
        occtype: NSI occupancy type (e.g. "RES1", "COM1")
        depth_above_grade_ft: Surge depth at this location, feet above grade

    Returns:
        Adjustment in percentage points (negative = reduction)
    """
    if num_story is None or num_story <= 1:
        return 0.0

    # Only apply to residential — commercial buildings tend to keep contents
    # on a single floor regardless of story count
    occ = (occtype or "").upper()
    if not occ.startswith("RES"):
        return 0.0

    # Fraction of floors flooded (approximate)
    # Assume 9 ft floor-to-floor height as a typical residential story
    floors_flooded = min(depth_above_grade_ft / 9.0, 1.0)
    undamaged_fraction = max(0.0, 1.0 - floors_flooded / num_story)

    adj = -contents_pct * undamaged_fraction * 0.5  # conservative: 50% of theoretical max
    return round(max(adj, -_MAX_SINGLE_ADJ), 2)


def adjust_damage_pct(
    structure_pct: float,
    contents_pct: float,
    found_ht: Optional[float] = None,
    med_yr_blt: Optional[int] = None,
    num_story: Optional[int] = None,
    occtype: Optional[str] = None,
    depth_above_grade_ft: float = 0.0,
) -> tuple[float, float]:
    """
    Apply all NSI-based adjustments to HAZUS base damage percentages.

    Args:
        structure_pct: Base HAZUS structure damage % (0-100)
        contents_pct:  Base HAZUS contents damage % (0-100)
        found_ht:      First-floor height above grade, ft (NSI found_ht)
        med_yr_blt:    Median year built (NSI med_yr_blt)
        num_story:     Number of stories (NSI num_story)
        occtype:       NSI occupancy type code
        depth_above_grade_ft: Surge depth at location, feet above grade

    Returns:
        (adjusted_structure_pct, adjusted_contents_pct) clamped to [0, 100]
    """
    # Structure adjustments
    ffh_adj  = foundation_height_adjustment(structure_pct, found_ht, depth_above_grade_ft)
    era_adj  = construction_era_adjustment(med_yr_blt)
    adj_struct = structure_pct + ffh_adj + era_adj

    # Contents adjustments (foundation height + multi-story)
    ms_adj   = multistory_contents_adjustment(contents_pct, num_story, occtype, depth_above_grade_ft)
    adj_cont = contents_pct + ffh_adj + era_adj + ms_adj

    adj_struct = round(max(0.0, min(100.0, adj_struct)), 2)
    adj_cont   = round(max(0.0, min(100.0, adj_cont)), 2)

    if logger.isEnabledFor(logging.DEBUG):
        logger.debug(
            "Damage adj: struct %.1f→%.1f (ffh%+.1f era%+.1f)  "
            "cont %.1f→%.1f (ms%+.1f)",
            structure_pct, adj_struct, ffh_adj, era_adj,
            contents_pct, adj_cont, ms_adj,
        )

    return adj_struct, adj_cont
