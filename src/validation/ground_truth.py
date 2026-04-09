"""
Ground Truth Ledger

Known actual damage figures for curated historical storms, sourced from
NHC Tropical Cyclone Reports, NCEI billion-dollar disaster data, and
FEMA Public Assistance records.

These serve as the "answer key" for backtesting — comparing SurgeDPS
modeled losses against verified post-event assessments.

Sources:
  - NCEI Costliest U.S. Tropical Cyclones (2024 update)
  - NHC Tropical Cyclone Reports (per-storm)
  - Wikipedia "List of costliest Atlantic hurricanes" (cross-referenced)

Notes on what these numbers represent:
  - actual_damage_B: Total economic damage in billions USD (nominal,
    year-of-event dollars).  Includes insured + uninsured losses,
    property + infrastructure + agriculture.  This is the NHC/NCEI
    headline number.
  - surge_fraction: Estimated fraction of total damage attributable to
    storm surge (vs wind, rain/flooding, tornadoes).  Sourced from
    post-event engineering assessments where available, otherwise
    estimated from storm characteristics.
  - surge_damage_B: actual_damage_B × surge_fraction — the portion
    of real-world damage that SurgeDPS's surge model should predict.
    This is the proper comparison target for modeled loss.

Why surge_fraction matters:
  SurgeDPS models storm surge damage only.  Harvey's $125B total was
  dominated by inland rainfall flooding (surge was ~5%).  Michael's
  $25.5B was overwhelmingly surge + wind at the coast (~60% surge).
  Comparing raw modeled loss to raw actual total would conflate
  model accuracy with scope mismatch.
"""

from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Dict, List, Optional


@dataclass
class GroundTruth:
    """Verified post-event damage data for one storm."""

    storm_id: str
    name: str
    year: int
    category: int

    # Total economic damage (billions USD, nominal year-of-event)
    actual_damage_B: float

    # Estimated fraction of total damage from storm surge
    surge_fraction: float

    # Derived: surge-attributable damage (billions USD)
    @property
    def surge_damage_B(self) -> float:
        return round(self.actual_damage_B * self.surge_fraction, 2)

    # Source for the damage figure
    source: str = "NCEI/NHC"

    # Additional context
    notes: str = ""

    def to_dict(self) -> dict:
        d = asdict(self)
        d["surge_damage_B"] = self.surge_damage_B
        return d


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Ground Truth Database
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#
# Surge fractions estimated from:
#   - FEMA Mitigation Assessment Team (MAT) reports
#   - USACE post-storm surge measurements vs total loss decomposition
#   - NOAA Technical Memoranda on surge vs wind damage
#
# Conservative estimates: when uncertain, bias toward lower surge
# fraction so the model doesn't appear artificially accurate.
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

GROUND_TRUTH: List[GroundTruth] = [
    # ── Category 5 ──────────────────────────────────────────
    GroundTruth(
        storm_id="michael_2018",
        name="Hurricane Michael", year=2018, category=5,
        actual_damage_B=25.5,
        surge_fraction=0.55,
        notes="Mexico Beach devastated by 14-ft surge; significant wind damage inland. "
              "FEMA MAT report attributes ~55% to surge/wave action at coast.",
    ),

    # ── Category 4 ──────────────────────────────────────────
    GroundTruth(
        storm_id="katrina_2005",
        name="Hurricane Katrina", year=2005, category=4,
        actual_damage_B=125.0,
        surge_fraction=0.70,
        notes="28-ft surge in Mississippi; levee failures in New Orleans. "
              "Vast majority of property loss from surge inundation.",
    ),
    GroundTruth(
        storm_id="ike_2008",
        name="Hurricane Ike", year=2008, category=4,
        actual_damage_B=38.0,
        surge_fraction=0.55,
        notes="20-ft surge on Bolivar Peninsula; widespread Galveston flooding. "
              "Significant wind damage inland across TX/LA.",
    ),
    GroundTruth(
        storm_id="harvey_2017",
        name="Hurricane Harvey", year=2017, category=4,
        actual_damage_B=125.0,
        surge_fraction=0.05,
        notes="Damage overwhelmingly from record inland rainfall (60+ inches). "
              "Surge was limited to immediate coast near Rockport.",
    ),
    GroundTruth(
        storm_id="ian_2022",
        name="Hurricane Ian", year=2022, category=4,
        actual_damage_B=112.0,
        surge_fraction=0.50,
        notes="12-18 ft surge in Fort Myers/Sanibel; massive wind damage. "
              "Also significant inland flooding in central FL.",
    ),
    GroundTruth(
        storm_id="laura_2020",
        name="Hurricane Laura", year=2020, category=4,
        actual_damage_B=23.3,
        surge_fraction=0.30,
        notes="Surge in Cameron Parish was severe but area sparsely populated. "
              "Bulk of dollar losses from wind damage in Lake Charles.",
    ),
    GroundTruth(
        storm_id="ida_2021",
        name="Hurricane Ida", year=2021, category=4,
        actual_damage_B=75.3,
        surge_fraction=0.25,
        notes="Surge significant in SE Louisiana marshes. Majority of total "
              "damage from wind (LA) and inland flooding (NE US remnants).",
    ),
    GroundTruth(
        storm_id="helene_2024",
        name="Hurricane Helene", year=2024, category=4,
        actual_damage_B=78.7,
        surge_fraction=0.15,
        notes="Big Bend FL surge was catastrophic but sparsely populated. "
              "Majority of $78.7B from historic inland flooding in WNC mountains.",
    ),

    # ── Category 3 ──────────────────────────────────────────
    GroundTruth(
        storm_id="irma_2017",
        name="Hurricane Irma", year=2017, category=3,
        actual_damage_B=77.2,
        surge_fraction=0.30,
        notes="Surge impacts in FL Keys and Jacksonville. Large portion from "
              "wind damage across entire FL peninsula.",
    ),
    GroundTruth(
        storm_id="milton_2024",
        name="Hurricane Milton", year=2024, category=3,
        actual_damage_B=34.6,
        surge_fraction=0.35,
        notes="Surge in Tampa Bay and Sarasota coast. Significant wind and "
              "tornado damage across central FL.",
    ),

    # ── Category 2 ──────────────────────────────────────────
    GroundTruth(
        storm_id="sandy_2012",
        name="Hurricane Sandy", year=2012, category=2,
        actual_damage_B=68.7,
        surge_fraction=0.75,
        notes="Record 9+ ft surge in NYC/NJ. Overwhelmingly a surge event "
              "despite relatively weak winds at landfall.",
    ),
    GroundTruth(
        storm_id="delta_2020",
        name="Hurricane Delta", year=2020, category=2,
        actual_damage_B=3.0,
        surge_fraction=0.20,
        notes="Landfall in already-damaged Cameron Parish (Laura 6 weeks prior). "
              "Mostly wind damage; moderate surge.",
    ),

    # ── Category 1 ──────────────────────────────────────────
    GroundTruth(
        storm_id="florence_2018",
        name="Hurricane Florence", year=2018, category=1,
        actual_damage_B=24.2,
        surge_fraction=0.15,
        notes="Slow-moving storm; damage dominated by historic inland rainfall "
              "flooding in NC. Surge limited to immediate coast.",
    ),
    GroundTruth(
        storm_id="nicholas_2021",
        name="Hurricane Nicholas", year=2021, category=1,
        actual_damage_B=1.1,
        surge_fraction=0.20,
        notes="Minor surge near Matagorda Bay. Primarily a rainfall/flooding event.",
    ),
    GroundTruth(
        storm_id="nate_2017",
        name="Hurricane Nate", year=2017, category=1,
        actual_damage_B=0.8,
        surge_fraction=0.40,
        notes="Fast-moving storm; moderate surge on MS/AL coast. "
              "Limited US damage overall. Total includes Central America losses.",
        source="NHC TCR / Captive.com",
    ),
]

# Index by storm_id
GROUND_TRUTH_INDEX: Dict[str, GroundTruth] = {gt.storm_id: gt for gt in GROUND_TRUTH}


def get_ground_truth(storm_id: str) -> Optional[GroundTruth]:
    """Look up ground truth for a storm by ID."""
    return GROUND_TRUTH_INDEX.get(storm_id)
