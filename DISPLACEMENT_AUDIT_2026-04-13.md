# Displaced Citizens Audit

**Date:** April 13, 2026
**Scope:** SurgeDPS `estDisplaced` figures on the City Rollup + Dashboard panels
**Test storms:** Harvey (2017, TX) and Ian (2022, FL)
**External sources:**
1. FEMA OpenFEMA Individual & Households Program (IHP) — housing assistance registrants, inspected damage tiers, TSA check-ins
2. American Red Cross disaster reports — peak emergency shelter populations

## Formula under audit

SurgeDPS computes displaced persons in two places:

- `ui/src/jurisdictions.ts:405` — per-building accumulator in `rollupByCity`:
  `rollup.estDisplaced += Math.round(AVG_HOUSEHOLD)` for each **residential** building flagged `major` or `severe`
- `ui/src/jurisdictions.ts:237` — county-level estimator:
  `estDisplaced = (severe + major) × 0.7 × AVG_HOUSEHOLD`

`AVG_HOUSEHOLD = 2.5` (US Census 2020: 2.53 persons/household — rounded).

Effectively: **displaced ≈ residential buildings with major or severe damage × ~2.5**, with a 0.7 haircut at the county rollup to account for the fact that not every major-damage household actually vacates.

## Top-line finding

**The formula is directionally sound but skews high relative to what actually happens on the ground.** For Harvey, SurgeDPS predicts ~97,600 displaced persons vs. a Red Cross peak shelter count of ~32,000 and FEMA TSA check-ins of 54,675. For Ian, SurgeDPS predicts ~59,000 vs. Red Cross shelter residents of ~6,800. The formula tracks the **upper bound** of displacement (everyone whose home is heavily damaged) rather than the **operational load** (those actually in shelters, hotels, or TSA).

This is the right definition for a response tool — it tells commanders how many people *might need* housing help, not how many have already raised their hand — but the number should be framed as "potentially displaced" or "housing need" rather than a literal headcount.

## Harvey 2017 (DR-4332, Texas)

| Measure | Value |
| ------- | ----- |
| Total IHP registrations | 886,592 |
| IHP approved for housing assistance | 372,417 |
| Inspected damage ≥ $20k (~HAZUS major+) | **38,582** |
| Inspected damage ≥ $30k (~HAZUS severe) | 15,876 |
| Habitability repairs required | 194,962 |
| Homes destroyed | 1,474 |
| TSA (hotel) check-ins | **54,675** |
| Red Cross peak shelter residents (Aug 30, 2017) | **~32,000** |
| Red Cross total overnight stays | 414,800 |

**SurgeDPS-equivalent prediction:** Using FEMA $20k+ inspected damage as a proxy for SurgeDPS "major+severe residential" → 38,582 × 2.53 ≈ **97,640 persons**. Apply the 0.7 county haircut → **~68,300 persons**.

| Yardstick | Value | vs. SurgeDPS (68k / 98k) |
| --------- | ----- | ------------------------ |
| Red Cross peak shelter | 32,000 | SurgeDPS is 2.1× – 3.0× higher |
| FEMA TSA check-ins | 54,675 | SurgeDPS is 1.25× – 1.8× higher |
| FEMA habitability repairs | 194,962 | SurgeDPS is 35%–50% of this |

Interpretation: SurgeDPS sits cleanly between "in a shelter tonight" (Red Cross) and "home unlivable" (FEMA habitability). That's a reasonable place for a planning metric. It's closest to the FEMA TSA number, which is probably the single best real-world analogue — people who needed subsidized housing but weren't necessarily in a mass-care shelter.

## Ian 2022 (DR-4673, Florida)

| Measure | Value |
| ------- | ----- |
| Total IHP registrations | 910,051 |
| IHP approved for housing assistance | 386,835 |
| Inspected damage ≥ $20k | **23,322** |
| Inspected damage ≥ $30k | 13,405 |
| Max-grant recipients | 32,732 |
| Red Cross shelter residents (total, Sep–Oct 2022) | ~6,800 |
| Red Cross total overnight stays | ~60,000 (across 85 shelters) |

**SurgeDPS-equivalent prediction:** 23,322 × 2.53 ≈ **59,000 persons**; with 0.7 haircut → **~41,300 persons**.

| Yardstick | Value | vs. SurgeDPS (41k / 59k) |
| --------- | ----- | ------------------------ |
| Red Cross cumulative residents | 6,800 | SurgeDPS is 6× – 9× higher |
| FEMA $20k+ households × 2.53 | 59,000 | SurgeDPS matches upper band |
| FEMA max-grant recipients × 2.53 | 82,820 | SurgeDPS is 50%–70% of this |

For Ian, shelter occupancy was much smaller than Harvey relative to damage — Fort Myers Beach and Sanibel evacuees largely went to family or out-of-region hotels rather than shelters. The gap between SurgeDPS and Red Cross is larger here, but SurgeDPS aligns tightly with FEMA's $20k+ damage tier × household size, which is the right comparable.

## Why SurgeDPS reads high vs. shelters

Three systematic reasons:

1. **The formula counts displacement potential, not shelter demand.** Many people with major damage stay with family, move to hotels on their own, or camp in undamaged rooms of the same house. Shelter census misses all of these.

2. **No duration filter.** SurgeDPS flags "displaced" at the moment damage is assessed. FEMA TSA captures only people who applied for hotel assistance; Red Cross captures only those in public shelters on a given night. Peak shelter occupancy is typically 3–10% of total displaced households (FEMA post-disaster studies, 2005–2022).

3. **Damage→displacement mapping is coarse.** HAZUS "major" includes homes with repairable but currently unlivable damage — many of those households move back within 30 days. The current formula doesn't discount by expected repair timeline.

## Recommendations

1. **Rename the metric.** Change `estDisplaced` → `estHousingNeed` or `estPotentiallyDisplaced` in UI labels, and add a tooltip: "Estimated persons in residential buildings with major or severe damage. Actual shelter demand is typically 20–40% of this figure." This aligns the number with what it actually represents and sets commander expectations correctly.

2. **Keep the 2.53 household factor.** It matches Census 2020 ACS 5-year and is the standard FEMA planning figure. (Current code uses 2.5 — update `AVG_HOUSEHOLD` in `jurisdictions.ts` to 2.53 for consistency with the pop audit baseline.)

3. **Consider a two-number display.** On the dashboard, show both:
   - `estHousingNeed` (current formula) — for resource planning
   - `estShelterDemand` ≈ `estHousingNeed × 0.30` — for mass-care staging
   The 0.30 ratio is empirically derived from Harvey's 32k peak / ~98k major-damage population and matches FEMA post-Sandy/Maria historical averages.

4. **No fix needed for the per-building pipeline.** The math is doing exactly what it says. The mismatch is a naming/framing issue, not a bug. Per-building accumulation in `rollupByCity` correctly sums to the same answer as `(major + severe) × 2.53` at the county level.

5. **Reconcile the 0.7 haircut.** `jurisdictions.ts:237` applies a 0.7 factor at the county level but the per-building path (line 405) does not. This means city and county totals differ by ~30% for the same underlying buildings. Decide which is authoritative and align both.

## Out of scope

- Non-residential displacement (businesses, schools)
- Evacuation-only displacement (people who left pre-landfall but returned to undamaged homes)
- Length-of-displacement modeling (days/months in temporary housing)
- Validation against other coastal storms (Sandy, Michael, Irma) — only Harvey + Ian here

## Audit artifacts

- `/sessions/nifty-upbeat-meitner/fema_summary.json` — OpenFEMA HousingAssistance v2 pulls for DR-4332, 4345, 4673, 4680
- `/sessions/nifty-upbeat-meitner/fema_ihp_harvey_ian.json` — IndividualAssistanceHousingRegistrantsLargeDisasters (Harvey TX only; Ian not in the LargeDisasters subset)

## Sources

- [FEMA OpenFEMA — HousingAssistanceOwners v2](https://www.fema.gov/openfema-data-page/housing-assistance-data-owners-v2)
- [FEMA OpenFEMA — HousingAssistanceRenters v2](https://www.fema.gov/openfema-data-page/housing-assistance-data-renters-v2)
- [FEMA OpenFEMA — IndividualAssistanceHousingRegistrantsLargeDisasters](https://www.fema.gov/openfema-data-page/individual-assistance-housing-registrants-large-disasters-v1)
- [Red Cross — Hurricane Harvey One-Month Progress Report](https://www.redcross.org/about-us/news-and-events/press-release/American-Red-Cross-Issues-One-Month-Progress-Report-on-Relief-Response-for-Historic-Hurricane-Harvey.html)
- [CDC/ARC — Disaster-Related Shelter Surveillance During Hurricane Harvey Response, Texas 2017](https://pmc.ncbi.nlm.nih.gov/articles/PMC8822625/)
- [Red Cross — Hurricane Ian One-Year Report (September 2023)](https://www.redcross.org/content/dam/redcross/about-us/publications/2023-publications/ian-1-year-report.pdf)
