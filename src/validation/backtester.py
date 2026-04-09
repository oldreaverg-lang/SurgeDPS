"""
Backtesting Scorer

Compares SurgeDPS modeled losses against known ground truth to produce
accuracy metrics.  This is the core validation engine.

Key concepts:

  Accuracy ratio = modeled_surge_loss / actual_surge_loss
    - 1.0 = perfect prediction
    - < 1.0 = model underestimates
    - > 1.0 = model overestimates

  We compare against surge_damage_B (not total damage) because SurgeDPS
  only models surge — not wind, rainfall, or tornado damage.

  The backtester also tracks which model inputs correlate with accuracy,
  so over time we learn which signals to trust for active storms.

Output tiers (for UI display):
  - "excellent":  ratio within 0.7–1.3  (±30%)
  - "good":       ratio within 0.5–1.5  (±50%)
  - "fair":       ratio within 0.3–2.0  (within one order of magnitude)
  - "poor":       ratio outside 0.3–2.0

For active storms, the backtester provides confidence intervals based
on historical accuracy at similar categories/data quality levels.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Tuple

from validation.ground_truth import (
    GroundTruth, GROUND_TRUTH, GROUND_TRUTH_INDEX, get_ground_truth,
)
from validation.run_ledger import get_latest_run, get_runs


@dataclass
class StormScore:
    """Accuracy score for one storm."""

    storm_id: str
    name: str
    category: int

    # Ground truth
    actual_total_B: float        # Total actual damage (billions)
    surge_fraction: float        # Estimated surge fraction
    actual_surge_B: float        # Surge-attributable damage (billions)

    # Model output
    modeled_loss_B: float        # SurgeDPS modeled loss (billions)
    building_count: int
    cells_loaded: int
    nsi_pct: float               # % of buildings from NSI
    avg_data_quality: float

    # Accuracy metrics
    accuracy_ratio: float        # modeled / actual_surge
    log_error: float             # log2(ratio) — symmetric error measure
    accuracy_tier: str           # "excellent" / "good" / "fair" / "poor"
    pct_error: float             # percentage over/under-estimate

    # Diagnostic notes
    notes: str = ""

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class BacktestReport:
    """Aggregated backtesting results across all scored storms."""

    storms_scored: int
    storms_available: int        # How many storms have both GT and run data

    # Overall accuracy
    median_accuracy_ratio: float
    mean_abs_log_error: float    # Lower = better; 0 = perfect
    tier_counts: Dict[str, int]  # {"excellent": 3, "good": 5, ...}

    # Per-storm scores (sorted by accuracy)
    scores: List[StormScore]

    # Confidence interval for active storm predictions
    # Based on historical spread of accuracy ratios
    prediction_interval_low: float    # 25th percentile ratio
    prediction_interval_high: float   # 75th percentile ratio

    # Correlation insights
    insights: List[str]

    model_version: str = "1.0"

    def to_dict(self) -> dict:
        d = asdict(self)
        return d


def _accuracy_tier(ratio: float) -> str:
    """Classify accuracy ratio into a tier."""
    if 0.7 <= ratio <= 1.3:
        return "excellent"
    elif 0.5 <= ratio <= 1.5:
        return "good"
    elif 0.3 <= ratio <= 2.0:
        return "fair"
    else:
        return "poor"


def score_storm(storm_id: str) -> Optional[StormScore]:
    """
    Score a single storm: compare its latest model run to ground truth.

    Returns None if either ground truth or model run is missing.
    """
    gt = get_ground_truth(storm_id)
    if not gt:
        return None

    run = get_latest_run(storm_id)
    if not run:
        return None

    modeled_B = run["modeled_loss"] / 1e9  # Convert $ to billions
    actual_surge_B = gt.surge_damage_B

    # Avoid division by zero
    if actual_surge_B <= 0:
        return None

    ratio = modeled_B / actual_surge_B
    log_err = math.log2(ratio) if ratio > 0 else -10
    pct_err = (ratio - 1.0) * 100

    nsi_total = run.get("nsi_count", 0)
    osm_total = run.get("osm_count", 0)
    total = nsi_total + osm_total
    nsi_pct = (nsi_total / total * 100) if total > 0 else 0

    # Diagnostic notes
    notes_parts = []
    if modeled_B < actual_surge_B * 0.5:
        notes_parts.append("Model significantly underestimates; "
                           "check cell coverage and NSI availability")
    elif modeled_B > actual_surge_B * 2.0:
        notes_parts.append("Model significantly overestimates; "
                           "check surge model calibration and building values")
    if nsi_pct < 50:
        notes_parts.append(f"Low NSI coverage ({nsi_pct:.0f}%); "
                           "OSM fallback uses generic replacement values")
    if run.get("cells_loaded", 0) < 9:
        notes_parts.append(f"Only {run.get('cells_loaded', 0)} cells loaded; "
                           "may miss outlying damage")

    return StormScore(
        storm_id=storm_id,
        name=gt.name,
        category=gt.category,
        actual_total_B=gt.actual_damage_B,
        surge_fraction=gt.surge_fraction,
        actual_surge_B=actual_surge_B,
        modeled_loss_B=round(modeled_B, 3),
        building_count=run.get("building_count", 0),
        cells_loaded=run.get("cells_loaded", 0),
        nsi_pct=round(nsi_pct, 1),
        avg_data_quality=run.get("avg_data_quality", 0),
        accuracy_ratio=round(ratio, 3),
        log_error=round(log_err, 3),
        accuracy_tier=_accuracy_tier(ratio),
        pct_error=round(pct_err, 1),
        notes="; ".join(notes_parts),
    )


def run_backtest() -> BacktestReport:
    """
    Score all storms that have both ground truth and model run data.

    Returns a comprehensive BacktestReport with per-storm scores,
    aggregate accuracy metrics, and confidence intervals for
    future predictions.
    """
    scores: List[StormScore] = []

    for gt in GROUND_TRUTH:
        score = score_storm(gt.storm_id)
        if score is not None:
            scores.append(score)

    # Sort by accuracy ratio (closest to 1.0 first)
    scores.sort(key=lambda s: abs(s.log_error))

    # Aggregate metrics
    ratios = [s.accuracy_ratio for s in scores]
    log_errors = [abs(s.log_error) for s in scores]

    n = len(scores)
    if n == 0:
        return BacktestReport(
            storms_scored=0,
            storms_available=len(GROUND_TRUTH),
            median_accuracy_ratio=0,
            mean_abs_log_error=0,
            tier_counts={},
            scores=[],
            prediction_interval_low=0,
            prediction_interval_high=0,
            insights=["No storms have been scored yet. Activate storms to populate the ledger."],
        )

    sorted_ratios = sorted(ratios)
    median_ratio = sorted_ratios[n // 2] if n % 2 == 1 else (
        sorted_ratios[n // 2 - 1] + sorted_ratios[n // 2]) / 2
    mean_log_err = sum(log_errors) / n

    tier_counts: Dict[str, int] = {"excellent": 0, "good": 0, "fair": 0, "poor": 0}
    for s in scores:
        tier_counts[s.accuracy_tier] = tier_counts.get(s.accuracy_tier, 0) + 1

    # Prediction interval (IQR of ratios)
    q25_idx = max(0, n // 4)
    q75_idx = min(n - 1, 3 * n // 4)
    p_low = sorted_ratios[q25_idx]
    p_high = sorted_ratios[q75_idx]

    # Generate insights
    insights = _generate_insights(scores)

    return BacktestReport(
        storms_scored=n,
        storms_available=len(GROUND_TRUTH),
        median_accuracy_ratio=round(median_ratio, 3),
        mean_abs_log_error=round(mean_log_err, 3),
        tier_counts=tier_counts,
        scores=scores,
        prediction_interval_low=round(p_low, 3),
        prediction_interval_high=round(p_high, 3),
        insights=insights,
    )


def predict_loss_range(
    modeled_loss: float,
    backtest: Optional[BacktestReport] = None,
) -> Dict:
    """
    Given a modeled loss for an active storm, produce a confidence
    interval based on historical backtesting accuracy.

    Returns:
        {
            "modeled": 2_093_100_000,
            "low": 1_395_400_000,
            "high": 3_139_650_000,
            "confidence": "Based on ±X% historical accuracy across N storms",
        }
    """
    if backtest is None:
        backtest = run_backtest()

    if backtest.storms_scored < 3:
        # Not enough data for meaningful interval
        return {
            "modeled": modeled_loss,
            "low": modeled_loss * 0.3,
            "high": modeled_loss * 3.0,
            "confidence": "Insufficient backtesting data (fewer than 3 storms scored)",
            "storms_scored": backtest.storms_scored,
        }

    # Use IQR of accuracy ratios to invert the prediction
    # If the model tends to overestimate by 1.2x, the real loss is modeled/1.2
    low_mult = 1.0 / backtest.prediction_interval_high if backtest.prediction_interval_high > 0 else 0.5
    high_mult = 1.0 / backtest.prediction_interval_low if backtest.prediction_interval_low > 0 else 2.0

    pct_spread = abs(high_mult - low_mult) / 2 * 100

    return {
        "modeled": round(modeled_loss, 2),
        "low": round(modeled_loss * min(low_mult, high_mult), 2),
        "high": round(modeled_loss * max(low_mult, high_mult), 2),
        "confidence": f"Based on ±{pct_spread:.0f}% historical accuracy across {backtest.storms_scored} storms",
        "storms_scored": backtest.storms_scored,
    }


def _generate_insights(scores: List[StormScore]) -> List[str]:
    """Analyze patterns in backtest scores to generate actionable insights."""
    insights = []

    if not scores:
        return ["No storm runs to analyze."]

    # Overall accuracy
    n = len(scores)
    excellent = sum(1 for s in scores if s.accuracy_tier == "excellent")
    good = sum(1 for s in scores if s.accuracy_tier in ("excellent", "good"))
    insights.append(
        f"{excellent}/{n} storms scored 'excellent' (±30%); "
        f"{good}/{n} scored 'good' or better (±50%)"
    )

    # Systematic bias check
    ratios = [s.accuracy_ratio for s in scores]
    mean_ratio = sum(ratios) / len(ratios)
    if mean_ratio < 0.7:
        insights.append(
            f"Model tends to underestimate (mean ratio {mean_ratio:.2f}). "
            "Consider: cell coverage may miss outlying damage, "
            "or replacement values may be too conservative."
        )
    elif mean_ratio > 1.5:
        insights.append(
            f"Model tends to overestimate (mean ratio {mean_ratio:.2f}). "
            "Consider: surge model may produce excessive depths, "
            "or damage curves may be too aggressive."
        )

    # NSI coverage correlation
    high_nsi = [s for s in scores if s.nsi_pct > 70]
    low_nsi = [s for s in scores if s.nsi_pct < 30]
    if high_nsi and low_nsi:
        avg_err_high = sum(abs(s.log_error) for s in high_nsi) / len(high_nsi)
        avg_err_low = sum(abs(s.log_error) for s in low_nsi) / len(low_nsi)
        if avg_err_high < avg_err_low * 0.7:
            insights.append(
                f"High NSI coverage storms are significantly more accurate "
                f"(avg error {avg_err_high:.2f} vs {avg_err_low:.2f}). "
                "NSI data quality directly improves predictions."
            )

    # Category-specific accuracy
    cat_groups: Dict[int, List[StormScore]] = {}
    for s in scores:
        cat_groups.setdefault(s.category, []).append(s)
    for cat in sorted(cat_groups.keys(), reverse=True):
        group = cat_groups[cat]
        if len(group) >= 2:
            avg_ratio = sum(s.accuracy_ratio for s in group) / len(group)
            if avg_ratio < 0.5 or avg_ratio > 2.0:
                insights.append(
                    f"Cat {cat} storms: avg ratio {avg_ratio:.2f} — "
                    f"model {'under' if avg_ratio < 1 else 'over'}estimates at this intensity."
                )

    # Harvey special case — rainfall-dominated storms
    for s in scores:
        if s.surge_fraction < 0.1 and s.accuracy_ratio > 2.0:
            insights.append(
                f"{s.name}: surge fraction only {s.surge_fraction:.0%} of total damage. "
                "This storm was rainfall-dominated; high model ratio is expected."
            )

    return insights
