"""
Spatial Validation Metrics

Compute depth-residual and contingency metrics from a list of
SampledObservation records. These are the core scores for judging
how well a modeled depth raster matches ground-truth point data.

Depth-residual metrics (for points with a numeric observed depth):
  - n                 Number of valid (modeled != None) samples
  - bias_ft           mean(modeled - observed)        signed
  - mae_ft            mean|modeled - observed|
  - rmse_ft           sqrt(mean((modeled - observed)^2))
  - pct_within_1ft    % of points where |residual| <= 1 ft
  - pct_within_2ft    % of points where |residual| <= 2 ft
  - max_under_ft      most extreme negative residual
  - max_over_ft       most extreme positive residual
  - r2                coefficient of determination

Contingency metrics (flooded / not flooded per point):
  - hits              observed flooded AND modeled flooded
  - misses            observed flooded AND NOT modeled flooded
  - false_alarms      NOT observed AND modeled flooded
  - correct_neg       neither flooded
  - pod               hits / (hits + misses)            Probability of Detection
  - far               false_alarms / (hits + false_alarms) False Alarm Ratio
  - csi               hits / (hits + misses + false_alarms) Critical Success Index
  - bias_ratio        (hits + FA) / (hits + miss)       > 1 = over-predicts extent

Interpretation cheatsheet:
  - bias near 0 is good. Positive = model over-predicts surge height.
  - POD and CSI close to 1 are good.
  - FAR close to 0 is good.
  - "Excellent" surge model: bias within ±0.5 ft, RMSE < 1.5 ft,
     CSI > 0.7, POD > 0.8 on high-quality HWMs.
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass, asdict, field
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class SpatialMetrics:
    """Aggregated spatial validation scores for a single raster run."""

    storm_id: str
    source: str = "mixed"        # "usgs_hwm", "noaa_gauge", or "mixed"
    n_total: int = 0             # total observations
    n_sampled: int = 0           # in-extent with numeric modeled value

    # Depth-residual metrics
    bias_ft: Optional[float] = None
    mae_ft: Optional[float] = None
    rmse_ft: Optional[float] = None
    pct_within_1ft: Optional[float] = None
    pct_within_2ft: Optional[float] = None
    max_under_ft: Optional[float] = None
    max_over_ft: Optional[float] = None
    r2: Optional[float] = None

    # Contingency metrics
    hits: int = 0
    misses: int = 0
    false_alarms: int = 0
    correct_neg: int = 0
    pod: Optional[float] = None
    far: Optional[float] = None
    csi: Optional[float] = None
    bias_ratio: Optional[float] = None

    # Diagnostic labels
    tier: str = "unknown"        # "excellent" / "good" / "fair" / "poor"
    insights: List[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return asdict(self)


def _safe_div(n: float, d: float) -> Optional[float]:
    return (n / d) if d > 0 else None


def _r_squared(observed: List[float], modeled: List[float]) -> Optional[float]:
    if len(observed) < 2:
        return None
    mean_obs = sum(observed) / len(observed)
    ss_tot = sum((o - mean_obs) ** 2 for o in observed)
    ss_res = sum((o - m) ** 2 for o, m in zip(observed, modeled))
    if ss_tot <= 0:
        return None
    return 1.0 - (ss_res / ss_tot)


def _classify_tier(
    bias: Optional[float],
    rmse: Optional[float],
    csi: Optional[float],
) -> str:
    """
    Composite tier based on bias, rmse, and CSI. All must be present
    and meet the tier's criteria; otherwise fall back to next lower.
    """
    if bias is None or rmse is None:
        return "unknown"
    abs_bias = abs(bias)
    if abs_bias <= 0.5 and rmse <= 1.5 and (csi is None or csi >= 0.7):
        return "excellent"
    if abs_bias <= 1.0 and rmse <= 2.5 and (csi is None or csi >= 0.5):
        return "good"
    if abs_bias <= 2.0 and rmse <= 4.0 and (csi is None or csi >= 0.3):
        return "fair"
    return "poor"


def _generate_insights(m: SpatialMetrics) -> List[str]:
    out: List[str] = []
    if m.n_sampled == 0:
        return ["No in-extent observations — is the raster over the right region?"]

    if m.bias_ft is not None:
        if m.bias_ft > 1.0:
            out.append(
                f"Model over-predicts depth by {m.bias_ft:+.2f} ft on average. "
                f"Check surge peak calibration and DEM vertical datum."
            )
        elif m.bias_ft < -1.0:
            out.append(
                f"Model under-predicts depth by {m.bias_ft:+.2f} ft on average. "
                f"Check cell coverage and surge extrapolation at landfall."
            )

    if m.pod is not None and m.pod < 0.5:
        out.append(
            f"POD = {m.pod:.2f}: model misses more than half of observed "
            f"flooded locations. Likely extent is too narrow."
        )
    if m.far is not None and m.far > 0.5:
        out.append(
            f"FAR = {m.far:.2f}: more than half of modeled-flooded locations "
            f"were dry in reality. Likely extent is too broad."
        )
    if m.bias_ratio is not None:
        if m.bias_ratio > 1.5:
            out.append(f"Extent bias ratio {m.bias_ratio:.2f}: over-predicting flooded area.")
        elif m.bias_ratio < 0.7:
            out.append(f"Extent bias ratio {m.bias_ratio:.2f}: under-predicting flooded area.")

    if m.pct_within_1ft is not None and m.pct_within_1ft >= 70:
        out.append(
            f"{m.pct_within_1ft:.0f}% of points within ±1 ft — strong spatial accuracy."
        )
    return out


def compute_metrics(
    samples: list,
    storm_id: str,
    source: str = "mixed",
) -> SpatialMetrics:
    """
    Aggregate a list of SampledObservation into a SpatialMetrics record.

    Only samples with a numeric modeled_ft contribute to depth residuals;
    all samples contribute to contingency counts (out-of-extent points
    are treated as modeled_flooded=False).
    """
    n_total = len(samples)
    metrics = SpatialMetrics(storm_id=storm_id, source=source, n_total=n_total)

    if n_total == 0:
        metrics.insights = ["No samples provided."]
        return metrics

    # ── Contingency (uses all samples) ──────────────────────────────
    for s in samples:
        c = s.contingency
        if c == "hit":
            metrics.hits += 1
        elif c == "miss":
            metrics.misses += 1
        elif c == "false_alarm":
            metrics.false_alarms += 1
        else:
            metrics.correct_neg += 1

    metrics.pod = _safe_div(metrics.hits, metrics.hits + metrics.misses)
    metrics.far = _safe_div(
        metrics.false_alarms, metrics.hits + metrics.false_alarms
    )
    metrics.csi = _safe_div(
        metrics.hits, metrics.hits + metrics.misses + metrics.false_alarms
    )
    metrics.bias_ratio = _safe_div(
        metrics.hits + metrics.false_alarms,
        metrics.hits + metrics.misses,
    )

    # ── Depth residuals (only in-extent, numeric modeled) ───────────
    valid = [s for s in samples if s.modeled_ft is not None]
    metrics.n_sampled = len(valid)

    if valid:
        residuals = [s.modeled_ft - s.observed_ft for s in valid]
        observed = [s.observed_ft for s in valid]
        modeled = [s.modeled_ft for s in valid]

        metrics.bias_ft = round(sum(residuals) / len(residuals), 3)
        metrics.mae_ft = round(sum(abs(r) for r in residuals) / len(residuals), 3)
        metrics.rmse_ft = round(
            math.sqrt(sum(r * r for r in residuals) / len(residuals)), 3
        )
        metrics.pct_within_1ft = round(
            100.0 * sum(1 for r in residuals if abs(r) <= 1.0) / len(residuals), 1
        )
        metrics.pct_within_2ft = round(
            100.0 * sum(1 for r in residuals if abs(r) <= 2.0) / len(residuals), 1
        )
        metrics.max_under_ft = round(min(residuals), 3)
        metrics.max_over_ft = round(max(residuals), 3)

        r2 = _r_squared(observed, modeled)
        metrics.r2 = round(r2, 3) if r2 is not None else None

    # ── Tier + insights ────────────────────────────────────────────
    metrics.tier = _classify_tier(metrics.bias_ft, metrics.rmse_ft, metrics.csi)
    metrics.insights = _generate_insights(metrics)
    return metrics


def metrics_to_summary_line(m: SpatialMetrics) -> str:
    """One-liner for logs."""
    bias = f"{m.bias_ft:+.2f}" if m.bias_ft is not None else "n/a"
    rmse = f"{m.rmse_ft:.2f}" if m.rmse_ft is not None else "n/a"
    csi = f"{m.csi:.2f}" if m.csi is not None else "n/a"
    return (
        f"[{m.tier}] n={m.n_sampled}/{m.n_total} "
        f"bias={bias} ft, RMSE={rmse} ft, CSI={csi}, "
        f"hits={m.hits} miss={m.misses} FA={m.false_alarms}"
    )


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Serialization helper
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def save_metrics(
    metrics: SpatialMetrics,
    out_path: str,
) -> str:
    """Write metrics + insights as JSON."""
    import json
    import os

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(metrics.to_dict(), f, indent=2)
    logger.info(f"Wrote spatial metrics → {out_path}")
    return out_path
