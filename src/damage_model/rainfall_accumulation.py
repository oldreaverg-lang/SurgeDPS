"""
Rainfall accumulation curves for time-series damage modeling.

Tropical-cyclone rainfall is not evenly distributed across the storm's
lifetime — it peaks around landfall and tapers over the following 1–2
days as the circulation decays and moves inland. For the time-series
peril pipeline we need a deterministic curve so every cell computes a
consistent rainfall fraction at every tick.

We model the cumulative fraction with a Gamma CDF (shape=2.0, scale=12 h).
This puts the median roughly 18 h after landfall and the 95th percentile
around 60 h — matches the shape of post-landfall QPF for typical TCs
(Harvey-class stalling events need a longer tail but are handled via the
``duration_hours`` parameter).

Public API
----------
    rainfall_fraction_at_hour(hours_since_landfall, duration_hours=72)
        Returns the cumulative fraction (0.0–1.0) of total rainfall that
        has fallen by *hours_since_landfall*.

    rainfall_increment_in_window(start_h, end_h, duration_hours=72)
        Fraction of total rainfall that falls between two tick times.
        Useful for computing "new rainfall this tick."

The functions are pure and dependency-light (math only) so they can be
called from both the API server and warm_cache without extra imports.
"""
from __future__ import annotations

import math
from typing import Iterable, List, Tuple


# Gamma shape / scale chosen empirically to match 18 h median accumulation
# for a typical TC. Override via the duration_hours knob for long-tail
# storms (Harvey 2017 took ~96 h to drain most rainfall) — the curve is
# renormalized so fraction(duration_hours) == 1.0.
_DEFAULT_SHAPE = 2.0
_DEFAULT_SCALE_H = 12.0


def _lower_incomplete_gamma(s: float, x: float) -> float:
    """Lower incomplete gamma γ(s, x), via series expansion.

    Converges fast for small x (we're always in 0..72 h / scale=12 →
    x ∈ [0, 6]). Adequate precision for our needs (~1e-6).
    """
    if x <= 0:
        return 0.0
    term = 1.0 / s
    total = term
    for n in range(1, 200):
        term *= x / (s + n)
        total += term
        if abs(term) < 1e-10:
            break
    return total * math.exp(-x + s * math.log(x))


def _gamma_cdf(x: float, shape: float, scale: float) -> float:
    """Gamma CDF at x. Normalized so CDF(∞) = 1.0."""
    if x <= 0:
        return 0.0
    return _lower_incomplete_gamma(shape, x / scale) / math.gamma(shape)


def rainfall_fraction_at_hour(
    hours_since_landfall: float,
    duration_hours: float = 72.0,
    shape: float = _DEFAULT_SHAPE,
    scale_hours: float = _DEFAULT_SCALE_H,
) -> float:
    """Cumulative fraction of storm-total rainfall by *hours_since_landfall*.

    The raw Gamma CDF asymptotes to 1.0 at infinity; we renormalize so
    that at ``duration_hours`` the fraction is exactly 1.0. Before 0 h
    returns 0; at landfall (0 h) returns ~0.0 (rainfall really kicks in
    as the eye approaches, so a small lead-in may happen — we fold that
    into the pre-landfall envelope by treating hours<0 as 0).
    """
    if hours_since_landfall <= 0.0:
        return 0.0
    if hours_since_landfall >= duration_hours:
        return 1.0
    cdf_now  = _gamma_cdf(hours_since_landfall, shape, scale_hours)
    cdf_full = _gamma_cdf(duration_hours, shape, scale_hours)
    if cdf_full <= 0.0:
        return 0.0
    return cdf_now / cdf_full


def rainfall_increment_in_window(
    start_h: float,
    end_h: float,
    duration_hours: float = 72.0,
    shape: float = _DEFAULT_SHAPE,
    scale_hours: float = _DEFAULT_SCALE_H,
) -> float:
    """Fraction of storm-total rainfall falling between start_h and end_h."""
    if end_h <= start_h:
        return 0.0
    a = rainfall_fraction_at_hour(start_h, duration_hours, shape, scale_hours)
    b = rainfall_fraction_at_hour(end_h,   duration_hours, shape, scale_hours)
    return max(0.0, b - a)


# ── Tick schedule helpers ────────────────────────────────────────────────────
DEFAULT_TICK_STEP_H = 3.0
DEFAULT_DURATION_H  = 72.0


def default_tick_hours(
    step_h: float = DEFAULT_TICK_STEP_H,
    duration_h: float = DEFAULT_DURATION_H,
) -> List[float]:
    """Return [0, step, 2·step, …, duration] inclusive. 3-h steps to 72 h
    yields 25 ticks by default (0, 3, 6, …, 72)."""
    n = int(round(duration_h / step_h))
    return [round(i * step_h, 2) for i in range(n + 1)]


def tick_fractions(
    tick_hours: Iterable[float],
    duration_hours: float = DEFAULT_DURATION_H,
) -> List[Tuple[float, float]]:
    """Returns [(hour, cumulative_rainfall_fraction)] for each tick.
    Convenience for driving the time-series damage loop."""
    return [
        (h, rainfall_fraction_at_hour(h, duration_hours=duration_hours))
        for h in tick_hours
    ]


if __name__ == "__main__":  # sanity check
    for h in default_tick_hours():
        f = rainfall_fraction_at_hour(h)
        print(f"t+{h:>4.1f}h  rainfall={f:5.3f}")
