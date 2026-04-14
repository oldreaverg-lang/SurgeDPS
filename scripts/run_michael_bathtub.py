#!/usr/bin/env python
"""
Michael 2018 Bathtub Validation Driver (Pointwise)

Runs the pointwise bathtub model against the USGS HWM cache for
Hurricane Michael using a digitized peak-surge profile, then pipes
the results through the spatial validator and writes a full report
to data/validation/michael_2018/.

Zero raster storage — uses USGS EPQS for ground elevations.

Usage:
  python scripts/run_michael_bathtub.py
  python scripts/run_michael_bathtub.py --field exponential --peak-ft 14
  python scripts/run_michael_bathtub.py --min-quality Good --coastal-only

Outputs:
  data/validation/michael_2018/michael_2018_hwms.csv       (cached)
  data/validation/michael_2018/michael_2018_samples.csv    (per-point)
  data/validation/michael_2018/metrics.json                (scorecard)
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(os.path.dirname(HERE), "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from data_ingest.usgs_hwm import (  # noqa: E402
    fetch_or_load, filter_quality, filter_coastal, save_hwms,
)
from flood_model.bathtub_pointwise import (  # noqa: E402
    exponential_surge_field,
    interpolated_surge_field,
    run_pointwise_bathtub_on_hwms,
)
from validation.spatial_sampler import save_samples  # noqa: E402
from validation.spatial_metrics import (  # noqa: E402
    compute_metrics, save_metrics, metrics_to_summary_line,
)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Michael 2018 surge profile
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#
# Peak water surface elevations (NAVD88) digitized from:
#   - NHC Tropical Cyclone Report AL142018 (Beven, Berg, Hagen 2019)
#   - USGS Open-File Report 2019-1039 (Hurricane Michael field survey)
#   - NOAA post-storm tide-gauge data, Panama City (8729108),
#     Apalachicola (8728690), Cedar Key (8727520)
#
# (lat, lon, peak_ft) anchors along the FL Panhandle coast:
MICHAEL_SURGE_ANCHORS = [
    # ── West far field ─────────────────────────────────────────
    (30.39, -86.50,  3.0),   # Destin
    (30.24, -85.88,  5.0),   # W Panama City Beach
    # ── Panama City Bay ────────────────────────────────────────
    (30.16, -85.66,  7.5),   # Panama City (gauge 8729108 ~7.4 ft)
    (30.07, -85.59, 10.5),   # Tyndall AFB
    # ── Landfall zone ──────────────────────────────────────────
    (29.93, -85.41, 14.7),   # Mexico Beach (USGS max)
    (29.86, -85.37, 12.0),   # Port St. Joe Bay west shore
    (29.81, -85.30,  9.5),   # Port St. Joe town
    # ── Apalachicola region ────────────────────────────────────
    (29.73, -85.00,  5.5),   # Apalachicola (gauge 8728690 ~4.5 ft)
    (29.73, -84.86,  4.0),   # St. George Island
    # ── East far field ─────────────────────────────────────────
    (29.90, -84.38,  3.5),   # Alligator Point
    (29.14, -83.03,  3.2),   # Cedar Key (gauge 8727520 ~3.2 ft)
]


MICHAEL_LANDFALL = (29.93, -85.41)   # Mexico Beach, 13:30 UTC 2018-10-10
MICHAEL_PEAK_FT = 14.7


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--field",
        choices=["idw", "exponential"],
        default="idw",
        help="Surge field: 'idw' (IDW across anchor points) or "
             "'exponential' (radial decay from landfall). Default idw.",
    )
    p.add_argument(
        "--peak-ft",
        type=float,
        default=MICHAEL_PEAK_FT,
        help="Peak surge height for exponential field (default 14.7)",
    )
    p.add_argument(
        "--decay-km",
        type=float,
        default=40.0,
        help="Exponential decay scale in km (default 40)",
    )
    p.add_argument(
        "--idw-power",
        type=float,
        default=2.0,
        help="IDW power exponent (default 2.0)",
    )
    p.add_argument(
        "--idw-radius-km",
        type=float,
        default=150.0,
        help="IDW anchor search radius in km (default 150)",
    )
    p.add_argument(
        "--min-quality",
        choices=["Excellent", "Good", "Fair", "Poor", "VeryPoor"],
        default="Fair",
        help="Minimum HWM quality to include (default Fair)",
    )
    p.add_argument(
        "--coastal-only",
        action="store_true",
        default=True,
        help="Keep only coastal HWMs (default true)",
    )
    p.add_argument(
        "--include-riverine",
        dest="coastal_only",
        action="store_false",
        help="Include riverine HWMs as well",
    )
    p.add_argument(
        "--flood-threshold-ft",
        type=float,
        default=0.5,
    )
    p.add_argument(
        "--no-cache",
        action="store_true",
        help="Bypass EPQS cache (re-query every point)",
    )
    p.add_argument(
        "--force-refetch",
        action="store_true",
        help="Re-fetch HWMs from USGS instead of using cache",
    )
    p.add_argument("-v", "--verbose", action="store_true")
    return p


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    log = logging.getLogger("michael_bathtub")

    storm_id = "michael_2018"
    out_dir = os.path.join("data", "validation", storm_id)
    os.makedirs(out_dir, exist_ok=True)

    # 1. HWMs
    hwms = fetch_or_load(storm_id, force=args.force_refetch)
    log.info(f"Raw HWM count: {len(hwms)}")
    before = len(hwms)
    hwms = filter_quality(hwms, min_quality=args.min_quality)
    log.info(f"After quality filter ({args.min_quality}+): "
             f"{len(hwms)} (dropped {before - len(hwms)})")
    if args.coastal_only:
        before = len(hwms)
        hwms = filter_coastal(hwms)
        log.info(f"After coastal-only filter: {len(hwms)} "
                 f"(dropped {before - len(hwms)})")
    save_hwms(hwms, storm_id, cache_dir=out_dir)

    # 2. Surge field
    if args.field == "exponential":
        log.info(
            f"Surge field: exponential, peak={args.peak_ft} ft at "
            f"{MICHAEL_LANDFALL}, decay={args.decay_km} km"
        )
        surge_fn = exponential_surge_field(
            landfall_lat=MICHAEL_LANDFALL[0],
            landfall_lon=MICHAEL_LANDFALL[1],
            peak_surge_ft=args.peak_ft,
            decay_km=args.decay_km,
        )
    else:
        log.info(
            f"Surge field: IDW across {len(MICHAEL_SURGE_ANCHORS)} "
            f"anchor points (peak {max(a[2] for a in MICHAEL_SURGE_ANCHORS)} ft), "
            f"p={args.idw_power}, r={args.idw_radius_km} km"
        )
        surge_fn = interpolated_surge_field(
            anchor_points=MICHAEL_SURGE_ANCHORS,
            power=args.idw_power,
            search_radius_km=args.idw_radius_km,
        )

    # 3. Run pointwise bathtub
    samples, summary = run_pointwise_bathtub_on_hwms(
        hwms=hwms,
        surge_field_fn=surge_fn,
        storm_id=storm_id,
        flood_threshold_ft=args.flood_threshold_ft,
        use_epqs_cache=not args.no_cache,
    )
    if not samples:
        log.error("No samples produced. Check HWMs have HAG populated.")
        return 3

    save_samples(samples, storm_id, cache_dir=out_dir)

    # 4. Metrics
    metrics = compute_metrics(samples, storm_id=storm_id, source="usgs_hwm")
    print()
    print("=" * 72)
    print(f"  Hurricane Michael (2018) — Bathtub Baseline Validation")
    print("=" * 72)
    print(f"  Points w/ observed HAG:     {summary.n_points}")
    print(f"  Points w/ EPQS ground elev: {summary.n_with_ground_elev}")
    print(f"  Mean modeled WSE:           {summary.mean_wse_ft:.2f} ft")
    print()
    print(f"  {metrics_to_summary_line(metrics)}")
    print()
    print(f"  bias:         {metrics.bias_ft:+.2f} ft"
          if metrics.bias_ft is not None else "  bias: n/a")
    print(f"  MAE:          {metrics.mae_ft:.2f} ft"
          if metrics.mae_ft is not None else "  MAE: n/a")
    print(f"  RMSE:         {metrics.rmse_ft:.2f} ft"
          if metrics.rmse_ft is not None else "  RMSE: n/a")
    print(f"  R^2:          {metrics.r2:.3f}"
          if metrics.r2 is not None else "  R^2: n/a")
    print(f"  within ±1 ft: {metrics.pct_within_1ft:.1f}%"
          if metrics.pct_within_1ft is not None else "  within ±1 ft: n/a")
    print(f"  within ±2 ft: {metrics.pct_within_2ft:.1f}%"
          if metrics.pct_within_2ft is not None else "  within ±2 ft: n/a")
    if metrics.pod is not None:
        print(f"  POD:          {metrics.pod:.2f}")
    if metrics.far is not None:
        print(f"  FAR:          {metrics.far:.2f}")
    if metrics.csi is not None:
        print(f"  CSI:          {metrics.csi:.2f}")
    print()
    for ins in metrics.insights:
        print(f"  • {ins}")
    print("=" * 72)

    metrics_path = os.path.join(out_dir, "metrics.json")
    save_metrics(metrics, metrics_path)
    log.info(f"Artifacts written to: {out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
