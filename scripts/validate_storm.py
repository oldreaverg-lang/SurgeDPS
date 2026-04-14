#!/usr/bin/env python
"""
validate_storm.py — Spatial validation driver

Given a storm_id and a modeled depth GeoTIFF, fetch USGS HWM ground
truth, sample the raster at each HWM location, compute depth-residual
and contingency metrics, and write the results to JSON + parquet.

Usage:
  python scripts/validate_storm.py michael_2018 \
      --raster data/runs/michael_2018/depth_compound_t000.tif \
      --out    data/validation/michael_2018

  # Only coastal HWMs, quality >= Good:
  python scripts/validate_storm.py michael_2018 --raster ... \
      --coastal-only --min-quality Good

  # Dry-run: just fetch HWMs and report counts (no sampling)
  python scripts/validate_storm.py michael_2018 --hwms-only

Outputs (under --out directory):
  hwms.parquet                raw HWM table
  samples.parquet             per-point (observed, modeled, residual)
  metrics.json                aggregate scores + insights
  residuals.csv               human-readable export for diffing
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

# Make src importable when running as a script
HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(os.path.dirname(HERE), "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from data_ingest.usgs_hwm import (  # noqa: E402
    fetch_or_load, filter_quality, filter_coastal, save_hwms,
)
from validation.spatial_sampler import sample_hwms, save_samples  # noqa: E402
from validation.spatial_metrics import (  # noqa: E402
    compute_metrics, save_metrics, metrics_to_summary_line,
)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("storm_id", help="Canonical storm key, e.g. michael_2018")
    p.add_argument(
        "--raster",
        help="Modeled depth GeoTIFF (feet). Required unless --hwms-only.",
    )
    p.add_argument(
        "--out",
        default=None,
        help="Output directory (default: data/validation/{storm_id})",
    )
    p.add_argument(
        "--min-quality",
        choices=["Excellent", "Good", "Fair", "Poor"],
        default="Fair",
        help="Minimum HWM quality to include (default: Fair)",
    )
    p.add_argument(
        "--coastal-only",
        action="store_true",
        help="Filter to coastal HWMs (exclude riverine/lacustrine)",
    )
    p.add_argument(
        "--flood-threshold-ft",
        type=float,
        default=0.5,
        help="Depth threshold for flooded classification (default 0.5 ft)",
    )
    p.add_argument(
        "--force-refetch",
        action="store_true",
        help="Ignore HWM cache and re-fetch from USGS STN",
    )
    p.add_argument(
        "--hwms-only",
        action="store_true",
        help="Only fetch + cache HWMs; do not sample raster",
    )
    p.add_argument("-v", "--verbose", action="store_true")
    return p


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    log = logging.getLogger("validate_storm")

    out_dir = args.out or os.path.join("data", "validation", args.storm_id)
    os.makedirs(out_dir, exist_ok=True)

    # ── 1. Fetch / load HWMs ─────────────────────────────────────────
    hwms = fetch_or_load(args.storm_id, force=args.force_refetch)
    log.info(f"Raw HWM count: {len(hwms)}")
    if not hwms:
        log.error(f"No HWMs available for {args.storm_id}. Aborting.")
        return 2

    before = len(hwms)
    hwms = filter_quality(hwms, min_quality=args.min_quality)
    log.info(f"After quality filter ({args.min_quality}+): {len(hwms)} "
             f"(dropped {before - len(hwms)})")

    if args.coastal_only:
        before = len(hwms)
        hwms = filter_coastal(hwms)
        log.info(f"After coastal-only filter: {len(hwms)} "
                 f"(dropped {before - len(hwms)})")

    save_hwms(hwms, args.storm_id, cache_dir=out_dir)

    if args.hwms_only:
        log.info("--hwms-only set; stopping before raster sampling.")
        return 0

    if not args.raster:
        log.error("--raster is required unless --hwms-only. Aborting.")
        return 2
    if not os.path.exists(args.raster):
        log.error(f"Raster not found: {args.raster}")
        return 2

    # ── 2. Sample raster at HWM coords ───────────────────────────────
    samples = sample_hwms(
        hwms=hwms,
        depth_raster_path=args.raster,
        storm_id=args.storm_id,
        flood_threshold_ft=args.flood_threshold_ft,
    )
    if not samples:
        log.error(
            "Sampler returned no samples. "
            "Check that HWMs have height_above_gnd_ft populated."
        )
        return 3

    save_samples(samples, args.storm_id, cache_dir=out_dir)

    # ── 3. Metrics ──────────────────────────────────────────────────
    metrics = compute_metrics(samples, storm_id=args.storm_id, source="usgs_hwm")
    log.info(metrics_to_summary_line(metrics))
    for ins in metrics.insights:
        log.info(f"  insight: {ins}")

    metrics_path = os.path.join(out_dir, "metrics.json")
    save_metrics(metrics, metrics_path)

    # samples are already written as CSV by save_samples; no extra export needed

    log.info(f"Validation complete for {args.storm_id}. Output: {out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
