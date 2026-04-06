#!/usr/bin/env python3
"""
Local development runner for the Storm Detector.

Usage:
    python scripts/run_detector.py              # Dry run against live NHC feeds
    python scripts/run_detector.py --basins at,ep   # Monitor Atlantic + East Pacific
    DRY_RUN=false python scripts/run_detector.py    # Actually write to AWS (careful!)
"""

import argparse
import os
import sys

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from storm_detector.handler import main

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run storm detector locally")
    parser.add_argument(
        "--basins",
        default="at",
        help="Comma-separated basin codes (default: at)",
    )
    parser.add_argument(
        "--live",
        action="store_true",
        help="Disable dry-run mode (writes to AWS)",
    )
    args = parser.parse_args()

    os.environ["ACTIVE_BASINS"] = args.basins
    if not args.live:
        os.environ["DRY_RUN"] = "true"

    main()
