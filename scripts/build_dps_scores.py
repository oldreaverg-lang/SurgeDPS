#!/usr/bin/env python3
"""
Regenerate data/dps_scores.json from StormDPS's compiled bundle.

dps_scores.json is a flattened snapshot of StormDPS's canonical per-storm
DPS (api_server._inject_dps reads it by storm id, then by "name_year").
It drifts every time StormDPS re-bakes (2026-07-10 review: SurgeDPS showed
Ike 86.5 while StormDPS's hero said 88.9) — rerun this after any StormDPS
bake that changes scores, then update the hardcoded dps_score= values in
src/storm_catalog/catalog.py HISTORICAL_STORMS (this script prints the
diff table for those).

Key forms emitted per storm (matching what _inject_dps looks up):
  - the bundle key lowercased      ("AL092008" -> "al092008", SIDs as-is)
  - "<name lower>_<year>"          ("ike_2008"; raw-id names stay raw)

Usage:
    python scripts/build_dps_scores.py \
        --bundle "C:/Users/Ryan/APPS/StormDPS-recovered/frontend/compiled_bundle.json"
"""
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "data" / "dps_scores.json"
CATALOG = ROOT / "src" / "storm_catalog" / "catalog.py"


def build(bundle_path: Path) -> dict[str, float]:
    storms = json.loads(bundle_path.read_text(encoding="utf-8"))["storms"]
    out: dict[str, float] = {}
    for key, s in sorted(storms.items()):
        dps = s.get("dps")
        if dps is None:
            continue
        score = round(float(dps), 1)
        out[key.lower()] = score
        name = str(s.get("name") or "").strip().lower()
        year = s.get("year")
        if name and year:
            out[f"{name}_{year}"] = score
    return out


def curated_diff(scores: dict[str, float]) -> list[tuple[str, float, float]]:
    """(storm_id, old_hardcoded, new_canonical) for HISTORICAL_STORMS."""
    src = CATALOG.read_text(encoding="utf-8")
    rows = []
    # entries look like: storm_id="ike_2008", ... dps_score=86.5
    for m in re.finditer(
            r'storm_id="([a-z0-9_-]+)"(.{0,600}?)dps_score=([\d.]+)',
            src, re.DOTALL):
        sid, _, old = m.group(1), m.group(2), float(m.group(3))
        new = scores.get(sid)
        if new is not None and abs(new - old) >= 0.05:
            rows.append((sid, old, new))
    return rows


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--bundle", required=True,
                    help="path to StormDPS frontend/compiled_bundle.json")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    scores = build(Path(args.bundle))
    old = json.loads(OUT.read_text(encoding="utf-8")) if OUT.exists() else {}
    changed = {k: (old.get(k), v) for k, v in scores.items()
               if old.get(k) != v}
    removed = [k for k in old if k not in scores]
    print(f"{len(scores)} keys ({len(changed)} changed, {len(removed)} stale removed)")

    print("\nHISTORICAL_STORMS (catalog.py) hardcoded values needing update:")
    for sid, o, n in curated_diff(scores):
        print(f"  {sid:<18} {o:>6} -> {n}")

    if args.dry_run:
        print("\n[dry-run] nothing written")
        return 0
    OUT.write_text(json.dumps(scores, indent=1, sort_keys=True), encoding="utf-8")
    print(f"\nwrote {OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
