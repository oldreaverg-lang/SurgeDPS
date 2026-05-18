"""
Seed the Railway flood-zone cache from a machine that can reach FEMA.

Background: FEMA's WAF blocks Railway's egress IP at the TLS handshake.
Phase 3 of warm_cache.py can't pull NFHL data from inside the container,
so we run this script from a developer laptop (which reaches FEMA fine)
and POST each tile to the token-gated /__val/seed_flood_zone endpoint.

Usage:
    # default: seed all 15 curated historical storms
    python scripts/seed_flood_zones_local.py

    # narrow to a single storm during testing
    python scripts/seed_flood_zones_local.py --storm harvey_2017

    # force re-fetch even if Railway already has the tile cached
    python scripts/seed_flood_zones_local.py --force

The script mirrors warm_cache.py's tile grid (4x4 grid of 2 deg tiles
around landfall) so the resulting cache keys match what api_server.py
expects for /api/flood_zones.
"""

from __future__ import annotations

import argparse
import gzip
import io
import json
import os
import sys
import time
import urllib.parse
import urllib.request
from typing import Iterable

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(BASE_DIR, "src"))

from storm_catalog.catalog import HISTORICAL_STORMS  # noqa: E402

# Must match scripts/warm_cache.py and src/validation/debug_inventory.py
# so cache keys + expected-count math align.
_FEMA_URL = "https://hazards.fema.gov/arcgis/rest/services/public/NFHL/MapServer/28/query"
_FZ_TILE_DEG = 0.25
_FZ_RADIUS_DEG = 2.0
_FZ_TIMEOUT = 60
_RAILWAY_BASE = os.environ.get(
    "SURGEDPS_BASE", "https://surgedps-production.up.railway.app"
)
_TOKEN = os.environ.get("VALIDATION_TOKEN", "")


def _fema_url(qw: float, qs: float, qe: float, qn: float) -> str:
    envelope = json.dumps(
        {
            "xmin": qw, "ymin": qs, "xmax": qe, "ymax": qn,
            "spatialReference": {"wkid": 4326},
        }
    )
    qs_str = urllib.parse.urlencode(
        {
            "where": "1=1",
            "geometry": envelope,
            "geometryType": "esriGeometryEnvelope",
            "inSR": "4326",
            "outSR": "4326",
            "spatialRel": "esriSpatialRelIntersects",
            # FLOODWAY no longer exists in FEMA's MapServer/28; requesting
            # it 400s the whole query. FLD_ZONE is what the frontend styles
            # by, so that's all we need.
            "outFields": "FLD_ZONE",
            "returnGeometry": "true",
            "resultRecordCount": "2000",
            "f": "geojson",
        }
    )
    return f"{_FEMA_URL}?{qs_str}"


def _fetch_fema(url: str) -> bytes:
    """Fetch a single FEMA NFHL tile. Returns raw GeoJSON bytes.

    FEMA's MapServer/28 throttles aggressively when our request rate
    spikes — the failure mode is HTTP 200 + ``{'error': {'code': 400,
    'message': 'Failed to execute query.'}}``. Once the rate-limit
    counter is tripped, every subsequent request returns the same body
    until ~60 seconds of quiet has passed.

    Strategy: 3 attempts with long backoffs (15s, 45s). After the third
    failure, give up on this tile and let the caller record it for a
    later retry run. Hammering more doesn't unstick the rate limiter —
    only quiet time does.
    """
    backoffs = [15.0, 45.0]
    last_err: str = ""
    for attempt in range(3):
        req = urllib.request.Request(
            url, headers={"User-Agent": "SurgeDPS-seed/1.0 (+local)"}
        )
        try:
            with urllib.request.urlopen(req, timeout=_FZ_TIMEOUT) as resp:
                raw = resp.read()
        except Exception as e:
            last_err = f"http: {type(e).__name__}: {e}"
            if attempt < len(backoffs):
                time.sleep(backoffs[attempt])
                continue
            raise RuntimeError(last_err) from None
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError as e:
            last_err = f"non-JSON body: {e}; first 120 bytes: {raw[:120]!r}"
            if attempt < len(backoffs):
                time.sleep(backoffs[attempt])
                continue
            raise RuntimeError(last_err) from None
        if isinstance(parsed, dict) and "error" in parsed:
            last_err = f"arcgis error: {parsed['error']}"
            if attempt < len(backoffs):
                time.sleep(backoffs[attempt])
                continue
            raise RuntimeError(last_err)
        return raw
    raise RuntimeError(last_err or "unreachable")


def _upload_to_railway(tile_key: str, raw: bytes) -> dict:
    """POST one tile to /__val/seed_flood_zone, sending gzipped body.

    The endpoint accepts gzip via the Content-Encoding header and writes
    the compressed bytes straight to a .json.gz file on the volume. At
    ±2° / 0.25° tiles, raw payloads run 10-35 MB each (~55 GB across
    15 storms); gzip shrinks them ~5× to fit comfortably in the volume.
    """
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb", compresslevel=6) as gz:
        gz.write(raw)
    gz_body = buf.getvalue()

    url = (
        f"{_RAILWAY_BASE}/__val/seed_flood_zone"
        f"?t={urllib.parse.quote(_TOKEN)}"
        f"&tile_key={urllib.parse.quote(tile_key)}"
    )
    req = urllib.request.Request(
        url, data=gz_body, method="POST",
        headers={
            "Content-Type": "application/json",
            "Content-Encoding": "gzip",
            "Content-Length": str(len(gz_body)),
            "User-Agent": "SurgeDPS-seed/1.0",
        },
    )
    with urllib.request.urlopen(req, timeout=120) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _railway_has_tile(tile_key: str) -> bool:
    """Cheap check: hit the public /api/flood_zones with the tile's
    bbox and see if the response is small (cache hit) or large.

    We don't have a clean 'does this cached file exist' endpoint, so we
    just always re-fetch unless --force is off and this returns False
    (placeholder for a future stat endpoint).
    """
    return False  # always re-fetch; cheap enough at 15 storms x 16 tiles


def _build_tile_grid(storm) -> Iterable[tuple]:
    """Yield (qw, qs, qe, qn, tile_key) for the 16 tiles around a storm
    landfall — exact same math as warm_cache.py Phase 3.
    """
    lat0 = storm.landfall_lat
    lon0 = storm.landfall_lon
    n_cols = n_rows = int(2 * _FZ_RADIUS_DEG / _FZ_TILE_DEG)
    for row in range(n_rows):
        for col in range(n_cols):
            raw_w = lon0 - _FZ_RADIUS_DEG + col * _FZ_TILE_DEG
            raw_s = lat0 - _FZ_RADIUS_DEG + row * _FZ_TILE_DEG
            raw_e = raw_w + _FZ_TILE_DEG
            raw_n = raw_s + _FZ_TILE_DEG
            qw = round(raw_w, 2)
            qs = round(raw_s, 2)
            qe = round(raw_e, 2)
            qn = round(raw_n, 2)
            tile_key = f"fz_{qw:+.2f}_{qs:+.2f}_{qe:+.2f}_{qn:+.2f}.json"
            yield qw, qs, qe, qn, tile_key


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--storm", help="seed a single storm_id (default: all 15)")
    parser.add_argument(
        "--force", action="store_true",
        help="re-upload even if --skip-cached would have skipped",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="fetch from FEMA but don't upload",
    )
    parser.add_argument(
        "--retry-from", default=None,
        help="path to a previous run's failed-tiles file; only retry those",
    )
    parser.add_argument(
        "--failed-out", default="seed_failed.txt",
        help="write failed (storm,tile_key) pairs to this file for later retry",
    )
    args = parser.parse_args()

    if not _TOKEN:
        print(
            "ERROR: set VALIDATION_TOKEN env var "
            "(same value as Railway's VALIDATION_TOKEN)"
        )
        return 2

    storms = [s for s in HISTORICAL_STORMS if (not args.storm or s.storm_id == args.storm)]
    if not storms:
        print(f"ERROR: no storm matched --storm={args.storm}")
        return 2

    # Load retry filter if provided. File format: "storm_id<TAB>tile_key" lines.
    retry_filter: set | None = None
    if args.retry_from:
        retry_filter = set()
        try:
            for line in open(args.retry_from, encoding="utf-8"):
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                parts = line.split("\t")
                if len(parts) == 2:
                    retry_filter.add((parts[0], parts[1]))
            print(f"Retry mode: only attempting {len(retry_filter)} tiles from {args.retry_from}")
        except FileNotFoundError:
            print(f"ERROR: --retry-from file not found: {args.retry_from}")
            return 2

    print(f"Seeding {len(storms)} storm(s). Base: {_RAILWAY_BASE}")
    print("=" * 60)

    total_fetched = total_uploaded = total_failed = 0
    total_features = 0
    total_bytes = 0
    failed_lines: list[str] = []

    for storm in storms:
        print(f"\n[{storm.storm_id}]  landfall ({storm.landfall_lat:.2f}, "
              f"{storm.landfall_lon:.2f})")
        s_ok = s_skip = s_fail = 0
        for qw, qs, qe, qn, tile_key in _build_tile_grid(storm):
            if retry_filter is not None and (storm.storm_id, tile_key) not in retry_filter:
                s_skip += 1
                continue
            if not args.force and _railway_has_tile(tile_key):
                s_skip += 1
                continue
            t0 = time.time()
            try:
                raw = _fetch_fema(_fema_url(qw, qs, qe, qn))
            except Exception as e:
                print(f"    {tile_key} — FEMA fetch failed: {e}")
                s_fail += 1
                total_failed += 1
                failed_lines.append(f"{storm.storm_id}\t{tile_key}")
                # Long cooldown after consecutive failures to let FEMA's
                # rate-limit counter drain.
                time.sleep(15.0)
                continue
            total_fetched += 1
            try:
                parsed = json.loads(raw)
                fc = len(parsed.get("features") or [])
            except Exception:
                fc = -1
            total_features += max(fc, 0)
            total_bytes += len(raw)

            if args.dry_run:
                print(f"    {tile_key} — fetched {len(raw):>8d}B "
                      f"({fc:>5d} feat) [dry-run]")
                # Maintain pacing in dry-run too so we don't blast FEMA
                # and trigger 400s on the subsequent live run.
                time.sleep(1.5)
                continue

            try:
                resp = _upload_to_railway(tile_key, raw)
            except Exception as e:
                print(f"    {tile_key} — upload failed: {e}")
                s_fail += 1
                total_failed += 1
                failed_lines.append(f"{storm.storm_id}\t{tile_key}")
                continue

            if not resp.get("ok"):
                print(f"    {tile_key} — upload rejected: {resp}")
                s_fail += 1
                total_failed += 1
                continue

            elapsed = time.time() - t0
            print(f"    {tile_key} — uploaded {resp['bytes_written']:>8d}B "
                  f"({resp['feature_count']:>5d} feat, {elapsed:.1f}s)")
            s_ok += 1
            total_uploaded += 1

            # Pacing — empirically FEMA's NFHL service starts returning
            # "Failed to execute query" 400s when requests arrive faster
            # than ~1/sec. 1.5s keeps us comfortably under the threshold.
            time.sleep(1.5)

        print(f"  Storm summary: {s_ok} uploaded · {s_skip} skipped · {s_fail} failed")

    print()
    print("=" * 60)
    print(f"TOTAL: {total_fetched} fetched · {total_uploaded} uploaded · "
          f"{total_failed} failed")
    print(f"       {total_features} features, {total_bytes/1024/1024:.1f} MB")

    if failed_lines:
        try:
            with open(args.failed_out, "w", encoding="utf-8") as f:
                f.write("# storm_id<TAB>tile_key — re-run with --retry-from <this file>\n")
                f.write("\n".join(failed_lines) + "\n")
            print(f"Wrote {len(failed_lines)} failed tiles to {args.failed_out}")
        except OSError as e:
            print(f"WARN: could not write failed-tiles list: {e}")

    return 0 if total_failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
