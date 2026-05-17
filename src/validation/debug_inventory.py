"""
SurgeDPS Debug Inventory

Read-only filesystem inspector that reports per-storm cache state across
all artifact types managed by the persistent volume. Used by the private
/__val/inventory dashboard to answer:

  - Which storms are fully pre-warmed vs partial vs cold?
  - Which fell back to parametric rainfall (no IEM data)?
  - How much disk does each artifact category consume?
  - Are there orphaned cache entries (artifacts present, storm removed from catalog)?

No side effects. No fetches. Just walks the persistent directory tree
and matches files to catalog entries.

Output is shaped for the dashboard template; keep field names stable.
"""
from __future__ import annotations

import glob
import hashlib
import logging
import os
import shutil
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# MRMS cache-key reconstruction
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Mirrors the cache_token formula in scripts/api_server.py so the
# inventory can predict the MRMS TIF filename without having to scan
# every file in the mrms directory.


def _mrms_iem_cache_key(landfall_lat: float, landfall_lon: float,
                        landfall_date: str, duration_hr: int = 72,
                        buffer_deg: float = 4.0) -> Optional[str]:
    """Return the 12-char IEM MRMS cache key for a storm, or None on bad date.

    Matches the cache_token format used by /api/rainfall handler:
      cache_token = f"iem|{valid_time.isoformat()}|{duration_hr}|{bbox_str}"
      ck = md5(cache_token.encode()).hexdigest()[:12]
    """
    if not landfall_date:
        return None
    try:
        valid_time = datetime.strptime(landfall_date, '%Y-%m-%d').replace(
            hour=18, tzinfo=timezone.utc
        ) + timedelta(hours=48)
    except ValueError:
        return None

    # bbox follows storm_bbox_from_catalog_entry(landfall_lat, landfall_lon, buffer_deg)
    # which returns (lon_min, lat_min, lon_max, lat_max) as a 4-tuple, rounded.
    lon_min = round(landfall_lon - buffer_deg, 3)
    lon_max = round(landfall_lon + buffer_deg, 3)
    lat_min = round(landfall_lat - buffer_deg, 3)
    lat_max = round(landfall_lat + buffer_deg, 3)
    bbox_str = "_".join(f"{v:.3f}" for v in (lon_min, lat_min, lon_max, lat_max))
    cache_token = f"iem|{valid_time.isoformat()}|{duration_hr}|{bbox_str}"
    return hashlib.md5(cache_token.encode()).hexdigest()[:12]


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Filesystem inspectors (read-only, side-effect-free)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def _file_size_mb(path: Path) -> float:
    try:
        return round(path.stat().st_size / 1_048_576, 2)
    except (OSError, FileNotFoundError):
        return 0.0


def _dir_size_mb(path: Path) -> float:
    total = 0
    try:
        for f in path.rglob("*"):
            if f.is_file():
                total += f.stat().st_size
    except OSError:
        pass
    return round(total / 1_048_576, 2)


def _file_mtime_iso(path: Path) -> Optional[str]:
    try:
        return datetime.fromtimestamp(
            path.stat().st_mtime, tz=timezone.utc
        ).isoformat()
    except (OSError, FileNotFoundError):
        return None


def _inspect_cells(cells_dir: Path, storm_id: str) -> Dict[str, Any]:
    """Count cached cells for a storm and report freshness."""
    sdir = cells_dir / storm_id
    if not sdir.is_dir():
        return {"cached": 0, "size_mb": 0.0, "complete": False, "newest": None}

    damage_files = list(sdir.glob("cell_*_*_damage.geojson"))
    ticks_files = list(sdir.glob("cell_*_*_ticks.json"))
    newest = max((f.stat().st_mtime for f in damage_files), default=0)
    return {
        "cached": len(damage_files),
        "ticks_cached": len(ticks_files),
        "size_mb": _dir_size_mb(sdir),
        "complete": len(damage_files) >= 9,  # 3×3 grid = baseline pre-warm
        "newest": datetime.fromtimestamp(newest, tz=timezone.utc).isoformat() if newest else None,
    }


def _inspect_mrms(mrms_dir: Path, storm: Any) -> Dict[str, Any]:
    """Look up the expected IEM TIF for this storm and report its state."""
    landfall_date = getattr(storm, "landfall_date", None)
    if not landfall_date:
        return {"cached": False, "source": "n/a", "reason": "no_landfall_date"}

    ck = _mrms_iem_cache_key(
        landfall_lat=storm.landfall_lat,
        landfall_lon=storm.landfall_lon,
        landfall_date=landfall_date,
    )
    if ck is None:
        return {"cached": False, "source": "n/a", "reason": "bad_landfall_date"}

    iem_tif = mrms_dir / f"iem_{ck}.tif"
    parametric_tif = mrms_dir / f"parametric_{storm.storm_id}.tif"

    if iem_tif.exists():
        return {
            "cached": True,
            "source": "iem",
            "size_mb": _file_size_mb(iem_tif),
            "valid_time": _file_mtime_iso(iem_tif),
            "filename": iem_tif.name,
        }
    if parametric_tif.exists():
        return {
            "cached": True,
            "source": "parametric_fallback",
            "size_mb": _file_size_mb(parametric_tif),
            "valid_time": _file_mtime_iso(parametric_tif),
            "filename": parametric_tif.name,
            "reason": "iem_unavailable_or_fetch_failed",
        }
    return {"cached": False, "source": "none", "reason": "not_yet_warmed"}


def _inspect_qpf(qpf_dir: Path, storm_id: str) -> Dict[str, Any]:
    """WPC QPF is only meaningful for live/active storms; historic = N/A."""
    storm_qpf_dir = qpf_dir / storm_id
    if not storm_qpf_dir.is_dir():
        return {"cached": False, "reason": "historical_or_not_yet_warmed"}
    tifs = list(storm_qpf_dir.glob("*.tif"))
    return {
        "cached": len(tifs) > 0,
        "frame_count": len(tifs),
        "size_mb": _dir_size_mb(storm_qpf_dir),
        "newest": _file_mtime_iso(
            max(tifs, key=lambda p: p.stat().st_mtime)
        ) if tifs else None,
    }


def _inspect_gauges(persistent_dir: Path, storm_id: str) -> Dict[str, Any]:
    """AHPS gauge cache.

    fetch_historical_gauges writes ONE JSON per storm at
    ``<persistent>/cache/gauges_historical/<storm_id>.json`` containing a
    GeoJSON FeatureCollection with all the NWIS peaks for that event.
    Report cached=True only when the file exists AND ``_fetch_error`` is
    False (a flagged error means the NWIS call failed and the cached body
    is just a sentinel that should be retried).
    """
    cache_path = persistent_dir / "cache" / "gauges_historical" / f"{storm_id}.json"
    if not cache_path.is_file():
        return {"cached": False, "count": 0, "size_mb": 0.0}
    size_mb = _file_size_mb(cache_path)
    count = 0
    fetch_error = False
    try:
        import json
        with open(cache_path) as f:
            data = json.load(f)
        count = int(data.get("gauge_count", 0))
        fetch_error = bool(data.get("_fetch_error", False))
    except Exception:
        pass
    return {
        "cached": (count > 0) and not fetch_error,
        "count": count,
        "size_mb": size_mb,
        "fetch_error": fetch_error,
    }


def _inspect_flood_zones(persistent_dir: Path, storm: Any) -> Dict[str, Any]:
    """FEMA NFHL: cached in persistent/cache/flood_zones/ keyed by bbox.

    Pre-warm / local-seed uses radius=2°, tile=0.25°, snapped to 0.01°
    → 16×16 = 256 tiles per storm. Keep these constants in lockstep
    with scripts/warm_cache.py and scripts/seed_flood_zones_local.py.
    """
    EXPECTED = 256
    fz_dir = persistent_dir / "cache" / "flood_zones"
    if not fz_dir.is_dir():
        return {"cached": 0, "expected": EXPECTED, "complete": False}

    lat0 = storm.landfall_lat
    lon0 = storm.landfall_lon
    radius = 2.0
    tile = 0.25
    n = int(2 * radius / tile)  # 16
    matching = 0
    for row in range(n):
        for col in range(n):
            qw = round(lon0 - radius + col * tile, 2)
            qs = round(lat0 - radius + row * tile, 2)
            qe = round(qw + tile, 2)
            qn = round(qs + tile, 2)
            cache_name = f"fz_{qw:+.2f}_{qs:+.2f}_{qe:+.2f}_{qn:+.2f}.json"
            # Either .json (legacy warm_cache writes) or .json.gz
            # (seed-uploaded compressed tiles) counts as cached.
            if (fz_dir / cache_name).exists() or (fz_dir / (cache_name + ".gz")).exists():
                matching += 1
    return {"cached": matching, "expected": EXPECTED, "complete": matching >= EXPECTED}


def _inspect_compound(cells_dir: Path, storm_id: str) -> Dict[str, Any]:
    """Compound mosaic VRT/TIF for surge ∪ rainfall ∪ fluvial."""
    sdir = cells_dir / storm_id
    if not sdir.is_dir():
        return {"cached": False}
    mosaic_files = list(sdir.glob("compound_mosaic*"))
    cell_compound = list(sdir.glob("cell_*_*_compound.tif"))
    return {
        "cached": len(mosaic_files) > 0,
        "cell_compound_count": len(cell_compound),
        "size_mb": sum(_file_size_mb(p) for p in mosaic_files + cell_compound),
    }


def _storm_status(artifacts: Dict[str, Dict]) -> str:
    """Reduce per-artifact state to a single coarse status label."""
    cells = artifacts.get("cells", {})
    mrms = artifacts.get("mrms", {})

    if not cells.get("complete"):
        return "cold"
    if mrms.get("source") == "parametric_fallback":
        return "parametric_fallback"
    if not mrms.get("cached"):
        return "partial"
    fz = artifacts.get("flood_zones", {})
    if not fz.get("complete"):
        return "partial"
    return "fully_warmed"


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Public entry point
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def build_inventory() -> Dict[str, Any]:
    """Walk the persistent volume and return per-storm cache state.

    Result is JSON-serializable; safe to ship over the wire to the dashboard.
    """
    # Local imports so this module is cheap to import (no catalog scan on load)
    from storm_catalog.catalog import HISTORICAL_STORMS, fetch_active_storms
    from persistent_paths import (
        CELLS_DIR, MRMS_DIR, QPF_DIR, PERSISTENT_DATA_DIR, MONITOR_STATE_FILE,
    )

    started = time.time()

    storms_out: List[Dict[str, Any]] = []
    counts = {"fully_warmed": 0, "partial": 0, "cold": 0, "parametric_fallback": 0}

    for storm in HISTORICAL_STORMS:
        artifacts = {
            "cells": _inspect_cells(CELLS_DIR, storm.storm_id),
            "mrms": _inspect_mrms(MRMS_DIR, storm),
            "qpf": _inspect_qpf(QPF_DIR, storm.storm_id),
            "gauges": _inspect_gauges(PERSISTENT_DATA_DIR, storm.storm_id),
            "flood_zones": _inspect_flood_zones(PERSISTENT_DATA_DIR, storm),
            "compound": _inspect_compound(CELLS_DIR, storm.storm_id),
        }
        status = _storm_status(artifacts)
        counts[status] = counts.get(status, 0) + 1

        storms_out.append({
            "storm_id": storm.storm_id,
            "name": storm.name,
            "year": storm.year,
            "category": getattr(storm, "category", None),
            "dps_score": getattr(storm, "dps_score", None),
            "landfall_date": getattr(storm, "landfall_date", None),
            "landfall_lat": storm.landfall_lat,
            "landfall_lon": storm.landfall_lon,
            "status": status,
            "artifacts": artifacts,
        })

    # Sort: cold first (most actionable), then partial, then fully_warmed.
    status_rank = {"cold": 0, "parametric_fallback": 1, "partial": 2, "fully_warmed": 3}
    storms_out.sort(key=lambda s: (status_rank.get(s["status"], 9), s["name"]))

    # Active storms (live NHC). Cheap call — uses cached RSS if available.
    active = []
    try:
        for s in fetch_active_storms() or []:
            active.append({
                "storm_id": getattr(s, "storm_id", "unknown"),
                "name": getattr(s, "name", "unknown"),
                "max_wind_kt": getattr(s, "max_wind_kt", None),
                "min_pressure_mb": getattr(s, "min_pressure_mb", None),
                "advisory": getattr(s, "advisory", None),
            })
    except Exception as e:
        active = [{"error": f"fetch_active_storms failed: {e}"}]

    # Monitor state (last poll, last advisory processed).
    monitor_state = {}
    if MONITOR_STATE_FILE.exists():
        try:
            import json
            with open(MONITOR_STATE_FILE) as f:
                monitor_state = json.load(f)
        except Exception as e:
            monitor_state = {"error": str(e)}

    # Storage roll-up.
    storage = {
        "persistent_root": str(PERSISTENT_DATA_DIR),
        "cells_mb": _dir_size_mb(CELLS_DIR),
        "mrms_mb": _dir_size_mb(MRMS_DIR),
        "qpf_mb": _dir_size_mb(QPF_DIR),
        # gauges live at cache/gauges_historical/<storm_id>.json — NOT
        # at gauges/<storm_id>/*.json (that path was an earlier guess).
        "gauges_mb": _dir_size_mb(PERSISTENT_DATA_DIR / "cache" / "gauges_historical"),
        "flood_zones_mb": _dir_size_mb(PERSISTENT_DATA_DIR / "cache" / "flood_zones"),
    }
    storage["total_known_mb"] = round(sum(
        v for k, v in storage.items() if k.endswith("_mb") and isinstance(v, (int, float))
    ), 2)
    try:
        usage = shutil.disk_usage(str(PERSISTENT_DATA_DIR))
        storage["volume_total_mb"] = round(usage.total / 1_048_576, 2)
        storage["volume_free_mb"] = round(usage.free / 1_048_576, 2)
        storage["volume_used_pct"] = round((usage.used / usage.total) * 100, 1)
    except OSError:
        pass

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "took_ms": int((time.time() - started) * 1000),
        "counts": counts,
        "total_historic": len(storms_out),
        "active_storms": active,
        "monitor_state": monitor_state,
        "storage": storage,
        "storms": storms_out,
    }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# HTML dashboard renderer
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


_DASHBOARD_HTML = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="robots" content="noindex, nofollow" />
<title>SurgeDPS — Cache Inventory</title>
<style>
  :root {{
    --bg: #0b1220; --panel: #111a2e; --fg: #e6edf7;
    --muted: #8a96ad; --accent: #6cb2ff; --good: #5bd38a;
    --warn: #f3c969; --bad: #ff7b7b; --neutral: #8a96ad;
  }}
  * {{ box-sizing: border-box; }}
  html, body {{ background: var(--bg); color: var(--fg);
    font: 13px/1.45 -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    margin: 0; padding: 20px 24px;
  }}
  h1 {{ margin: 0 0 4px; font-size: 22px; }}
  .sub {{ color: var(--muted); font-size: 12px; margin-bottom: 18px; }}
  .summary-grid {{
    display: grid; grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
    gap: 10px; margin-bottom: 24px;
  }}
  .card {{
    background: var(--panel); border-radius: 8px;
    padding: 10px 14px; border: 1px solid #1f2a44;
  }}
  .card .k {{ font-size: 10px; color: var(--muted);
    text-transform: uppercase; letter-spacing: 0.06em; }}
  .card .v {{ font-size: 22px; font-weight: 600; margin-top: 2px;
    font-variant-numeric: tabular-nums; }}
  .card.good .v {{ color: var(--good); }}
  .card.warn .v {{ color: var(--warn); }}
  .card.bad  .v {{ color: var(--bad); }}
  h2 {{ font-size: 13px; color: var(--muted); text-transform: uppercase;
    letter-spacing: 0.08em; margin: 24px 0 10px; }}
  table {{ width: 100%; border-collapse: collapse; font-size: 12px; }}
  th, td {{ padding: 6px 8px; border-bottom: 1px solid #1c263f;
    text-align: left; }}
  th {{ color: var(--muted); font-weight: 600; text-transform: uppercase;
    letter-spacing: 0.05em; font-size: 10px;
    position: sticky; top: 0; background: var(--bg); }}
  tr:hover td {{ background: rgba(108, 178, 255, 0.05); }}
  td.num {{ text-align: right; font-variant-numeric: tabular-nums; }}
  .pill {{
    display: inline-block; padding: 2px 8px; border-radius: 999px;
    font-size: 10px; font-weight: 600; text-transform: uppercase;
    letter-spacing: 0.04em;
  }}
  .pill-fully_warmed {{ background: #16382a; color: var(--good); }}
  .pill-partial {{ background: #3a2f14; color: var(--warn); }}
  .pill-parametric_fallback {{ background: #2a2c4a; color: #ab8cff; }}
  .pill-cold {{ background: #3b1f1f; color: var(--bad); }}
  .dot-ok {{ color: var(--good); }}
  .dot-miss {{ color: var(--bad); }}
  .dot-na {{ color: var(--neutral); }}
  .filter-bar {{ display: flex; gap: 8px; margin-bottom: 10px; }}
  .filter-bar input {{ flex: 1; background: #0e1830; color: var(--fg);
    border: 1px solid #1f2a44; border-radius: 6px; padding: 6px 10px;
    font-size: 12px; }}
  .filter-bar select {{ background: #0e1830; color: var(--fg);
    border: 1px solid #1f2a44; border-radius: 6px; padding: 6px 8px;
    font-size: 12px; }}
  .raw-link {{ color: var(--accent); font-size: 11px; }}
</style>
</head>
<body>
<h1>SurgeDPS — Cache Inventory</h1>
<div class="sub">
  Generated <span id="generated_at"></span> ·
  <a class="raw-link" href="/__val/inventory.json?t={t}" target="_blank">Raw JSON</a> ·
  <a class="raw-link" href="javascript:location.reload()">Refresh</a>
</div>

<div class="summary-grid" id="summary"></div>

<h2>Active NHC storms</h2>
<div id="active"></div>

<h2>Historical storm cache</h2>
<div class="filter-bar">
  <input id="filter" placeholder="Filter by name…" />
  <select id="status_filter">
    <option value="">All statuses</option>
    <option value="cold">Cold only</option>
    <option value="partial">Partial only</option>
    <option value="parametric_fallback">Parametric fallback only</option>
    <option value="fully_warmed">Fully warmed only</option>
  </select>
</div>
<div style="max-height: 70vh; overflow: auto; border: 1px solid #1f2a44; border-radius: 6px;">
<table id="storms">
  <thead>
    <tr>
      <th>Status</th><th>Storm</th><th>Year</th><th>Cat</th><th>DPS</th>
      <th>Cells</th><th>Ticks</th><th>MRMS</th><th>QPF</th>
      <th>Gauges</th><th>FZ</th><th>Compound</th><th>Newest</th>
    </tr>
  </thead>
  <tbody></tbody>
</table>
</div>

<h2 style="margin-top: 24px;">Storage</h2>
<div id="storage" class="summary-grid"></div>

<script>
const TOKEN = {token_js};

async function load() {{
  const res = await fetch('/__val/inventory.json?t=' + encodeURIComponent(TOKEN));
  if (!res.ok) {{
    document.body.innerHTML = '<h1>Auth failed.</h1>';
    return;
  }}
  const data = await res.json();
  document.getElementById('generated_at').textContent =
    new Date(data.generated_at).toLocaleString() + ' (in ' + data.took_ms + 'ms)';

  // Summary cards
  const c = data.counts;
  document.getElementById('summary').innerHTML =
    cardHTML('Fully warmed', c.fully_warmed || 0, 'good') +
    cardHTML('Partial', c.partial || 0, 'warn') +
    cardHTML('Parametric fallback', c.parametric_fallback || 0, 'warn') +
    cardHTML('Cold', c.cold || 0, c.cold > 0 ? 'bad' : 'good') +
    cardHTML('Total historic', data.total_historic || 0, '');

  // Storage cards
  const s = data.storage || {{}};
  document.getElementById('storage').innerHTML =
    cardHTML('Cells', (s.cells_mb||0) + ' MB', '') +
    cardHTML('MRMS', (s.mrms_mb||0) + ' MB', '') +
    cardHTML('QPF', (s.qpf_mb||0) + ' MB', '') +
    cardHTML('Gauges', (s.gauges_mb||0) + ' MB', '') +
    cardHTML('Flood zones', (s.flood_zones_mb||0) + ' MB', '') +
    cardHTML('Volume used', (s.volume_used_pct||0) + '%',
             (s.volume_used_pct||0) > 80 ? 'bad' : (s.volume_used_pct||0) > 50 ? 'warn' : 'good');

  // Active storms
  const act = data.active_storms || [];
  if (act.length === 0) {{
    document.getElementById('active').innerHTML =
      '<div class="card"><div class="k">no active NHC storms</div></div>';
  }} else {{
    document.getElementById('active').innerHTML = act.map(a =>
      `<div class="card"><div class="k">${{escapeHTML(a.name || a.storm_id)}}</div>` +
      `<div class="v">${{a.max_wind_kt || '?'}}kt</div>` +
      `<div class="k">adv ${{a.advisory || '?'}}</div></div>`
    ).join('');
  }}

  // Storm table
  window._storms = data.storms;
  renderStorms();
}}

function cardHTML(label, value, cls) {{
  return `<div class="card ${{cls}}"><div class="k">${{label}}</div><div class="v">${{value}}</div></div>`;
}}

function escapeHTML(s) {{
  return String(s ?? '').replace(/[&<>"']/g, c => ({{
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
  }})[c]);
}}

function dot(state) {{
  if (state === 'ok') return '<span class="dot-ok">●</span>';
  if (state === 'na') return '<span class="dot-na">○</span>';
  return '<span class="dot-miss">○</span>';
}}

function renderStorms() {{
  const filter = (document.getElementById('filter').value || '').toLowerCase();
  const statusF = document.getElementById('status_filter').value;
  const rows = (window._storms || []).filter(s => {{
    if (filter && !s.name.toLowerCase().includes(filter)
        && !s.storm_id.toLowerCase().includes(filter)) return false;
    if (statusF && s.status !== statusF) return false;
    return true;
  }});
  const tbody = document.querySelector('#storms tbody');
  tbody.innerHTML = rows.map(s => {{
    const a = s.artifacts;
    const mrmsState = a.mrms.cached ? (a.mrms.source === 'iem' ? 'ok' : 'partial') : 'miss';
    const mrmsLabel = a.mrms.cached
      ? (a.mrms.source === 'iem' ? `IEM ${{a.mrms.size_mb||0}}MB` : `Param ${{a.mrms.size_mb||0}}MB`)
      : 'none';
    return `<tr>
      <td><span class="pill pill-${{s.status}}">${{s.status.replace(/_/g, ' ')}}</span></td>
      <td>${{escapeHTML(s.name)}}</td>
      <td class="num">${{s.year}}</td>
      <td class="num">${{s.category||''}}</td>
      <td class="num">${{s.dps_score ? s.dps_score.toFixed(1) : ''}}</td>
      <td class="num">${{a.cells.cached}}/9</td>
      <td class="num">${{a.cells.ticks_cached||0}}</td>
      <td>${{dot(mrmsState === 'ok' ? 'ok' : (mrmsState === 'partial' ? 'na' : 'miss'))}} ${{mrmsLabel}}</td>
      <td>${{dot(a.qpf.cached ? 'ok' : 'na')}} ${{a.qpf.cached ? a.qpf.frame_count+'f' : '—'}}</td>
      <td>${{dot(a.gauges.cached ? 'ok' : 'miss')}} ${{a.gauges.count||0}}</td>
      <td>${{dot(a.flood_zones.complete ? 'ok' : (a.flood_zones.cached>0 ? 'na' : 'miss'))}} ${{a.flood_zones.cached}}/${{a.flood_zones.expected}}</td>
      <td>${{dot(a.compound.cached ? 'ok' : 'miss')}}</td>
      <td>${{a.cells.newest ? new Date(a.cells.newest).toLocaleDateString() : '—'}}</td>
    </tr>`;
  }}).join('');
}}

document.getElementById('filter').addEventListener('input', renderStorms);
document.getElementById('status_filter').addEventListener('change', renderStorms);
load();
</script>
</body>
</html>"""


def render_dashboard(token: str) -> bytes:
    import json
    return _DASHBOARD_HTML.format(
        t=_escape(token), token_js=json.dumps(token)
    ).encode("utf-8")


def _escape(s) -> str:
    import html
    return html.escape(str(s), quote=True)
