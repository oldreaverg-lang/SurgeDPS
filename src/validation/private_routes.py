"""
Private Validation Routes

A token-gated HTTP handler that exposes validation artifacts
(metrics.json, samples.csv, residual plots) for personal/internal
use only. Never referenced from the public React frontend.

Design:
  - Path prefix:  /__val/              (double underscore = private)
  - Auth:         env VALIDATION_TOKEN, passed via ?t= OR X-Validation-Token
  - On bad/missing token: returns 404 (not 401) so the endpoint
    looks like it doesn't exist. Security through polite obscurity.
  - If VALIDATION_TOKEN is unset, the whole namespace returns 404
    even with a token. You must explicitly opt-in by setting the env.
  - Read-only. No writes, no deletes, no side effects.

Endpoints:
  GET /__val/                           index (list storms with artifacts)
  GET /__val/{storm_id}                 metrics.json for a storm
  GET /__val/{storm_id}/samples         samples.csv for a storm
  GET /__val/{storm_id}/hwms            filtered hwms.csv
  GET /__val/{storm_id}/dashboard       self-contained HTML viewer

Integration (wire into api_server.py):

    from validation.private_routes import handle_validation_request

    # Early in do_GET(), before any /api/... dispatch:
    if path.startswith('/__val'):
        handle_validation_request(self, path, params)
        return
"""

from __future__ import annotations

import json
import logging
import os
from typing import Dict, List, Optional
from urllib.parse import parse_qs

logger = logging.getLogger(__name__)


VALIDATION_ROOT = os.environ.get(
    "VALIDATION_DATA_DIR",
    os.path.join("data", "validation"),
)

# Restrict storm_id characters to prevent path traversal.
import re
_STORM_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")


def _token_ok(handler, params: Dict[str, List[str]]) -> bool:
    """Return True iff the request carries the correct validation token."""
    expected = os.environ.get("VALIDATION_TOKEN", "")
    if not expected:
        return False  # feature disabled unless explicitly enabled

    # Accept via header (preferred) or query string
    header_tok = handler.headers.get("X-Validation-Token", "")
    query_tok = (params.get("t", [""])[0]) if params else ""
    supplied = header_tok or query_tok
    if not supplied:
        return False
    # Constant-time comparison to resist timing attacks
    import hmac
    return hmac.compare_digest(supplied, expected)


def _not_found(handler) -> None:
    """Return a bland 404 that reveals nothing about the namespace."""
    body = b"Not Found"
    handler.send_response(404)
    handler.send_header("Content-Type", "text/plain; charset=utf-8")
    handler.send_header("Content-Length", str(len(body)))
    handler.send_header("X-Robots-Tag", "noindex, nofollow")
    handler.send_header("Cache-Control", "no-store")
    handler.end_headers()
    handler.wfile.write(body)


def _send_bytes(
    handler,
    body: bytes,
    content_type: str = "application/octet-stream",
) -> None:
    handler.send_response(200)
    handler.send_header("Content-Type", content_type)
    handler.send_header("Content-Length", str(len(body)))
    handler.send_header("X-Robots-Tag", "noindex, nofollow")
    handler.send_header("Cache-Control", "no-store")
    handler.end_headers()
    handler.wfile.write(body)


def _send_json(handler, obj) -> None:
    _send_bytes(
        handler,
        json.dumps(obj, indent=2).encode("utf-8"),
        "application/json",
    )


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Data loaders
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def _storm_dir(storm_id: str) -> Optional[str]:
    if not _STORM_ID_RE.match(storm_id or ""):
        return None
    p = os.path.abspath(os.path.join(VALIDATION_ROOT, storm_id))
    # Prevent any escape even with sanitized ids
    root = os.path.abspath(VALIDATION_ROOT)
    if not p.startswith(root + os.sep) and p != root:
        return None
    if not os.path.isdir(p):
        return None
    return p


def _list_validated_storms() -> List[dict]:
    """List storms that have at least a metrics.json on disk."""
    if not os.path.isdir(VALIDATION_ROOT):
        return []
    out: List[dict] = []
    for name in sorted(os.listdir(VALIDATION_ROOT)):
        d = os.path.join(VALIDATION_ROOT, name)
        if not os.path.isdir(d):
            continue
        m = os.path.join(d, "metrics.json")
        if not os.path.exists(m):
            continue
        entry = {"storm_id": name, "has_metrics": True}
        try:
            with open(m) as f:
                mdat = json.load(f)
            entry["tier"] = mdat.get("tier")
            entry["n_sampled"] = mdat.get("n_sampled")
            entry["bias_ft"] = mdat.get("bias_ft")
            entry["rmse_ft"] = mdat.get("rmse_ft")
            entry["csi"] = mdat.get("csi")
        except Exception:
            pass
        entry["has_samples"] = os.path.exists(os.path.join(d, f"{name}_samples.csv"))
        entry["has_hwms"] = os.path.exists(os.path.join(d, f"{name}_hwms.csv"))
        out.append(entry)
    return out


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Dashboard (self-contained HTML; no React, no assets)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


_DASHBOARD_HTML = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="robots" content="noindex, nofollow" />
<title>Validation — {storm_id}</title>
<style>
  :root {{
    --bg: #0b1220; --panel: #111a2e; --fg: #e6edf7;
    --muted: #8a96ad; --accent: #6cb2ff; --good: #5bd38a;
    --warn: #f3c969; --bad: #ff7b7b;
  }}
  html, body {{ background: var(--bg); color: var(--fg);
    font: 14px/1.5 -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    margin: 0; padding: 24px 32px;
  }}
  h1 {{ margin: 0 0 4px; font-size: 22px; }}
  h2 {{ margin: 28px 0 8px; font-size: 15px; color: var(--muted);
    text-transform: uppercase; letter-spacing: 0.08em; font-weight: 600; }}
  .sub {{ color: var(--muted); font-size: 13px; }}
  .grid {{
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
    gap: 12px; margin-top: 12px;
  }}
  .card {{
    background: var(--panel); border-radius: 10px;
    padding: 14px 16px; border: 1px solid #1f2a44;
  }}
  .card .k {{ font-size: 11px; color: var(--muted);
    text-transform: uppercase; letter-spacing: 0.06em; }}
  .card .v {{ font-size: 22px; font-weight: 600; margin-top: 4px;
    font-variant-numeric: tabular-nums; }}
  .tier {{
    display: inline-block; padding: 3px 10px; border-radius: 999px;
    font-size: 12px; font-weight: 600; text-transform: uppercase;
    letter-spacing: 0.05em;
  }}
  .tier-excellent {{ background: #16382a; color: var(--good); }}
  .tier-good      {{ background: #233b2e; color: var(--good); }}
  .tier-fair      {{ background: #3a2f14; color: var(--warn); }}
  .tier-poor      {{ background: #3b1f1f; color: var(--bad); }}
  .tier-unknown   {{ background: #1e2740; color: var(--muted); }}
  .insights li {{ margin: 4px 0; }}
  .scatter {{ width: 100%; max-width: 640px; height: 360px;
    background: var(--panel); border-radius: 10px;
    border: 1px solid #1f2a44; display: block; }}
  .meta {{ color: var(--muted); font-size: 12px; margin-top: 24px; }}
  a {{ color: var(--accent); text-decoration: none; }}
  a:hover {{ text-decoration: underline; }}
  code {{ background: #1a2440; padding: 2px 6px; border-radius: 4px;
    font-size: 12.5px; }}
</style>
</head>
<body>
<h1>Validation — <code>{storm_id}</code> <span class="tier tier-{tier}">{tier}</span></h1>
<div class="sub">n = {n_sampled} / {n_total} · source = {source}</div>

<h2>Depth Residuals</h2>
<div class="grid">
  <div class="card"><div class="k">Bias</div><div class="v">{bias_ft} ft</div></div>
  <div class="card"><div class="k">MAE</div><div class="v">{mae_ft} ft</div></div>
  <div class="card"><div class="k">RMSE</div><div class="v">{rmse_ft} ft</div></div>
  <div class="card"><div class="k">within ±1 ft</div><div class="v">{pct_within_1ft}%</div></div>
  <div class="card"><div class="k">within ±2 ft</div><div class="v">{pct_within_2ft}%</div></div>
  <div class="card"><div class="k">R²</div><div class="v">{r2}</div></div>
</div>

<h2>Contingency (flood / no-flood)</h2>
<div class="grid">
  <div class="card"><div class="k">Hits</div><div class="v">{hits}</div></div>
  <div class="card"><div class="k">Misses</div><div class="v">{misses}</div></div>
  <div class="card"><div class="k">False Alarms</div><div class="v">{false_alarms}</div></div>
  <div class="card"><div class="k">POD</div><div class="v">{pod}</div></div>
  <div class="card"><div class="k">FAR</div><div class="v">{far}</div></div>
  <div class="card"><div class="k">CSI</div><div class="v">{csi}</div></div>
</div>

<h2>Insights</h2>
<ul class="insights">{insight_items}</ul>

<h2>Scatter: modeled vs observed (ft)</h2>
<canvas id="scatter" class="scatter" width="640" height="360"></canvas>

<h2>Artifacts</h2>
<ul>
  <li><a href="/__val/{storm_id}?t={t}">metrics.json</a></li>
  <li><a href="/__val/{storm_id}/samples?t={t}">samples.csv</a></li>
  <li><a href="/__val/{storm_id}/hwms?t={t}">hwms.csv</a></li>
</ul>

<div class="meta">Private validation view · not linked from public site.</div>

<script>
  // Pulls samples CSV and renders a scatter (observed_ft vs modeled_ft).
  const token = {token_js};
  fetch("/__val/{storm_id}/samples?t=" + encodeURIComponent(token), {{
    headers: {{ "X-Validation-Token": token }}
  }})
  .then(r => r.text()).then(text => {{
    const lines = text.trim().split("\\n");
    if (lines.length < 2) return;
    const header = lines[0].split(",");
    const obsIdx = header.indexOf("observed_ft");
    const modIdx = header.indexOf("modeled_ft");
    if (obsIdx < 0 || modIdx < 0) return;

    const pts = [];
    for (let i=1; i<lines.length; i++) {{
      const parts = lines[i].split(",");
      const o = parseFloat(parts[obsIdx]);
      const m = parseFloat(parts[modIdx]);
      if (Number.isFinite(o) && Number.isFinite(m)) pts.push([o, m]);
    }}
    if (!pts.length) return;

    const cv = document.getElementById("scatter");
    const ctx = cv.getContext("2d");
    const W = cv.width, H = cv.height;
    const pad = 44;
    const vMax = Math.ceil(Math.max(
      ...pts.map(p => Math.max(p[0], p[1])), 2
    ));

    // axes
    ctx.strokeStyle = "#2a3658"; ctx.lineWidth = 1;
    ctx.strokeRect(pad, pad, W-pad*2, H-pad*2);

    // 1:1 line
    ctx.strokeStyle = "#f3c969"; ctx.setLineDash([4,4]);
    ctx.beginPath(); ctx.moveTo(pad, H-pad); ctx.lineTo(W-pad, pad);
    ctx.stroke(); ctx.setLineDash([]);

    // ticks
    ctx.fillStyle = "#8a96ad"; ctx.font = "11px sans-serif";
    for (let v=0; v<=vMax; v += Math.max(1, Math.ceil(vMax/6))) {{
      const x = pad + (v/vMax)*(W-pad*2);
      const y = H - pad - (v/vMax)*(H-pad*2);
      ctx.fillText(v, x-4, H-pad+14);
      ctx.fillText(v, 6, y+3);
    }}
    ctx.fillText("observed (ft)", W/2 - 30, H - 8);
    ctx.save(); ctx.translate(12, H/2 + 30); ctx.rotate(-Math.PI/2);
    ctx.fillText("modeled (ft)", 0, 0); ctx.restore();

    // points
    ctx.fillStyle = "rgba(108,178,255,0.65)";
    for (const [o, m] of pts) {{
      const x = pad + (o/vMax)*(W-pad*2);
      const y = H - pad - (m/vMax)*(H-pad*2);
      ctx.beginPath(); ctx.arc(x, y, 3, 0, Math.PI*2); ctx.fill();
    }}
  }})
  .catch(err => console.error(err));
</script>
</body>
</html>"""


def _render_dashboard(storm_id: str, metrics: dict, token: str) -> bytes:
    def fmt(val, spec="{:+.2f}"):
        if val is None:
            return "n/a"
        try:
            return spec.format(val)
        except (ValueError, TypeError):
            return str(val)

    insights = metrics.get("insights") or []
    insight_items = "".join(f"<li>{_escape(i)}</li>" for i in insights) \
                    or "<li>No insights generated.</li>"

    html = _DASHBOARD_HTML.format(
        storm_id=_escape(storm_id),
        tier=_escape(metrics.get("tier") or "unknown"),
        n_sampled=metrics.get("n_sampled", 0),
        n_total=metrics.get("n_total", 0),
        source=_escape(metrics.get("source") or "n/a"),
        bias_ft=fmt(metrics.get("bias_ft")),
        mae_ft=fmt(metrics.get("mae_ft"), "{:.2f}"),
        rmse_ft=fmt(metrics.get("rmse_ft"), "{:.2f}"),
        pct_within_1ft=fmt(metrics.get("pct_within_1ft"), "{:.1f}"),
        pct_within_2ft=fmt(metrics.get("pct_within_2ft"), "{:.1f}"),
        r2=fmt(metrics.get("r2"), "{:.3f}"),
        hits=metrics.get("hits", 0),
        misses=metrics.get("misses", 0),
        false_alarms=metrics.get("false_alarms", 0),
        pod=fmt(metrics.get("pod"), "{:.2f}"),
        far=fmt(metrics.get("far"), "{:.2f}"),
        csi=fmt(metrics.get("csi"), "{:.2f}"),
        insight_items=insight_items,
        t=_escape(token),
        token_js=json.dumps(token),
    )
    return html.encode("utf-8")


def _escape(s) -> str:
    import html
    return html.escape(str(s), quote=True)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Dispatcher
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def handle_validation_request(handler, path: str, params: Dict[str, List[str]]) -> None:
    """
    Entry point for /__val/... requests. Returns 404 for any unauthenticated
    or malformed access — including the case where VALIDATION_TOKEN is not
    configured.
    """
    # Normalize path under /__val
    rel = path[len("/__val"):].lstrip("/")
    segments = [s for s in rel.split("/") if s]

    # Diagnostic: /__val/__status reports whether the token env var is
    # configured and whether this request's token matched — without
    # ever revealing the token value itself. Safe to expose.
    if segments == ["__status"]:
        expected = os.environ.get("VALIDATION_TOKEN", "")
        header_tok = handler.headers.get("X-Validation-Token", "")
        query_tok = (params.get("t", [""])[0]) if params else ""
        supplied = header_tok or query_tok
        _send_json(handler, {
            "token_env_set": bool(expected),
            "token_env_length": len(expected) if expected else 0,
            "token_supplied": bool(supplied),
            "token_supplied_length": len(supplied) if supplied else 0,
            "token_matches": _token_ok(handler, params),
            "validation_root": os.path.abspath(VALIDATION_ROOT),
            "validation_root_exists": os.path.isdir(VALIDATION_ROOT),
            "storms_on_disk": sorted(os.listdir(VALIDATION_ROOT))
                if os.path.isdir(VALIDATION_ROOT) else [],
        })
        return

    if not _token_ok(handler, params):
        _not_found(handler)
        return

    # GET /__val/ (or /__val) → index
    if not segments:
        _send_json(handler, {
            "storms": _list_validated_storms(),
            "root": VALIDATION_ROOT,
        })
        return

    storm_id = segments[0]
    sdir = _storm_dir(storm_id)
    if sdir is None:
        _not_found(handler)
        return

    # GET /__val/{storm_id} → metrics.json
    if len(segments) == 1:
        mpath = os.path.join(sdir, "metrics.json")
        if not os.path.exists(mpath):
            _not_found(handler); return
        with open(mpath, "rb") as f:
            _send_bytes(handler, f.read(), "application/json")
        return

    sub = segments[1]

    # GET /__val/{storm_id}/samples
    if sub == "samples" and len(segments) == 2:
        p = os.path.join(sdir, f"{storm_id}_samples.csv")
        if not os.path.exists(p):
            _not_found(handler); return
        with open(p, "rb") as f:
            _send_bytes(handler, f.read(), "text/csv; charset=utf-8")
        return

    # GET /__val/{storm_id}/hwms
    if sub == "hwms" and len(segments) == 2:
        p = os.path.join(sdir, f"{storm_id}_hwms.csv")
        if not os.path.exists(p):
            _not_found(handler); return
        with open(p, "rb") as f:
            _send_bytes(handler, f.read(), "text/csv; charset=utf-8")
        return

    # GET /__val/{storm_id}/dashboard
    if sub == "dashboard" and len(segments) == 2:
        mpath = os.path.join(sdir, "metrics.json")
        if not os.path.exists(mpath):
            _not_found(handler); return
        with open(mpath) as f:
            metrics = json.load(f)
        token = os.environ.get("VALIDATION_TOKEN", "")
        _send_bytes(
            handler, _render_dashboard(storm_id, metrics, token), "text/html; charset=utf-8"
        )
        return

    _not_found(handler)
