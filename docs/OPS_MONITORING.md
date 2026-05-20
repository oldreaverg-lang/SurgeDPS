# SurgeDPS / StormDPS — uptime monitoring runbook

Configure these checks in UptimeRobot, Better Stack, Cronitor, or whatever
external monitor you prefer. Two HTTP services to watch (one per Railway
project) plus one optional deep check that requires a token.

## Why this exists

Tonight's Railway outage took ~30 min to diagnose because the only signal
was `curl` returning 404 from a console. With these checks in place, your
phone would have pinged the moment the container went unhealthy.

---

## Service A — SurgeDPS API (Railway: surgedps-production)

### Quick check — TCP/HTTP reachability

| Field | Value |
|---|---|
| URL | `https://surgedps-production.up.railway.app/api/health` |
| Method | GET |
| Interval | 60 s |
| Expected status | 200 |
| Expected body contains | `"status": "ok"` |
| Alert after | 2 consecutive failures (avoid blip-alerts during deploys) |

### Deep check — service is fully warmed

| Field | Value |
|---|---|
| URL | `https://surgedps-production.up.railway.app/api/health` |
| Method | GET |
| Interval | 5 min |
| Expected JSON | `"warm_phase_complete": true` |
| Expected JSON | `"mrms_jobs_pending": 0` |
| Expected JSON | `"volume_used_pct" < 90` (warn if higher) |
| Alert after | 3 consecutive failures |

The fields available from `/api/health`:

```json
{
  "status": "ok",
  "build_sha": "0f68d0d...",         // Railway commit SHA (or git rev-parse fallback)
  "uptime_s": 8421,                  // seconds since last container start
  "warm_phase_complete": true,       // Phase 1-5 of warm_cache.py finished
  "warm_completed_at": "2026-05-19T22:01:33+00:00",
  "volume_used_pct": 8.2,            // /app/persistent volume usage
  "last_monitor_poll_at": "...",     // last NHC RSS poll from storm_monitor.py
  "mrms_jobs_pending": 0,            // queue depth for /api/rainfall fetches
  "active_storm": null               // legacy, last-activated; safe to ignore
}
```

### Cold-storm activation latency

Optional. Triggers the full pipeline. Run no more than once an hour.

| Field | Value |
|---|---|
| URL | `https://surgedps-production.up.railway.app/api/cell?storm_id=chantal_2025&col=0&row=0` |
| Method | GET |
| Interval | 60 min |
| Expected status | 200 |
| Timeout | 60 s |
| Alert on | response > 30 s (indicates the heal path is slow or the cache is cold) |

Per-IP rate limit defaults to 30 req/min, so don't blast this — monitor frequency must be sane.

---

## Service B — StormDPS (Railway: stormdps)

### Quick check — homepage reachable

| Field | Value |
|---|---|
| URL | `https://stormdps.com/` |
| Method | GET |
| Interval | 60 s |
| Expected status | 200 |
| Expected body contains | the marketing copy / a meta tag that's stable across deploys |
| Alert after | 2 failures |

### SurgeDPS-mount proxy works

| Field | Value |
|---|---|
| URL | `https://stormdps.com/surgedps/` |
| Method | GET |
| Interval | 5 min |
| Expected status | 200 |
| Expected body contains | `<div id="root"` or the current Vite-injected `index-*.js` |
| Alert after | 2 failures |

If `surgedps-production.up.railway.app/api/health` is healthy but
`stormdps.com/surgedps/` is not, the proxy / Cloudflare layer between
the two is broken.

---

## Optional — admin inventory check (token-gated)

If your monitor supports custom headers / secret env vars:

| Field | Value |
|---|---|
| URL | `https://surgedps-production.up.railway.app/__val/inventory.json?t=$VALIDATION_TOKEN` |
| Method | GET |
| Interval | 15 min |
| Expected JSON | `"counts.partial" + "counts.fully_warmed" == "total_historic"` (i.e. no cold storms) |
| Expected JSON | `"storage.volume_used_pct" < 90` |

This is the strongest "service is actually serving data correctly" check
because it exercises the catalog → disk → JSON path end-to-end. Don't share
the URL publicly — the token gates admin-level visibility into the
persistent volume.

---

## Per-storm spot-checks (manual, post-deploy)

Run these after any deploy that touched gauge or compound logic. Not for
continuous monitoring — just a smoke run after pushing.

```bash
TOKEN=<from Railway service env>

# 1. Catalog is current
curl -s 'https://surgedps-production.up.railway.app/api/storms/historic' \
  | python -c 'import json,sys; d=json.load(sys.stdin); print(f"catalog: {len(d)} storms ({sorted({s[\"year\"] for s in d})})")'

# 2. NWPS thresholds resolving (sample storm)
curl -s 'https://surgedps-production.up.railway.app/api/gauges?storm_id=harvey_2017&category=none' \
  | python -c 'import json,sys; d=json.load(sys.stdin); print(f"harvey: {d[\"gauge_count\"]} gauges, {d[\"at_or_above_major\"]} major flood")'

# 3. Compound layer healed
curl -s 'https://surgedps-production.up.railway.app/api/compound?storm_id=harvey_2017' \
  | python -c 'import json,sys; d=json.load(sys.stdin); print(f"compound: {d.get(\"cell_count\",0)} cells, mosaic={d.get(\"available\")}")'

# 4. Inventory roll-up
curl -s "https://surgedps-production.up.railway.app/__val/inventory.json?t=$TOKEN" \
  | python -c 'import json,sys; d=json.load(sys.stdin); print(f"total: {d[\"total_historic\"]}, fz_mb: {d[\"storage\"][\"flood_zones_mb\"]}, vol: {d[\"storage\"][\"volume_used_pct\"]}%")'
```

---

## Alert routing recommendations

1. **Critical** (page immediately): Quick checks fail on either Service A or B.
   These mean the site is down for users.
2. **High** (alert within 5 min): Deep check fails for >15 min. Service is up
   but degraded — `warm_phase_complete: false`, `mrms_jobs_pending` rising,
   `volume_used_pct > 85`.
3. **Low** (email digest): Cold-activation latency > 30 s for 3 consecutive
   runs — indicates the heal path is doing real work, not necessarily broken
   but worth investigating.

If you use Better Stack, set up a status page sharing the quick checks so
you can link `status.stormdps.com` from the SPA's error states.
