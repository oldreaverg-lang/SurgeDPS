# SurgeDPS — Current Handoff

> **⚠️ RECOVERY NOTE (2026-07-09):** the original local working copy died in
> a Windows reset; this repo was re-cloned fresh to
> `C:\Users\Ryan\APPS\SurgeDPS-recovered` (note the new folder name — old
> absolute paths in docs/scripts may say `...\APPS\SurgeDPS`). State of the
> restore: `.venv` rebuilt on Python 3.13 via the `py` launcher (the bare
> `python` command on this machine is the WindowsApps stub — don't use it)
> and the full source tree compiles; **`.env` is a placeholder — paste
> `VALIDATION_TOKEN` from the Railway dashboard (SurgeDPS service →
> Variables)**; `.env.example` was created 2026-07-09 (it never existed in
> git before — nothing auto-loads .env, it's the local reference copy);
> Node/npm restored 2026-07-09 (v24.18.0 LTS, user-scope zip at
> `%LOCALAPPDATA%\Programs\nodejs`, on user PATH; `npm ci` + `tsc -b` +
> `vite build` all verified green in `ui/`); any local work
> after the 2026-05-23 last commit died with the reset (operator flagged
> none); `*.xlsx`/`*.docx` were gitignored and are unrecoverable from git.
> The live site never broke (deploys GitHub→Railway). Everything below this
> note is the pre-reset handoff and remains accurate about the CODE.

**Last updated:** 2026-05-21 (recovery note added 2026-07-09)
**Consolidated from:** `HANDOFF.md` (Apr 4), `HANDOFF_VALIDATION_LAYER.md` (Apr 14), `HANDOFF_RAINFALL_LAYER.md` (May 16), `HANDOFF_UX_AUDIT.md` (May 18), `HANDOFF_LOAD_TIME.md` (May 21)
**Status:** All subsystems live. Activate cache warm across deploys. Cloudflare hardened.

For a new agent: read top-to-bottom once. Section 4 (architecture) is the part most outdated docs got wrong — start there if you only have time for one.

---

## 1. Project overview

**SurgeDPS** is a public-facing hurricane storm-surge damage estimation tool. Users select a historical hurricane from a sidebar, the SPA fetches a pre-computed manifest, then streams per-cell GeoJSON of flood polygons + damage points onto a MapLibre map. Goal: FEMA-grade damage modeling accessible to anyone with a browser.

- **Live URL:** `https://stormdps.com/surgedps`
- **Underlying service:** `https://surgedps-production.up.railway.app`
- **Owner email (security.txt contact):** `reavesrg@gmail.com`

The user is technically competent, patient, and prefers architectural reasoning over pep talks. Memory file `~/.claude/projects/C--Users-Ryan-APPS-StormDPS/memory/MEMORY.md` documents their preferences (see §11 below).

---

## 2. Architecture — two services, two repos, one URL

```
                       Cloudflare (DNS + edge)
                              │
              ┌───────────────┴───────────────┐
              │                               │
              ▼                               ▼
   stormdps.com (StormDPS service)   surgedps-production... (SurgeDPS service)
   Railway #1, FastAPI/uvicorn       Railway #2, BaseHTTPRequestHandler
                  │                              │
                  │  / and most paths            │  /api/* (all data)
                  │  serves index.html, the      │  /__val/* (token-gated debug)
                  │  static SPA shell, and       │  /surgedps/* (alt path,
                  │  /surgedps/* (the React      │   serves its own bundle too)
                  │  bundle from a baked copy)   │
                  │                              │
                  └──── proxy routing ───────────┘
                  (Cloudflare or Railway routes
                   /surgedps/api/* to SurgeDPS
                   service. The exact layer isn't
                   important — what matters is it
                   works and you don't need to
                   touch it.)
```

**Key architectural truths:**

- **Two separate Railway projects, two separate git repos.** They do NOT share code at runtime.
- `C:\Users\Ryan\APPS\StormDPS\` → main DPS site (FastAPI, has its own HANDOFF.md)
- `C:\Users\Ryan\APPS\SurgeDPS\` → this repo (BaseHTTPRequestHandler, what this doc covers)
- **Both repos auto-deploy** on push to `main`. SurgeDPS push deploys the API service. StormDPS push deploys the frontend bundle update.
- The React frontend lives in `SurgeDPS/ui/src/`, builds to `SurgeDPS/ui/dist/`, and is synced to `StormDPS/frontend/surgedps/` via `scripts/sync_frontend.py`.

**Common confusion** (the old `HANDOFF.md` got this wrong): SurgeDPS does NOT use FastAPI and there is NO `surgedps_routes.py` inside StormDPS anymore. That file was removed when SurgeDPS became its own service. StormDPS's `/surgedps/{path:path}` route serves static assets only and explicitly 404s any `/api/*` request — those go to the SurgeDPS service directly.

---

## 3. Tech stack

| Layer | Technology |
|---|---|
| Frontend SPA | React 18 + TypeScript + Vite + Tailwind v4 + MapLibre GL (lazy-loaded) |
| Map basemap | Carto dark-matter (free, no API key) |
| Backend (SurgeDPS) | Python 3.12, `BaseHTTPRequestHandler` + `ThreadingMixIn`, no framework |
| Backend (StormDPS) | FastAPI + uvicorn (separate service, see its own HANDOFF) |
| Hosting | Railway (auto-deploys on git push, persistent volume at `/app/persistent`) |
| DNS / edge / WAF | Cloudflare (Free plan + Bot Fight Mode + AI Labyrinth beta) |
| Building data | FEMA NSI (primary), OSM Overpass (fallback) |
| Geocoding | Nominatim (reverse + forward, frontend only) |
| Damage model | FEMA HAZUS depth-damage curves |
| Surge model | Parametric raster generator (Holland B vortex) |

No new dependencies for any of the changes in this consolidated set.

---

## 4. Key file locations

### SurgeDPS (this repo)

```
SurgeDPS/
├── scripts/
│   ├── api_server.py            ← THE server. BaseHTTPRequestHandler.
│   │                              Activate cache, all /api/*, /__val/*.
│   ├── warm_cache.py            ← Container-start prewarm (cells, gauges, MRMS)
│   ├── storm_monitor.py         ← NHC polling every 30 min
│   └── sync_frontend.py         ← Vite build → StormDPS/frontend/surgedps/
├── src/
│   ├── persistent_paths.py      ← Single source of truth for volume dirs
│   ├── storm_catalog/
│   │   ├── catalog.py           ← HISTORICAL_STORMS list (~19 curated)
│   │   ├── hurdat2_parser.py    ← HURDAT2 best-track parsing
│   │   ├── surge_model.py       ← Parametric surge raster generation
│   │   └── forecast_track.py    ← NHC forecast cone fetcher
│   ├── data_ingest/
│   │   ├── nsi_fetcher.py       ← FEMA NSI building inventory (primary)
│   │   ├── building_fetcher.py  ← OSM Overpass (fallback)
│   │   └── census_fetcher.py    ← Population context for landfall
│   ├── damage_model/
│   │   └── depth_damage.py      ← HAZUS depth-damage curves
│   ├── flood_model/
│   │   ├── bathtub.py
│   │   ├── bathtub_pointwise.py ← Used by validation layer
│   │   ├── compound.py
│   │   ├── hand_model.py        ← HAND/NWM fluvial layer
│   │   └── raster_utils.py      ← Shared rasterio I/O helpers
│   ├── rainfall/
│   │   ├── mrms_fetcher.py      ← MRMS QPE + timelapse milestone frames
│   │   ├── ahps_historical.py   ← NWPS/NWIS gauge data
│   │   └── ...
│   └── validation/
│       ├── private_routes.py    ← /__val/* dispatcher
│       ├── debug_inventory.py   ← Cache inventory + HTML dashboard
│       ├── spatial_metrics.py   ← Michael 2018 baseline
│       └── ...
├── ui/
│   └── src/
│       ├── App.tsx              ← React SPA, single file (~5,900 lines)
│       ├── lazyMapLib.ts        ← Memoized MapLibre + CSS loader
│       ├── components/          ← DashboardPanel, StormBrowser, MapLegend
│       └── ...
├── data/
│   ├── cache/                   ← Pre-computed cells (built into image)
│   ├── validation/              ← Spatial validation artifacts
│   └── dps_scores.json          ← Per-storm DPS scores
└── HANDOFF.md                   ← This file
```

### StormDPS (separate repo, separate handoff)

Only the frontend-sync target matters for SurgeDPS work:

```
StormDPS/frontend/surgedps/
├── index.html               ← Copied verbatim from Vite build by sync_frontend.py
├── assets/
│   ├── index-<hash>.js      ← Main bundle (~240 KB gzipped)
│   ├── maplibre-<hash>.js   ← MapLibre split out (~270 KB gzipped)
│   ├── vendor-<hash>.js     ← React + deps
│   ├── counties-coastal-<hash>.js
│   ├── cities-coastal-<hash>.js
│   └── index-<hash>.css
├── logo-*.{png,webp}
├── icons.svg
└── .well-known/security.txt ← RFC 9116 security contact
```

---

## 5. Build, sync, and deploy workflow

### Frontend change (in `SurgeDPS/ui/src/`)

```bash
# From SurgeDPS/ui/
npx tsc -b                  # Match Railway: tsc -b, NOT --noEmit
npx vite build              # → SurgeDPS/ui/dist/

# From SurgeDPS/
python scripts/sync_frontend.py --skip-build
# Copies dist/assets/* and dist/index.html verbatim to
# StormDPS/frontend/surgedps/. Hash rewriting is automatic via
# the verbatim index.html copy — no manual hash editing.

# Commit + push BOTH repos
git -C ../SurgeDPS  add ui/src/...  && git -C ../SurgeDPS  commit && git -C ../SurgeDPS  push
git -C ../StormDPS add frontend/surgedps/ && git -C ../StormDPS commit && git -C ../StormDPS push
```

Railway picks up the StormDPS push and serves the new bundle within ~90 seconds.

### Backend change (in `SurgeDPS/scripts/` or `SurgeDPS/src/`)

```bash
git -C SurgeDPS add scripts/api_server.py
git -C SurgeDPS commit -m "..."
git -C SurgeDPS push        # Railway auto-deploys SurgeDPS service
```

No frontend sync needed.

### Verifying a deploy is live

```bash
curl -s https://stormdps.com/surgedps/api/health | python -m json.tool
# Look at "build_sha" — should match your latest commit's short SHA
```

CI runs `py_compile` on every Python file + `tsc -b` + `vite build` on push (`.github/workflows/ci.yml`). Local sanity check before pushing: `npx tsc -b && python -m py_compile scripts/api_server.py`.

---

## 6. Major subsystems

### 6.1 Frontend SPA (App.tsx)

Storm browser sidebar → storm activation → map + dashboard:

- Sidebar lists active NHC storms + curated historical storms; collapses on storm click
- Storm activation: `POST /api/storm/<id>/activate` returns manifest, then SPA fetches each cell via `/api/cell?col=X&row=Y`
- Center cell loads first, surrounding cells via a 3-worker queue
- Empty cells (building_count=0 AND flood_count=0) are filtered out client-side using the manifest's `cell_summary`
- Map renders surge gradient + damage bubbles + optional rainfall overlay + optional flood-zone overlay
- DashboardPanel (top-left): storm metadata, ELI score, total modeled loss, confidence badge, CAT deployment summary, surge legend
- Building hover popup: reverse-geocoded address (Nominatim, cached) + HAZUS type + damage % + estimated loss
- Welcome card on first visit; slim pill on returning visits (`localStorage["surgedps:welcomed"]`)

### 6.2 Backend API (api_server.py)

Single-file `BaseHTTPRequestHandler` + `ThreadingMixIn` on port 8000. Major endpoints:

| Endpoint | Purpose |
|---|---|
| `GET /api/health` | Build SHA, uptime, volume usage, last monitor poll |
| `GET /api/storms/historic` | Curated historic storm list |
| `GET /api/storms/active` | Live NHC active storms |
| `GET /api/seasons` | Season list for sidebar |
| `GET /api/season/<year>` | Storms for a year |
| `GET /api/storms/search?q=name` | Storm name search |
| `GET /api/storm/<id>/activate` | Returns the activate manifest (cached — see §6.5) |
| `GET /api/cell?col=N&row=N&storm_id=X` | Streams one cell's GeoJSON |
| `GET /api/cell_ticks` | Per-cell peril time series |
| `GET /api/progress` | Server-side activation progress (poll) |
| `GET /api/simulate?...` | Custom-landfall simulation |
| `GET /api/forecast/track` | NHC forecast cone for active storms |
| `GET /api/rainfall` | MRMS QPE accumulation |
| `GET /api/rainfall_tile?hour=N` | XYZ rainfall tiles |
| `GET /api/rainfall_frames` | List of available milestone frames |
| `GET /api/gauges` | NWIS/NWPS gauge readings |
| `GET /api/geocode/search`, `/api/geocode/reverse` | Nominatim proxy |
| `GET /__val/*` | Token-gated debug/admin (§6.6) |

Inbound rate limiter (per-IP sliding window) + structured request logging (`[req] rid=... method=GET path=/... storm=... ms=...`).

### 6.3 Damage model

- **NSI (primary):** FEMA National Structure Inventory REST API. Returns real `val_struct`, `val_cont`, `sqft`, `found_ht`, `num_story`, `med_yr_blt`, `occtype` per building.
- **Overpass (fallback):** if NSI fails or returns empty, falls back to OSM building footprints. Shoelace area calc → estimated `val_struct` based on type + size.
- **Deterministic cost multiplier:** MD5 hash of building ID → [0.60, 1.40] range. Gives realistic per-building variation so every building doesn't show the same dollar value.
- **HAZUS depth-damage curves:** structure + contents damage by occupancy code and flood depth.

Common HAZUS occupancy codes:
```
RES1  Single-Family    COM1  Retail       IND1  Heavy Industrial
RES2  Mobile Home      COM2  Warehouse    IND2  Light Industrial
RES3  Multi-Family     COM3  Service      AGR1  Agricultural
RES4  Hotel/Motel      COM4  Office       REL1  Church
RES5  Dormitory        COM6  Hospital     GOV1  Government
RES6  Nursing Home     COM7  Med Clinic   EDU1  School
```

NSI bbox format: closed polygon `lon_min,lat_min, lon_max,lat_min, lon_max,lat_max, lon_min,lat_max, lon_min,lat_min` (5 points).

### 6.4 Spatial validation layer (`/__val/*`)

Token-gated namespace serving the Hurricane Michael 2018 baseline validation scorecard plus per-storm cache inventory. Never linked from the public UI; 404s without a valid `VALIDATION_TOKEN`.

**Endpoints:**

| Path | Returns |
|---|---|
| `GET /__val/` | JSON index of storms with artifacts |
| `GET /__val/__status` | Diagnostic — token presence, path info (no auth required, no sensitive fields) |
| `GET /__val/inventory.json` | Full cache inventory across all storms |
| `GET /__val/inventory.html` | Visual dashboard for inventory |
| `GET /__val/{storm_id}` | `metrics.json` for that storm |
| `GET /__val/{storm_id}/samples` | `{storm_id}_samples.csv` |
| `GET /__val/{storm_id}/hwms` | High-water-mark CSV |
| `GET /__val/{storm_id}/dashboard` | Self-contained HTML viewer |
| `GET /__val/__backup` | Streams a tar.gz of the persistent volume |

**Auth:**
- Preferred: `X-Validation-Token: <token>` header
- Fallback: `?t=<token>` query string
- Comparison: `hmac.compare_digest` (constant time)
- `VALIDATION_TOKEN` env var lives in Railway → SurgeDPS service → Variables

**Hurricane Michael 2018 baseline** (the first benchmark; pointwise bathtub model vs 303 USGS HWMs):
- bias ≈ 0 ft, RMSE ~2.5 ft, CSI ~0.46, POD ~0.47, tier `fair`
- Any Tier-2/3 model that replaces this must beat RMSE < 2.55 ft AND CSI > 0.46.

To add another benchmark storm: discover the STN event ID, add to `STORM_EVENT_IDS` in `src/data_ingest/usgs_hwm.py`, digitize surge anchors from NHC TCR + NOAA gauges, write a driver following `scripts/run_michael_bathtub.py`.

### 6.5 Activate-manifest cache (two-tier, persistent)

The performance centerpiece. Every `/api/storm/<id>/activate` request goes through:

```
                /api/storm/<id>/activate request
                            │
                            ▼
                ┌───────────────────────────┐
                │ active storm?              │── yes → skip cache, full compute
                │ refresh=1 query param?     │── yes → invalidate, full compute
                └───────────────────────────┘
                            │ neither
                            ▼
            ┌──────────────────────────────┐
            │ Tier 1: in-memory OrderedDict │── hit (~5 ms) → return manifest
            │       (LRU, 32 entries)       │
            └──────────────────────────────┘
                            │ miss
                            ▼
            ┌──────────────────────────────┐
            │ Tier 2: persistent volume      │── hit (~50 ms) → hydrate Tier 1
            │ ACTIVATE_CACHE_DIR/<id>.json   │                    → return manifest
            └──────────────────────────────┘
                            │ miss
                            ▼
            ┌──────────────────────────────┐
            │ Cold compute (25–60 s):       │
            │ • 9-cell walk via load_cell() │
            │ • DPS/ELI/validated-DPS       │
            │ • Census population           │
            │ • Ground truth                │
            │ • Validation ledger entry     │── handler-only, not on
            │ • Gauge-cache warm spawn      │   pre-warm path
            └──────────────────────────────┘
                            │
                            ▼
                  write-through to both tiers
                            │
                            ▼
                       return manifest
```

**Disk format** (`<PERSISTENT_DATA_DIR>/activate_cache/<storm_id>.json`):
```json
{
  "_schema": 1,
  "_cells_mtime": 1747825912.45,
  "storm_data": { "name": "...", "eli": 7.5, ... },
  "cells_available": ["-1,-1", "-1,0", ..., "1,1"],
  "cell_summary": { "0,0": {"building_count": 1500, "flood_count": 200}, ... }
}
```

- `_schema` — bump in `api_server.py` to wholesale-invalidate every entry on next deploy
- `_cells_mtime` — invalidates if `warm_cache.py` regenerates the underlying cells (1 s slack)

**Boot-time pre-warm:** `api_server.main()` spawns a daemon thread that sleeps 45 s (so `warm_cache.py` settles) then iterates `HISTORICAL_STORMS`, calling `populate_activate_cache(storm)` for any without a fresh disk file. After ~3 minutes from boot, every storm in the curated catalog is hot on disk.

**Single source of truth:** `populate_activate_cache(storm) -> (payload, grid_cells)` is the only path that produces a cache entry. Both the request handler and the pre-warm sweep call it. Side effects (validation ledger, `_active_storm` global, gauge warm, progress polling) stay in the request handler — pre-warm doesn't fire them.

**Verified performance** (post-deploy, fresh container):

| Storm | First call (disk hydration) | Subsequent (memory) | Without cache |
|---|---|---|---|
| Sandy 2012 | 351 ms | 95 ms | 8.3 s |
| Ian 2022 | 422 ms | 426 ms | 13.7 s |
| Milton 2024 | 443 ms | 474 ms | 21.6 s |
| ike_2008 (worst pre-cache) | — | — | 39.8 s |

### 6.6 Frontend storm-activation flow

Client side of the same flow:

1. User clicks a storm row → `activateStorm(storm_id)` callback
2. **Storm-switch abort:** if `activatingRef.current` is non-null (another storm loading), abort its `activateAbortRef` controller + drain `cellAbortsRef` (kills all in-flight cell fetches at the HTTP layer, saving server bandwidth — this is THE data-spike fix)
3. **flushSync the data-heavy clears** (`setAllBuildings(null)`, `setAllFlood(null)`, etc.) so MapLibre tears down the prior storm's sources before the new fetch starts (eliminates paint-pipeline freeze on rapid switches)
4. `POST /api/storm/<id>/activate` — returns the small manifest (~1.5 KB)
5. Parse `cellsAvailable` from manifest, filter out empty cells using `cell_summary`, pre-populate `loadedCells` with the skipped keys
6. Load center cell `(0,0)` first (so the visible area paints immediately)
7. Remaining cells via 3-worker queue from `cellsAvailable.slice(1)`
8. Each cell fetch registers its `AbortController` in `cellAbortsRef` so storm-switch can abort it
9. flood polygons paint immediately; buildings deferred by one tick (so flood renders before React commits the much-larger building feature set)

The 7-bug audit pass that fixed all this:
- Storm-switch silently ignored mid-activation → now aborts prior + starts new
- Duplicate cell fetches when map-click loadCell raced with activator → both write to `loadingCells` now
- `loadedCells` global → now gated on `activatingRef === stormId` + `activeStormRef.current?.storm_id === stormId`
- `loadCell` flood-guard for cells with no flood data
- `flyToPopupTimer` cleared on storm switch
- `progressIntervalRef` unmount cleanup
- `activateStorm.finally` only clears state if still the active activation (prevents fast B-then-A-finish from clobbering B)

Plus a second pass that added concurrency guards on `runSimulation`, `handleBatchLookup`, `handleAddressSearch`, popup geocode storm-id capture, Safari <14 `matchMedia.addListener` fallback, and `{ once: true }` on the PDF popup load listener.

### 6.7 Rainfall layer + MRMS timelapse

Per-property HAZUS damage map has a 🌧️ Rain toggle that overlays:
- **Cumulative MRMS QPE** — fetched from IEM mtarchive for historical storms (2015+)
- **Per-hour milestone frames** — pre-extracted at landfall ± selected hour offsets
- **Slider UI** with ▶ Play / ⏮ Landfall / Full buttons; labels are landfall-relative ("L+12h" etc.)

**Pre-IEM storms** (Katrina 2005, Ike 2008, Sandy 2012) have NO MRMS data. IEM's archive starts in 2015. The parametric Lonfat fallback path in `api_server.py` will generate a `parametric_<sid>.tif` if `/api/rainfall` is hit for one of these; otherwise the slider shows an empty layer with an informational tooltip.

**MRMS storage:**
- `iem_<key>.tif` — full accumulation rasters
- `parametric_<sid>.tif` — Lonfat fallback for pre-IEM storms
- Milestone frames pre-extracted during the IEM fetch using `_write_timelapse_frame()` in `mrms_fetcher.py`

**Self-heal invariant** (learned the hard way after Phase 5 v1 destroyed 8 storms' data): cache invalidation MUST use the rename-to-`.stale` + restore-on-failure pattern. Never `os.remove` before a successful replacement is written. The current `warm_cache.py` Phase 5 v2 uses `os.rename(iem_tif, iem_tif + '.stale')` → try fetch with 5-min timeout → success: `os.remove(.stale)`; failure: `os.rename(.stale, iem_tif)`.

### 6.8 UX polish (May 18 audit, all shipped)

Visible behavior changes the next session should know about:

- **Welcome-card persistence** via `localStorage["surgedps:welcomed"]`. To force the full welcome card during testing: `localStorage.removeItem('surgedps:welcomed')`.
- **Hardest-Hit dedupe** — single cities like Jean Lafitte previously appeared 3–5 times in the top-5 list because they spanned multiple 0.005° bins. The dedupe merges sibling bins sharing the same modal `areaName`.
- **Unincorporated bubble keys** now include county GEOID — buildings on either side of a parish line in the same 0.2° tile no longer merge into one mislabeled cluster.
- **DPS color** changed from red→yellow to indigo→sky. Red is reserved for damage-severity semantics. Note: the sidebar storm-row dots still use `CAT_COLORS` (cat 4/5 dots are still red).
- **Surge raster opacity curve** has two extra stops at low zoom (6, 0.18; 8, 0.28) so the impact polygon doesn't blanket the entire state in solid red at the state-wide zoom level.
- **MapLegend component** (`components/MapLegend.tsx`) — collapsible bottom-right panel with color keys for visible layers. Color stops mirror layer paint expressions in `layers/flood.ts` and `layers/overlays.ts` — kept in sync **manually**, so if you change a layer's paint expression, update the legend too.
- **FEMA partial-coverage badge** — when `showFloodZones` is on and coverage < 100%, an amber `XX%` chip appears on the FEMA Zones toggle.
- **Welcome-card Notable Storm chips** now read `category` from `/api/storms/historic` instead of hardcoded values.

### 6.9 Security posture (Cloudflare + server)

| Layer | Status |
|---|---|
| MFA on Cloudflare account | ✅ Enabled (was the only real finding from Security Insights) |
| `/.well-known/security.txt` | ✅ Live, RFC 9116, expires 2027-05-21 (refresh before then) |
| Bot Fight Mode | ✅ Enabled |
| AI Labyrinth (beta) | ✅ Enabled |
| WAF Skip rule for `/api/*` and `/surgedps/api/*` | Required — Bot Fight Mode would otherwise break XHR storm activation |
| 404 handler scanner-probe block list | ✅ 107 patterns, shared between root + /surgedps handlers |
| `/__val/*` token-gated | ✅ `VALIDATION_TOKEN` env, hmac.compare_digest, 404 (not 401) on miss |
| `/__val/__status` strips sensitive fields when unauth | ✅ |
| Rate limiting (per-IP sliding window) | ✅ Uses CF-Connecting-IP, not raw XFF |
| Generic 500 error bodies | ✅ Stack traces never leak to clients |
| `seed_flood_zone` POST body cap + semaphore | ✅ |
| `Access-Control-Allow-Origin: *` scoped to `/api/*` only | ✅ Not on `/__val/*` |
| `.claude/` in `.gitignore` | ✅ Both repos — prior incident leaked `VALIDATION_TOKEN` via `.claude/worktrees/*/settings.local.json` permission allowlist |

**Scanner-probe block list** lives in `StormDPS/main.py` as the module-level constant `SCANNER_PROBE_PATTERNS`. Substring-matched against the lowercased request path. Adding patterns: mentally scan the legit-routes list for collisions; verify with the simulation block in the commit history of `8d742a5` if unsure.

---

## 7. Persistent volume layout

```
$PERSISTENT_DATA_DIR/   (Railway volume mount, env var = /app/persistent)
├── cells/                          # Per-storm pre-computed cells (the heavy data)
│   └── <storm_id>/
│       ├── cell_C_R_depth.tif
│       ├── cell_C_R_flood.geojson
│       ├── cell_C_R_damage.geojson
│       └── building_index.json
├── validation/run_ledger.json      # Model activation history
├── census/                         # Cached county population data
├── forecasts/                      # NHC forecast track JSON cache
├── geocode/                        # Reverse geocoding cache
├── mrms/                           # MRMS QPE GeoTIFF cache
├── hand_fim/                       # HAND raster per HUC8 (permanent)
├── nwm/                            # NWM discharge per storm
├── qpf/                            # WPC QPF cache
├── atlas14/                        # NOAA PFDS frequency tables (permanent)
└── activate_cache/                 # ~6 KB per storm, ~150 KB total
    ├── sandy_2012.json
    ├── ian_2022.json
    └── ...
```

Cells are the big ones (~1–2 GB total across the catalog). Everything else is small. Volume is 30 GB; currently ~10.5% used.

---

## 8. Operations runbook

### Inspecting cache state

```bash
# How many storms cached?
ls /app/persistent/activate_cache/ | wc -l

# Specific storm's cache timestamp
stat /app/persistent/activate_cache/milton_2024.json

# Read cached storm_data
cat /app/persistent/activate_cache/milton_2024.json | python -m json.tool
```

### Cache invalidation

| When | How |
|---|---|
| Single storm's cells regenerated | Auto-invalidates via `_cells_mtime` check |
| Manual single-storm refresh | `curl '.../api/storm/<id>/activate?refresh=1'` |
| Wholesale invalidate | Bump `_CACHE_SCHEMA` in `api_server.py`, push |
| DPS/ELI math changes | Bump `_CACHE_SCHEMA` (the schema doesn't auto-detect this) |

### Health checks

```bash
curl -s https://stormdps.com/surgedps/api/health | python -m json.tool
# Shows: build_sha, uptime_s, warm_phase_complete, volume_used_pct,
#        last_monitor_poll_at, active_storm
```

### Token-gated debug dashboard

```bash
TOKEN=$(read from Railway → SurgeDPS service → Variables → VALIDATION_TOKEN)
curl "https://surgedps-production.up.railway.app/__val/inventory.json?t=$TOKEN"
# Or open in browser:
# https://surgedps-production.up.railway.app/__val/inventory.html?t=$TOKEN
```

### Volume backup (for DR)

```bash
curl -H "X-Validation-Token: $TOKEN" \
     -o backup.tar.gz \
     https://surgedps-production.up.railway.app/__val/__backup
```

Currently manual; cron'ing this is a deferred item.

### If activation feels slow

In order:
1. `curl https://stormdps.com/surgedps/api/health` — confirm server is up
2. `curl '...activate?refresh=1'` — force cold recompute; if it stays fast, cache is fine and slowness is in the underlying cell-walk (likely a cell got regenerated mid-flight)
3. `ls /app/persistent/activate_cache/` — confirm files exist and aren't 0 bytes
4. Check Railway logs for `[cache] disk write failed` — if disk writes fail the in-memory cache still works but cross-deploy hydration won't
5. Check Railway logs for `[prewarm]` lines — if `failed > 0` a storm is broken; look up the storm_id

### Cloudflare cache after deploy

Usually unnecessary — `Cache-Control: no-store` headers on API paths + content-hashed asset filenames mean fresh deploys propagate within ~90 s. If you ever need to nuke it: Cloudflare → Caching → Configuration → Purge Everything.

---

## 9. Known sharp edges

1. **Process-local in-memory cache.** Tier 1 (`_ACTIVATE_CACHE` OrderedDict) is per-process. If we ever move to multi-process (gunicorn workers), each worker has its own Tier 1. Tier 2 (disk) is shared, so worst case is N × disk reads instead of N × cold computes — still fine, but worth knowing.

2. **`_cells_mtime` only checks the storm's cell directory.** If DPS scoring or any other compute changes WITHOUT touching a cell file, the cache won't notice. **Bump `_CACHE_SCHEMA` whenever DPS/ELI math or storm_data shape changes.**

3. **No LRU on the disk tier.** Memory tier capped at 32 entries; disk is unbounded. Trivial at current ~25-storm catalog; revisit if it ever grows past ~1000.

4. **Active-storm cache bypass is by `storm.status == 'active'` literal.** If a new storm status value is added (e.g. `'forecast'`, `'potential'`), the cache could serve stale forecasts unless explicitly excluded.

5. **`os.replace()` is atomic on POSIX and Windows.** But two workers computing the same storm in parallel both write; last write wins. Both are deterministic so the bytes match — fine in practice.

6. **`exposure_region` is always empty** in the returned `storm_data`. `StormEntry.to_dict()` uses `dataclasses.asdict()` and `exposure_region` isn't a dataclass field. The validated-DPS baseline defaults to 2,000 buildings instead of a regional value. Pre-existing latent issue, not a regression. Worth fixing eventually but not blocking.

7. **EPQS returns `None` over water** — affects validation pipeline. Expected; ~4% of coastal HWMs are dropped in intertidal zones.

8. **Pre-IEM storms (Katrina 2005, Ike 2008, Sandy 2012)** have no MRMS rainfall data and the parametric Lonfat fallback only fires when `/api/rainfall` is hit. Slider shows empty for these unless someone activates each storm via UI first.

9. **Rainfall LRU-miss when bypassing UI** — `/api/rainfall_frames` only checks `_rainfall_tif_by_storm` in-memory LRU which is populated when `/api/rainfall` is hit. Direct curl-style probes show `no_rainfall_cached_yet` even when the TIF is on disk. In normal flow this never matters; for debug scripting it's annoying. Patch sketched but not shipped — see Section 4.3 of the old `HANDOFF_RAINFALL_LAYER.md` if you need it.

10. **Dead component files** in `ui/src/components/`: `DashboardPanel.tsx`, `StormBrowser.tsx`, `useImpactAggregates.ts`. They're imported via `void _XX;` to keep tree-shaking happy but the active versions are inline in `App.tsx`. Either wire these in or delete them; current parallel-implementations state invites future drift.

---

## 10. Open / deferred items

In rough priority order:

| Priority | Item | Notes |
|---|---|---|
| Low | Cache-stats endpoint `/api/__cache/stats` | hit/miss counters, disk size, oldest entry, last pre-warm result. Not strictly needed; perf numbers are validation enough. |
| Low | Active-storm forecast cache with TTL | Keyed by `(storm_id, advisory_number)`, 10-min TTL. Skipped since active storms are rare. |
| Low | Eager hydration of Tier 1 on boot | Pre-load every disk entry into Tier 1 at startup. ~5 ms saved per first-request. Probably overkill. |
| Med | NWIS Phase 2 still failing for some storms | Bbox clamp shipped but USGS may be rate-limiting our IP from earlier abuse. Check Railway `[gauges]` logs for actual HTTP codes. |
| Med | FEMA NFHL Phase 3 at 0 MB for every storm | Phase 3 retry logic uses 45 s timeouts, 16 tiles per storm, no exponential backoff. Silent failures likely. Check `[fz]` logs. |
| Med | Damage Breakdown bar can sum >100% | When multiple categories <1.5% pad to that floor. Cosmetic. |
| Med | Pre-IEM storms (Katrina, Ike, Sandy) have no rainfall | Trigger parametric fallback per storm via UI or write a one-shot warming script. |
| Low | `exposure_region` plumbed into validated-DPS | Latent — currently defaults to baseline 2,000. |
| Low | Static residual scatter+map PNG for docs | Dashboard does it live; static export is doc-embedding nice-to-have. |
| Low | Wind validation pipeline | Explicitly deferred (water-only validation for now). |
| Low | Second benchmark storm | Ian 2022 or Ida 2021 (different basin). |
| Low | IDW sensitivity sweep | Vary power × radius over Michael 2018 baseline → `metrics_sweep.json`. |
| Low | Validation metrics in `run_ledger.py` | Every storm activation logs nearest-validated scorecard. |
| Low | Static welcome-card seenWelcome via direct-link detection | Currently set on any activation. |
| Low | Cron the `/__val/__backup` endpoint | DR runbook exists; backup endpoint exists; nothing actually runs it on a schedule. |

---

## 11. User preferences (from memory)

These are codified in `~/.claude/projects/C--Users-Ryan-APPS-StormDPS/memory/MEMORY.md` and have been honored consistently:

1. **Commit straight to main on this repo.** No worktree isolation, no PRs — direct push deploys via Railway.
2. **Claude handles git push.** Don't ask for confirmation on normal merges + pushes.
3. **User sleeps in 1-hour increments.** Do NOT suggest "stopping points" or "let's wrap up" or "get some rest." They sleep when they sleep; don't manage their time.
4. **Don't over-document or over-justify after rejection.** When they say no, brief acceptance + move on. Long mea-culpa docs read as still insisting.
5. **Build the upstream pipeline before designing downstream.** For tool-boundary work (Blender↔Unity, SurgeDPS↔StormDPS): verify auto-export + correct file paths first; design downstream architecture only against geometry that's already shipped.
6. **Before adding `defer`/`async` to scripts, grep inline JS for top-level lib refs.** `Chart.register(...)` at top level will `ReferenceError` during parse and silently halt the entire inline script.
7. **`addEventListener` leaks the event object as first arg.** Wrap with `() => fn()` when `fn` has meaningful positional args.
8. **Use `tsc -b` for sanity checks** — `tsc --noEmit` can pass while Railway's `tsc -b && vite build` fails on the same source.
9. **Verify live-build hash matches local hash after pushing** — Railway + Cloudflare cache lag can make a push look "live" while the SPA still serves the old bundle.
10. **StormDPS SEO backlink outreach deferred to first 2026 Atlantic storm** (~June 2026).

---

## 12. Pitfalls / gotchas hit before (don't re-learn)

- **`python` is not on PATH on this Windows machine.** Use `C:\Python314\python.exe` explicitly. Many shell scripts use this absolute path.
- **Windows file encoding default is cp1252.** Python `ast.parse(f.read())` on em-dash-containing files chokes. Use `open(path, encoding='utf-8')` and set `PYTHONIOENCODING=utf-8` for scripts that print Unicode.
- **`git checkout main` fails in worktrees** when main is held by another worktree. Use `git push origin claude/<branch>:main` to push a worktree branch to remote main.
- **NEVER delete persistent data before having a working replacement.** Use rename-to-`.stale` + restore-on-failure pattern. Phase 5 v1 destroyed 8 storms' MRMS data because it deleted before refetching.
- **Always wrap unbounded I/O in a timeout.** Use `concurrent.futures.ThreadPoolExecutor` with `future.result(timeout=N)`. Note Python can't actually kill the worker thread on timeout — it leaks until the underlying I/O finishes — but the main loop becomes unblocked.
- **`min(iterable, key=...)` ties-breaks to the FIRST equal value.** Add an explicit at-or-after filter before `min()` if you need post-landfall preference.
- **Bash heredocs choke on Python with single-quoted strings.** Write Python to a `.py` file instead of inlining.
- **Don't `cd` into worktree subdirs and forget where you are.** Shell cwd persists across Bash tool calls; print `pwd` if unsure.
- **`tsc -b`, not `tsc --noEmit`** for local sanity. The `--noEmit` form uses a stale `.tsbuildinfo` and can pass when `-b` fails.
- **CI runs `py_compile` + `tsc -b` + `vite build`** on every push (`.github/workflows/ci.yml`). Run those three locally before pushing if the change is non-trivial.
- **`HANDOFF.md` April 2026 version said SurgeDPS API was inside StormDPS at `/surgedps/api/*`.** That was true once, no longer — SurgeDPS is its own service. Trust the live code, not old docs.
- **`scripts/sync_frontend.py` copies index.html verbatim** since hashed asset filenames can contain hyphens (regex was `[A-Za-z0-9_]` before, now `[A-Za-z0-9_-]`). Don't try to hand-edit hashes in index.html.

---

## 13. Where to look first when picking this up

1. `git -C SurgeDPS log --oneline -20` — recent commits tell the story of what was last changed
2. `curl -s https://stormdps.com/surgedps/api/health` — what's running and how long it's been up
3. `ls /c/Users/Ryan/APPS/SurgeDPS/scripts/` — entry points
4. This doc, sections 4 and 6 — architecture + subsystem map
5. The relevant subsystem source file once you know what you're touching

End of handoff. Memory file gets read on session start; this doc covers the SurgeDPS code itself.
