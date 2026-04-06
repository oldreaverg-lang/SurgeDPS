# SurgeDPS — New Session Handoff Document
*Last updated: April 4, 2026*

---

## Project Overview

**SurgeDPS** is a public-facing hurricane storm surge damage estimation tool. Users select a historical hurricane from a sidebar, and the app overlays flood depth data and per-property damage estimates on an interactive map. The goal is to make FEMA-grade damage modeling accessible to the general public and eventually emergency managers.

**Live site:** `https://stormdps.com/surgedps`

---

## Repository Structure — Two Repos, One Site

```
C:\Users\Ryan\APPS\SurgeDPS\    ← Source repo (React frontend + Python damage model)
C:\Users\Ryan\APPS\StormDPS\    ← Deploy repo (FastAPI backend + compiled frontend)
```

### Why two repos?
StormDPS is the main website (`stormdps.com`). SurgeDPS is a sub-tool mounted inside it at `/surgedps`. The React frontend is built with Vite in SurgeDPS and the compiled output is **manually copied** into StormDPS before each deploy.

### Deployment chain:
```
GitHub (StormDPS repo) → Railway (auto-deploys on push) → stormdps.com (Cloudflare DNS/proxy)
```

SurgeDPS repo is pushed to GitHub separately but **does not trigger a deploy** — it's just source control for the frontend and damage model code.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Frontend | React + TypeScript + Vite + Tailwind CSS + MapLibre GL |
| Map tiles | Carto dark-matter basemap (free, no API key) |
| Backend | FastAPI (Python 3.12), served by Uvicorn |
| Hosting | Railway (ephemeral storage — cache resets on each deploy) |
| DNS/CDN | Cloudflare |
| Building data | FEMA NSI (primary), OpenStreetMap Overpass API (fallback) |
| Geocoding | Nominatim (OSM reverse geocoder, frontend only) |
| Damage model | FEMA HAZUS depth-damage curves |

---

## Key File Locations

### SurgeDPS (source)
```
SurgeDPS/
├── ui/
│   └── src/
│       └── App.tsx                  ← Entire React frontend (single file)
├── src/
│   ├── data_ingest/
│   │   ├── building_fetcher.py      ← OSM/Overpass fetcher + Shoelace area calc
│   │   └── nsi_fetcher.py          ← FEMA NSI API integration (NEW)
│   └── damage_model/
│       └── depth_damage.py          ← HAZUS damage curves + per-building cost variation
```

### StormDPS (deploy)
```
StormDPS/
├── api/
│   └── surgedps_routes.py           ← All FastAPI routes for SurgeDPS
├── surgedps/
│   ├── data_ingest/
│   │   ├── building_fetcher.py      ← Copy of SurgeDPS version
│   │   └── nsi_fetcher.py          ← Copy of SurgeDPS version
│   └── damage_model/
│       └── depth_damage.py          ← Copy of SurgeDPS version
└── frontend/
    └── surgedps/
        ├── index.html               ← Must be manually updated with hashed asset filenames after each build
        └── assets/
            ├── index-BG2rzHSA.js   ← Current build hash (changes every build)
            └── index-DHvGkkDS.css  ← Current build hash (changes every build)
```

---

## Build & Deploy Workflow

Every time App.tsx or any frontend file changes:

```bash
# 1. In SurgeDPS/ui/
rm -rf dist
npm run build
# Note the new hashed filenames in dist/assets/

# 2. Copy assets to StormDPS
cp -r dist/assets/* ../StormDPS/frontend/surgedps/assets/

# 3. Update StormDPS/frontend/surgedps/index.html
# Replace old hashed filenames with new ones (two lines: .js and .css)
# Also copy the maplibre chunk if it changed

# 4. Commit and push StormDPS
cd ../StormDPS
git add frontend/surgedps/
git commit -m "..."
git push   # ← This triggers Railway deploy
```

Backend Python changes (surgedps_routes.py, damage model, fetchers):
```bash
# In StormDPS/
git add api/ surgedps/
git commit -m "..."
git push
```

---

## What Has Been Built (Completed Features)

### Frontend (App.tsx)
- **Storm browser sidebar** — lists NHC active storms + historical storms by season with live search
- **Responsive layout** — collapsible sidebar drawer on mobile/tablet; hamburger button inside dashboard panel
- **Depth-damage map** — flood depth polygons colored by severity (feet, not body parts)
- **Building damage dots** — colored by category (minor/moderate/major/severe), clustered at low zoom
- **Grid cell system** — 0.4°×0.3° cells; click dashed borders to expand coverage area
- **DashboardPanel** — anchored top-left of map (not centered), shows:
  - Storm name, category, wind speed in kt AND mph
  - ELI score (Exposure Loss Index) with tier label
  - Total Modeled Loss in $M
  - Confidence badge (high/medium/low) with contextual explanation
  - Map Coverage section (# areas analyzed)
  - Surge depth legend (< 0.5 ft, 0.5–1.5 ft, 1.5–3.5 ft, 3.5–6 ft, > 6 ft)
  - Building damage color legend
- **Building hover popup** — shows:
  - **Street address** (reverse-geocoded from Nominatim, cached in-memory per session)
  - Building type (friendly name from HAZUS code lookup table)
  - Damage severity, damage %, estimated loss
- **Search** — clears query text on storm selection
- **Empty state** — "Browse Storms" button visible on mobile

### Backend (surgedps_routes.py)
- **Per-cell asyncio locks** — prevents duplicate pipeline runs when multiple users request the same uncached cell simultaneously (critical for live storm scenarios)
- **503 error handling** — when NSI + Overpass both fail, returns a friendly error instead of crashing with 500
- **Fast path serving** — cached cells served by splicing raw bytes (no JSON parse/dump), ~0.5s vs ~5s
- **Startup pre-warming** — top 5 historic storms (Ian, Katrina, Harvey, Sandy, Michael) pre-generate center cell in staggered background threads on server start

### Damage Model
- **FEMA NSI integration** — primary data source; uses real tabulated `val_struct`, `val_cont` per building from the National Structure Inventory REST API
- **Overpass fallback** — if NSI fails, fetches OSM building footprints
- **Shoelace formula** — computes actual polygon area in sqft from OSM geometry vertices
- **Deterministic cost multiplier** — MD5 hash of building ID → [0.60, 1.40] range, gives realistic per-building variation instead of every property showing the same value
- **HAZUS depth-damage curves** — structure + contents damage by building type and flood depth

---

## Current Known Issues / Things to Watch

1. **Railway ephemeral storage** — the damage cache (`surgedps_data/cells/`) resets on every deploy. Pre-warming rebuilds the top 5 storms automatically but it takes a few minutes after each deploy before those are ready.

2. **Overpass rate limits** — the public Overpass API (`overpass-api.de`) is unreliable under load. NSI is the primary source now, which helps. The 503 handler means users see a clean error rather than a crash.

3. **SurgeDPS git push is separate** — pushing SurgeDPS to GitHub does NOT deploy anything. Only StormDPS pushes trigger Railway. Don't confuse the two.

4. **index.html must be manually updated** — Vite generates new content-hashed filenames on every build. The `<script>` and `<link>` tags in `StormDPS/frontend/surgedps/index.html` must be updated by hand after each build.

---

## Pending / Future Goals

### High Priority
- **Live storm mechanism** — the biggest next feature. Architecture agreed upon:
  - Poll `nhc.noaa.gov/CurrentStorms.json` every NHC advisory (~6 hours)
  - Download NHC **P-Surge** rasters (NOAA's operational surge forecast) for active storms
  - Convert P-Surge raster format (shapefile/NetCDF) → internal GeoTIFF format
  - Run existing building damage model against forecast surge
  - Display "FORECAST" badge and storm track cone of uncertainty on map
  - Target audience: general public following an approaching storm
  - **Blocker:** Railway ephemeral storage means the 6-hour poller has nowhere to write between advisory cycles → needs S3 bucket or small Postgres/Redis instance

### Medium Priority
- **Property ownership data** — deferred until live storm mode exists. For historical storms, current ownership is misleading (properties change hands post-storm). Best use case is live storms where emergency managers want to contact owners. Likely data source: Regrid API (commercial) or county assessor APIs (free but inconsistent).

### Lower Priority / Ideas
- Private Overpass instance or self-hosted OSM extract to eliminate public API reliability issues
- WebSocket or SSE for live advisory push updates (instead of client polling)
- Time-lapse slider showing surge extent evolving over storm duration
- Evacuation zone overlay from FEMA data

---

## HAZUS Occupancy Code Reference (used throughout the codebase)

```
RES1 → Single-Family Home    COM1 → Retail Store        IND1 → Heavy Industrial
RES2 → Mobile Home           COM2 → Warehouse            IND2 → Light Industrial
RES3 → Multi-Family Housing  COM3 → Service Business     AGR1 → Agricultural
RES4 → Hotel / Motel         COM4 → Office Building      REL1 → Church
RES5 → Dormitory             COM6 → Hospital             GOV1 → Government
RES6 → Nursing Home          COM7 → Medical Clinic       EDU1 → School
```

## NSI API Reference
- Endpoint: `https://nsi.sec.usace.army.mil/nsiapi/structures`
- BBox format: closed polygon `lon_min,lat_min,lon_max,lat_min,lon_max,lat_max,lon_min,lat_max,lon_min,lat_min` (5 points)
- Key fields returned: `val_struct`, `val_cont`, `sqft`, `found_ht`, `num_story`, `med_yr_blt`, `occtype`

## Nominatim Reverse Geocoding (frontend)
- Used in App.tsx to show street addresses in building hover popup
- In-memory cache keyed on `${lng.toFixed(5)},${lat.toFixed(5)}`
- `User-Agent: SurgeDPS/1.0 (surgedps.com)` header required by Nominatim policy
- Returns `house_number, road, city` formatted as "123 Main St, New Orleans"
