# SurgeDPS Handoff — Spatial Validation Layer

**Date:** 2026-04-14
**Author:** Claude (Opus 4.6) + Ryan
**Status:** Deployed to Railway, token-gated, awaiting final URL verification

---

## What shipped this session

A zero-raster-storage spatial validation pipeline with Hurricane Michael (2018) as the first baseline benchmark set. Results are served through a private, token-gated route namespace (`/__val/`) that is never linked from the public React UI.

### Commits on `main` (in order)

1. `f14502d` — Extract shared rasterio read/write helpers into `raster_utils`
2. `e8c4439` — Fix peril-dominant posture, rainfall split, and generator sizing (UI)
3. `93e61f3` — Add spatial validation layer with Michael 2018 baseline
4. `717e60c` — Fix TS build: drop unused `_CAT_COLOR`, widen progress payload type
5. `a879b84` — Add `/__val/__status` diagnostic for deploy verification

---

## Architecture

### Data flow

```
USGS STN API                       USGS EPQS
(HWMs.json, event 287)             (elevation per point)
        │                                  │
        ▼                                  ▼
 src/data_ingest/usgs_hwm.py    src/flood_model/bathtub_pointwise.py
        │                                  │
        └────────────────┬─────────────────┘
                         ▼
              IDW surge field (11 digitized anchors)
                         │
                         ▼
   SampledObservation records (modeled vs observed per HWM)
                         │
                         ▼
       src/validation/spatial_metrics.py  (bias, RMSE, CSI, …)
                         │
                         ▼
             data/validation/michael_2018/metrics.json
                  michael_2018_samples.csv
                  michael_2018_hwms.csv
                         │
                         ▼
 src/validation/private_routes.py  (token-gated /__val/ namespace)
                         │
                         ▼
      https://surgedps-production.up.railway.app/__val/...
```

### Why pointwise instead of raster

Railway storage was capped at 5 GB / 4 GB used when the validation work began (later lifted to 30 GB). Full-extent Michael DEM + SLOSH MOM + modeled WSE raster would run 200–500 MB. Pointwise approach needs **zero raster storage**: ground elevations are pulled per-point from USGS EPQS (free 3DEP-derived API) and cached as a tiny CSV (`data/cache/epqs_elevations.csv`).

### Surge field

11 digitized anchor points along the FL Panhandle coast, peak 14.7 ft at Mexico Beach (USGS field survey max). IDW interpolation with power=2, 150 km search radius. Anchors are pinned in `scripts/run_michael_bathtub.py` and sourced from:

- NHC TCR AL142018 (Beven, Berg, Hagen 2019)
- USGS OFR 2019-1039 field survey
- NOAA tide gauges: Panama City 8729108, Apalachicola 8728690, Cedar Key 8727520

---

## Files added/modified

### New modules

| Path | Purpose |
|---|---|
| `src/data_ingest/usgs_hwm.py` | USGS STN fetcher, HWM dataclass, quality/coastal filters, CSV cache |
| `src/flood_model/bathtub_pointwise.py` | Pointwise bathtub + EPQS fetcher + IDW/exponential surge fields |
| `src/validation/spatial_sampler.py` | `SampledObservation` dataclass, raster/HWM/gauge sampler |
| `src/validation/spatial_metrics.py` | Metric aggregation (bias, MAE, RMSE, R², POD/FAR/CSI, tier) |
| `src/validation/private_routes.py` | Token-gated `/__val/` handler with dashboard HTML |
| `src/flood_model/raster_utils.py` | Shared rasterio I/O helpers |
| `scripts/run_michael_bathtub.py` | Michael-specific driver (anchors, landfall, defaults) |
| `scripts/validate_storm.py` | Generic CLI (`python scripts/validate_storm.py <storm_id> --raster …`) |

### Modified

| Path | Change |
|---|---|
| `scripts/api_server.py` | Added `/__val/` early dispatch in `do_GET` |
| `src/flood_model/{bathtub,compound,hand_model,rainfall}.py` | Use `raster_utils` helpers |
| `ui/src/App.tsx`, `ui/src/catTeam.ts` | Peril-dominant fix + rainfall split + generator sizing |

### Data committed to repo

| Path | Size | Notes |
|---|---|---|
| `data/validation/hwms/michael_2018_hwms.csv` | master 522-HWM cache (raw STN dump) |
| `data/validation/michael_2018/michael_2018_hwms.csv` | 315 filtered (Good+, coastal) |
| `data/validation/michael_2018/michael_2018_samples.csv` | 303 modeled-vs-observed rows |
| `data/validation/michael_2018/metrics.json` | Scorecard JSON |
| `data/cache/epqs_elevations.csv` | 201 cached EPQS ground elevations |

Total ~92 KB.

---

## Private route contract

All endpoints require a valid `VALIDATION_TOKEN`. If the token is missing, malformed, or the env var is unset, the handler returns **404 `Not Found`** (not 401) — the namespace is designed to look like it doesn't exist to unauthenticated visitors.

### Auth

- Header (preferred): `X-Validation-Token: <token>`
- Query string: `?t=<token>`
- Comparison: `hmac.compare_digest` (constant time)

### Endpoints

| Method | Path | Returns |
|---|---|---|
| GET | `/__val/` | JSON index of storms with artifacts |
| GET | `/__val/__status` | Diagnostic (token presence, path info). No auth required. |
| GET | `/__val/{storm_id}` | `metrics.json` |
| GET | `/__val/{storm_id}/samples` | `{storm_id}_samples.csv` |
| GET | `/__val/{storm_id}/hwms` | `{storm_id}_hwms.csv` |
| GET | `/__val/{storm_id}/dashboard` | Self-contained HTML viewer with canvas scatter |

### Response headers on every endpoint

- `X-Robots-Tag: noindex, nofollow`
- `Cache-Control: no-store`
- 404 body is always the bytes `Not Found` (no hint about the namespace)

---

## Current deploy state

- **Service:** SurgeDPS on Railway (`surgedps-production.up.railway.app`)
- **Last deploy:** `a879b84` — built and active
- **Volume:** 30 GB persistent at `/app/persistent` (validation data lives outside it, in the image at `/app/data/validation`)
- **Env var:** `VALIDATION_TOKEN` set on SurgeDPS service
- **Token:** `bz6HnEseHeUWdC90f2i5Y3F0WxcyfoyHwUI7MXEnXlA` (43 chars)

### Current blocker (as of handoff)

`/__val/__status` reports:

```json
{
  "token_env_set": true,
  "token_env_length": 43,
  "token_supplied": true,
  "token_supplied_length": 44,
  "token_matches": false,
  ...
  "storms_on_disk": ["hwms", "michael_2018"]
}
```

The supplied token has **one extra character** vs the stored env var. Most likely a trailing whitespace/slash when the URL was pasted. Data is deployed fine — only the URL paste needs to be cleaned.

**Resolution:** open this URL with no trailing characters after the final `A`:

```
https://surgedps-production.up.railway.app/__val/michael_2018/dashboard?t=bz6HnEseHeUWdC90f2i5Y3F0WxcyfoyHwUI7MXEnXlA
```

If it still shows 44 chars supplied, inspect the Railway var value for invisible leading/trailing characters.

---

## Michael 2018 baseline scorecard

From `data/validation/michael_2018/metrics.json` (approximate — see file for exact values):

| Metric | Value | Meaning |
|---|---|---|
| n_sampled | 303 | HWMs with Good+ quality, coastal, with EPQS ground elev |
| bias | ≈ 0 ft | Mean(modeled − observed) |
| RMSE | ~2.5 ft | Spread of residuals |
| %within ±1 ft | ~32% |  |
| %within ±2 ft | ~56% |  |
| CSI | ~0.46 | Critical success index (flood / no-flood at 0.5 ft threshold) |
| POD | ~0.47 | Prob of detection |
| FAR | moderate | False alarm ratio |
| Tier | `fair` | Per `_classify_tier` thresholds |

**Interpretation:** The bathtub baseline explains meaningful structure but misses ~50% of flooded cells (surge asymmetry right of track isn't captured by radial IDW). Any Tier-2/3 model that replaces this must beat RMSE < 2.55 ft and CSI > 0.46 to claim improvement.

---

## How to re-run or extend

### Re-run Michael validation

```bash
python scripts/run_michael_bathtub.py          # default IDW, Good+ quality, coastal only
python scripts/run_michael_bathtub.py --field exponential --peak-ft 14.7
python scripts/run_michael_bathtub.py --min-quality Excellent
```

Outputs land in `data/validation/michael_2018/` and overwrite existing artifacts.

### Add another storm

1. Discover the STN event ID:
   ```bash
   curl https://stn.wim.usgs.gov/STNServices/Events.json | grep -i <name>
   ```
2. Add to `STORM_EVENT_IDS` in `src/data_ingest/usgs_hwm.py`
3. Digitize surge anchors from NHC TCR + NOAA gauges
4. Write a `scripts/run_<storm>_bathtub.py` driver following the Michael pattern

### Swap in a better model

Replace the surge field function passed to `run_pointwise_bathtub_on_hwms()` with anything that implements `(lat, lon) -> WSE_ft_NAVD88`. The validation pipeline is model-agnostic — any `SurgeFieldFn` works.

---

## Open / deferred

- **Residual scatter+map PNG** — dashboard does this live in canvas; a static PNG for embedding in docs is TBD
- **IDW sensitivity sweep** — vary power (1, 2, 3), radius (75, 150, 300 km); log results to `metrics_sweep.json`
- **Wire metrics into `run_ledger.py`** — every storm activation would log the nearest-validated storm's scorecard
- **Wind validation pipeline** — HRRR reanalysis or ARA wind swaths vs gust recorder data (explicitly deferred; water-only validation for now)
- **Second benchmark storm** — Ian 2022 or Ida 2021 (more surge data, different basin characteristics)

---

## Known sharp edges

1. **EPQS returns `None` over water** — expected; those HWMs are dropped from metrics. Occurs for ~4% of coastal marks in very flat intertidal zones.
2. **CSV not parquet** — `pyarrow` adds ~70 MB to the image. Kept CSV to stay lean; revisit if row counts exceed ~10 k.
3. **STN field name weirdness** — `height_above_gnd` (no `_ft` suffix), `latitude_dd`/`longitude_dd`, quality as numeric ID that maps to name via `QUALITY_NAME_BY_ID`. Frozen against STN schema as of 2026-04-14.
4. **Surge anchors are fixed** — updating them requires a code change and a re-run. Fine for a reproducible baseline; would need to be parametrized for operational use.
5. **No UI exposure** — deliberate. If the frontend ever needs a validation panel, add a separate public `/api/val/summary/<storm_id>` endpoint that returns sanitized aggregate metrics only, and leave `/__val/` alone.

---

## Contact / next steps

Next session should (a) verify the dashboard URL loads successfully after the trailing-char fix, (b) sanity check Michael's scatter plot visually, then (c) move on to either a second benchmark storm or wiring metrics into the run ledger. The pipeline is ready; the question is which of those two yields more signal for CAT-team formula tuning.
