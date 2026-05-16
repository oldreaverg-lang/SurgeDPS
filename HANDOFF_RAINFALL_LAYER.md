# SurgeDPS Rainfall Layer + Timelapse Handoff

**Date:** 2026-05-16
**Status:** Mid-deploy. SurgeDPS Phase-5 self-heal running, ~2 of 15 storms processed
**Next agent:** read this doc end-to-end before touching code. Do not re-explore from scratch.

---

## 1. What this session was about

The user wanted three things, in this order:

1. **Florence audit-of-audit follow-through.** Closed out earlier — Florence audit v2 with rainfall internals + 5-storm comparison + persistence-pathway framing shipped to `StormDPS/audits/florence_2018/` at commit `4c134bc`.
2. **A persistence-pathway shadow score** that doesn't touch the StormDPS public-facing DPS formula. ChatGPT's reframing convinced everyone that **SurgeDPS *is* the embryonic Path B**, not a sibling needing to be built. Result: pivoted from "write a new shadow score" to "make SurgeDPS's existing rainfall pathway actually work end-to-end."
3. **A rainfall toggle layer + accumulation timelapse with play button** on SurgeDPS's per-property HAZUS damage map. Already mostly built in source; needed deploy + fixes.

The shadow-score work is **deferred** until SurgeDPS infrastructure is fully working. Don't start it yet.

---

## 2. Architecture (don't get confused by this — I did, repeatedly)

```
┌──────────────────────────────────────────────────────────────────────┐
│ stormdps.com           (StormDPS Railway service)                   │
│   ├── FastAPI main.py serving the public DPS site                  │
│   ├── /surgedps         ← serves a STATIC COPY of SurgeDPS's SPA   │
│   └── /surgedps/api/*   ← 404s deliberately (was moved)            │
│                                                                      │
│ surgedps-production.up.railway.app  (SurgeDPS Railway service)      │
│   ├── api_server.py (BaseHTTPRequestHandler, not FastAPI)          │
│   │     ├── /api/storms, /api/cell, /api/rainfall, /api/qpf, etc. │
│   │     └── /__val/*    ← token-gated admin/debug namespace       │
│   ├── warm_cache.py     ← Phases 1–5 prewarm on container start    │
│   └── storm_monitor.py  ← polls NHC every 30 min for live storms   │
└──────────────────────────────────────────────────────────────────────┘
```

**Two separate repos, two separate Railway projects.** They are NOT a monorepo:

| Repo path | Remote | Triggers Railway redeploy |
|---|---|---|
| `C:\Users\Ryan\APPS\StormDPS\` | github.com/oldreaverg-lang/StormDPS | StormDPS service (stormdps.com) |
| `C:\Users\Ryan\APPS\SurgeDPS\` | github.com/oldreaverg-lang/SurgeDPS | SurgeDPS service (surgedps-production...) |

**Frontend deploy pattern** (this confused me; don't repeat the mistake):
- SurgeDPS source is in `SurgeDPS/ui/src/App.tsx`
- `npm run build` outputs to `SurgeDPS/ui/dist/`
- The built assets must be **manually copied** to `StormDPS/frontend/surgedps/` AND `StormDPS/frontend/surgedps/index.html` updated with the new hash filenames
- StormDPS commit + push deploys the static SPA shell on stormdps.com
- The SurgeDPS-side `api_server.py` ALSO has its own copy of the frontend (Dockerfile builds it independently), so direct hits to `surgedps-production.up.railway.app` work too
- The user views the SPA at `stormdps.com/surgedps` which proxies API calls through to `surgedps-production...` via Cloudflare (or similar routing — I never fully verified the proxy layer, but it works)

**StormDPS uses git worktrees.** The main checkout is at `C:\Users\Ryan\APPS\StormDPS\` on branch `main`. Worktrees live under `.claude/worktrees/<name>/`. This conversation worked from `vibrant-bartik-510df5`. To push to `origin/main`, use:
```bash
git push origin claude/vibrant-bartik-510df5:main
```
Do NOT try `git checkout main` in the worktree — it'll fail because main is already checked out elsewhere.

---

## 3. What's currently deployed

| Repo | Commit | What it ships | Status |
|---|---|---|---|
| SurgeDPS | `ad6ad5e` | `/__val/inventory.html` debug dashboard | ✅ Live |
| SurgeDPS | `6f3bc92` | MRMS timelapse milestone TIFs + `/api/rainfall_frames` + `&hour=N` tile param | ✅ Live |
| SurgeDPS | `f900da3` | App.tsx slider (initial version, window-relative labels) | Superseded |
| SurgeDPS | `2b54cad` | NWIS bbox clamp 8°→6° + polite sleep 1s→3s + inventory gauges path fix | ✅ Live |
| SurgeDPS | `e0134b0` | `/api/qpf` refuses historical storms + `/api/rainfall_frames` returns landfall context | ✅ Live |
| SurgeDPS | `eda0041` | App.tsx slider v2: play/pause/landfall buttons + landfall-relative labels | ✅ Live |
| SurgeDPS | `aa334e1` | warm_cache.py Phase-5 self-heal: detect pre-milestone TIFs, purge, re-fetch | ✅ Live (running NOW) |
| StormDPS | `c71bcdd` | First slider deploy (asset swap `index-Bzmm4bjd.js` + CSS) | Superseded |
| StormDPS | `a509c90` | Slider v2 asset swap `index-BWR4wTGq.js` + `index-C0_aMSMh.css` | ✅ Live |

**As of last check (04:10 UTC):** Phase 5 of warm_cache.py has self-healed ~2 of 15 storms (Harvey done, Florence pending). `mrms_mb` has grown from 360.9 → 386.89 MB. Estimated ~15–20 min until Phase 5 completes.

---

## 4. Active issues

### 4.1 NWIS gauges still failing despite bbox clamp

**What we see:** Inventory shows `gauges.fetch_error: true` for Harvey and Florence (and presumably others). The bbox clamp in `_NWIS_BBOX_MAX_RADIUS_DEG = 3.0` (in `src/rainfall/ahps_historical.py`) should have brought the bbox to 6°×6° (under USGS's 7° limit). But it's still failing.

**Hypotheses (untested):**
- USGS may still be rate-limiting our IP from the earlier abuse cascade (8°×8° bbox triggered HTTP 400s, then Errno 104 resets across 17 storms in 20s)
- Some other NWIS parameter is being rejected
- Phase 2 may have run before the bbox clamp took effect (it didn't — clamp was in `2b54cad`, pushed earlier)

**Next action:** wait for the current Phase 5 to finish, then check Railway logs for actual NWIS HTTP codes + body previews. The `_get_json` helper in `ahps_historical.py` line 477-480 logs the body for non-200 responses. Look for that.

### 4.2 FEMA NFHL Phase 3 still at 0 MB

We didn't actually fix Phase 3. We only fixed Phase 2 (gauges) and hoped Phase 3 might improve indirectly. The inventory still shows `flood_zones_mb: 0.0` and `flood_zones.cached: 0/16` for every storm.

FEMA's `hazards.fema.gov` is notoriously flaky with TLS errors + slow timeouts. The retry logic in `warm_cache.py` Phase 3 uses 45s timeouts per tile, 16 tiles per storm, no exponential backoff. Likely silent failures.

**Next action:** unblocked after Phase 5 completes. Check logs for FEMA-side errors.

### 4.3 `/api/rainfall_frames` LRU-miss bug

The endpoint only checks `_rainfall_tif_by_storm` (in-memory LRU). It's empty after every restart until `/api/rainfall` is hit (which requires an `_active_storm` context). So **direct curl-style verification fails** even when the TIF is on disk.

In normal user flow this isn't a problem — SPA activates storm → calls `/api/rainfall` → registers TIF → calls `/api/rainfall_frames` → works.

But for debug/scripting it's annoying. Fix: in `rainfall_frames` handler, when LRU misses, compute the expected `iem_<key>.tif` path from the catalog entry and check disk directly. If found, register in LRU + return frames.

**Path I sketched (in conversation but didn't ship):**
```python
# In /api/rainfall_frames handler, after the LRU lookup miss:
storm_entry = _historical_index.get(storm_id)
if storm_entry and getattr(storm_entry, 'landfall_date', None):
    from rainfall.mrms_fetcher import iem_cache_key, storm_bbox_from_catalog_entry
    valid_time = datetime.strptime(storm_entry.landfall_date, '%Y-%m-%d').replace(
        hour=18, tzinfo=timezone.utc) + timedelta(hours=48)
    bbox = storm_bbox_from_catalog_entry(
        storm_entry.landfall_lat, storm_entry.landfall_lon, buffer_deg=4.0)
    ck = iem_cache_key(valid_time, 72, bbox)
    possible_tif = os.path.join(PERSISTENT_DIR, 'mrms', f'iem_{ck}.tif')
    if os.path.exists(possible_tif):
        with _rainfall_tif_lock:
            _lru_set(_rainfall_tif_by_storm, storm_id, possible_tif)
        base_tif = possible_tif
```

Not critical — defer until other issues are resolved.

### 4.4 Catalog scope mismatch (deferred)

User said earlier they wanted ~200 storms (2015–2025+). SurgeDPS catalog has 15 hand-curated + 32 season-auto storms = 47 total (per the Phase 5 log from earlier: "Found 47 storms"). Persistence-pathway corpus is a DIFFERENT concept — a future shadow score doesn't need full SurgeDPS HAZUS pipeline per storm.

Don't expand the catalog yet. Get the existing 15-47 working first.

---

## 5. What to expect over the next 24 hours

**0–30 min from now:** Phase 5 finishes. `mrms_mb` settles at ~540 MB. All 15 storms have milestone frames.

**30 min – 2 hours:** Stable state unless someone investigates Phase 2 (NWIS) or Phase 3 (FEMA NFHL).

**2 hours – 24 hours:** No spontaneous changes. `storm_monitor.py` polls NHC every 30 min, writes nothing (no active Atlantic storms in May 2026 — season starts ~mid-June).

**Beyond 24 hours:** Once Atlantic season starts, live storms will exercise the full pipeline — including the per-hour MRMS Pass1 fetch path that's the natural source of timelapse frames for live events. `storm_monitor.py` already has the hook; just nothing to actually monitor yet.

---

## 6. How to verify the deploy worked (concrete URLs)

The user shared the `VALIDATION_TOKEN` value during the session. It's set in Railway → SurgeDPS service → Variables. To get the value: ask the user or check the Railway dashboard's Variables tab.

```
TOKEN=<from user or Railway>
BASE=https://surgedps-production.up.railway.app
```

| What | URL | Expected (post-Phase-5) |
|---|---|---|
| Token status | `$BASE/__val/__status` | `token_env_set: true` |
| Token + your value | `$BASE/__val/__status?t=$TOKEN` | `token_matches: true` |
| Inventory JSON | `$BASE/__val/inventory.json?t=$TOKEN` | `mrms_mb: ~540`, every storm's `mrms.valid_time` shows today's date |
| Inventory dashboard | `$BASE/__val/inventory.html?t=$TOKEN` | Visual: 15 storm rows, status pills, filter bar |
| Slider UI test | `stormdps.com/surgedps` → click Harvey → 🌧️ Rain | Slider with ▶ Play / ⏮ Landfall / Full buttons appears |
| Florence-specific check | Same flow, click Florence | Same slider behavior |

**If `/api/rainfall_frames?storm_id=harvey_2017` returns `no_rainfall_cached_yet`:** that's the LRU-miss issue (§4.3). The frames ARE on disk; just open the storm in the UI first.

---

## 7. Key files modified this session

### SurgeDPS

| File | Purpose | Key changes |
|---|---|---|
| `src/rainfall/mrms_fetcher.py` | MRMS fetching + parsing | Added `_write_timelapse_frame()`, `iem_cache_key()`, `list_timelapse_frames()`; modified `fetch_iem_historical` to write milestone TIFs during accumulation |
| `src/rainfall/ahps_historical.py` | AHPS gauge fetching | Added `_NWIS_BBOX_MAX_RADIUS_DEG = 3.0` and clamp logic |
| `scripts/api_server.py` | HTTP server | Added `/api/rainfall_frames`, `&hour=N` param on `/api/rainfall_tile`, historical-storm guard on `/api/qpf`, landfall context in frames response |
| `scripts/warm_cache.py` | Container-start prewarm | Phase 2 polite sleep 1s→3s + error counting; Phase 5 self-heal logic |
| `src/validation/private_routes.py` | `/__val/*` dispatcher | Added inventory routes |
| `src/validation/debug_inventory.py` | NEW — cache inventory builder + HTML dashboard | Read-only filesystem walker, 600+ lines |
| `ui/src/App.tsx` | React frontend | Slider state + fetch effect + play controls + landfall-relative labels + Forecast tab hidden on historical |

### StormDPS

| File | Purpose | Key changes |
|---|---|---|
| `frontend/surgedps/index.html` | SPA shell | Updated to hashed JS/CSS filenames (currently `index-BWR4wTGq.js` + `index-C0_aMSMh.css`) |
| `frontend/surgedps/assets/*` | Vite build artifacts | Asset swaps mirror SurgeDPS `npm run build` output |
| `audits/florence_2018/*` | Florence audit v2 | 5-storm comparison + rainfall internals + persistence-pathway framing (commit `4c134bc`) |

---

## 8. User preferences / constraints (read these)

These are codified in the user's `~/.claude` MEMORY.md and have been consistently honored:

1. **Commit straight to main on StormDPS.** No worktree isolation, no PRs — direct push deploys via Railway.
2. **Claude handles git push.** Don't ask for confirmation on normal merges + pushes.
3. **User sleeps in 1-hour increments.** Do NOT suggest "stopping points" or "let's wrap up" or "get some rest." They sleep when they sleep; don't manage their time.
4. **Don't over-document or over-justify after rejection.** When they say no, brief acceptance + move on. Long mea-culpa docs read as still insisting.
5. **Before adding defer/async, grep inline JS for top-level lib refs.** Has caused silent inline-script halts via `Chart.register(...)` at top level.
6. **`addEventListener` leaks the event object as first arg.** Wrap with `() => fn()` when fn has meaningful positional args.

The user is technically competent and patient. They like architectural reasoning, not pep talks. They'll push back if your plan is wrong; trust them.

---

## 9. Recommended next actions (in priority order)

When you pick this up:

1. **Verify Phase 5 finished.** Hit `/__val/inventory.json?t=...` and confirm `mrms_mb` is around 540 MB and every storm's `mrms.valid_time` shows a 2026-05-16 timestamp.
2. **Verify the slider works in the UI.** stormdps.com/surgedps → Florence → 🌧️ Rain. Look for ▶ Play / ⏮ Landfall / Full buttons. Test that play animates through post-landfall frames.
3. **Investigate Phase 2 (NWIS gauges) still failing.** Pull Railway logs from the current deploy, look at the `[gauges]` lines and the `NWIS/NWPS HTTP` warnings. If body preview shows a specific parameter rejection, fix the URL construction. If it's a generic 429-style block, give the IP another 12 hours to cool off and re-test.
4. **Investigate Phase 3 (FEMA NFHL) still failing.** Same drill — Railway logs from `[fz]` lines.
5. **Fix the `/api/rainfall_frames` LRU-miss bug** (§4.3) so direct API hits work. Small defensive change.
6. **THEN** — only after debug pipeline is fully working — return to the persistence-pathway shadow score discussion from earlier in the conversation. The Florence audit motivated it; SurgeDPS is the home; the actual scoring module hasn't been started.

Do NOT start the shadow score until items 1–5 are clean.

---

## 10. Pitfalls I hit so I don't repeat them

- **`python` is not on PATH on this Windows machine.** Use `C:\Python314\python.exe` explicitly.
- **Node was not installed.** Installed via `winget install OpenJS.NodeJS.LTS`. After install, a fresh shell is needed — or refresh PATH via `$env:Path = [System.Environment]::GetEnvironmentVariable('Path','Machine') + ';' + [System.Environment]::GetEnvironmentVariable('Path','User')`.
- **Bash heredocs choke on Python with single-quoted strings.** Write the Python to a `.py` file first instead of inlining.
- **`git checkout main` fails in worktrees** (main is held by another worktree). Use `git push origin claude/<branch>:main` instead.
- **Don't `cd` into worktree subdirs and forget you're there.** The shell cwd persists across Bash tool calls. Always print `pwd` if unsure.
- **Windows file encoding default is cp1252.** Python `ast.parse(f.read())` will choke on em-dashes etc. — use `open(path, encoding='utf-8')`.
- **`min(iterable, key=...)` ties-breaks to the FIRST equal value.** I had to add an at-or-after filter before `min()` to make `landfall_frame_hour` pick post-landfall on ties.
- **HANDOFF.md in SurgeDPS is from April and is stale on architecture** (says SurgeDPS API is mounted inside StormDPS at /surgedps/api/*; that was removed in commit `f08ea1f`). Trust the live code, not that doc.

---

## 11. Conversation thread summary (for context)

The session arc was roughly:

1. Florence audit-of-audit work (closed at `4c134bc`)
2. ChatGPT review proposed dual-pathway architecture for StormDPS DPS formula
3. We deferred that in favor of "SurgeDPS is already Path B in embryo" framing
4. Discovered SurgeDPS has full rainfall layer + MRMS pipeline already built — just stale-deployed
5. Built `/__val/inventory.*` debug dashboard
6. Built MRMS timelapse milestone frames + frontend slider
7. Diagnosed NWIS bbox-too-large bug → clamped to 3°
8. User saw QPF showing wrong data on historical Florence → fixed `/api/qpf` to refuse + relabeled slider with landfall-relative times + added Play/Pause/Landfall buttons
9. Realized existing 15 storms have pre-milestone TIFs that won't naturally evict (no eviction policy on mrms dir)
10. Added self-heal to warm_cache.py Phase 5
11. Phase 5 currently running through 15-storm re-fetch
12. Next agent picks up here.
