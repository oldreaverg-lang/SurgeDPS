# SurgeDPS — CAT Team & Emergency Manager Specialization Plan

**Author:** Agent (roleplaying CAT Team Deployment Lead)
**Date:** 2026-04-10
**Status:** Draft for review (no code changes yet)

---

## 1. Framing: Who are we building for?

SurgeDPS today is a **post-storm damage analysis tool**. It answers "how bad was it, and where?" with modeled losses, hotspots, and per-building reports. That's a great technical foundation, but it's framed as an *estimating* tool — it competes with Xactimate, HAZUS, and CoreLogic on their turf (precise dollar amounts) without their field-verified data behind it.

**This plan repositions SurgeDPS as first-48-hour catastrophe intelligence** — not "what will each claim pay out" but **"where do I send my people, how many, and for how long."** That's a gap nobody is filling well, and it's where the current hotspots + severity + peril data already shines.

Two primary user types, with overlapping but distinct needs:

| Role | Core question | Time horizon | Pain point |
|---|---|---|---|
| **Insurance CAT / CRT Team** | Where do I deploy adjusters, how many, and for how long? | T+0 → T+72h | Wasted truck rolls; flood vs wind coverage confusion; overwhelm from CAT 4+ events |
| **Emergency Manager / Planner** | Where do I stage resources, open shelters, and message the public? | T-48h → T+14d | Pre-positioning blind spots; critical facility exposure; mutual-aid sizing |

Both need the same underlying intelligence (hardest-hit areas, severity mix, critical facilities, peril breakdown). They diverge on the **verbs** — deploy, report, export *(CAT)* vs. stage, shelter, brief *(EM)*.

---

## 2. Current UI inventory (as of HEAD)

The `DashboardPanel` right-rail has, top to bottom:

1. Compact header (storm name, category, close/expand)
2. Storm info card (wind, pressure, year, county population)
3. Confidence badge (data coverage quality)
4. Critical Facilities in Surge Zone
5. Total Modeled Loss scoreboard
6. Damage Severity Breakdown (severe → none)
7. Nuisance Flood Warning (conditional)
8. Hardest-Hit Areas (top hotspots, fly-to)
9. Map Coverage status
10. Building Damage legend

The per-building **Claims Documentation Report** (HTML export, line ~1275) also contains: Property ID, Damage Assessment, Loss Estimate, FEMA IHP, Peril Attribution, Comparable Properties, Field Assessment, Storm Parameters.

What's **already strong** and should stay:
- Hotspots with fly-to (aggregation by loss)
- Peril attribution (wind vs water) — just buried inside the per-building report
- Critical facilities in surge zone (huge for CAT + EM)
- Confidence badge (rare in tools like this — keep and extend)
- Nuisance flood flag (aggregate intuition)

What's **missing** for CAT/EM use:
- No area-level peril aggregation — wind/water split only exists per building
- No "deployment" verb — tool ends at analysis, not action
- No adjuster/responder count recommendations
- No "time to clear" estimates
- No Ops Mode framing (CAT-only vocabulary today, unfriendly to EM)
- Export is per-building only; no CAT Deployment Report
- "$X.XM" false-precision numbers across the panel

---

## 3. Mode toggle: Analyst vs. Ops

The simplest way to serve both personas without forking the tool is a **persistent Mode toggle** in the header, next to the storm name.

```
  [SurgeDPS]    Katrina (2005) CAT 5      [ Analyst | Ops ]   ☰
```

- **Analyst Mode** (default): today's experience — loss dollars, severity %, technical vocabulary. The "estimator" view.
- **Ops Mode**: deployment-focused — counts of people/resources, "priority areas," plain-language urgency. Hides dollar figures by default, surfaces adjuster/responder counts.

Mode is stored in `localStorage` (NOT session storage per the tool rules — use an in-memory `useState` backed by a small persistence wrapper, or skip persistence entirely for v1). The toggle never hides data irreversibly; it's a presentation layer over the same `impactTotals`, `severityCounts`, `hotspots`, and `criticalBreakdown` state that already drives the DashboardPanel.

**Sub-personas within Ops Mode** (future, not v1): a secondary pill selector for `[ Insurance CAT | Emergency Mgr ]` — changes the specific panel labels but not the data. v1 can just ship Ops Mode as a single shared surface and gather feedback before splitting.

---

## 4. Feature catalog — Relevance matrix

Categorized per the user's request: features relevant to both, CAT-only, and EM-only.

### 4a. Relevant to BOTH (build first)

| # | Feature | Source |
|---|---|---|
| B1 | **CAT Deployment / Response Summary panel** (top of panel, replaces scoreboard in Ops Mode) | New |
| B2 | **Area-level peril aggregation** — wind% vs water% per hardest-hit area | Aggregate existing `windWaterSplit` over hotspots |
| B3 | **Severity → workload translation** — "X buildings likely uninhabitable, Y need inspection" | Derived from `severityCounts` |
| B4 | **Critical facilities rollup** — already exists, just promote position in Ops Mode | Existing `criticalBreakdown` |
| B5 | **Confidence indicators everywhere** — every number gets a small `±` or confidence pip | Extend existing `confidence` state |
| B6 | **Remove false precision** — round `$X.XM` to "~$80M" or "Tens of $M"; round building counts to nearest 100 above 1,000 | Presentation-layer formatter |
| B7 | **Rainfall overlay** — inland flooding layer (where SurgeDPS only shows surge today) | Integrate with StormDPS rainfall service or MRMS; new map layer |
| B8 | **Export: "Situation Report" PDF** — one-page, mode-aware, timestamped | New export path alongside existing Claims Doc |

### 4b. Relevant to CAT TEAM only

| # | Feature | Source |
|---|---|---|
| C1 | **Adjuster recommendation per area** — "Top priority: St. Bernard Parish — 12 adjusters, 5 days" | Derived from severity counts × industry staffing heuristics (1 adjuster / ~20 severe, / ~40 major, / ~80 moderate per day) |
| C2 | **Flood vs Wind claim routing hint** — "NFIP primary" / "HO3 primary" / "Mixed — dual-route" tag per hotspot | From area peril aggregation (B2) |
| C3 | **"X adjusters → Y days" planning simulator** (the differentiator) — slider: "I have 40 adjusters, 10 days" → shows which areas get fully covered, which don't | Derived, pure-client computation over hotspots |
| C4 | **CAT Deployment Report** — multi-page PDF: summary, top 5 areas with adjuster counts, peril mix, confidence caveats, contact template for claims routing vendors | Extends B8 |
| C5 | **"Time to Clear" estimate** at the storm level | Sum of per-area workload / configured adjuster throughput |
| C6 | **Claims routing coverage hint** — which areas have known repair vendor coverage (future MCP integration) | Placeholder panel for v1 |

### 4c. Relevant to EMERGENCY MANAGER only

| # | Feature | Source |
|---|---|---|
| E1 | **Shelter-in-place vs evacuate indicator per hotspot** — based on surge depth + critical facility mix | Derived |
| E2 | **Resource staging suggestion** — "Stage generators at [nearest unflooded critical facility]" | Derived from critical facility layer outside surge zone |
| E3 | **Mutual aid sizing** — "Request N swift-water rescue teams" based on residential pop in surge + severity mix | Derived from `estimatedPop` + severityCounts |
| E4 | **Public messaging draft** — auto-generated plain-language advisory ready to post | LLM-generated template, pre-filled |
| E5 | **Shelter capacity overlay** — Red Cross / county shelter capacity vs displaced pop estimate | New data source (placeholder for v1) |
| E6 | **"Time to access" estimate** — when roads are likely clear enough for assessment teams | Derived from depth × road network (v2) |

Color-coding in the UI once implemented:
- 🔵 **Both** (blue left-border on panel section)
- 🟠 **CAT only** (orange left-border)
- 🟢 **EM only** (green left-border)
- Sections hidden in the wrong mode rather than greyed out.

---

## 5. Where each feature lands in the current UI

Annotated layout of `DashboardPanel` with proposed inserts. Existing panels in plain text, new inserts in **bold**, moves marked `→`.

```
┌─────────────────────────────────────────────────────┐
│  [SurgeDPS]  Katrina (2005) CAT 5  [Analyst|Ops] ☰ │  ← MODE TOGGLE (§3)
├─────────────────────────────────────────────────────┤
│  Storm info card (wind, pressure, year, pop)       │  unchanged
│                                                     │
│  ┌ CAT DEPLOYMENT SUMMARY ──────────────── Ops ┐  │  **NEW (B1)** — shown in Ops Mode only
│  │ 🌊 Major surge event — CAT 5 landfall        │  │     replaces "Total Modeled Loss" panel
│  │ Exposure: ~450k residents in surge zone      │  │
│  │ Peril mix: 🌊 68% water · 🌬️ 32% wind         │  │     **(B2)**
│  │ ~8,200 buildings need inspection             │  │     **(B3)**
│  │ ~1,400 likely uninhabitable                  │  │
│  │ Top priority: St. Bernard Parish             │  │     **(C1)** — CAT only
│  │   🚗 12 adjusters · ~5 days                   │  │
│  │   Claims routing: NFIP primary                │  │     **(C2)**
│  │ Data confidence: ▓▓▓▓░ Medium                 │  │     **(B5)**
│  │ [ Generate CAT Report ↓ ]                     │  │     **(C4)** — CAT only
│  │ [ Generate SitRep ↓ ]                         │  │     **(B8)** — EM mode label
│  └──────────────────────────────────────────────┘  │
│                                                     │
│  Critical Facilities in Surge Zone                 │  → MOVED UP in Ops Mode (B4)
│  Confidence badge                                   │  unchanged
│                                                     │
│  ┌ TOTAL MODELED LOSS ────────────── Analyst ┐    │  Hidden in Ops Mode (B6)
│  │ ~$80M (tens of millions)                  │    │  Rounded, not $81.3M
│  └────────────────────────────────────────────┘    │
│                                                     │
│  Damage Severity Breakdown                         │  unchanged, but Ops Mode adds
│                                                     │  "X need inspection" footer (B3)
│  Nuisance Flood Warning (conditional)              │  unchanged
│                                                     │
│  Hardest-Hit Areas                                  │  ENHANCED — each row gains:
│    #1  St. Bernard Parish   ~$22M   1,400 bldgs    │     🌊 68% · 🌬️ 32%     (B2)
│    #2  Plaquemines Parish   ~$14M     820 bldgs    │     CAT: 8 adjusters · 4 days  (C1)
│    ...                                              │     EM:  Evacuate / Shelter    (E1)
│                                                     │
│  ┌ DEPLOYMENT PLANNER ────────────── CAT only ┐   │  **NEW (C3)** — collapsible
│  │ You have: [ 40 ] adjusters · [ 10 ] days      │ │     placed below Hardest-Hit
│  │ Coverage: ████████░░ 82%                       │ │
│  │ ✓ Areas 1–4 fully covered                      │ │
│  │ ⚠ Area 5 partial (6 days needed, 4 available) │ │
│  │ ✗ Areas 6–8 uncovered                          │ │
│  └─────────────────────────────────────────────┘ │
│                                                     │
│  ┌ RESOURCE STAGING ───────────────── EM only ┐   │  **NEW (E2, E3)** — collapsible
│  │ Stage generators: Belle Chasse EOC (dry)   │   │
│  │ Request 4 swift-water rescue teams          │   │
│  │ Est. displaced pop: ~12,400                 │   │
│  │ [ Draft public advisory ↓ ]                  │   │  **(E4)**
│  └────────────────────────────────────────────┘  │
│                                                     │
│  Map Coverage                                       │  unchanged
│  Building Damage legend                             │  unchanged
└─────────────────────────────────────────────────────┘
```

### Map layer additions

- **Rainfall / inland flood overlay** (B7) — toggle in the existing basemap/more menu. Pulls from MRMS or StormDPS's rainfall service.
- **Deployment pin layer** (CAT Ops): each recommended priority area gets a numbered flag with adjuster count on hover.
- **Staging pin layer** (EM Ops): generators, shelters, rescue staging as icon markers, distinguishable from critical facilities.

### Claims Documentation Report → stays

The per-building HTML report stays untouched — it's still useful for Analyst Mode and individual adjusters on the ground. The new CAT Deployment Report is additive, not replacing.

---

## 6. Feature detail specs

### B1. CAT Deployment Summary panel

Replaces the Total Modeled Loss scoreboard as the top analytics panel in Ops Mode. In Analyst Mode, loss scoreboard stays where it is.

Contents (top to bottom):

1. **Headline** — one-line summary: event severity + "deploy now / prepare / monitor" recommendation. Drives from `storm.category` × `severityCounts.severe+major` × `criticalCount`.
2. **Exposure overview** — residents in surge zone + critical facility count.
3. **Peril mix bar** — reuse the `.peril-bar` CSS from the Claims Report. Computed by summing `windWaterSplit` across the hotspot footprint, weighted by building count.
4. **Workload line** — "X need inspection · Y likely uninhabitable" from `severityCounts`.
5. **Top priority area callout** — pulls the #1 from `hotspots`, adds adjuster count + routing hint (CAT) or evac posture (EM).
6. **Data confidence pip row** — surge ▓▓▓▓▓, building inventory ▓▓▓░░, population ▓▓▓▓░ — gives users a visible honesty signal.
7. **Action buttons** — "Generate CAT Report" (CAT) or "Generate SitRep" (EM).

### C1. Adjuster recommendation heuristic

Rough industry staffing ratios (source: conversations with CAT managers; tune later):

- 1 adjuster / day clears ~15 **severe**, ~25 **major**, ~40 **moderate**, ~60 **minor** inspections.
- Per-area `required_adjuster_days = severe/15 + major/25 + moderate/40 + minor/60`.
- Default assumption: 8-hour days, 5-day rotations. Both tunable in the Deployment Planner (C3).

Show as "`N adjusters · D days`" where `N × D ≈ required_adjuster_days`, with a sensible pairing (prefer round numbers, max N = team size from simulator).

### C2. Flood vs Wind routing hint

Thresholds (from area-level wind/water split):
- water ≥ 70% → **NFIP primary** (flood carrier)
- wind ≥ 70% → **HO3 primary** (standard homeowners)
- 30–70% either way → **Mixed — dual-route** (both carriers in play; flag for complex claims)

Displayed as a colored tag on each hotspot row and on the CAT Deployment Summary.

### C3. Deployment Planner ("X adjusters → Y days")

**This is the differentiator.** Pure client-side. State:

```ts
const [teamSize, setTeamSize] = useState(20);
const [windowDays, setWindowDays] = useState(7);
```

Compute: for each hotspot in descending priority, allocate adjuster-days until `teamSize × windowDays` is exhausted. Mark each area as `covered | partial | uncovered`. Show a coverage bar + per-area checklist.

Bonus: "**Suggest a team size**" button that back-solves for full coverage — "You need ~54 adjusters over 7 days for full coverage."

### E1. Shelter-in-place vs evacuate indicator

Per hotspot:
- Max surge > 6 ft AND critical facility count > 0 → **Evacuate**
- Max surge 3–6 ft → **Shelter in place (upper floors)**
- Max surge < 3 ft → **Shelter in place**

### E4. Public advisory draft

Template-filled, not LLM for v1 (LLM can be added later via the brief skill). Example:

> ⚠️ **[Storm Name]** — Category [X] — [Landfall Time]
> Surge up to [depth] ft expected in [top 3 parishes].
> Residents in surge zones: [action] immediately.
> Critical facilities impacted: [N hospitals, M fire stations].
> Shelter locations: [list].
> Next update: [time].

Output as a copyable text block + a "Copy to clipboard" button.

---

## 7. False-precision cleanup (B6)

Audit of current display strings and proposed replacements:

| Today | Ops Mode | Analyst Mode |
|---|---|---|
| `$81.3M` | `~$80M` | `$81.3M` *(kept)* |
| `1,427 buildings` | `~1,400 buildings` | `1,427` *(kept)* |
| Avg loss `$57,293` | *(hidden)* | `$57,000` *(rounded)* |
| `12.3 ft depth` | `~12 ft` | `12.3 ft` |

Rationale: CAT managers routinely **mistrust** precise numbers from models ("if you're telling me it's $81.3M, I know you're guessing"). Rounded numbers with explicit confidence indicators are trusted more than decimals.

---

## 8. Implementation roadmap

### Phase 1 — Foundations (1–2 days of work)
1. Add **Mode toggle** (§3) — header component + context/state. No feature gating yet.
2. **False-precision cleanup** (B6) — presentation helper `formatOps()`, applied to key panels.
3. **Confidence pips** (B5) — extend `confidence` state with sub-components (surge, buildings, pop).

### Phase 2 — CAT Deployment Summary (2–3 days)
4. **Area-level peril aggregation** (B2) — extend hotspots calculation in `App.tsx`.
5. **CAT Deployment Summary panel** (B1) — new component replacing scoreboard in Ops Mode.
6. **Adjuster recommendation heuristic** (C1) — pure-function module.
7. **Routing hint tags** (C2) on hotspot rows.
8. **Severity → workload translation** (B3) — footer strings in severity panel.

### Phase 3 — Differentiator + exports (2–3 days)
9. **Deployment Planner simulator** (C3).
10. **CAT Deployment Report** (C4) export — reuse the HTML-to-print pattern from Claims Doc.
11. **SitRep export** (B8) — same pipeline, different template.
12. **Time to Clear** (C5) — one-line summary at the bottom of the summary panel.

### Phase 4 — EM specialization (3–5 days)
13. **Shelter/evac indicator** (E1) on hotspot rows.
14. **Resource Staging panel** (E2, E3).
15. **Public advisory draft** (E4).
16. Sub-persona toggle inside Ops Mode: `[Insurance CAT | Emergency Mgr]`.

### Phase 5 — Data layer enhancements (parallel, longer-running)
17. **Rainfall overlay** (B7) — requires backend work (MRMS or StormDPS rainfall integration).
18. **Shelter capacity overlay** (E5).
19. Future: claims routing vendor coverage (C6), time to access (E6).

---

## 9. Risks and honest caveats

- **Heuristic trust** — adjuster-per-severity ratios are made up until we talk to real CAT managers. Ship the simulator with the ratios editable in the UI so sophisticated users can override.
- **Rainfall overlay scope creep** — surge + rainfall is a large lift. Scope v1 to surge only, with rainfall as a v2 feature behind a feature flag.
- **Mode toggle discoverability** — A/B test placement. If users don't find it, promote to a full header bar.
- **"CAT Report" legal exposure** — any export labeled "Deployment Report" will be read by non-technical users. Every page needs the existing "MODELED ESTIMATE — NOT FIELD VERIFIED" disclaimer, prominent on page 1, not buried at the bottom.
- **EM persona is aspirational in v1** — without real EM user interviews, the EM features are informed guesses. Consider parking E1–E6 behind a "beta" flag until we validate with at least two EM contacts.

---

## 10. What this plan does NOT do (intentional)

- **No Xactimate-style estimating.** We resist the temptation to price individual claims. Per-building Claims Documentation Report already does enough of that.
- **No real-time forecasting.** SurgeDPS stays post-storm analysis. Forecasting belongs to StormDPS.
- **No account/permission system.** Modes are per-device, not per-user. If enterprise adoption happens, add auth then.
- **No mobile app.** Ops Mode must be responsive, but the primary form factor is still desktop in a CAT ops center.

---

## 11. Open questions for Ryan

1. Do we ship the Ops/Analyst toggle in the header, or as a setting in the ☰ menu? I'd argue header — discoverability.
2. Are the adjuster-per-severity ratios in §C1 reasonable, or should we pull numbers from an actual CAT manager contact first?
3. Is the EM persona a real target or aspirational for a future pitch deck? That decides whether Phase 4 is v1 or v2.
4. Rainfall overlay (B7) — are we comfortable depending on MRMS / StormDPS backend integration, or should SurgeDPS stand alone?
5. Should the "CAT Report" PDF be a hard PDF (via the `pdf` skill) or an HTML print-friendly like the current Claims Doc? (I recommend HTML for v1 — faster, matches existing pattern.)

---

**Next step on your go:** Start with Phase 1 (mode toggle + false-precision cleanup + confidence pips) since it's low-risk and immediately visible, then move to B1/B2 for the CAT Deployment Summary panel as the first big feature.
