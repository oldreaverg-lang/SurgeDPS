# Phase 5 — Beta Data Layer Contracts

Backend handoff spec for the four Phase 5 items from `CAT_TEAM_PLAN.md §8`:

- **B7** — Rainfall overlay
- **E5** — Shelter capacity overlay
- **C6** — Claims routing vendor coverage *(future)*
- **E6** — Time-to-access estimate *(future)*

The frontend ships placeholder panels today behind a single
`localStorage` flag (`surgedps.betaDataLayers`, wired via
`ui/src/betaLayers.ts`). Each panel calls a stub fetcher that currently
returns `{ available: false, ... }`. Once a backend endpoint below is
live, flip the stub to a real `fetch()` call returning the same shape —
no further UI changes required.

TypeScript types for every response shape live in
`ui/src/betaLayers.ts`. Treat that file as the source of truth and
evolve it jointly with the backend.

---

## 1. B7 — Rainfall overlay

### Motivation
SurgeDPS currently models only coastal surge. Inland rainfall-driven
flooding is invisible in our hotspot ranking, which underweights
parishes that are wet from rain even when surge is modest. A raster
overlay lets the CAT lead visually correlate storm-total rainfall with
our surge footprint.

### v1 target source
MRMS (Multi-Radar Multi-Sensor) QPE composite, 1-km, storm-total
accumulation. Fallback: StormDPS rainfall service if it ships first.

### Endpoint
```
GET /surgedps/api/rainfall?storm_id={id}
```

### Response shape — `RainfallOverlay`
```ts
{
  available: boolean;
  source: 'mrms' | 'stormdps' | 'none';
  tileUrlTemplate: string | null;     // MapLibre {z}/{x}/{y} template
  validTime: string | null;           // ISO — when raster was generated
  bboxInches: [number, number] | null;// [min, max] for legend
  notes: string;                      // human-readable caveat
}
```

### Notes for backend
- Return the raster as pre-rendered PNG tiles so MapLibre can consume
  directly as a `raster` source — no client-side reprojection.
- `tileUrlTemplate` must include the `{z}/{x}/{y}` placeholders and any
  storm-scoping query params baked in.
- `bboxInches` drives the color-ramp legend; keep it consistent across
  time steps for a given storm so the legend doesn't flicker.
- Valid only for the active storm. The frontend re-fetches on storm
  change.

---

## 2. E5 — Shelter capacity overlay

### Motivation
The Emergency Manager persona needs to compare displaced-population
estimates (already computed by `stagingPlan()`) against real shelter
bed availability, not just a rule-of-thumb 1.10× buffer. This surfaces
under-capacity areas where mutual aid or additional shelter activation
is needed.

### v1 source candidates
- Red Cross Open API (authoritative for Red Cross shelters)
- FEMA OpenFEMA shelter registry
- State EM feeds where available (LA GOHSEP, FL DEM, TX TDEM)

The backend should blend these and dedupe on `(lat, lon, name)`.

### Endpoint
```
GET /surgedps/api/shelters?lat={f}&lon={f}&radius_km={n}
```
Alternatively accept `bbox=minLon,minLat,maxLon,maxLat`. Pick one and
document.

### Response shape — `ShelterCapacityLayer`
```ts
{
  available: boolean;
  shelters: Array<{
    id: string;
    name: string;
    lat: number;
    lon: number;
    capacity: number;
    occupancy: number | null;  // null = unknown, not zero
    operator: string;          // "Red Cross", county name, etc.
    isAccessible: boolean;     // ADA / medical needs
    isPetFriendly: boolean;
    lastUpdated: string | null;// ISO
    notes?: string;
  }>;
  totalCapacity: number;
  totalOccupancy: number | null; // null if any shelter occupancy unknown
  notes: string;
}
```

### Notes for backend
- Use `null` for unknown `occupancy`, never `0` — the UI differentiates
  "unknown" from "empty".
- `totalOccupancy` propagates `null` if any shelter is unknown; the UI
  shows "Occupancy partially reported" in that case.
- Prefer capacity > 0 filters at the API layer to keep payload small.

---

## 3. C6 — Claims routing vendor coverage

### Motivation
Once a CAT is declared, the CAT lead cold-calls vendors (water mit,
board-up, reconstruction) parish-by-parish to find available crews.
A per-vendor polygon layer lets us short-circuit that: highlight the
fraction of affected area already covered by a vendor we have a
contract with, and route claims accordingly.

### v1 target source
Either a static GeoJSON coverage file per national vendor
(ServiceMaster, BELFOR, ServPro, etc.) curated by ops, or a future
MCP connector that queries a vendor portal directly. Either is fine
as long as the response shape matches.

### Endpoint
```
GET /surgedps/api/vendor_coverage?storm_id={id}
```

### Response shape — `VendorCoverageLayer`
```ts
{
  available: boolean;
  vendors: Array<{
    vendorId: string;
    vendorName: string;
    specialties: Array<'water' | 'wind' | 'fire' | 'mold' | 'reconstruction'>;
    coveragePct: number;       // 0..100 of the affected area covered
    contactUrl: string | null;
    notes?: string;
  }>;
  notes: string;
}
```

### Notes for backend
- `coveragePct` should be computed server-side against the active
  storm's footprint (union of all hotspots at or above the display
  severity cutoff), not a static service-area percentage.
- If a vendor's polygon set is stale (> 90 days), annotate in `notes`.
- This is the place to eventually plug in the MCP connector
  referenced in the plan — shape stays identical.

---

## 4. E6 — Time-to-access estimate

### Motivation
After a storm, the question CAT/EM leads ask is "when can my
assessment team actually reach area #3?" That depends less on our
surge severity ranking and more on road-network reachability while
major arterials are inundated.

### v1 approach
Overlay SurgeDPS depth rasters onto OSM road centerlines; treat any
arterial above N feet as impassable to non-amphibious vehicles; ETA
is (depth-over-road duration) projected forward from current storm
time. If a state DOT closures feed exists, prefer it.

### Endpoint
```
POST /surgedps/api/time_to_access
Body: { storm_id: string, hotspot_ranks: number[] }
```

POST so the hotspot list can be passed without blowing up the query
string.

### Response shape — `TimeToAccessLayer`
```ts
{
  available: boolean;
  estimates: Array<{
    hotspotRank: number;
    etaHours: number | null;        // hours until likely accessible
    limitingFactor: 'surge' | 'road_closure' | 'debris' | 'unknown';
    confidence: 'low' | 'medium' | 'high';
    notes?: string;
  }>;
  generatedAt: string | null;
  notes: string;
}
```

### Notes for backend
- `etaHours === null` means "too uncertain to estimate" — UI shows
  a neutral "—" rather than a number.
- `confidence` is required so the UI can dim low-confidence rows.
- Re-compute at most every 15 minutes per storm; cache aggressively.

---

## Rollout plan

1. **Frontend (today)** — Beta panels live under the
   `🧪 Beta data layers` toggle in the More menu. Each panel renders
   its fetcher's `notes` string and a "Data layer pending" badge.
2. **Backend (one endpoint at a time)** — Ship `/rainfall` and
   `/shelters` first (highest-value). Vendor coverage and
   time-to-access can follow independently.
3. **Cut-over** — As each endpoint ships, swap the stub fetcher in
   `ui/src/betaLayers.ts` from the placeholder return to a real
   `fetch()` call, and remove the "pending" copy from the panel. No
   other frontend changes required.
4. **Graduation** — Once all four layers have real data and have
   been reviewed with a CAT lead and an EM, remove the beta flag and
   promote the panels into the default Ops Mode layout.

---

## Contact

See `CAT_TEAM_PLAN.md §8` for the original phased plan. Questions on
response shapes should go to whoever owns `ui/src/betaLayers.ts`.
