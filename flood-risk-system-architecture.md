# Real-Time Flood Risk Mapping System — Technical Architecture

## Executive Summary

This document describes a complete serverless geospatial system for real-time hurricane flood risk mapping. The system operates in two modes: a near-zero-cost **Monitoring Mode** that watches for NOAA storm advisories, and an **Analysis Mode** that spins up on-demand compute to ingest data, run flood models, generate map tiles, and serve them to a MapLibre GL JS frontend. The architecture is designed to cost under $20/month when idle while scaling elastically during active storms.

---

## 1. System Architecture Overview

### 1.1 Operating States

**Monitoring Mode (Idle)**
- A single scheduled function polls NOAA/NHC advisory feeds every 15 minutes
- All compute is off; only object storage and a static site are running
- Estimated cost: $3–8/month (storage for pre-processed base data + DNS/CDN)

**Analysis Mode (Active Storm)**
- Triggered automatically when a tropical storm watch/warning is detected
- Spins up a processing pipeline: data ingestion → flood modeling → tile generation → delivery
- Scales compute horizontally across the storm-affected area
- Estimated cost: $5–50 per storm event depending on area size and resolution

### 1.2 High-Level Component Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                        EVENT LAYER                              │
│                                                                 │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐      │
│  │ Cron Trigger  │    │ Webhook/RSS  │    │ Manual Start │      │
│  │ (15 min poll) │    │ (NHC feed)   │    │ (dashboard)  │      │
│  └──────┬───────┘    └──────┬───────┘    └──────┬───────┘      │
│         └──────────────┬────┘───────────────────┘              │
│                        ▼                                        │
│              ┌─────────────────┐                                │
│              │  Storm Detector  │                                │
│              │   (Function)     │                                │
│              └────────┬────────┘                                │
└───────────────────────┼─────────────────────────────────────────┘
                        │ storm detected
                        ▼
┌─────────────────────────────────────────────────────────────────┐
│                    ORCHESTRATION LAYER                           │
│                                                                 │
│              ┌─────────────────┐                                │
│              │  Pipeline        │                                │
│              │  Orchestrator    │                                │
│              └────────┬────────┘                                │
│                       │                                         │
│         ┌─────────────┼─────────────┐                          │
│         ▼             ▼             ▼                           │
│  ┌────────────┐ ┌────────────┐ ┌────────────┐                  │
│  │ Data Ingest│ │ Flood Model│ │ Tile Gen   │                  │
│  │ Workers    │ │ Workers    │ │ Workers    │                  │
│  └─────┬──────┘ └─────┬──────┘ └─────┬──────┘                  │
│        │              │              │                          │
└────────┼──────────────┼──────────────┼──────────────────────────┘
         │              │              │
         ▼              ▼              ▼
┌─────────────────────────────────────────────────────────────────┐
│                      STORAGE LAYER                              │
│                                                                 │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐          │
│  │ Raw Data     │  │ Model Output │  │ Tile Store   │          │
│  │ (DEM, NWM)   │  │ (depth rast) │  │ (PMTiles/COG)│          │
│  └──────────────┘  └──────────────┘  └──────┬───────┘          │
│                                              │                  │
└──────────────────────────────────────────────┼──────────────────┘
                                               │
                                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                      DELIVERY LAYER                             │
│                                                                 │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐          │
│  │ CDN (tiles)  │  │ Auth/Paywall │  │ Static Site  │          │
│  └──────────────┘  └──────────────┘  └──────────────┘          │
│                         │                                       │
│                         ▼                                       │
│              ┌─────────────────┐                                │
│              │  MapLibre GL JS │                                │
│              │  Web Frontend   │                                │
│              └─────────────────┘                                │
└─────────────────────────────────────────────────────────────────┘
```

---

## 2. Technology Stack

### 2.1 Cloud Provider: AWS (Primary Recommendation)

AWS is recommended because of its mature serverless ecosystem, S3's native support for range requests (critical for COG/PMTiles), and CloudFront's cost-effective CDN. The architecture can be adapted to GCP or Azure, but AWS provides the best price/performance for this workload.

| Component | Service | Why |
|---|---|---|
| Event Triggers | EventBridge Scheduler + Lambda | Cron polling, event routing |
| Orchestration | Step Functions | Coordinates multi-step pipeline |
| Short Compute (< 15 min) | Lambda (up to 10 GB RAM) | Data ingestion, tile gen |
| Long Compute (modeling) | AWS Batch with Fargate Spot | HEC-RAS, heavy raster ops |
| Object Storage | S3 | Tiles, COGs, raw data, static site |
| CDN | CloudFront | Tile delivery, signed URLs |
| Auth & Payment | Cognito + Stripe | User accounts, premium access |
| DNS | Route 53 | Domain management |
| Monitoring | CloudWatch | Alerts, cost tracking |
| IaC | Terraform or AWS CDK | Reproducible deployments |

### 2.2 Alternative: Cloudflare-Centric Stack (Lower Floor Cost)

For even lower idle costs, a Cloudflare-centric approach uses R2 (free egress), Workers, and Pages. The tradeoff is less flexibility for heavy compute, which still requires AWS Batch or a similar service.

| Component | Service |
|---|---|
| Storage + Tiles | Cloudflare R2 (zero egress fees) |
| CDN + Edge Logic | Cloudflare Workers |
| Static Site | Cloudflare Pages |
| Auth | Cloudflare Access or custom Workers logic |
| Heavy Compute | AWS Batch Fargate Spot (cross-cloud) |

### 2.3 Geospatial Processing Stack

| Tool | Purpose |
|---|---|
| GDAL / rasterio / Fiona | Raster and vector I/O, reprojection, clipping |
| NumPy / SciPy | Array math for flood depth calculations |
| pysheds or TauDEM | Watershed delineation, flow direction |
| HEC-RAS 6.x (headless Linux) | 2D hydraulic modeling (compound flooding) |
| HAND algorithm (custom) | Fast flood approximation from NWM discharge |
| tippecanoe | Vector tile generation |
| cogeo / rio-cogeo | Cloud Optimized GeoTIFF creation |
| PMTiles (go-pmtiles or pmtiles Python) | Single-file tile archives |
| MapLibre GL JS | Frontend map rendering |

---

## 3. Data Sources and Ingestion

### 3.1 Static Baseline Data (Pre-Processed and Stored)

These datasets are downloaded once, processed into analysis-ready formats, and stored in S3. They are updated infrequently (annually or less).

**USGS 3DEP Digital Elevation Models**
- Source: USGS National Map / AWS Open Data (`s3://usgs-lidar-public`)
- Resolution: 10m nationally; 1m LiDAR where available
- Format: Download as GeoTIFF, convert to Cloud Optimized GeoTIFF (COG)
- Storage strategy: Store COGs covering the full US coastline and Gulf/Atlantic states. Organize by HUC-8 watershed or USGS quad tile grid. Total storage estimate: 50–200 GB for hurricane-prone coastal zones at 10m; 1m LiDAR is much larger and should be fetched on-demand per storm area.
- Pre-processing: Reproject to EPSG:5070 (CONUS Albers) for modeling, with EPSG:3857 copies for tile serving. Fill voids. Compute slope and flow direction grids.

**NLCD Land Cover**
- Source: MRLC / AWS Open Data
- Purpose: Derive Manning's roughness coefficients for hydraulic modeling
- Pre-processing: Reclassify NLCD classes to Manning's n values (e.g., open water = 0.02, developed = 0.12, forest = 0.15). Store as a single COG aligned to the DEM grid.

**HAND (Height Above Nearest Drainage) Raster**
- Source: Compute from 3DEP DEMs, or use pre-computed HAND from NOAA/OWP
- Purpose: Enables fast, low-cost flood mapping by relating river stage to inundation area
- Pre-processing: Generate HAND raster for all coastal HUC-8 watersheds. Store as COGs.

**River and Coastal Geometry**
- NHDPlus HR flowlines (stream network)
- NOAA coastal bathymetry (where available)
- FEMA flood zone boundaries (for reference/validation)
- Store as FlatGeobuf or GeoParquet for fast spatial queries

### 3.2 Dynamic Storm Data (Fetched Per-Event)

These datasets are fetched only when a storm event is detected, and only for the geographic area within the storm cone.

**NHC Advisory Data (Trigger + Storm Geometry)**
- Source: `https://www.nhc.noaa.gov/gis/` — GIS shapefiles and KML updated every advisory
- Key products: forecast track, cone of uncertainty, watches/warnings polygons, wind radii
- Ingestion: Lambda function downloads the latest advisory shapefile, extracts the cone polygon, and uses it to define the processing extent for all subsequent steps
- Polling: Check RSS feed at `https://www.nhc.noaa.gov/index-at.xml` every 15 minutes

**P-Surge / SLOSH Storm Surge Forecasts**
- Source: NOAA/NHC Probabilistic Storm Surge (P-Surge) — GIS data via NHC
- Format: Shapefiles or NetCDF with surge height above ground at various exceedance levels
- Ingestion: Download the latest P-Surge product, clip to storm cone, convert to COG
- Fallback: If P-Surge is not yet available, use the SLOSH Maximum Envelope of Water (MEOW/MOM) basins as a static lookup

**National Water Model (NWM) Forecasts**
- Source: NOAA/OWP via AWS Open Data (`s3://noaa-nwm-pds`)
- Products: Medium-range (10-day) and short-range (18-hour) river discharge forecasts
- Format: NetCDF
- Ingestion: Lambda reads NWM channel output for NHDPlus reaches within the storm cone. Map forecast discharge to HAND raster to estimate riverine inundation.

**WPC Quantitative Precipitation Forecasts**
- Source: Weather Prediction Center excessive rainfall outlooks and QPF grids
- Format: GRIB2 or GeoTIFF
- Ingestion: Download, clip to storm cone, convert to COG for use as rainfall input

**USGS Real-Time Stream Gauges**
- Source: USGS NWIS web services (`https://waterservices.usgs.gov/nwis/iv/`)
- Purpose: Ground truth and real-time calibration of model outputs
- Ingestion: API call to get current stage/discharge for gauges in the storm area

### 3.3 Spatial Filtering Strategy

All dynamic data ingestion is bounded by the NHC cone of uncertainty polygon, buffered by 50 km. This is the single most important cost optimization: it prevents downloading and processing data for unaffected areas.

```
storm_cone = fetch_nhc_cone(advisory_id)
processing_extent = storm_cone.buffer(50_km)
dem_tiles = spatial_index.query(processing_extent)  # Only fetch relevant COG tiles
nwm_reaches = nhd_flowlines.clip(processing_extent)
```

---

## 4. Event Detection and Pipeline Orchestration

### 4.1 Storm Detection Function

A Lambda function runs on a 15-minute EventBridge schedule. It checks the NHC Atlantic and Eastern Pacific RSS feeds for new advisories. When it detects a new advisory containing a watch or warning, it triggers the analysis pipeline.

```
Detection Logic (pseudocode):
1. Fetch NHC RSS feed
2. Parse latest advisory entries
3. Compare advisory IDs against a DynamoDB "last seen" record
4. If new advisory AND contains watch/warning keywords:
     a. Download advisory GIS data (cone, track, wind radii)
     b. Store in S3: s3://bucket/storms/{storm_id}/{advisory_num}/
     c. Trigger Step Functions pipeline execution
5. Update "last seen" record
```

Cost when idle: One Lambda invocation every 15 minutes = ~2,880/month = effectively free tier.

### 4.2 Pipeline Orchestrator (AWS Step Functions)

The pipeline is a Step Functions state machine with the following stages:

```
┌─────────────────────┐
│ 1. INGEST           │
│    ├─ Fetch P-Surge  │───┐
│    ├─ Fetch NWM      │   │  (parallel)
│    ├─ Fetch QPF      │   │
│    └─ Clip DEM       │───┘
└──────────┬──────────┘
           ▼
┌─────────────────────┐
│ 2. MODEL            │
│    ├─ Surge Model    │───┐
│    ├─ Rainfall Model │   │  (parallel)
│    └─ HAND Lookup    │───┘
└──────────┬──────────┘
           ▼
┌─────────────────────┐
│ 3. COMPOUND         │
│    └─ Merge surge +  │
│       rainfall depth │
└──────────┬──────────┘
           ▼
┌─────────────────────┐
│ 4. TILE GENERATION   │
│    ├─ COGs (raster)  │
│    ├─ PMTiles        │
│    └─ Metadata JSON  │
└──────────┬──────────┘
           ▼
┌─────────────────────┐
│ 5. PUBLISH           │
│    ├─ Upload to S3   │
│    ├─ Invalidate CDN │
│    └─ Update manifest│
└─────────────────────┘
```

Each stage is implemented as either a Lambda function (for lightweight tasks) or an AWS Batch job (for heavy compute). Step Functions handles retries, error handling, and parallelization.

---

## 5. Flood Modeling Engine

### 5.1 Three-Tier Modeling Strategy

The system implements three tiers of flood modeling, each with increasing fidelity and compute cost. The tier used depends on available time, data, and whether the user is on the free or premium plan.

**Tier 1: Bathtub Model (Fastest, Lowest Cost)**
- Method: Subtract surge height from DEM. Any cell where surge height > elevation is flooded. Flood depth = surge height − elevation.
- Use case: Free-tier storm surge maps, initial rapid assessment
- Compute: Lambda function, < 2 minutes for a county-sized area at 10m
- Limitation: No flow connectivity; overpredicts flooding in disconnected low areas

```python
# Bathtub model pseudocode
import rasterio
import numpy as np

dem = rasterio.open("dem.tif").read(1)
surge_height = 3.5  # meters above NAVD88, from P-Surge

flood_depth = surge_height - dem
flood_depth[flood_depth < 0] = 0  # no flooding above surge
flood_depth[dem == nodata] = nodata
```

**Tier 2: HAND-Based Model (Fast, Moderate Fidelity)**
- Method: Use the pre-computed HAND raster plus NWM discharge forecasts. For each NHDPlus reach, convert forecast discharge to river stage using a synthetic rating curve, then map stage to inundation via HAND.
- Use case: Riverine/rainfall flooding for free tier; provides connectivity-aware results
- Compute: Lambda function, 2–5 minutes per HUC-8
- Advantage over bathtub: Respects drainage connectivity; only floods cells hydraulically connected to a drainage channel

```python
# HAND-based inundation pseudocode
hand_raster = rasterio.open("hand.tif").read(1)
forecast_stage = 4.2  # meters, from NWM discharge → rating curve

flood_depth = forecast_stage - hand_raster
flood_depth[flood_depth < 0] = 0
flood_depth[hand_raster == nodata] = nodata
```

**Tier 3: HEC-RAS 2D (Highest Fidelity, Highest Cost)**
- Method: Full 2D shallow water equation solver. Accounts for flow momentum, roughness, rainfall, surge boundary conditions, and time-varying inputs.
- Use case: Premium-tier compound flood modeling; critical infrastructure analysis
- Compute: AWS Batch Fargate Spot, 30–120 minutes per sub-domain depending on resolution
- Setup: Pre-build HEC-RAS project files (geometry, mesh, boundary conditions) for key coastal zones. At runtime, inject the storm-specific boundary conditions (surge hydrograph, rainfall time series) and execute headless.

**Compound Flooding (Tier 2 and Tier 3)**

Compound flooding combines surge and rainfall/riverine flooding. The approach differs by tier:

- Tier 2 (HAND + Bathtub): Take the maximum depth at each cell from the surge model and the HAND-based riverine model. This is a conservative approximation.
- Tier 3 (HEC-RAS): Surge is applied as a time-varying downstream boundary condition, rainfall as a distributed source term. HEC-RAS solves the compound interaction natively.

```python
# Compound flooding (Tier 2 approximation)
surge_depth = bathtub_model(dem, psurge_height)
riverine_depth = hand_model(hand, nwm_stage)
compound_depth = np.maximum(surge_depth, riverine_depth)

# Identify overlap zones
overlap_mask = (surge_depth > 0) & (riverine_depth > 0)
# In overlap zones, depths may compound (additive component)
compound_depth[overlap_mask] = surge_depth[overlap_mask] + riverine_depth[overlap_mask] * 0.5
```

### 5.2 Processing Grid and Parallelization

The storm area is subdivided into processing tiles (e.g., 10 km × 10 km). Each tile is processed independently with a buffer overlap of 500 m to avoid edge artifacts. Tiles are processed in parallel using Lambda (Tier 1–2) or Batch array jobs (Tier 3).

```
Storm Cone → Subdivide into 10km tiles → Process in parallel → Merge → Generate output tiles
```

### 5.3 Time-Stepped Forecasts

The system produces flood depth maps at 6-hour intervals along the storm forecast timeline (e.g., T+0, T+6, T+12, ... T+72). Each timestep uses the corresponding NWM forecast, P-Surge exceedance, and QPF accumulation. This enables the frontend time slider.

---

## 6. Tile Generation and Delivery

### 6.1 Output Formats

**Cloud Optimized GeoTIFFs (COGs)**
- Used for: Raster flood depth data, served via HTTP range requests
- Generation: `rio cogeo create` or GDAL `gdal_translate -of COG`
- Internal tiling: 256×256 or 512×512 with overviews at zoom levels 8–16
- Compression: DEFLATE with float32 depth values; LZW for uint8 classified data

**PMTiles (Vector + Raster Tiles)**
- Used for: Pre-rendered vector tile archives served as a single file from S3
- Generation: `tippecanoe` for vector data (flood polygons, infrastructure) → `.pmtiles`; `go-pmtiles` or `pmtiles` CLI for raster tile archives
- Advantage: Single file per dataset, served via HTTP range requests — no tile server needed

**Metadata JSON**
- A manifest file per storm event listing all available layers, timesteps, bounding boxes, and tile URLs
- The frontend fetches this manifest to know what data is available

### 6.2 Tile Organization in S3

```
s3://flood-tiles/
├── base/                          # Static baseline tiles
│   ├── terrain/                   # Hillshade, elevation context
│   └── hand/                      # Pre-computed HAND raster
├── storms/
│   └── {storm_id}/
│       ├── manifest.json          # Layer catalog, timesteps, bounds
│       ├── advisory_{num}/
│       │   ├── surge/
│       │   │   ├── free/          # Low-res (zoom 8-12)
│       │   │   │   └── surge_depth.pmtiles
│       │   │   └── premium/       # High-res (zoom 13-16)
│       │   │       └── surge_depth.pmtiles
│       │   ├── rainfall/
│       │   │   ├── free/
│       │   │   └── premium/
│       │   ├── compound/
│       │   │   ├── free/
│       │   │   └── premium/
│       │   └── timesteps/
│       │       ├── t+00/
│       │       ├── t+06/
│       │       └── ...
│       └── latest -> advisory_12/ # Symlink to current advisory
└── static-site/                   # Frontend HTML/JS/CSS
```

### 6.3 Delivery Architecture

```
User's Browser (MapLibre GL JS)
        │
        ▼
CloudFront CDN ──────────────────────────────┐
        │                                     │
        ├── /tiles/free/*  → S3 (public)      │
        │                                     │
        └── /tiles/premium/* → S3 (private)   │
             │                                │
             └── Requires signed URL ─────────┘
                 (issued by auth Lambda)
```

Free-tier tiles are served directly from S3 via CloudFront with aggressive caching (TTL = 1 hour during active storms, 24 hours post-storm). Premium tiles are in a private S3 prefix; access requires a signed CloudFront URL issued by an authentication Lambda that validates the user's subscription status.

### 6.4 Cost Optimization for Delivery

- PMTiles served via range requests eliminate the need for a tile server
- CloudFront caches aggressively, so repeated requests for the same tile don't hit S3
- Tiles are pre-rendered, not generated on request
- Client-side rendering means the server sends raw data and the browser applies color ramps

---

## 7. Frontend Web Application

### 7.1 Technology

| Component | Technology |
|---|---|
| Map Engine | MapLibre GL JS |
| UI Framework | React (or Svelte for smaller bundle) |
| State Management | Zustand or React Context |
| Geocoding | Nominatim (free) or Mapbox Geocoding API |
| Hosting | S3 + CloudFront (static site) |
| PWA | Service worker for offline map caching |

### 7.2 Map Layer Architecture

```javascript
// Layer configuration pseudocode
const layers = {
  surge: {
    source: "pmtiles://s3-url/surge_depth.pmtiles",
    type: "raster",
    colorRamp: ["#e0f7fa", "#00bcd4", "#006064"],  // cyan/blue
    opacity: 0.7,
    legend: "Storm Surge Depth (ft)"
  },
  rainfall: {
    source: "pmtiles://s3-url/rainfall_depth.pmtiles",
    type: "raster",
    colorRamp: ["#f3e5f5", "#9c27b0", "#4a148c"],  // magenta/purple
    opacity: 0.7,
    legend: "Rainfall Flood Depth (ft)"
  },
  compound: {
    source: "pmtiles://s3-url/compound_depth.pmtiles",
    type: "raster",
    colorRamp: ["#ede7f6", "#673ab7", "#1a237e"],  // violet/indigo
    opacity: 0.7,
    legend: "Combined Flood Depth (ft)"
  },
  overlap: {
    // Derived client-side where surge AND rainfall > 0
    type: "fill",
    paint: { "fill-color": "#311b92", "fill-opacity": 0.5 },
    legend: "Surge + Rainfall Overlap Zone"
  }
};
```

### 7.3 Key UI Features

**Layer Toggle Panel** — Checkboxes to show/hide surge, rainfall, compound, and overlap layers. Each layer has an opacity slider.

**Time Slider** — A horizontal slider from T+0 to T+72 (or the forecast horizon). Changing the timestep swaps the tile source URL to the corresponding timestep's PMTiles file. Debounced to avoid rapid tile reloads.

**Address Search** — A search bar using Nominatim or the Census Geocoder (free) to geocode an address, fly to it on the map, and display the predicted flood depth at that location. Depth is read from the COG via a point query (HTTP range request for the specific pixel).

**Point Query (Click-to-Query)** — Click anywhere on the map to get the flood depth at that pixel. Uses the `georaster-layer-for-leaflet` pattern adapted for MapLibre: the browser fetches the relevant COG tile via a range request and reads the pixel value client-side. No server round-trip.

**GPS Location** — Browser geolocation API to center the map on the user's position and show their local flood risk.

**Zoom-Level Paywall** — At zoom levels 8–12, free-tier tiles are displayed. When the user zooms past level 12, a semi-transparent overlay appears prompting them to subscribe for high-resolution data. If authenticated and subscribed, the map seamlessly loads premium tiles.

### 7.4 Client-Side Rendering

Color ramps are applied in the browser using MapLibre's `raster-color` expression or a custom WebGL shader. This means the tiles themselves contain raw float depth values (or quantized uint8), and the browser maps them to colors. Benefits: smaller tile sizes, user-customizable color scales, and the ability to do client-side math (e.g., show only depths > 2 feet).

---

## 8. Paywall and Monetization

### 8.1 Two-Tier Data Model

| Aspect | Free Tier | Premium Tier |
|---|---|---|
| Resolution | 10m DEM, zoom 8–12 | 1m LiDAR, zoom 13–16 |
| Modeling | Bathtub + HAND | HEC-RAS 2D compound |
| Flood types | Surge OR rainfall | Surge + rainfall + compound |
| Update freq | Every 6 hours | Every advisory (~3 hours) |
| Address query | County-level risk | Parcel-level depth |
| Price | Free | $4.99/storm or $9.99/month |

### 8.2 Authentication Flow

```
User clicks "Unlock HD" → Cognito login/signup → Stripe Checkout
→ Webhook confirms payment → DynamoDB records subscription
→ Auth Lambda issues signed CloudFront URL (TTL 1 hour)
→ Frontend stores token → Requests premium tiles with signed URL
```

**Signed URL Strategy**: Premium PMTiles files are stored in a private S3 prefix. A Lambda@Edge function or CloudFront Function validates the user's JWT token and, if valid, issues a CloudFront signed URL for the specific PMTiles file. URLs expire after 1 hour and are refreshed automatically by the frontend.

### 8.3 Pricing Implementation

- **Stripe** handles payment processing (subscriptions + one-time storm passes)
- **Cognito** manages user identity (email/password or Google/Apple social login)
- **DynamoDB** stores user subscription status, keyed by Cognito user ID
- Auth Lambda checks DynamoDB on each premium tile request

---

## 9. Infrastructure as Code

### 9.1 Terraform Module Structure

```
terraform/
├── modules/
│   ├── storage/          # S3 buckets, lifecycle policies
│   ├── compute/          # Lambda functions, Batch compute environments
│   ├── orchestration/    # Step Functions state machine
│   ├── delivery/         # CloudFront distributions, DNS
│   ├── auth/             # Cognito, DynamoDB, auth Lambdas
│   └── monitoring/       # CloudWatch alarms, budgets
├── environments/
│   ├── dev/
│   └── prod/
└── main.tf
```

### 9.2 Key Infrastructure Decisions

**Lambda Configuration**
- Runtime: Python 3.12 with GDAL Lambda layer (or container image)
- Memory: 2048–10240 MB depending on raster size
- Timeout: 15 minutes (max)
- Ephemeral storage: 10 GB (`/tmp`) for raster staging

**AWS Batch (HEC-RAS)**
- Compute environment: Fargate Spot (up to 70% cost savings)
- Container: Custom Docker image with HEC-RAS 6.x headless, GDAL, Python
- vCPUs: 4–16 per job depending on mesh complexity
- Memory: 16–64 GB

**S3 Lifecycle Policies**
- Raw storm data: Transition to Glacier after 90 days
- Processed tiles: Keep in Standard for active storm + 30 days, then Infrequent Access
- Base data (DEMs, HAND): Standard, no lifecycle (permanent)

---

## 10. Cost Model

### 10.1 Idle Month (No Storms)

| Component | Monthly Cost |
|---|---|
| S3 storage (100 GB base data) | $2.30 |
| Lambda (storm detector, 2880 invocations) | $0.01 |
| CloudFront (minimal traffic) | $0.00 |
| Route 53 hosted zone | $0.50 |
| DynamoDB (on-demand, minimal) | $0.00 |
| **Total** | **~$3–5/month** |

### 10.2 Active Storm Event

| Component | Per-Storm Cost |
|---|---|
| Lambda (data ingestion, 50 invocations × 5 min × 4 GB) | $1.50 |
| Lambda (tile generation, 200 invocations × 3 min × 8 GB) | $4.80 |
| Batch Fargate Spot (HEC-RAS, 4 vCPU × 16 GB × 2 hrs) | $3.00 |
| S3 PUT requests + storage (10 GB new tiles) | $0.50 |
| CloudFront (50 GB transfer during storm) | $4.25 |
| Step Functions (state transitions) | $0.10 |
| **Total per storm** | **~$15–25** |

### 10.3 Monthly Budget Target

- 0 storms: ~$5/month
- 1 storm: ~$20–30/month
- 3 storms (busy month): ~$50–80/month

Revenue from even a small number of premium subscribers ($5–10/storm × 100 users = $500–1000) easily covers compute costs.

---

## 11. Implementation Roadmap

### Phase 1: Foundation (Weeks 1–4)

**Goal**: Storm detection, data ingestion, bathtub model, basic map viewer.

Tasks:
1. Set up AWS account, Terraform scaffolding, S3 buckets
2. Download and pre-process 10m DEMs for the Gulf Coast (TX to FL) as COGs
3. Compute HAND rasters for coastal HUC-8 watersheds
4. Build storm detector Lambda (NHC RSS polling)
5. Build P-Surge ingestion Lambda
6. Implement bathtub surge model in Lambda
7. Generate PMTiles from surge depth output
8. Build minimal MapLibre frontend with surge layer
9. Deploy static site to S3 + CloudFront
10. End-to-end test with a historical storm (e.g., Hurricane Ian advisories)

**Deliverable**: A working prototype that detects a simulated storm advisory, runs a bathtub surge model, and displays results on a web map.

### Phase 2: Rainfall and HAND Modeling (Weeks 5–8)

**Goal**: Add riverine flooding, rainfall flooding, and compound layer.

Tasks:
1. Build NWM data ingestion Lambda (fetch from S3 Open Data)
2. Build WPC QPF ingestion Lambda
3. Implement HAND-based riverine flood model
4. Implement rainfall runoff approximation (SCS Curve Number or Green-Ampt)
5. Implement compound flooding merge logic
6. Add rainfall, compound, and overlap layers to frontend
7. Build time slider for forecast timesteps
8. Add address search and point query
9. Add layer toggle panel with opacity controls

**Deliverable**: Full three-layer flood map with time slider and address lookup.

### Phase 3: HEC-RAS Integration (Weeks 9–12)

**Goal**: Premium-tier HEC-RAS 2D compound modeling.

Tasks:
1. Build Docker container with HEC-RAS 6.x headless Linux
2. Pre-build HEC-RAS project files for 3–5 key coastal zones
3. Configure AWS Batch compute environment (Fargate Spot)
4. Build orchestration to inject storm-specific boundary conditions
5. Build Step Functions state machine for full pipeline
6. Generate high-resolution (1m) tiles from HEC-RAS output
7. Set up dual-resolution tile structure (free vs. premium)

**Deliverable**: Working HEC-RAS pipeline producing premium-tier flood maps.

### Phase 4: Paywall and Polish (Weeks 13–16)

**Goal**: Monetization, user auth, production hardening.

Tasks:
1. Set up Cognito user pool with social login
2. Integrate Stripe for subscriptions and storm passes
3. Build auth Lambda for signed URL generation
4. Implement zoom-level paywall in frontend
5. Add CloudWatch alarms for errors and cost thresholds
6. Set up AWS Budget alerts
7. Load test CDN with simulated traffic spike
8. Add PWA service worker for offline map caching
9. Build admin dashboard for storm monitoring
10. Write operational runbook

**Deliverable**: Production-ready system with monetization.

### Phase 5: Expansion (Ongoing)

- Extend DEM coverage to the full Atlantic and Gulf coasts
- Add historical storm archive for comparison
- Add push notifications (SNS/email) for storm alerts by location
- Build mobile-optimized UI
- Add flood insurance estimate integration
- Explore partnerships with emergency management agencies

---

## 12. Operational Considerations

### 12.1 Monitoring and Alerting

- **CloudWatch Alarm**: Lambda errors, Batch job failures, Step Functions execution failures
- **Budget Alert**: Email when monthly spend exceeds $30, $50, $100
- **Storm Activity Dashboard**: CloudWatch dashboard showing active pipelines, tile generation progress, CDN cache hit ratio
- **Data Freshness**: Alarm if the last successful storm detector run was > 30 minutes ago

### 12.2 Data Quality and Validation

- Compare model output against USGS stream gauge observations
- Compare surge predictions against NOS tide gauge observations during past storms
- Archive all model outputs for post-storm validation
- Log all input data versions for reproducibility

### 12.3 Disaster Recovery

- All infrastructure is defined in Terraform — full environment can be recreated
- S3 data has versioning enabled
- No persistent servers to maintain
- Frontend is a static site — can be redeployed in minutes

### 12.4 Security

- All S3 buckets have public access blocked (except the static site bucket)
- Premium tiles served exclusively via signed URLs
- Lambda functions run with least-privilege IAM roles
- Cognito handles all authentication; no custom password storage
- CloudFront enforces HTTPS

---

## Appendix A: Key API Endpoints and Data URLs

| Data Source | URL / Endpoint |
|---|---|
| NHC RSS (Atlantic) | `https://www.nhc.noaa.gov/index-at.xml` |
| NHC GIS Data | `https://www.nhc.noaa.gov/gis/` |
| P-Surge Products | `https://www.nhc.noaa.gov/surge/` |
| NWM on AWS | `s3://noaa-nwm-pds/` |
| 3DEP on AWS | `s3://usgs-lidar-public/` |
| NLCD | `https://www.mrlc.gov/data` |
| USGS Water Services | `https://waterservices.usgs.gov/nwis/iv/` |
| NHDPlus HR | `https://www.usgs.gov/national-hydrography/nhdplus-high-resolution` |

## Appendix B: Manning's Roughness Coefficient Lookup

| NLCD Class | Description | Manning's n |
|---|---|---|
| 11 | Open Water | 0.020 |
| 21 | Developed, Open Space | 0.040 |
| 22 | Developed, Low Intensity | 0.080 |
| 23 | Developed, Medium Intensity | 0.120 |
| 24 | Developed, High Intensity | 0.150 |
| 41 | Deciduous Forest | 0.150 |
| 42 | Evergreen Forest | 0.160 |
| 43 | Mixed Forest | 0.155 |
| 52 | Shrub/Scrub | 0.070 |
| 71 | Grassland/Herbaceous | 0.035 |
| 81 | Pasture/Hay | 0.035 |
| 82 | Cultivated Crops | 0.040 |
| 90 | Woody Wetlands | 0.100 |
| 95 | Emergent Herbaceous Wetlands | 0.060 |

## Appendix C: Recommended Python Dependencies

```
# Core geospatial
gdal>=3.7
rasterio>=1.3
fiona>=1.9
shapely>=2.0
pyproj>=3.6
geopandas>=0.14

# Raster processing
numpy
scipy
xarray
netCDF4

# Tile generation
rio-cogeo
pmtiles
# tippecanoe (system binary)

# Hydrology
pysheds>=0.3
# HEC-RAS (system binary, headless)

# AWS
boto3
aws-lambda-powertools

# Web / API
fastapi  # for any API endpoints
mangum   # FastAPI on Lambda adapter
pyjwt    # JWT validation for signed URLs
```
