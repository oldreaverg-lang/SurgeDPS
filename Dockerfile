# ─── Stage 1: Build React frontend ───────────────────────────────────────────
FROM node:20-slim AS frontend-builder

WORKDIR /app/ui
COPY ui/package*.json ./
RUN npm ci

COPY ui/ ./
# Cap Node heap to 512 MB so tsc doesn't OOM-kill (Railway builder ~2 GB RAM).
RUN NODE_OPTIONS="--max-old-space-size=512" npm run build
# Output: /app/ui/dist/


# ─── Stage 2: Python runtime ──────────────────────────────────────────────────
# rasterio wheels bundle GDAL but still need libexpat1 from the system.
FROM python:3.12-slim-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends \
        libexpat1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Python dependencies — install heavy packages individually to limit peak RAM
COPY requirements.txt ./
RUN pip install --no-cache-dir numpy>=1.24 \
 && pip install --no-cache-dir rasterio>=1.3 \
 && pip install --no-cache-dir duckdb>=1.1 \
 && pip install --no-cache-dir -r requirements.txt

# Copy source code and data files (hurdat2.txt, dps_scores.json)
COPY scripts/ ./scripts/
COPY src/ ./src/
COPY data/ ./data/

# Copy built React frontend from stage 1
COPY --from=frontend-builder /app/ui/dist/ ./ui/dist/

# Persistent data — mount a Railway volume at /app/persistent
# Set PERSISTENT_DATA_DIR=/app/persistent in Railway env vars.
# Both SurgeDPS and StormDPS use /app/persistent as the mount point.
# Falls back to /app/tmp_integration for local dev.
#
# Directory layout:
#   cells/        per-storm surge/damage cache
#   validation/   run ledger
#   census/       county population cache
#   forecasts/    NHC track cache
#   geocode/      reverse geocoding cache
#   mrms/         MRMS QPE GeoTIFF cache
#   hand_fim/     NOAA OWP HAND rasters by HUC8 (permanent, ~50-200MB/HUC8)
#   nwm/          NWM discharge cache by storm (evicted with cell cache)
RUN mkdir -p persistent/cells persistent/validation \
    persistent/census persistent/forecasts persistent/geocode \
    persistent/mrms persistent/hand_fim persistent/nwm \
    persistent/qpf persistent/atlas14 \
    tmp_integration/cells tmp_integration/validation \
    tmp_integration/census tmp_integration/forecasts tmp_integration/geocode \
    tmp_integration/mrms tmp_integration/hand_fim tmp_integration/nwm \
    tmp_integration/qpf tmp_integration/atlas14

# Railway injects PORT at runtime; default to 8000 for local dev
ENV PORT=8000
EXPOSE 8000

# Start background processes, then the API server (foreground):
#   1. warm_cache.py — pre-generates cell data for sidebar storms
#   2. storm_monitor.py — polls NHC every 30 min, auto-runs pipeline
#   3. api_server.py — HTTP server (foreground, keeps container alive)
#
# NOTE (one-time cleanup, remove after next successful deploy):
# The `rm -f /app/persistent/mrms/iem_*.tif` line purges stale zero-filled
# rainfall TIFs written by the pre-fix longitude-space bug. Once you confirm
# the rain layer renders correctly post-deploy, delete that line so future
# boots don't re-nuke a healthy cache.
CMD ["sh", "-c", "rm -f /app/persistent/mrms/iem_*.tif\npython scripts/warm_cache.py &\npython scripts/storm_monitor.py &\npython scripts/api_server.py"]
