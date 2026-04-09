# ─── Stage 1: Build React frontend ───────────────────────────────────────────
FROM node:20-slim AS frontend-builder

WORKDIR /app/ui
COPY ui/package*.json ./
RUN npm ci

COPY ui/ ./
RUN npm run build
# Output: /app/ui/dist/


# ─── Stage 2: Python runtime ──────────────────────────────────────────────────
# rasterio wheels bundle GDAL but still need libexpat1 from the system.
FROM python:3.11-slim-bookworm

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

# Cache dir for cell data — mount a Railway volume here for persistence
# Railway volume mount point: /app/tmp_integration/cells
RUN mkdir -p tmp_integration/cells

# Railway sets PORT; default to 8000
ENV SURGE_API_PORT=8000
EXPOSE 8000

# Start background processes, then the API server (foreground):
#   1. warm_cache.py — pre-generates cell data for sidebar storms
#   2. storm_monitor.py — polls NHC every 30 min, auto-runs pipeline
#   3. api_server.py — HTTP server (foreground, keeps container alive)
CMD ["sh", "-c", "python scripts/warm_cache.py &\npython scripts/storm_monitor.py &\npython scripts/api_server.py"]
