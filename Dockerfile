# ─── Stage 1: Build React frontend ───────────────────────────────────────────
FROM node:20-slim AS frontend-builder

WORKDIR /app/ui
COPY ui/package*.json ./
RUN npm ci

COPY ui/ ./
RUN npm run build
# Output: /app/ui/dist/


# ─── Stage 2: Python runtime with GDAL ───────────────────────────────────────
FROM python:3.11-slim

# Suppress debconf interactive prompts during apt-get
ENV DEBIAN_FRONTEND=noninteractive

# GDAL and rasterio system deps
RUN apt-get update && apt-get install -y --no-install-recommends \
        gdal-bin \
        libgdal-dev \
        libproj-dev \
        gcc \
        g++ \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Python dependencies
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY scripts/ ./scripts/
COPY src/ ./src/

# Copy built React frontend from stage 1
COPY --from=frontend-builder /app/ui/dist/ ./ui/dist/

# Cache dir for cell data
RUN mkdir -p tmp_integration/cells

# Railway sets PORT; default to 8000
ENV SURGE_API_PORT=8000
EXPOSE 8000

CMD ["python", "scripts/api_server.py"]
