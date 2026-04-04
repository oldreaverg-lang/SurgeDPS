# ─── Stage 1: Build React frontend ───────────────────────────────────────────
FROM node:20-slim AS frontend-builder

WORKDIR /app/ui
COPY ui/package*.json ./
RUN npm ci

COPY ui/ ./
RUN npm run build
# Output: /app/ui/dist/


# ─── Stage 2: Python runtime ──────────────────────────────────────────────────
# rasterio ≥1.3, shapely ≥2.0, and numpy all ship pre-built binary wheels
# on PyPI that bundle their own native libs — no system GDAL needed.
FROM python:3.11-slim-bookworm

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
