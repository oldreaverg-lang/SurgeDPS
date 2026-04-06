#!/usr/bin/env bash
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# HEC-RAS Container Entrypoint
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#
# Orchestrates the full HEC-RAS workflow:
#   1. Download project data from S3
#   2. Inject storm-specific boundary conditions
#   3. Run geometry preprocessor
#   4. Run unsteady flow simulation
#   5. Extract results to GeoTIFF
#   6. Upload outputs to S3
#
# Environment variables:
#   STORM_ID           — ATCF storm identifier (e.g., AL142024)
#   ADVISORY_NUM       — Advisory number (e.g., 012)
#   DATA_BUCKET        — S3 bucket with input data
#   TEMPLATE_NAME      — HEC-RAS project template to use
#   SURGE_S3_PATH      — S3 path to P-Surge data
#   RAINFALL_S3_PATH   — S3 path to QPF rainfall data
#   DEM_S3_PATH        — S3 path to clipped DEM
#   OUTPUT_S3_PREFIX   — S3 prefix for output upload
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

set -euo pipefail

echo "═══════════════════════════════════════════════"
echo " SurgeDPS HEC-RAS Runner"
echo " Storm: ${STORM_ID:-unknown} Advisory: ${ADVISORY_NUM:-unknown}"
echo "═══════════════════════════════════════════════"

WORK_DIR="/data/projects/${STORM_ID:-test}_${ADVISORY_NUM:-000}"
mkdir -p "$WORK_DIR"
cd "$WORK_DIR"

# ── Step 1: Download inputs from S3 ──────────────────────────────
echo ""
echo ">>> Step 1: Downloading input data..."

if [ -n "${DATA_BUCKET:-}" ]; then
    # Download the project template
    TEMPLATE_DIR="/data/templates/${TEMPLATE_NAME:-gulf_coast}"
    if [ ! -d "$TEMPLATE_DIR" ]; then
        aws s3 sync "s3://${DATA_BUCKET}/hecras/templates/${TEMPLATE_NAME:-gulf_coast}/" "$TEMPLATE_DIR/"
    fi
    cp -r "$TEMPLATE_DIR/"* "$WORK_DIR/"

    # Download storm-specific data
    [ -n "${SURGE_S3_PATH:-}" ] && aws s3 cp "s3://${DATA_BUCKET}/${SURGE_S3_PATH}" "$WORK_DIR/input_surge.tif"
    [ -n "${RAINFALL_S3_PATH:-}" ] && aws s3 cp "s3://${DATA_BUCKET}/${RAINFALL_S3_PATH}" "$WORK_DIR/input_rainfall.tif"
    [ -n "${DEM_S3_PATH:-}" ] && aws s3 cp "s3://${DATA_BUCKET}/${DEM_S3_PATH}" "$WORK_DIR/terrain.tif"
    echo "  Downloaded inputs from S3"
else
    echo "  No S3 bucket configured — using local data"
fi

# ── Step 2: Inject boundary conditions ────────────────────────────
echo ""
echo ">>> Step 2: Injecting storm-specific boundary conditions..."

python3 -m hecras.boundary_injector \
    --project-dir "$WORK_DIR" \
    --storm-id "${STORM_ID:-test}" \
    --advisory "${ADVISORY_NUM:-000}" \
    --surge-file "$WORK_DIR/input_surge.tif" \
    --rainfall-file "$WORK_DIR/input_rainfall.tif" \
    --dem-file "$WORK_DIR/terrain.tif"

echo "  Boundary conditions injected"

# ── Step 3: Geometry preprocessing ────────────────────────────────
echo ""
echo ">>> Step 3: Running geometry preprocessor..."

GEOM_FILE=$(find "$WORK_DIR" -name "*.g0[1-9]" | head -1)
if [ -n "$GEOM_FILE" ]; then
    if command -v RasGeomPreprocess &>/dev/null; then
        RasGeomPreprocess "$GEOM_FILE"
        echo "  Geometry preprocessed: $(basename "$GEOM_FILE")"
    else
        echo "  WARN: RasGeomPreprocess not found — skipping (dev mode)"
    fi
else
    echo "  WARN: No geometry file found"
fi

# ── Step 4: Run unsteady simulation ──────────────────────────────
echo ""
echo ">>> Step 4: Running HEC-RAS unsteady flow simulation..."

PLAN_FILE=$(find "$WORK_DIR" -name "*.p0[1-9]" | head -1)
if [ -n "$PLAN_FILE" ]; then
    PLAN_BASE=$(basename "$PLAN_FILE" | sed 's/\..*//')

    if command -v RasUnsteady &>/dev/null; then
        # Compiled geometry file (.c01)
        COMP_FILE=$(find "$WORK_DIR" -name "*.c0[1-9]" | head -1)
        if [ -n "$COMP_FILE" ]; then
            RasUnsteady "$COMP_FILE" "$PLAN_BASE"
            echo "  Simulation complete"
        else
            echo "  WARN: No compiled geometry file — cannot run simulation"
        fi
    else
        echo "  WARN: RasUnsteady not found — generating synthetic results (dev mode)"
        python3 -m hecras.synthetic_results \
            --project-dir "$WORK_DIR" \
            --dem-file "$WORK_DIR/terrain.tif" \
            --surge-file "$WORK_DIR/input_surge.tif"
    fi
else
    echo "  WARN: No plan file found"
fi

# ── Step 5: Extract results to GeoTIFF ───────────────────────────
echo ""
echo ">>> Step 5: Extracting flood depth results..."

OUTPUT_DIR="$WORK_DIR/output"
mkdir -p "$OUTPUT_DIR"

python3 -m hecras.result_extractor \
    --project-dir "$WORK_DIR" \
    --output-dir "$OUTPUT_DIR" \
    --storm-id "${STORM_ID:-test}" \
    --advisory "${ADVISORY_NUM:-000}"

echo "  Results extracted to $OUTPUT_DIR"

# ── Step 6: Upload outputs to S3 ─────────────────────────────────
echo ""
echo ">>> Step 6: Uploading results..."

if [ -n "${DATA_BUCKET:-}" ] && [ -n "${OUTPUT_S3_PREFIX:-}" ]; then
    aws s3 sync "$OUTPUT_DIR/" "s3://${DATA_BUCKET}/${OUTPUT_S3_PREFIX}/" \
        --content-type "image/tiff"
    echo "  Uploaded to s3://${DATA_BUCKET}/${OUTPUT_S3_PREFIX}/"
else
    echo "  No S3 target configured — results in $OUTPUT_DIR"
fi

echo ""
echo "═══════════════════════════════════════════════"
echo " HEC-RAS run complete"
echo "═══════════════════════════════════════════════"
