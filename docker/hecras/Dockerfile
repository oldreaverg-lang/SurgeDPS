# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# HEC-RAS 6.5 Headless Linux Docker Image
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#
# Uses native Linux HEC-RAS compute engines (no Wine required).
# Designed for AWS Batch Fargate Spot execution.
#
# Build:
#   docker build -t surgedps/hecras:6.5 -f docker/hecras/Dockerfile .
#
# Run locally:
#   docker run --rm -v $(pwd)/data:/data surgedps/hecras:6.5 \
#     /data/project.p01
#
# AWS ECR push:
#   aws ecr get-login-password | docker login --username AWS --password-stdin <account>.dkr.ecr.us-east-1.amazonaws.com
#   docker tag surgedps/hecras:6.5 <account>.dkr.ecr.us-east-1.amazonaws.com/surgedps-hecras:6.5
#   docker push <account>.dkr.ecr.us-east-1.amazonaws.com/surgedps-hecras:6.5
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

FROM amazonlinux:2023 AS base

# ── System dependencies ───────────────────────────────────────────
# HEC-RAS 6.5 Linux binaries require Intel oneAPI runtime libraries
RUN dnf install -y \
    python3.11 \
    python3.11-pip \
    unzip \
    wget \
    gdal \
    gdal-libs \
    gdal-devel \
    libgfortran \
    libgomp \
    && dnf clean all

# Symlink python
RUN alternatives --install /usr/bin/python3 python3 /usr/bin/python3.11 1 \
    && alternatives --install /usr/bin/pip3 pip3 /usr/bin/pip3.11 1

# ── Install HEC-RAS Linux Compute Engines ─────────────────────────
# NOTE: The official Linux binaries must be downloaded from
# https://www.hec.usace.army.mil/software/hec-ras/download.aspx
# and placed in docker/hecras/ras_binaries/ before building.
#
# Expected files:
#   - RasGeomPreprocess (geometry preprocessor)
#   - RasUnsteady        (unsteady flow solver)
#   - RasSteady          (steady flow solver)
#   - *.so               (shared libraries)

WORKDIR /opt/hecras

# Copy HEC-RAS binaries (operator must provide these)
COPY docker/hecras/ras_binaries/ /opt/hecras/bin/
RUN chmod +x /opt/hecras/bin/Ras* 2>/dev/null || true

# Add to PATH
ENV PATH="/opt/hecras/bin:${PATH}"
ENV LD_LIBRARY_PATH="/opt/hecras/bin:${LD_LIBRARY_PATH}"

# ── Python dependencies for result extraction ─────────────────────
COPY docker/hecras/requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

# ── Copy SurgeDPS HEC-RAS modules ────────────────────────────────
COPY src/hecras/ /opt/surgedps/hecras/
ENV PYTHONPATH="/opt/surgedps:${PYTHONPATH}"

# ── Working directories ──────────────────────────────────────────
RUN mkdir -p /data/input /data/output /data/projects /data/templates

# ── Entrypoint ───────────────────────────────────────────────────
COPY docker/hecras/entrypoint.sh /opt/hecras/entrypoint.sh
RUN chmod +x /opt/hecras/entrypoint.sh

ENTRYPOINT ["/opt/hecras/entrypoint.sh"]
