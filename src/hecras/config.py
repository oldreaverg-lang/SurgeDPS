"""
HEC-RAS configuration for SurgeDPS.

Defines project template structure, coastal zones, and compute settings.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple


@dataclass
class CoastalZone:
    """Pre-defined coastal zone with a HEC-RAS 2D project template."""

    name: str
    display_name: str
    bounds: Tuple[float, float, float, float]  # (west, south, east, north) in EPSG:4326
    template_name: str
    crs_epsg: int  # Local projection for modeling
    cell_size_m: float  # 2D mesh cell size
    manning_default: float = 0.035  # Default Manning's n
    description: str = ""


# Pre-built coastal zones — each has a HEC-RAS project template
# with terrain, 2D flow areas, and pre-configured mesh.
COASTAL_ZONES: Dict[str, CoastalZone] = {
    "gulf_tx": CoastalZone(
        name="gulf_tx",
        display_name="Texas Gulf Coast",
        bounds=(-97.5, 26.0, -93.5, 30.5),
        template_name="gulf_tx_2d",
        crs_epsg=32615,  # UTM 15N
        cell_size_m=30.0,
        description="Galveston Bay to Corpus Christi",
    ),
    "gulf_la": CoastalZone(
        name="gulf_la",
        display_name="Louisiana Coast",
        bounds=(-93.5, 28.5, -88.5, 31.0),
        template_name="gulf_la_2d",
        crs_epsg=32615,
        cell_size_m=30.0,
        description="Lake Charles to New Orleans",
    ),
    "gulf_ms_al": CoastalZone(
        name="gulf_ms_al",
        display_name="Mississippi-Alabama Coast",
        bounds=(-88.5, 29.5, -87.0, 31.5),
        template_name="gulf_ms_al_2d",
        crs_epsg=32616,  # UTM 16N
        cell_size_m=30.0,
        description="Biloxi to Mobile Bay",
    ),
    "gulf_fl_pan": CoastalZone(
        name="gulf_fl_pan",
        display_name="Florida Panhandle",
        bounds=(-87.0, 29.5, -84.0, 31.0),
        template_name="gulf_fl_pan_2d",
        crs_epsg=32616,
        cell_size_m=30.0,
        description="Pensacola to Apalachicola",
    ),
    "gulf_fl_west": CoastalZone(
        name="gulf_fl_west",
        display_name="Florida West Coast",
        bounds=(-84.0, 25.5, -81.5, 29.5),
        template_name="gulf_fl_west_2d",
        crs_epsg=32617,  # UTM 17N
        cell_size_m=30.0,
        description="Tampa Bay to Naples",
    ),
}


@dataclass
class HECRASConfig:
    """Configuration for HEC-RAS execution."""

    # Docker image
    ecr_repository: str = os.getenv(
        "HECRAS_ECR_REPO", "surgedps-hecras"
    )
    image_tag: str = os.getenv("HECRAS_IMAGE_TAG", "6.5")

    # AWS Batch
    job_queue: str = os.getenv(
        "HECRAS_JOB_QUEUE", "surgedps-hecras-queue"
    )
    job_definition: str = os.getenv(
        "HECRAS_JOB_DEF", "surgedps-hecras-job"
    )

    # Compute sizing
    vcpus: int = int(os.getenv("HECRAS_VCPUS", "4"))
    memory_mb: int = int(os.getenv("HECRAS_MEMORY_MB", "16384"))
    timeout_seconds: int = int(os.getenv("HECRAS_TIMEOUT", "3600"))

    # S3 paths
    template_s3_prefix: str = os.getenv(
        "HECRAS_TEMPLATE_PREFIX", "hecras/templates"
    )
    output_s3_prefix: str = os.getenv(
        "HECRAS_OUTPUT_PREFIX", "storms"
    )

    # Modeling parameters
    simulation_hours: int = int(os.getenv("HECRAS_SIM_HOURS", "72"))
    output_interval_minutes: int = int(os.getenv("HECRAS_OUTPUT_INTERVAL", "60"))
    timestep_seconds: float = float(os.getenv("HECRAS_TIMESTEP", "10.0"))

    # Which zones have pre-built templates
    available_zones: Dict[str, CoastalZone] = field(
        default_factory=lambda: COASTAL_ZONES
    )

    def get_zones_for_bounds(
        self,
        west: float,
        south: float,
        east: float,
        north: float,
    ) -> List[CoastalZone]:
        """Find all coastal zones that intersect the given bounding box."""
        matching = []
        for zone in self.available_zones.values():
            zw, zs, ze, zn = zone.bounds
            # Check for intersection
            if zw < east and ze > west and zs < north and zn > south:
                matching.append(zone)
        return matching

    def get_zone_for_point(self, lon: float, lat: float) -> Optional[CoastalZone]:
        """Find the coastal zone containing a point (storm center)."""
        for zone in self.available_zones.values():
            zw, zs, ze, zn = zone.bounds
            if zw <= lon <= ze and zs <= lat <= zn:
                return zone
        return None
