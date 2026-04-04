"""
HEC-RAS Runner

Manages the lifecycle of a HEC-RAS 2D simulation:
    1. Select appropriate coastal zone template
    2. Generate project files from template
    3. Inject storm-specific boundary conditions
    4. Execute HEC-RAS (native Linux or Docker)
    5. Extract results to GeoTIFF
    6. Return results for tile generation

Supports two execution modes:
    - LOCAL:  Run HEC-RAS binaries directly (dev/test)
    - BATCH:  Submit to AWS Batch Fargate Spot (production)
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class HECRASRunRequest:
    """Request to run a HEC-RAS simulation."""

    storm_id: str
    advisory_num: str
    storm_center: tuple  # (lon, lat)
    surge_s3_path: str = ""
    rainfall_s3_path: str = ""
    dem_s3_path: str = ""
    discharge_s3_path: str = ""
    simulation_hours: int = 72
    data_bucket: str = ""


@dataclass
class HECRASRunResult:
    """Result of a HEC-RAS simulation."""

    success: bool
    storm_id: str
    advisory_num: str
    mode: str  # "local", "batch", "synthetic"
    output_dir: str = ""
    output_files: List[str] = field(default_factory=list)
    max_depth_m: float = 0.0
    error: str = ""
    batch_job_id: str = ""
    duration_seconds: float = 0.0

    @property
    def depth_rasters(self) -> List[str]:
        """Return only the depth GeoTIFF files."""
        return [f for f in self.output_files if f.endswith(".tif")]


class HECRASRunner:
    """
    Orchestrates HEC-RAS 2D simulation execution.

    Handles zone selection, template generation, boundary injection,
    model execution, and result extraction.
    """

    def __init__(
        self,
        data_bucket: str = "",
        work_dir: Optional[str] = None,
        batch_client: Any = None,
    ):
        from .config import HECRASConfig

        self.config = HECRASConfig()
        self.data_bucket = data_bucket
        self.work_dir = work_dir or tempfile.mkdtemp(prefix="hecras_")
        self._batch_client = batch_client

    def run(self, request: HECRASRunRequest) -> HECRASRunResult:
        """
        Execute a HEC-RAS simulation for the given storm.

        Automatically selects the correct coastal zone, generates
        project files, and chooses execution mode based on whether
        HEC-RAS binaries are available.

        Args:
            request: Run request with storm details and S3 paths

        Returns:
            HECRASRunResult with output file paths and statistics
        """
        start_time = time.time()
        lon, lat = request.storm_center

        # Step 1: Select coastal zone
        zone = self.config.get_zone_for_point(lon, lat)
        if zone is None:
            # Try broader search
            zones = self.config.get_zones_for_bounds(
                lon - 3, lat - 3, lon + 3, lat + 3
            )
            zone = zones[0] if zones else None

        if zone is None:
            return HECRASRunResult(
                success=False,
                storm_id=request.storm_id,
                advisory_num=request.advisory_num,
                mode="none",
                error=f"No coastal zone template for ({lon}, {lat})",
            )

        logger.info(
            f"Selected zone '{zone.name}' ({zone.display_name}) "
            f"for storm at ({lon}, {lat})"
        )

        # Step 2: Generate project files
        from .template_gen import HECRASTemplateGenerator

        project_dir = os.path.join(
            self.work_dir,
            f"{request.storm_id}_{request.advisory_num}",
        )
        generator = HECRASTemplateGenerator(self.config)
        project = generator.generate(
            zone=zone,
            output_dir=project_dir,
            storm_id=request.storm_id,
            terrain_path=self._resolve_path(request.dem_s3_path, "terrain.tif"),
        )

        # Step 3: Inject boundary conditions
        from .boundary_injector import BoundaryInjector

        injector = BoundaryInjector(project_dir)
        bc = injector.build_conditions(
            storm_id=request.storm_id,
            advisory_num=request.advisory_num,
            surge_file=self._resolve_path(request.surge_s3_path, "surge.tif"),
            rainfall_file=self._resolve_path(
                request.rainfall_s3_path, "rainfall.tif"
            ),
            dem_file=self._resolve_path(request.dem_s3_path, "terrain.tif"),
            discharge_file=self._resolve_path(
                request.discharge_s3_path, "discharge.csv"
            ),
            simulation_hours=request.simulation_hours,
        )
        injector.inject(bc)

        # Step 4: Choose execution mode
        output_dir = os.path.join(project_dir, "output")

        if self._has_hecras_binaries():
            result = self._run_local(project, output_dir, request, zone)
        elif self._batch_client and self.data_bucket:
            result = self._run_batch(request, zone)
        else:
            result = self._run_synthetic(
                project_dir, output_dir, request, zone
            )

        result.duration_seconds = time.time() - start_time
        return result

    def _has_hecras_binaries(self) -> bool:
        """Check if HEC-RAS Linux binaries are available."""
        try:
            subprocess.run(
                ["RasGeomPreprocess", "--version"],
                capture_output=True,
                timeout=5,
            )
            return True
        except (FileNotFoundError, subprocess.TimeoutExpired):
            return False

    def _run_local(
        self, project, output_dir, request, zone
    ) -> HECRASRunResult:
        """Run HEC-RAS locally using native Linux binaries."""
        logger.info("Running HEC-RAS locally (native binaries)")

        try:
            # Geometry preprocessing
            geom_file = project.geom_file
            subprocess.run(
                ["RasGeomPreprocess", geom_file],
                check=True,
                capture_output=True,
                timeout=300,
            )

            # Find compiled geometry
            comp_files = list(Path(project.project_dir).glob("*.c0[1-9]"))
            if not comp_files:
                raise FileNotFoundError("Geometry preprocessing failed")

            # Run unsteady simulation
            plan_name = Path(project.plan_file).stem
            subprocess.run(
                ["RasUnsteady", str(comp_files[0]), plan_name],
                check=True,
                capture_output=True,
                timeout=self.config.timeout_seconds,
            )

            # Extract results
            from .result_extractor import HECRASResultExtractor

            extractor = HECRASResultExtractor(project.project_dir)
            extraction = extractor.extract_all(
                output_dir=output_dir,
                storm_id=request.storm_id,
                advisory_num=request.advisory_num,
                crs_epsg=zone.crs_epsg,
            )

            output_files = [r.output_path for r in extraction.rasters]

            return HECRASRunResult(
                success=True,
                storm_id=request.storm_id,
                advisory_num=request.advisory_num,
                mode="local",
                output_dir=output_dir,
                output_files=output_files,
                max_depth_m=extraction.max_depth_m,
            )

        except subprocess.CalledProcessError as e:
            return HECRASRunResult(
                success=False,
                storm_id=request.storm_id,
                advisory_num=request.advisory_num,
                mode="local",
                error=f"HEC-RAS execution failed: {e.stderr.decode()[:500]}",
            )
        except Exception as e:
            return HECRASRunResult(
                success=False,
                storm_id=request.storm_id,
                advisory_num=request.advisory_num,
                mode="local",
                error=str(e),
            )

    def _run_batch(self, request: HECRASRunRequest, zone) -> HECRASRunResult:
        """Submit HEC-RAS job to AWS Batch Fargate Spot."""
        logger.info("Submitting HEC-RAS to AWS Batch Fargate Spot")

        try:
            job_name = (
                f"hecras-{request.storm_id}-{request.advisory_num}"
            ).lower()[:128]

            # Build container environment overrides
            env_vars = [
                {"name": "STORM_ID", "value": request.storm_id},
                {"name": "ADVISORY_NUM", "value": request.advisory_num},
                {"name": "DATA_BUCKET", "value": self.data_bucket},
                {"name": "TEMPLATE_NAME", "value": zone.template_name},
                {"name": "SURGE_S3_PATH", "value": request.surge_s3_path},
                {"name": "RAINFALL_S3_PATH", "value": request.rainfall_s3_path},
                {"name": "DEM_S3_PATH", "value": request.dem_s3_path},
                {
                    "name": "OUTPUT_S3_PREFIX",
                    "value": (
                        f"storms/{request.storm_id}/"
                        f"advisory_{request.advisory_num}/hecras"
                    ),
                },
            ]

            response = self._batch_client.submit_job(
                jobName=job_name,
                jobQueue=self.config.job_queue,
                jobDefinition=self.config.job_definition,
                containerOverrides={
                    "environment": env_vars,
                    "resourceRequirements": [
                        {
                            "type": "VCPU",
                            "value": str(self.config.vcpus),
                        },
                        {
                            "type": "MEMORY",
                            "value": str(self.config.memory_mb),
                        },
                    ],
                },
                timeout={
                    "attemptDurationSeconds": self.config.timeout_seconds,
                },
            )

            job_id = response["jobId"]
            logger.info(f"AWS Batch job submitted: {job_id}")

            return HECRASRunResult(
                success=True,
                storm_id=request.storm_id,
                advisory_num=request.advisory_num,
                mode="batch",
                batch_job_id=job_id,
            )

        except Exception as e:
            return HECRASRunResult(
                success=False,
                storm_id=request.storm_id,
                advisory_num=request.advisory_num,
                mode="batch",
                error=f"Batch submission failed: {e}",
            )

    def _run_synthetic(
        self, project_dir, output_dir, request, zone
    ) -> HECRASRunResult:
        """Generate synthetic results when HEC-RAS is not available."""
        logger.info("HEC-RAS not available — generating synthetic results")

        from .synthetic_results import generate_synthetic_depth

        dem_path = self._resolve_path(request.dem_s3_path, "terrain.tif")
        surge_path = self._resolve_path(request.surge_s3_path, "surge.tif")

        try:
            generate_synthetic_depth(
                dem_path=dem_path,
                surge_path=surge_path,
                output_dir=output_dir,
                storm_id=request.storm_id,
                num_timesteps=13,
                crs_epsg=zone.crs_epsg,
            )

            output_files = sorted(
                str(p) for p in Path(output_dir).glob("*.tif")
            )

            # Calculate max depth from envelope
            max_depth = 0.0
            try:
                import rasterio
                import numpy as np
                max_file = os.path.join(output_dir, "hecras_max_depth.tif")
                if os.path.exists(max_file):
                    with rasterio.open(max_file) as src:
                        data = src.read(1)
                        max_depth = float(np.max(data))
            except Exception:
                pass

            return HECRASRunResult(
                success=True,
                storm_id=request.storm_id,
                advisory_num=request.advisory_num,
                mode="synthetic",
                output_dir=output_dir,
                output_files=output_files,
                max_depth_m=max_depth,
            )

        except Exception as e:
            return HECRASRunResult(
                success=False,
                storm_id=request.storm_id,
                advisory_num=request.advisory_num,
                mode="synthetic",
                error=str(e),
            )

    def _resolve_path(self, s3_path: str, local_name: str) -> Optional[str]:
        """
        Resolve an S3 path to a local file.

        If S3 client is available and path exists, download it.
        Otherwise return the local_name in work_dir (may not exist).
        """
        local_path = os.path.join(self.work_dir, local_name)

        if s3_path and self.data_bucket:
            try:
                import boto3
                s3 = boto3.client("s3")
                s3.download_file(self.data_bucket, s3_path, local_path)
                return local_path
            except Exception as e:
                logger.warning(f"Could not download {s3_path}: {e}")

        return local_path if os.path.exists(local_path) else None

    def check_batch_status(self, job_id: str) -> Dict[str, Any]:
        """
        Check the status of an AWS Batch job.

        Returns:
            Dict with 'status', 'reason', and timing info.
        """
        if not self._batch_client:
            return {"status": "UNKNOWN", "reason": "No Batch client"}

        try:
            response = self._batch_client.describe_jobs(jobs=[job_id])
            if response["jobs"]:
                job = response["jobs"][0]
                return {
                    "status": job["status"],
                    "reason": job.get("statusReason", ""),
                    "created": job.get("createdAt"),
                    "started": job.get("startedAt"),
                    "stopped": job.get("stoppedAt"),
                }
            return {"status": "NOT_FOUND"}
        except Exception as e:
            return {"status": "ERROR", "reason": str(e)}
