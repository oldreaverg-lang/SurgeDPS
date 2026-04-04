"""
HEC-RAS Project Template Generator

Generates HEC-RAS project files (.prj, .g01, .p01, .u01) for a
given coastal zone. Templates define the 2D flow area geometry,
mesh configuration, terrain references, and default plan settings.

In production, pre-built templates are stored in S3 and downloaded
by the Docker container. This module generates the text-format
project files programmatically for development and testing.
"""

from __future__ import annotations

import os
import textwrap
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple

from .config import CoastalZone, HECRASConfig


@dataclass
class ProjectFiles:
    """Collection of generated HEC-RAS project file paths."""

    project_dir: str
    prj_file: str   # .prj — project index
    geom_file: str  # .g01 — geometry
    plan_file: str  # .p01 — plan
    flow_file: str  # .u01 — unsteady flow
    terrain_file: Optional[str] = None  # Terrain .tif reference


class HECRASTemplateGenerator:
    """Generate HEC-RAS 2D project files from a coastal zone definition."""

    def __init__(self, config: Optional[HECRASConfig] = None):
        self.config = config or HECRASConfig()

    def generate(
        self,
        zone: CoastalZone,
        output_dir: str,
        storm_id: str = "UNKNOWN",
        terrain_path: Optional[str] = None,
    ) -> ProjectFiles:
        """
        Generate a complete set of HEC-RAS project files.

        Args:
            zone: Coastal zone with geometry definition
            output_dir: Directory to write project files
            storm_id: Storm identifier for naming
            terrain_path: Path to DEM terrain file (.tif)

        Returns:
            ProjectFiles with paths to all generated files
        """
        os.makedirs(output_dir, exist_ok=True)
        base_name = f"{zone.template_name}_{storm_id}"

        prj_path = os.path.join(output_dir, f"{base_name}.prj")
        geom_path = os.path.join(output_dir, f"{base_name}.g01")
        plan_path = os.path.join(output_dir, f"{base_name}.p01")
        flow_path = os.path.join(output_dir, f"{base_name}.u01")

        # Generate each file
        self._write_project_file(prj_path, base_name, zone)
        self._write_geometry_file(geom_path, zone, terrain_path)
        self._write_plan_file(plan_path, base_name, zone)
        self._write_flow_file(flow_path, zone)

        return ProjectFiles(
            project_dir=output_dir,
            prj_file=prj_path,
            geom_file=geom_path,
            plan_file=plan_path,
            flow_file=flow_path,
            terrain_file=terrain_path,
        )

    def _write_project_file(
        self, path: str, base_name: str, zone: CoastalZone
    ) -> None:
        """Write the .prj project index file."""
        content = textwrap.dedent(f"""\
            Proj Title={zone.display_name} - SurgeDPS Storm Surge Model
            Current Plan=p01
            Default Exp/Contr=0.3,0.1
            English Units
            Geom File=g01
            Flow File=u01
            Plan File=p01
            Y Axis Title=Elevation
            X Axis Title(1)=Main Channel Distance
            BEGIN DESCRIPTION:
            SurgeDPS automated 2D compound flood model for {zone.display_name}.
            Zone: {zone.name}
            CRS: EPSG:{zone.crs_epsg}
            Cell size: {zone.cell_size_m}m
            Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}
            END DESCRIPTION:
        """)
        with open(path, "w") as f:
            f.write(content)

    def _write_geometry_file(
        self,
        path: str,
        zone: CoastalZone,
        terrain_path: Optional[str] = None,
    ) -> None:
        """
        Write the .g01 geometry file with 2D flow area definition.

        Defines:
        - 2D flow area covering the coastal zone bounds
        - Computation point spacing (mesh cell size)
        - Terrain file reference
        - Manning's roughness defaults
        """
        west, south, east, north = zone.bounds
        cell = zone.cell_size_m

        # Convert bounds to projected coordinates for the 2D flow area
        # perimeter (simplified rectangular domain).
        # In production, pre-built templates have detailed coastline
        # perimeters. For auto-generation, use the bounding box.
        content = textwrap.dedent(f"""\
            Geom Title={zone.display_name} 2D Geometry
            Program Version=6.50
            Viewing Rectangle= {west}  , {south}  , {east}  , {north}

            BEGIN GEOM DESCRIPTION:
            2D flow area for {zone.display_name} coastal flooding.
            Mesh cell size: {cell}m
            END GEOM DESCRIPTION:

            2D Flow Area={zone.name}_2d
              Storage Area Is2D=-1
              2D Cell Size X={cell}
              2D Cell Size Y={cell}
              Mannings n= {zone.manning_default}

              2D Flow Area Perimeter= 5
                {west}  , {south}
                {east}  , {south}
                {east}  , {north}
                {west}  , {north}
                {west}  , {south}

              2D Flow Area BC Lines= 2
                BC Line=Coastal_BC
                  BC Line Type=Stage Hydrograph
                  BC Line Points= 2
                    {west}  , {south}
                    {east}  , {south}
                BC Line=Upstream_BC
                  BC Line Type=Flow Hydrograph
                  BC Line Points= 2
                    {west}  , {north}
                    {east}  , {north}
        """)

        if terrain_path:
            content += textwrap.dedent(f"""\

              Terrain Filename={terrain_path}
            """)

        with open(path, "w") as f:
            f.write(content)

    def _write_plan_file(
        self, path: str, base_name: str, zone: CoastalZone
    ) -> None:
        """
        Write the .p01 plan file with simulation settings.

        Configures:
        - Simulation duration (72 hours default)
        - Computational timestep
        - Output interval
        - 2D equation set (Diffusion Wave for speed)
        """
        sim_hours = self.config.simulation_hours
        dt = self.config.timestep_seconds
        output_min = self.config.output_interval_minutes

        # Simulation window: T-0 to T+72h
        # Start date/time will be overwritten by boundary_injector
        content = textwrap.dedent(f"""\
            Plan Title=SurgeDPS Auto-Plan {zone.name}
            Program Version=6.50
            Short Identifier={zone.name[:12]}

            Geom File=g01
            Flow File=u01

            Simulation Date=01JAN2024,0000,01JAN2024,0000
            Computation Interval={dt}
            Output Interval={output_min}MIN

            2D Equation Set=Diffusion Wave
            2D Theta=1.0
            2D Theta Warmup=1.0

            2D Vol and Iter Tolerances= 0.003 , 20

            Run HTab= 0
            Run UNet= 1
            Run Sediment= 0
            Run PostProcess= 1
            Run WQNet= 0

            Write IC File= 0
            Write Warm File= 0
            Echo Input= 0
            Echo Parameters= 0
            Echo Output= 0

            Map Output=2D Flow Area
            Map Output Depth= 1
            Map Output Velocity= 1
            Map Output WSE= 1
            Map Output Interval={output_min}MIN
        """)
        with open(path, "w") as f:
            f.write(content)

    def _write_flow_file(self, path: str, zone: CoastalZone) -> None:
        """
        Write the .u01 unsteady flow file with placeholder
        boundary conditions.

        The boundary_injector module will overwrite these with
        storm-specific surge hydrographs and rainfall inputs.
        """
        content = textwrap.dedent(f"""\
            Flow Title=SurgeDPS Unsteady Flow - {zone.display_name}
            Program Version=6.50

            Boundary Location=Coastal_BC
              Interval=1HOUR
              Stage Hydrograph=  3
                01JAN2024,0000, 0.0
                01JAN2024,1200, 0.0
                02JAN2024,0000, 0.0

            Boundary Location=Upstream_BC
              Interval=1HOUR
              Flow Hydrograph=  3
                01JAN2024,0000, 10.0
                01JAN2024,1200, 10.0
                02JAN2024,0000, 10.0
        """)
        with open(path, "w") as f:
            f.write(content)
