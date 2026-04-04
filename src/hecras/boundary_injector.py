"""
Storm-Specific Boundary Condition Injector

Reads P-Surge and QPF data, converts them into HEC-RAS boundary
condition hydrographs, and rewrites the .u01 flow file with
storm-specific inputs.

Boundary condition mapping:
    - Coastal BC: Stage hydrograph from P-Surge probabilistic surge heights
    - Upstream BC: Flow hydrograph from NWM discharge + rainfall excess
    - 2D Rain-on-Grid: Direct precipitation applied to 2D mesh cells

Supported input formats:
    - GeoTIFF (surge, DEM, rainfall depth)
    - NetCDF (NWM discharge)
    - CSV (synthetic test data)
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

logger = logging.getLogger(__name__)


@dataclass
class Hydrograph:
    """Time series of stage or flow values for a boundary condition."""

    times_hours: List[float]
    values: List[float]
    bc_type: str  # "Stage Hydrograph" or "Flow Hydrograph"
    location_name: str = ""

    @property
    def count(self) -> int:
        return len(self.values)


@dataclass
class RainfallTimeSeries:
    """Spatially-uniform rainfall for rain-on-grid 2D modeling."""

    times_hours: List[float]
    rates_mm_hr: List[float]  # Rainfall intensity at each timestep


@dataclass
class BoundaryConditions:
    """Complete set of boundary conditions for a storm event."""

    storm_id: str
    advisory_num: str
    start_time: datetime
    simulation_hours: int = 72

    coastal_bc: Optional[Hydrograph] = None
    upstream_bc: Optional[Hydrograph] = None
    rainfall: Optional[RainfallTimeSeries] = None

    # Metadata
    max_surge_m: float = 0.0
    max_rainfall_mm: float = 0.0
    max_discharge_cms: float = 0.0


class BoundaryInjector:
    """
    Converts raw storm data into HEC-RAS boundary conditions
    and writes them into the project's .u01 flow file.
    """

    def __init__(self, project_dir: str):
        self.project_dir = project_dir

    def build_conditions(
        self,
        storm_id: str,
        advisory_num: str,
        surge_file: Optional[str] = None,
        rainfall_file: Optional[str] = None,
        dem_file: Optional[str] = None,
        discharge_file: Optional[str] = None,
        simulation_hours: int = 72,
    ) -> BoundaryConditions:
        """
        Build boundary conditions from storm input files.

        Args:
            storm_id: ATCF storm identifier
            advisory_num: Advisory number (e.g., "012")
            surge_file: Path to P-Surge GeoTIFF (max surge height)
            rainfall_file: Path to QPF GeoTIFF (total rainfall depth)
            dem_file: Path to terrain DEM GeoTIFF
            discharge_file: Path to NWM discharge CSV/NetCDF
            simulation_hours: Total simulation duration

        Returns:
            BoundaryConditions ready for injection into .u01 file
        """
        start_time = datetime.utcnow().replace(minute=0, second=0, microsecond=0)

        bc = BoundaryConditions(
            storm_id=storm_id,
            advisory_num=advisory_num,
            start_time=start_time,
            simulation_hours=simulation_hours,
        )

        # Build coastal surge hydrograph
        bc.coastal_bc = self._build_surge_hydrograph(
            surge_file, simulation_hours
        )
        bc.max_surge_m = max(bc.coastal_bc.values) if bc.coastal_bc else 0.0

        # Build upstream flow hydrograph
        bc.upstream_bc = self._build_flow_hydrograph(
            discharge_file, simulation_hours
        )
        bc.max_discharge_cms = max(bc.upstream_bc.values) if bc.upstream_bc else 0.0

        # Build rainfall time series
        bc.rainfall = self._build_rainfall_series(
            rainfall_file, simulation_hours
        )
        bc.max_rainfall_mm = (
            sum(bc.rainfall.rates_mm_hr) if bc.rainfall else 0.0
        )

        logger.info(
            f"Built boundary conditions for {storm_id} advisory {advisory_num}: "
            f"surge={bc.max_surge_m:.1f}m, "
            f"discharge={bc.max_discharge_cms:.0f}cms, "
            f"rainfall={bc.max_rainfall_mm:.0f}mm"
        )

        return bc

    def _build_surge_hydrograph(
        self,
        surge_file: Optional[str],
        sim_hours: int,
    ) -> Hydrograph:
        """
        Convert P-Surge max envelope into a time-varying stage
        hydrograph for the coastal boundary.

        Shape: gradual ramp-up → peak at landfall → exponential decay
        """
        max_surge = 0.0

        if surge_file and os.path.exists(surge_file):
            try:
                import rasterio
                with rasterio.open(surge_file) as src:
                    data = src.read(1)
                    valid = data[data > -9999]
                    if len(valid) > 0:
                        max_surge = float(np.percentile(valid, 95))
            except Exception as e:
                logger.warning(f"Could not read surge file: {e}")

        if max_surge <= 0:
            max_surge = 3.0  # Default synthetic: 3m surge (Cat 2)
            logger.info(f"Using synthetic surge: {max_surge}m")

        # Build temporal profile:
        # 0-12h: ramp from 0 to 30% of peak (approaching storm)
        # 12-24h: ramp from 30% to 100% of peak (landfall)
        # 24-36h: decay from 100% to 40% of peak
        # 36-72h: slow decay from 40% to 5%
        times = list(range(0, sim_hours + 1, 1))  # Hourly
        values = []
        for t in times:
            if t <= 12:
                frac = 0.3 * (t / 12.0)
            elif t <= 24:
                frac = 0.3 + 0.7 * ((t - 12) / 12.0)
            elif t <= 36:
                frac = 1.0 - 0.6 * ((t - 24) / 12.0)
            else:
                frac = 0.4 * np.exp(-0.05 * (t - 36))
            values.append(round(max_surge * frac, 3))

        return Hydrograph(
            times_hours=times,
            values=values,
            bc_type="Stage Hydrograph",
            location_name="Coastal_BC",
        )

    def _build_flow_hydrograph(
        self,
        discharge_file: Optional[str],
        sim_hours: int,
    ) -> Hydrograph:
        """
        Build upstream flow hydrograph from NWM discharge data.

        Falls back to synthetic flood hydrograph if no data available.
        """
        base_flow = 50.0  # m³/s baseflow
        peak_flow = 500.0  # m³/s peak discharge

        if discharge_file and os.path.exists(discharge_file):
            try:
                import csv
                with open(discharge_file) as f:
                    reader = csv.DictReader(f)
                    discharges = []
                    for row in reader:
                        q = float(row.get("discharge_cms", 0))
                        discharges.append(q)
                    if discharges:
                        base_flow = min(discharges)
                        peak_flow = max(discharges)
            except Exception as e:
                logger.warning(f"Could not read discharge file: {e}")

        # Synthetic flood hydrograph shape:
        # Unit hydrograph with peak at T+24h (concurrent with surge)
        times = list(range(0, sim_hours + 1, 1))
        values = []
        tp = 24.0  # Time to peak (hours)
        for t in times:
            if t <= tp:
                # Rising limb (SCS dimensionless UH shape)
                ratio = t / tp
                q = base_flow + (peak_flow - base_flow) * (ratio ** 2.5)
            else:
                # Recession limb (exponential decay)
                decay_hours = t - tp
                q = base_flow + (peak_flow - base_flow) * np.exp(
                    -0.08 * decay_hours
                )
            values.append(round(max(q, base_flow), 2))

        return Hydrograph(
            times_hours=times,
            values=values,
            bc_type="Flow Hydrograph",
            location_name="Upstream_BC",
        )

    def _build_rainfall_series(
        self,
        rainfall_file: Optional[str],
        sim_hours: int,
    ) -> RainfallTimeSeries:
        """
        Convert QPF total rainfall depth into a temporal distribution.

        Uses SCS Type III distribution (common for coastal storms).
        """
        total_mm = 0.0

        if rainfall_file and os.path.exists(rainfall_file):
            try:
                import rasterio
                with rasterio.open(rainfall_file) as src:
                    data = src.read(1)
                    valid = data[data > 0]
                    if len(valid) > 0:
                        total_mm = float(np.mean(valid))
            except Exception as e:
                logger.warning(f"Could not read rainfall file: {e}")

        if total_mm <= 0:
            total_mm = 250.0  # Default: 250mm (10 inches) over 72 hours
            logger.info(f"Using synthetic rainfall: {total_mm}mm")

        # SCS Type III temporal distribution (simplified)
        # Heavy band of rainfall 12-36h (concurrent with surge approach)
        times = list(range(0, sim_hours + 1, 1))
        rates = []
        for t in times:
            if t < 6:
                frac = 0.01
            elif t < 12:
                frac = 0.02
            elif t < 18:
                frac = 0.05
            elif t < 24:
                frac = 0.08
            elif t < 30:
                frac = 0.06
            elif t < 36:
                frac = 0.04
            elif t < 48:
                frac = 0.02
            else:
                frac = 0.005

            rate_mm_hr = total_mm * frac  # mm/hr for this hour
            rates.append(round(rate_mm_hr, 2))

        return RainfallTimeSeries(times_hours=times, rates_mm_hr=rates)

    def inject(self, bc: BoundaryConditions) -> str:
        """
        Write boundary conditions into the project's .u01 flow file.

        Finds the existing .u01 file, rewrites it with storm-specific
        hydrographs, and updates the .p01 plan file simulation dates.

        Returns:
            Path to the updated flow file
        """
        # Find existing flow file
        flow_files = list(Path(self.project_dir).glob("*.u0[1-9]"))
        if not flow_files:
            raise FileNotFoundError(
                f"No .u01 flow file found in {self.project_dir}"
            )
        flow_path = str(flow_files[0])

        # Format HEC-RAS date strings
        start = bc.start_time
        end = start + timedelta(hours=bc.simulation_hours)
        date_fmt = "%d%b%Y".upper()

        def ras_datetime(dt: datetime) -> str:
            return f"{dt.strftime(date_fmt)},{dt.strftime('%H%M')}"

        # Build flow file content
        lines = []
        lines.append(f"Flow Title=SurgeDPS {bc.storm_id} Advisory {bc.advisory_num}")
        lines.append("Program Version=6.50")
        lines.append("")

        # Coastal boundary (stage hydrograph)
        if bc.coastal_bc:
            h = bc.coastal_bc
            lines.append(f"Boundary Location={h.location_name}")
            lines.append("  Interval=1HOUR")
            lines.append(f"  {h.bc_type}=  {h.count}")
            for t, v in zip(h.times_hours, h.values):
                dt = start + timedelta(hours=t)
                lines.append(f"    {ras_datetime(dt)}, {v}")
            lines.append("")

        # Upstream boundary (flow hydrograph)
        if bc.upstream_bc:
            h = bc.upstream_bc
            lines.append(f"Boundary Location={h.location_name}")
            lines.append("  Interval=1HOUR")
            lines.append(f"  {h.bc_type}=  {h.count}")
            for t, v in zip(h.times_hours, h.values):
                dt = start + timedelta(hours=t)
                lines.append(f"    {ras_datetime(dt)}, {v}")
            lines.append("")

        # Rain-on-grid (if available)
        if bc.rainfall:
            r = bc.rainfall
            lines.append("Precipitation=Rain on Grid")
            lines.append("  Interval=1HOUR")
            lines.append(f"  Rain Data=  {len(r.rates_mm_hr)}")
            for t, rate in zip(r.times_hours, r.rates_mm_hr):
                dt = start + timedelta(hours=t)
                lines.append(f"    {ras_datetime(dt)}, {rate}")
            lines.append("")

        # Write flow file
        with open(flow_path, "w") as f:
            f.write("\n".join(lines))

        # Update plan file simulation dates
        self._update_plan_dates(bc)

        logger.info(f"Injected boundary conditions into {flow_path}")
        return flow_path

    def _update_plan_dates(self, bc: BoundaryConditions) -> None:
        """Update the .p01 plan file with correct simulation dates."""
        plan_files = list(Path(self.project_dir).glob("*.p0[1-9]"))
        if not plan_files:
            return

        plan_path = str(plan_files[0])
        start = bc.start_time
        end = start + timedelta(hours=bc.simulation_hours)

        date_fmt = "%d%b%Y".upper()
        sim_date = (
            f"Simulation Date="
            f"{start.strftime(date_fmt)},{start.strftime('%H%M')},"
            f"{end.strftime(date_fmt)},{end.strftime('%H%M')}"
        )

        with open(plan_path) as f:
            content = f.read()

        # Replace the simulation date line
        content = re.sub(
            r"Simulation Date=.*",
            sim_date,
            content,
        )

        with open(plan_path, "w") as f:
            f.write(content)


# ── CLI entry point (used by Docker entrypoint) ──────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Inject storm boundary conditions into HEC-RAS project"
    )
    parser.add_argument("--project-dir", required=True)
    parser.add_argument("--storm-id", required=True)
    parser.add_argument("--advisory", required=True)
    parser.add_argument("--surge-file", default=None)
    parser.add_argument("--rainfall-file", default=None)
    parser.add_argument("--dem-file", default=None)
    parser.add_argument("--discharge-file", default=None)
    parser.add_argument("--sim-hours", type=int, default=72)

    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    injector = BoundaryInjector(args.project_dir)
    bc = injector.build_conditions(
        storm_id=args.storm_id,
        advisory_num=args.advisory,
        surge_file=args.surge_file,
        rainfall_file=args.rainfall_file,
        dem_file=args.dem_file,
        discharge_file=args.discharge_file,
        simulation_hours=args.sim_hours,
    )
    injector.inject(bc)


if __name__ == "__main__":
    main()
