"""
HEC-RAS HDF5 Result Extractor

Reads the HDF5 output file produced by HEC-RAS 2D simulation and
extracts flood depth, water surface elevation, and velocity grids
as GeoTIFF rasters.

HDF5 structure for 2D results:
    Results/
        Unsteady/
            Output/
                Output Blocks/
                    Base Output/
                        Unsteady Time Series/
                            2D flow areas/
                                <area_name>/
                                    Water Surface
                                    Depth
                                    Face Velocity

Supported extraction modes:
    - Maximum depth envelope (peak depth at every cell)
    - Time series snapshots (depth at each output interval)
    - Maximum WSE (water surface elevation)
"""

from __future__ import annotations

import argparse
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

logger = logging.getLogger(__name__)


@dataclass
class ExtractedResult:
    """A single extracted raster from HEC-RAS output."""

    output_path: str
    variable: str  # "depth", "wse", "velocity"
    timestep: Optional[int] = None  # None = max envelope
    max_value: float = 0.0
    min_value: float = 0.0
    nonzero_cells: int = 0
    total_cells: int = 0

    @property
    def flooded_pct(self) -> float:
        if self.total_cells == 0:
            return 0.0
        return 100.0 * self.nonzero_cells / self.total_cells


@dataclass
class ExtractionResult:
    """Complete extraction results for a HEC-RAS run."""

    storm_id: str
    advisory_num: str
    rasters: List[ExtractedResult]
    hdf_path: Optional[str] = None

    @property
    def max_depth_m(self) -> float:
        depths = [r for r in self.rasters if r.variable == "depth"]
        return max((r.max_value for r in depths), default=0.0)


class HECRASResultExtractor:
    """Extract flood depth grids from HEC-RAS HDF5 output."""

    # HDF5 paths within the results file
    HDF_PATHS = {
        "depth": (
            "Results/Unsteady/Output/Output Blocks/Base Output/"
            "Unsteady Time Series/2D flow areas/{area}/Depth"
        ),
        "wse": (
            "Results/Unsteady/Output/Output Blocks/Base Output/"
            "Unsteady Time Series/2D flow areas/{area}/Water Surface"
        ),
        "velocity": (
            "Results/Unsteady/Output/Output Blocks/Base Output/"
            "Unsteady Time Series/2D flow areas/{area}/Face Velocity"
        ),
    }

    # Geometry HDF5 paths for cell coordinates
    GEOM_HDF_PATHS = {
        "cell_centers": (
            "Geometry/2D Flow Areas/{area}/Cells Center Coordinate"
        ),
        "cell_face_info": (
            "Geometry/2D Flow Areas/{area}/Cells Face Info"
        ),
    }

    def __init__(self, project_dir: str):
        self.project_dir = project_dir
        self._hdf_path: Optional[str] = None

    def find_hdf_output(self) -> Optional[str]:
        """Locate the HDF5 results file in the project directory."""
        if self._hdf_path:
            return self._hdf_path

        # HEC-RAS outputs to <plan_name>.p01.hdf
        hdf_files = list(Path(self.project_dir).glob("*.p0[1-9].hdf"))
        if hdf_files:
            self._hdf_path = str(hdf_files[0])
            return self._hdf_path

        # Also check for .hdf without plan extension
        hdf_files = list(Path(self.project_dir).glob("*.hdf"))
        if hdf_files:
            self._hdf_path = str(hdf_files[0])
            return self._hdf_path

        return None

    def list_2d_areas(self) -> List[str]:
        """List all 2D flow area names in the HDF5 file."""
        hdf_path = self.find_hdf_output()
        if not hdf_path:
            return []

        try:
            import h5py
            with h5py.File(hdf_path, "r") as f:
                base = "Results/Unsteady/Output/Output Blocks/Base Output/Unsteady Time Series/2D flow areas"
                if base in f:
                    return list(f[base].keys())
        except Exception as e:
            logger.warning(f"Could not read HDF5: {e}")

        return []

    def extract_max_depth(
        self,
        output_dir: str,
        area_name: Optional[str] = None,
        crs_epsg: int = 5070,
    ) -> Optional[ExtractedResult]:
        """
        Extract the maximum flood depth envelope from 2D results.

        This is the peak depth reached at each cell across all timesteps.

        Args:
            output_dir: Directory to write the output GeoTIFF
            area_name: 2D flow area name (auto-detected if None)
            crs_epsg: CRS for the output raster

        Returns:
            ExtractedResult with path and statistics
        """
        hdf_path = self.find_hdf_output()
        if not hdf_path:
            logger.warning("No HDF5 output file found")
            return None

        try:
            import h5py
            import rasterio
            from rasterio.transform import from_bounds

            with h5py.File(hdf_path, "r") as f:
                # Auto-detect area name
                if area_name is None:
                    areas = self.list_2d_areas()
                    if not areas:
                        logger.error("No 2D flow areas in HDF5")
                        return None
                    area_name = areas[0]

                depth_path = self.HDF_PATHS["depth"].format(area=area_name)
                if depth_path not in f:
                    logger.error(f"Depth data not found at {depth_path}")
                    return None

                # Read depth time series: shape = (timesteps, cells)
                depth_ds = f[depth_path]
                depth_data = depth_ds[:]

                # Max across all timesteps
                max_depth = np.max(depth_data, axis=0)

                # Get cell center coordinates for georeferencing
                coord_path = self.GEOM_HDF_PATHS["cell_centers"].format(
                    area=area_name
                )
                if coord_path in f:
                    coords = f[coord_path][:]
                    xs, ys = coords[:, 0], coords[:, 1]
                else:
                    logger.warning("No cell coordinates — using index grid")
                    n = len(max_depth)
                    side = int(np.ceil(np.sqrt(n)))
                    xs = np.arange(side, dtype=float)
                    ys = np.arange(side, dtype=float)

            # Rasterize unstructured cell data to regular grid
            grid, transform = self._rasterize_cells(
                xs, ys, max_depth, crs_epsg
            )

            # Write GeoTIFF
            os.makedirs(output_dir, exist_ok=True)
            out_path = os.path.join(output_dir, "hecras_max_depth.tif")
            self._write_geotiff(out_path, grid, transform, crs_epsg)

            nonzero = int(np.count_nonzero(grid > 0.01))
            return ExtractedResult(
                output_path=out_path,
                variable="depth",
                timestep=None,
                max_value=float(np.nanmax(grid)),
                min_value=float(np.nanmin(grid[grid > 0])) if nonzero > 0 else 0.0,
                nonzero_cells=nonzero,
                total_cells=int(grid.size),
            )

        except ImportError:
            logger.error("h5py or rasterio not available")
            return None
        except Exception as e:
            logger.error(f"Failed to extract max depth: {e}")
            return None

    def extract_timestep(
        self,
        output_dir: str,
        timestep_index: int,
        area_name: Optional[str] = None,
        crs_epsg: int = 5070,
    ) -> Optional[ExtractedResult]:
        """Extract flood depth at a specific timestep."""
        hdf_path = self.find_hdf_output()
        if not hdf_path:
            return None

        try:
            import h5py

            with h5py.File(hdf_path, "r") as f:
                if area_name is None:
                    areas = self.list_2d_areas()
                    if not areas:
                        return None
                    area_name = areas[0]

                depth_path = self.HDF_PATHS["depth"].format(area=area_name)
                if depth_path not in f:
                    return None

                depth_ds = f[depth_path]
                if timestep_index >= depth_ds.shape[0]:
                    logger.error(
                        f"Timestep {timestep_index} out of range "
                        f"(max {depth_ds.shape[0] - 1})"
                    )
                    return None

                depth_data = depth_ds[timestep_index, :]

                coord_path = self.GEOM_HDF_PATHS["cell_centers"].format(
                    area=area_name
                )
                if coord_path in f:
                    coords = f[coord_path][:]
                    xs, ys = coords[:, 0], coords[:, 1]
                else:
                    n = len(depth_data)
                    side = int(np.ceil(np.sqrt(n)))
                    xs = np.arange(side, dtype=float)
                    ys = np.arange(side, dtype=float)

            grid, transform = self._rasterize_cells(xs, ys, depth_data, crs_epsg)

            os.makedirs(output_dir, exist_ok=True)
            out_path = os.path.join(
                output_dir, f"hecras_depth_t{timestep_index:03d}.tif"
            )
            self._write_geotiff(out_path, grid, transform, crs_epsg)

            nonzero = int(np.count_nonzero(grid > 0.01))
            return ExtractedResult(
                output_path=out_path,
                variable="depth",
                timestep=timestep_index,
                max_value=float(np.nanmax(grid)),
                min_value=float(np.nanmin(grid[grid > 0])) if nonzero > 0 else 0.0,
                nonzero_cells=nonzero,
                total_cells=int(grid.size),
            )

        except Exception as e:
            logger.error(f"Failed to extract timestep {timestep_index}: {e}")
            return None

    def extract_all(
        self,
        output_dir: str,
        storm_id: str,
        advisory_num: str,
        crs_epsg: int = 5070,
    ) -> ExtractionResult:
        """
        Full extraction: max depth envelope + timestep snapshots.

        Returns:
            ExtractionResult with all extracted rasters
        """
        result = ExtractionResult(
            storm_id=storm_id,
            advisory_num=advisory_num,
            rasters=[],
            hdf_path=self.find_hdf_output(),
        )

        # Max depth envelope
        max_depth = self.extract_max_depth(output_dir, crs_epsg=crs_epsg)
        if max_depth:
            result.rasters.append(max_depth)

        # Timestep snapshots (every 6 hours = indices 0, 6, 12, ...)
        # Only if HDF5 exists
        if self.find_hdf_output():
            try:
                import h5py
                with h5py.File(self.find_hdf_output(), "r") as f:
                    areas = self.list_2d_areas()
                    if areas:
                        depth_path = self.HDF_PATHS["depth"].format(
                            area=areas[0]
                        )
                        if depth_path in f:
                            n_steps = f[depth_path].shape[0]
                            # Extract every 6th timestep
                            for i in range(0, n_steps, 6):
                                ts_result = self.extract_timestep(
                                    output_dir, i, crs_epsg=crs_epsg
                                )
                                if ts_result:
                                    result.rasters.append(ts_result)
            except Exception as e:
                logger.warning(f"Could not extract timesteps: {e}")

        return result

    @staticmethod
    def _rasterize_cells(
        xs: np.ndarray,
        ys: np.ndarray,
        values: np.ndarray,
        crs_epsg: int,
        cell_size: float = 30.0,
    ) -> Tuple[np.ndarray, Any]:
        """
        Convert unstructured cell center data to a regular raster grid.

        Uses scipy griddata interpolation to resample from irregular
        HEC-RAS mesh cells to a regular grid.
        """
        from rasterio.transform import from_bounds

        # Determine bounds
        x_min, x_max = float(np.min(xs)), float(np.max(xs))
        y_min, y_max = float(np.min(ys)), float(np.max(ys))

        # Create regular grid
        ncols = max(1, int((x_max - x_min) / cell_size))
        nrows = max(1, int((y_max - y_min) / cell_size))

        try:
            from scipy.interpolate import griddata

            grid_x = np.linspace(x_min, x_max, ncols)
            grid_y = np.linspace(y_max, y_min, nrows)  # top to bottom
            gx, gy = np.meshgrid(grid_x, grid_y)

            # Truncate values to match coordinate count
            n = min(len(xs), len(ys), len(values))
            grid = griddata(
                (xs[:n], ys[:n]),
                values[:n],
                (gx, gy),
                method="linear",
                fill_value=0.0,
            )
        except ImportError:
            # Fallback: nearest neighbor binning
            logger.warning("scipy not available — using nearest neighbor")
            grid = np.zeros((nrows, ncols), dtype=np.float32)
            for x, y, v in zip(xs, ys, values):
                col = min(ncols - 1, max(0, int((x - x_min) / cell_size)))
                row = min(nrows - 1, max(0, int((y_max - y) / cell_size)))
                grid[row, col] = max(grid[row, col], v)

        transform = from_bounds(x_min, y_min, x_max, y_max, ncols, nrows)
        return grid.astype(np.float32), transform

    @staticmethod
    def _write_geotiff(
        path: str,
        data: np.ndarray,
        transform: Any,
        crs_epsg: int,
    ) -> None:
        """Write a 2D numpy array to a GeoTIFF."""
        import rasterio
        from rasterio.crs import CRS

        with rasterio.open(
            path,
            "w",
            driver="GTiff",
            height=data.shape[0],
            width=data.shape[1],
            count=1,
            dtype=data.dtype,
            crs=CRS.from_epsg(crs_epsg),
            transform=transform,
            compress="deflate",
            tiled=True,
            blockxsize=256,
            blockysize=256,
        ) as dst:
            dst.write(data, 1)
            dst.update_tags(
                MODEL="HEC-RAS 6.5",
                GENERATOR="SurgeDPS",
                VARIABLE="flood_depth_m",
            )


# ── CLI entry point ──────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Extract flood depth from HEC-RAS HDF5 output"
    )
    parser.add_argument("--project-dir", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--storm-id", default="UNKNOWN")
    parser.add_argument("--advisory", default="000")
    parser.add_argument("--crs", type=int, default=5070)

    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)

    extractor = HECRASResultExtractor(args.project_dir)
    result = extractor.extract_all(
        output_dir=args.output_dir,
        storm_id=args.storm_id,
        advisory_num=args.advisory,
        crs_epsg=args.crs,
    )

    print(f"Extracted {len(result.rasters)} rasters")
    print(f"Max depth: {result.max_depth_m:.2f}m")
    for r in result.rasters:
        print(f"  {r.output_path}: {r.variable} max={r.max_value:.2f}m flooded={r.flooded_pct:.1f}%")


if __name__ == "__main__":
    main()
