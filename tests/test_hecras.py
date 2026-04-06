"""
Tests for HEC-RAS integration module (Tier 3 flood modeling).

Tests cover:
    - Coastal zone configuration and spatial lookup
    - Project template generation
    - Boundary condition injection
    - Synthetic result generation
    - Runner orchestration
    - Result extraction dataclasses

Run with: pytest tests/test_hecras.py -v
"""

import os
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Config & Coastal Zone Tests
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestCoastalZones:

    def test_five_coastal_zones_defined(self):
        from hecras.config import COASTAL_ZONES
        assert len(COASTAL_ZONES) == 5

    def test_zone_names(self):
        from hecras.config import COASTAL_ZONES
        names = set(COASTAL_ZONES.keys())
        assert "gulf_tx" in names
        assert "gulf_fl_west" in names

    def test_zone_bounds_valid(self):
        from hecras.config import COASTAL_ZONES
        for name, zone in COASTAL_ZONES.items():
            w, s, e, n = zone.bounds
            assert w < e, f"{name}: west >= east"
            assert s < n, f"{name}: south >= north"

    def test_zone_for_point_florida(self):
        from hecras.config import HECRASConfig
        config = HECRASConfig()
        zone = config.get_zone_for_point(-82.5, 27.5)
        assert zone is not None
        assert zone.name == "gulf_fl_west"

    def test_zone_for_point_texas(self):
        from hecras.config import HECRASConfig
        config = HECRASConfig()
        zone = config.get_zone_for_point(-95.0, 29.0)
        assert zone is not None
        assert zone.name == "gulf_tx"

    def test_zone_for_point_outside(self):
        from hecras.config import HECRASConfig
        config = HECRASConfig()
        # Point in the middle of the Atlantic
        zone = config.get_zone_for_point(-60.0, 30.0)
        assert zone is None

    def test_zones_for_bounds_overlap(self):
        from hecras.config import HECRASConfig
        config = HECRASConfig()
        # Broad Gulf coast search
        zones = config.get_zones_for_bounds(-95.0, 26.0, -82.0, 30.0)
        assert len(zones) >= 3  # Should match TX, LA, and FL zones


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Template Generator Tests
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestTemplateGenerator:

    def test_generates_all_files(self, tmp_path):
        from hecras.config import COASTAL_ZONES
        from hecras.template_gen import HECRASTemplateGenerator

        zone = COASTAL_ZONES["gulf_fl_west"]
        gen = HECRASTemplateGenerator()
        project = gen.generate(zone, str(tmp_path), storm_id="AL142024")

        assert os.path.exists(project.prj_file)
        assert os.path.exists(project.geom_file)
        assert os.path.exists(project.plan_file)
        assert os.path.exists(project.flow_file)

    def test_prj_file_has_plan_reference(self, tmp_path):
        from hecras.config import COASTAL_ZONES
        from hecras.template_gen import HECRASTemplateGenerator

        zone = COASTAL_ZONES["gulf_tx"]
        gen = HECRASTemplateGenerator()
        project = gen.generate(zone, str(tmp_path))

        with open(project.prj_file) as f:
            content = f.read()
        assert "Current Plan=p01" in content
        assert "Geom File=g01" in content
        assert "Flow File=u01" in content

    def test_geom_file_has_2d_area(self, tmp_path):
        from hecras.config import COASTAL_ZONES
        from hecras.template_gen import HECRASTemplateGenerator

        zone = COASTAL_ZONES["gulf_la"]
        gen = HECRASTemplateGenerator()
        project = gen.generate(zone, str(tmp_path))

        with open(project.geom_file) as f:
            content = f.read()
        assert "2D Flow Area=" in content
        assert "Coastal_BC" in content
        assert "Upstream_BC" in content

    def test_plan_file_has_simulation_settings(self, tmp_path):
        from hecras.config import COASTAL_ZONES
        from hecras.template_gen import HECRASTemplateGenerator

        zone = COASTAL_ZONES["gulf_fl_pan"]
        gen = HECRASTemplateGenerator()
        project = gen.generate(zone, str(tmp_path))

        with open(project.plan_file) as f:
            content = f.read()
        assert "Simulation Date=" in content
        assert "Diffusion Wave" in content
        assert "Map Output Depth= 1" in content

    def test_flow_file_has_boundary_conditions(self, tmp_path):
        from hecras.config import COASTAL_ZONES
        from hecras.template_gen import HECRASTemplateGenerator

        zone = COASTAL_ZONES["gulf_ms_al"]
        gen = HECRASTemplateGenerator()
        project = gen.generate(zone, str(tmp_path))

        with open(project.flow_file) as f:
            content = f.read()
        assert "Stage Hydrograph" in content
        assert "Flow Hydrograph" in content


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Boundary Injector Tests
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestBoundaryInjector:

    def _make_project(self, tmp_path):
        """Create a minimal HEC-RAS project for testing."""
        from hecras.config import COASTAL_ZONES
        from hecras.template_gen import HECRASTemplateGenerator

        zone = COASTAL_ZONES["gulf_fl_west"]
        gen = HECRASTemplateGenerator()
        return gen.generate(zone, str(tmp_path), storm_id="AL142024")

    def test_build_conditions_synthetic(self, tmp_path):
        from hecras.boundary_injector import BoundaryInjector

        self._make_project(tmp_path)
        injector = BoundaryInjector(str(tmp_path))

        bc = injector.build_conditions(
            storm_id="AL142024",
            advisory_num="012",
        )

        assert bc.storm_id == "AL142024"
        assert bc.coastal_bc is not None
        assert bc.upstream_bc is not None
        assert bc.rainfall is not None
        assert bc.max_surge_m > 0

    def test_surge_hydrograph_shape(self, tmp_path):
        from hecras.boundary_injector import BoundaryInjector

        self._make_project(tmp_path)
        injector = BoundaryInjector(str(tmp_path))
        bc = injector.build_conditions("AL142024", "012")

        h = bc.coastal_bc
        # Should ramp up then decay
        assert h.values[0] < h.values[24]  # Rising
        assert h.values[24] > h.values[60]  # Decaying
        # Peak should be near max surge
        assert max(h.values) == pytest.approx(bc.max_surge_m, rel=0.1)

    def test_flow_hydrograph_shape(self, tmp_path):
        from hecras.boundary_injector import BoundaryInjector

        self._make_project(tmp_path)
        injector = BoundaryInjector(str(tmp_path))
        bc = injector.build_conditions("AL142024", "012")

        h = bc.upstream_bc
        # Peak at T+24h
        assert h.values[24] > h.values[0]
        assert h.values[24] > h.values[60]

    def test_inject_rewrites_flow_file(self, tmp_path):
        from hecras.boundary_injector import BoundaryInjector

        project = self._make_project(tmp_path)
        injector = BoundaryInjector(str(tmp_path))

        bc = injector.build_conditions("AL142024", "012")
        flow_path = injector.inject(bc)

        with open(flow_path) as f:
            content = f.read()

        assert "AL142024" in content
        assert "Advisory 012" in content
        assert "Stage Hydrograph" in content
        assert "Flow Hydrograph" in content
        assert "Rain on Grid" in content

    def test_inject_updates_plan_dates(self, tmp_path):
        from hecras.boundary_injector import BoundaryInjector

        project = self._make_project(tmp_path)
        injector = BoundaryInjector(str(tmp_path))

        bc = injector.build_conditions("AL142024", "012")
        injector.inject(bc)

        # Check plan file was updated
        plan_files = list(Path(tmp_path).glob("*.p01"))
        assert len(plan_files) > 0

        with open(plan_files[0]) as f:
            content = f.read()

        # Original placeholder date should be replaced
        assert "01JAN2024" not in content


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Synthetic Results Tests
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestSyntheticResults:

    def test_generates_max_depth_raster(self, tmp_path):
        from hecras.synthetic_results import generate_synthetic_depth

        output_dir = str(tmp_path / "output")
        generate_synthetic_depth(
            dem_path=None,
            surge_path=None,
            output_dir=output_dir,
            storm_id="TEST",
            num_timesteps=3,
        )

        max_depth = Path(output_dir) / "hecras_max_depth.tif"
        assert max_depth.exists()

    def test_generates_timestep_rasters(self, tmp_path):
        from hecras.synthetic_results import generate_synthetic_depth

        output_dir = str(tmp_path / "output")
        generate_synthetic_depth(
            dem_path=None,
            surge_path=None,
            output_dir=output_dir,
            num_timesteps=5,
        )

        tif_files = list(Path(output_dir).glob("*.tif"))
        assert len(tif_files) == 6  # 5 timesteps + 1 max envelope

    def test_max_depth_is_positive(self, tmp_path):
        import rasterio
        from hecras.synthetic_results import generate_synthetic_depth

        output_dir = str(tmp_path / "output")
        generate_synthetic_depth(
            dem_path=None,
            surge_path=None,
            output_dir=output_dir,
            num_timesteps=1,
        )

        with rasterio.open(str(tmp_path / "output" / "hecras_max_depth.tif")) as src:
            data = src.read(1)
            assert np.max(data) > 0

    def test_channel_network_pattern(self):
        from hecras.synthetic_results import _generate_channel_network

        mask = _generate_channel_network(100, 100)
        assert mask.shape == (100, 100)
        assert np.max(mask) > 0
        assert np.min(mask) >= 0


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Runner Tests
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestRunner:

    def test_run_synthetic_mode(self, tmp_path):
        from hecras.runner import HECRASRunner, HECRASRunRequest

        runner = HECRASRunner(work_dir=str(tmp_path))
        request = HECRASRunRequest(
            storm_id="AL142024",
            advisory_num="012",
            storm_center=(-82.5, 27.5),  # Tampa Bay
        )

        result = runner.run(request)

        assert result.success
        assert result.mode == "synthetic"
        assert result.max_depth_m > 0
        assert len(result.depth_rasters) > 0

    def test_run_no_zone_returns_error(self, tmp_path):
        from hecras.runner import HECRASRunner, HECRASRunRequest

        runner = HECRASRunner(work_dir=str(tmp_path))
        request = HECRASRunRequest(
            storm_id="AL142024",
            advisory_num="012",
            storm_center=(-60.0, 30.0),  # Middle of Atlantic
        )

        result = runner.run(request)

        assert not result.success
        assert "No coastal zone" in result.error

    def test_run_result_dataclass(self):
        from hecras.runner import HECRASRunResult

        result = HECRASRunResult(
            success=True,
            storm_id="AL142024",
            advisory_num="012",
            mode="synthetic",
            output_files=[
                "/tmp/hecras_max_depth.tif",
                "/tmp/hecras_depth_t000.tif",
                "/tmp/hecras_depth_t001.tif",
                "/tmp/metadata.json",
            ],
        )

        assert len(result.depth_rasters) == 3
        assert result.depth_rasters[0].endswith(".tif")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Result Extractor Dataclass Tests
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestResultExtractor:

    def test_extracted_result_flooded_pct(self):
        from hecras.result_extractor import ExtractedResult

        result = ExtractedResult(
            output_path="/tmp/test.tif",
            variable="depth",
            nonzero_cells=250,
            total_cells=1000,
        )
        assert result.flooded_pct == 25.0

    def test_extracted_result_zero_cells(self):
        from hecras.result_extractor import ExtractedResult

        result = ExtractedResult(
            output_path="/tmp/test.tif",
            variable="depth",
            total_cells=0,
        )
        assert result.flooded_pct == 0.0

    def test_extraction_result_max_depth(self):
        from hecras.result_extractor import ExtractionResult, ExtractedResult

        result = ExtractionResult(
            storm_id="AL142024",
            advisory_num="012",
            rasters=[
                ExtractedResult("/a.tif", "depth", max_value=3.5),
                ExtractedResult("/b.tif", "depth", timestep=6, max_value=2.1),
                ExtractedResult("/c.tif", "wse", max_value=10.0),
            ],
        )
        # Should only consider depth variable
        assert result.max_depth_m == 3.5

    def test_find_hdf_none(self, tmp_path):
        from hecras.result_extractor import HECRASResultExtractor

        extractor = HECRASResultExtractor(str(tmp_path))
        assert extractor.find_hdf_output() is None

    def test_list_2d_areas_no_file(self, tmp_path):
        from hecras.result_extractor import HECRASResultExtractor

        extractor = HECRASResultExtractor(str(tmp_path))
        assert extractor.list_2d_areas() == []
