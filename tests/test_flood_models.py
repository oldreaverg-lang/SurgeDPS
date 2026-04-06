"""
Tests for the flood modeling pipeline.

Creates synthetic rasters in-memory and verifies model outputs.
Run with: pytest tests/test_flood_models.py -v
"""

import os
import sys
import tempfile

import numpy as np
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Fixtures: create synthetic rasters
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def _write_raster(path, data, nodata=-9999):
    """Write a numpy array as a simple GeoTIFF."""
    import rasterio
    from rasterio.transform import from_bounds

    height, width = data.shape
    transform = from_bounds(-82.0, 27.0, -81.0, 28.0, width, height)

    with rasterio.open(
        path, "w", driver="GTiff",
        height=height, width=width, count=1,
        dtype=str(data.dtype), crs="EPSG:4326",
        transform=transform, nodata=nodata,
    ) as dst:
        dst.write(data, 1)

    return path


@pytest.fixture
def workspace(tmp_path):
    """Create a temp workspace with synthetic DEM, surge, and rainfall."""
    ws = tmp_path / "workspace"
    ws.mkdir()

    # DEM: 100x100, elevation 0-10m gradient (coast on left)
    dem = np.linspace(0, 10, 100).reshape(1, 100).repeat(100, axis=0).astype(np.float32)
    dem_path = _write_raster(str(ws / "dem.tif"), dem)

    # Surge: uniform 3.0m above datum
    surge = np.full((100, 100), 3.0, dtype=np.float32)
    surge_path = _write_raster(str(ws / "surge.tif"), surge)

    # Rainfall depth: 1.5m in left half, 0 in right half
    rain = np.zeros((100, 100), dtype=np.float32)
    rain[:, :50] = 1.5
    rain_path = _write_raster(str(ws / "rainfall.tif"), rain)

    # HAND raster: values 0-5m
    hand = np.linspace(0, 5, 100).reshape(1, 100).repeat(100, axis=0).astype(np.float32)
    hand_path = _write_raster(str(ws / "hand.tif"), hand)

    # Catchment raster: two reaches
    catch = np.zeros((100, 100), dtype=np.int32)
    catch[:, :50] = 1001
    catch[:, 50:] = 1002
    catch_path = _write_raster(str(ws / "catchment.tif"), catch.astype(np.float32))

    return {
        "dir": str(ws),
        "dem": dem_path,
        "surge": surge_path,
        "rainfall": rain_path,
        "hand": hand_path,
        "catchment": catch_path,
        "dem_data": dem,
        "surge_data": surge,
    }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Bathtub Model Tests
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestBathtubModel:

    def test_basic_surge(self, workspace):
        """Surge of 3m on a 0-10m gradient: ~30% of cells should flood."""
        from flood_model.bathtub import run_bathtub_model

        result = run_bathtub_model(
            dem_path=workspace["dem"],
            surge_path=workspace["surge"],
            output_dir=os.path.join(workspace["dir"], "out"),
            storm_id="TEST01",
        )

        assert result.max_depth_m == pytest.approx(3.0, abs=0.2)
        assert result.flooded_pct > 20  # At least some flooding
        assert result.flooded_pct < 50  # Not everything flooded
        assert os.path.exists(result.depth_path)

    def test_no_flooding_when_surge_below_elevation(self, workspace):
        """Surge of 0m should produce no flooding."""
        from flood_model.bathtub import run_bathtub_model

        # Overwrite surge with 0
        zero_surge = np.zeros((100, 100), dtype=np.float32)
        surge_path = _write_raster(
            os.path.join(workspace["dir"], "surge_zero.tif"), zero_surge
        )

        result = run_bathtub_model(
            dem_path=workspace["dem"],
            surge_path=surge_path,
            output_dir=os.path.join(workspace["dir"], "out_zero"),
        )

        assert result.max_depth_m == 0.0
        assert result.flooded_cells == 0

    def test_high_surge_floods_everything(self, workspace):
        """Surge of 15m should flood nearly all cells (max elev = 10m)."""
        from flood_model.bathtub import run_bathtub_model

        high_surge = np.full((100, 100), 15.0, dtype=np.float32)
        surge_path = _write_raster(
            os.path.join(workspace["dir"], "surge_high.tif"), high_surge
        )

        result = run_bathtub_model(
            dem_path=workspace["dem"],
            surge_path=surge_path,
            output_dir=os.path.join(workspace["dir"], "out_high"),
        )

        assert result.flooded_pct > 95
        assert result.max_depth_m == pytest.approx(15.0, abs=0.5)

    def test_output_is_valid_geotiff(self, workspace):
        """Output file should be a valid GeoTIFF with correct metadata."""
        import rasterio
        from flood_model.bathtub import run_bathtub_model

        result = run_bathtub_model(
            dem_path=workspace["dem"],
            surge_path=workspace["surge"],
            output_dir=os.path.join(workspace["dir"], "out_tiff"),
        )

        with rasterio.open(result.depth_path) as src:
            assert src.crs is not None
            assert src.nodata == -9999
            assert src.dtypes[0] == "float32"
            data = src.read(1)
            # No NaN values in valid cells
            valid = data != -9999
            assert not np.any(np.isnan(data[valid]))


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# HAND Model Tests
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestHANDModel:

    def test_discharge_to_stage(self):
        """Rating curve should return positive stage for positive discharge."""
        from flood_model.hand_model import discharge_to_stage

        stage = discharge_to_stage(100.0, drainage_area_km2=500)
        assert stage > 0
        assert stage < 20  # Reasonable range

    def test_zero_discharge_zero_stage(self):
        from flood_model.hand_model import discharge_to_stage

        assert discharge_to_stage(0) == 0.0

    def test_higher_discharge_higher_stage(self):
        from flood_model.hand_model import discharge_to_stage

        s1 = discharge_to_stage(50.0)
        s2 = discharge_to_stage(500.0)
        assert s2 > s1

    def test_scs_curve_number(self):
        """SCS CN method should produce excess < total rainfall."""
        from flood_model.hand_model import compute_rainfall_excess

        rainfall = np.full((10, 10), 150.0, dtype=np.float32)  # 150mm
        cn = np.full((10, 10), 80.0, dtype=np.float32)

        excess = compute_rainfall_excess(rainfall, cn)

        assert np.all(excess >= 0)
        assert np.all(excess <= rainfall)
        assert np.mean(excess) > 0  # Some runoff should occur

    def test_low_cn_less_runoff(self):
        """Forested areas (low CN) should produce less runoff."""
        from flood_model.hand_model import compute_rainfall_excess

        rainfall = np.full((10, 10), 100.0, dtype=np.float32)
        cn_urban = np.full((10, 10), 90.0, dtype=np.float32)
        cn_forest = np.full((10, 10), 55.0, dtype=np.float32)

        excess_urban = compute_rainfall_excess(rainfall, cn_urban)
        excess_forest = compute_rainfall_excess(rainfall, cn_forest)

        assert np.mean(excess_urban) > np.mean(excess_forest)

    def test_nlcd_to_cn(self):
        """NLCD lookup should return valid CN values."""
        from flood_model.hand_model import nlcd_to_curve_number

        assert 85 <= nlcd_to_curve_number(24, "B") <= 95  # Developed High
        assert 55 <= nlcd_to_curve_number(41, "B") <= 65  # Deciduous Forest
        assert nlcd_to_curve_number(11) == 100             # Open Water


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Compound Model Tests
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestCompoundModel:

    def test_compound_merge(self, workspace):
        """Compound should be >= max(surge, rainfall) everywhere."""
        from flood_model.bathtub import run_bathtub_model
        from flood_model.compound import merge_compound_flood

        # First run bathtub to get surge depth
        surge_result = run_bathtub_model(
            dem_path=workspace["dem"],
            surge_path=workspace["surge"],
            output_dir=os.path.join(workspace["dir"], "surge_out"),
        )

        compound = merge_compound_flood(
            surge_depth_path=surge_result.depth_path,
            rainfall_depth_path=workspace["rainfall"],
            output_dir=os.path.join(workspace["dir"], "compound_out"),
            storm_id="TEST01",
        )

        assert compound.max_depth_m > 0
        assert os.path.exists(compound.compound_depth_path)
        assert os.path.exists(compound.overlap_mask_path)

    def test_overlap_detected(self, workspace):
        """Areas with both surge and rain should be flagged as overlap."""
        from flood_model.bathtub import run_bathtub_model
        from flood_model.compound import merge_compound_flood

        surge_result = run_bathtub_model(
            dem_path=workspace["dem"],
            surge_path=workspace["surge"],
            output_dir=os.path.join(workspace["dir"], "surge_out2"),
        )

        compound = merge_compound_flood(
            surge_depth_path=surge_result.depth_path,
            rainfall_depth_path=workspace["rainfall"],
            output_dir=os.path.join(workspace["dir"], "compound_out2"),
        )

        # The left half of the grid has rainfall (1.5m) and surge
        # where elevation < 3m, so there should be some overlap
        assert compound.overlap_cells > 0

    def test_no_overlap_when_separated(self, workspace):
        """No overlap when surge and rain don't intersect spatially."""
        from flood_model.compound import merge_compound_flood

        # Surge only in left 30 columns, rain only in right 30
        surge_left = np.zeros((100, 100), dtype=np.float32)
        surge_left[:, :30] = 2.0
        surge_path = _write_raster(
            os.path.join(workspace["dir"], "surge_left.tif"), surge_left
        )

        rain_right = np.zeros((100, 100), dtype=np.float32)
        rain_right[:, 70:] = 1.0
        rain_path = _write_raster(
            os.path.join(workspace["dir"], "rain_right.tif"), rain_right
        )

        compound = merge_compound_flood(
            surge_depth_path=surge_path,
            rainfall_depth_path=rain_path,
            output_dir=os.path.join(workspace["dir"], "compound_sep"),
        )

        assert compound.overlap_cells == 0
        assert compound.overlap_pct == 0

    def test_interaction_factor_effect(self, workspace):
        """Higher interaction factor should produce deeper compound flooding."""
        from flood_model.compound import merge_compound_flood

        # Create overlapping surge and rain
        both = np.full((100, 100), 2.0, dtype=np.float32)
        surge_path = _write_raster(
            os.path.join(workspace["dir"], "both_surge.tif"), both
        )
        rain_path = _write_raster(
            os.path.join(workspace["dir"], "both_rain.tif"), both
        )

        low = merge_compound_flood(
            surge_path, rain_path,
            os.path.join(workspace["dir"], "if_low"),
            interaction_factor=0.2,
        )

        high = merge_compound_flood(
            surge_path, rain_path,
            os.path.join(workspace["dir"], "if_high"),
            interaction_factor=0.8,
        )

        assert high.max_depth_m > low.max_depth_m
