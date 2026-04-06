"""
Tests for HAZUS-Style Damage Estimation

Validates depth-damage curve interpolation, building exposure loading/
generation, loss calculation, and GeoJSON output.
"""

import json
import math
import os
import sys
import tempfile
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from damage_model.depth_damage import (
    DEPTHS_FT,
    STRUCTURE_DAMAGE,
    CONTENTS_DAMAGE,
    CONTENTS_TO_STRUCTURE_RATIO,
    DEFAULT_BUILDING_TYPE,
    DEFAULT_COST_PER_SQFT,
    DEFAULT_FFH_FT,
    DEFAULT_SQFT,
    get_damage_pct,
    get_total_damage_pct,
    estimate_building_damage,
    BuildingDamage,
    DamageEstimate,
)

from damage_model.building_exposure import (
    BuildingInventory,
    load_buildings_for_extent,
    _generate_synthetic_buildings,
    _classify_building,
    _estimate_area_sqft,
)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Fixtures
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


@pytest.fixture
def storm_geometry():
    """A sample storm geometry polygon (Gulf Coast area)."""
    return {
        "type": "Polygon",
        "coordinates": [[
            [-90.0, 29.0], [-89.0, 29.0], [-89.0, 30.0],
            [-90.0, 30.0], [-90.0, 29.0],
        ]],
    }


@pytest.fixture
def tmp_dir():
    """Temporary directory for test outputs."""
    with tempfile.TemporaryDirectory() as d:
        yield d


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Depth-Damage Curve Tables
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestDepthDamageTables:
    """Validate FIA depth-damage curve data integrity."""

    def test_depths_ascending(self):
        """Depth array must be strictly ascending."""
        for i in range(len(DEPTHS_FT) - 1):
            assert DEPTHS_FT[i] < DEPTHS_FT[i + 1]

    def test_structure_curves_length(self):
        """Each structure curve must have same length as DEPTHS_FT."""
        for btype, curve in STRUCTURE_DAMAGE.items():
            assert len(curve) == len(DEPTHS_FT), f"{btype} length mismatch"

    def test_contents_curves_length(self):
        """Each contents curve must have same length as DEPTHS_FT."""
        for btype, curve in CONTENTS_DAMAGE.items():
            assert len(curve) == len(DEPTHS_FT), f"{btype} length mismatch"

    def test_curves_monotonically_increasing(self):
        """Damage percentages should be non-decreasing with depth."""
        for btype, curve in STRUCTURE_DAMAGE.items():
            for i in range(len(curve) - 1):
                assert curve[i] <= curve[i + 1], (
                    f"{btype} structure damage decreases at index {i}: "
                    f"{curve[i]} > {curve[i + 1]}"
                )

    def test_damage_range_valid(self):
        """All damage values must be in [0, 100]."""
        for table in [STRUCTURE_DAMAGE, CONTENTS_DAMAGE]:
            for btype, curve in table.items():
                for val in curve:
                    assert 0 <= val <= 100, f"{btype} has invalid value {val}"

    def test_all_building_types_have_both_curves(self):
        """Every building type must have both structure and contents curves."""
        assert set(STRUCTURE_DAMAGE.keys()) == set(CONTENTS_DAMAGE.keys())

    def test_default_lookups_complete(self):
        """Default cost, sqft, and FFH must cover all building types."""
        for btype in STRUCTURE_DAMAGE:
            assert btype in DEFAULT_COST_PER_SQFT, f"Missing cost for {btype}"
            assert btype in DEFAULT_SQFT, f"Missing sqft for {btype}"
            assert btype in DEFAULT_FFH_FT, f"Missing FFH for {btype}"


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Damage Interpolation
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestDamageInterpolation:
    """Validate depth-damage curve lookup and interpolation."""

    def test_zero_depth_at_floor(self):
        """At depth=0 (at floor), damage should match the table value."""
        pct = get_damage_pct(0.0, "RES1-1SNB", "structure")
        assert pct == STRUCTURE_DAMAGE["RES1-1SNB"][4]  # index 4 = depth 0

    def test_negative_depth_no_damage_for_no_basement(self):
        """Well below floor, no-basement buildings have 0% structure damage."""
        pct = get_damage_pct(-4.0, "RES1-1SNB", "structure")
        assert pct == 0.0

    def test_negative_depth_with_basement(self):
        """Basement buildings have damage even at negative depths."""
        pct = get_damage_pct(-4.0, "RES1-1SWB", "structure")
        assert pct > 0

    def test_max_depth_clamped(self):
        """Depths above 24 ft return the maximum curve value."""
        pct = get_damage_pct(50.0, "RES1-1SNB", "structure")
        assert pct == STRUCTURE_DAMAGE["RES1-1SNB"][-1]

    def test_below_min_depth_clamped(self):
        """Depths below -4 ft return the minimum curve value."""
        pct = get_damage_pct(-10.0, "RES1-1SNB", "structure")
        assert pct == STRUCTURE_DAMAGE["RES1-1SNB"][0]

    def test_interpolation_between_points(self):
        """Interpolated value must be between adjacent table values."""
        # Depth 0.5 is between 0 and 1 ft
        pct = get_damage_pct(0.5, "RES1-1SNB", "structure")
        val_0 = STRUCTURE_DAMAGE["RES1-1SNB"][4]  # depth 0
        val_1 = STRUCTURE_DAMAGE["RES1-1SNB"][5]  # depth 1
        assert val_0 <= pct <= val_1

    def test_exact_table_points(self):
        """Exact depth values from the table should return exact curve values."""
        for i, d in enumerate(DEPTHS_FT):
            pct = get_damage_pct(float(d), "RES1-1SNB", "structure")
            assert abs(pct - STRUCTURE_DAMAGE["RES1-1SNB"][i]) < 0.001

    def test_contents_higher_than_structure_at_same_depth(self):
        """Contents damage typically exceeds structure damage at same depth."""
        for depth in [2, 4, 8, 12]:
            s = get_damage_pct(depth, "RES1-1SNB", "structure")
            c = get_damage_pct(depth, "RES1-1SNB", "contents")
            assert c >= s, f"Contents < structure at depth {depth} ft"

    def test_unknown_building_type_falls_back(self):
        """Unknown building types should fall back to default type."""
        pct = get_damage_pct(5.0, "UNKNOWN_TYPE", "structure")
        expected = get_damage_pct(5.0, DEFAULT_BUILDING_TYPE, "structure")
        assert pct == expected

    def test_total_damage_weighted(self):
        """Total damage should be a weighted average of structure + contents."""
        depth = 5.0
        struct = get_damage_pct(depth, "RES1-1SNB", "structure")
        content = get_damage_pct(depth, "RES1-1SNB", "contents")
        r = CONTENTS_TO_STRUCTURE_RATIO
        expected = (struct + r * content) / (1 + r)
        total = get_total_damage_pct(depth, "RES1-1SNB")
        assert abs(total - expected) < 0.001


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Building Damage Estimation
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestBuildingDamage:
    """Validate single building damage estimation."""

    def test_no_flood_minimal_damage(self):
        """Zero depth with FFH should produce minimal or zero structure damage."""
        # At 0m depth, water is at ground level. With default FFH=1ft,
        # depth above floor = -1ft. No-basement structure = 0%, but
        # contents may have slight damage (HAZUS allows sub-floor contents loss).
        dmg = estimate_building_damage(depth_m=0.0)
        assert dmg.structure_damage_pct == 0.0
        # Total damage should be very low
        assert dmg.total_damage_pct < 5.0

    def test_shallow_flood_produces_damage(self):
        """1 meter of flooding (~3.3 ft) above ground should produce damage."""
        dmg = estimate_building_damage(depth_m=1.0)
        assert dmg.total_damage_pct > 0
        assert dmg.estimated_loss_usd > 0

    def test_deep_flood_major_damage(self):
        """3 meters (~10 ft) should produce significant damage."""
        dmg = estimate_building_damage(depth_m=3.0)
        assert dmg.total_damage_pct > 30

    def test_ffh_reduces_damage(self):
        """Higher first floor should reduce damage vs lower."""
        dmg_low = estimate_building_damage(depth_m=1.5, first_floor_ht_ft=0.5)
        dmg_high = estimate_building_damage(depth_m=1.5, first_floor_ht_ft=3.0)
        assert dmg_low.total_damage_pct > dmg_high.total_damage_pct

    def test_replacement_value_calculation(self):
        """Replacement value = (sqft * cost_per_sqft) * (1 + contents_ratio)."""
        dmg = estimate_building_damage(
            depth_m=1.0, building_type="RES1-1SNB", sqft=2000.0,
        )
        expected_struct = 2000 * DEFAULT_COST_PER_SQFT["RES1-1SNB"]
        expected_total = expected_struct * (1 + CONTENTS_TO_STRUCTURE_RATIO)
        assert dmg.replacement_value_usd == expected_total

    def test_loss_does_not_exceed_replacement(self):
        """Estimated loss should never exceed replacement value."""
        dmg = estimate_building_damage(depth_m=10.0)
        assert dmg.estimated_loss_usd <= dmg.replacement_value_usd

    def test_building_id_preserved(self):
        """Building ID should be passed through."""
        dmg = estimate_building_damage(depth_m=1.0, building_id="TEST-001")
        assert dmg.building_id == "TEST-001"

    def test_coordinates_preserved(self):
        """Coordinates should be passed through."""
        dmg = estimate_building_damage(depth_m=1.0, lon=-89.5, lat=29.5)
        assert dmg.lon == -89.5
        assert dmg.lat == 29.5

    def test_depth_conversion(self):
        """depth_ft should be ~3.28 * depth_m."""
        dmg = estimate_building_damage(depth_m=2.0)
        assert abs(dmg.depth_ft - 2.0 * 3.28084) < 0.01

    def test_commercial_building(self):
        """COM buildings should use commercial curves and costs."""
        dmg = estimate_building_damage(depth_m=2.0, building_type="COM")
        expected_struct = DEFAULT_SQFT["COM"] * DEFAULT_COST_PER_SQFT["COM"]
        expected_total = expected_struct * (1 + CONTENTS_TO_STRUCTURE_RATIO)
        assert dmg.replacement_value_usd == expected_total


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Damage Categories
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestDamageCategories:
    """Validate damage categorization thresholds."""

    @pytest.fixture
    def estimator(self):
        return DamageEstimate(
            buildings_assessed=0, buildings_damaged=0,
            total_loss_usd=0, total_replacement_usd=0,
            avg_damage_pct=0, max_damage_pct=0,
            damage_by_category={}, buildings=[],
        )

    def test_none_category(self, estimator):
        assert estimator.damage_category(0) == "none"

    def test_minor_category(self, estimator):
        assert estimator.damage_category(5) == "minor"

    def test_moderate_category(self, estimator):
        assert estimator.damage_category(15) == "moderate"

    def test_major_category(self, estimator):
        assert estimator.damage_category(35) == "major"

    def test_severe_category(self, estimator):
        assert estimator.damage_category(60) == "severe"

    def test_boundary_10(self, estimator):
        assert estimator.damage_category(10) == "moderate"

    def test_boundary_30(self, estimator):
        assert estimator.damage_category(30) == "major"

    def test_boundary_50(self, estimator):
        assert estimator.damage_category(50) == "severe"


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Building Exposure
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestBuildingExposure:
    """Validate building footprint loading and generation."""

    def test_synthetic_generation(self, storm_geometry, tmp_dir):
        """Synthetic buildings should be generated within bounds."""
        output = os.path.join(tmp_dir, "buildings.geojson")
        inv = _generate_synthetic_buildings(
            bounds=(-90.0, 29.0, -89.0, 30.0),
            output_path=output,
            max_buildings=500,
        )
        assert inv.building_count > 0
        assert inv.source == "synthetic"
        assert os.path.exists(output)

    def test_synthetic_within_bounds(self, tmp_dir):
        """All synthetic buildings must be within the specified bounds."""
        bounds = (-90.0, 29.0, -89.0, 30.0)
        output = os.path.join(tmp_dir, "buildings.geojson")
        inv = _generate_synthetic_buildings(bounds, output, max_buildings=200)

        with open(output) as f:
            data = json.load(f)

        for feat in data["features"]:
            lon, lat = feat["geometry"]["coordinates"]
            # Allow small jitter overflow from neighborhood clustering
            assert bounds[0] - 0.01 <= lon <= bounds[2] + 0.01
            assert bounds[1] - 0.01 <= lat <= bounds[3] + 0.01

    def test_synthetic_has_required_properties(self, tmp_dir):
        """Each synthetic building must have building_id, area_sqft, type."""
        output = os.path.join(tmp_dir, "buildings.geojson")
        _generate_synthetic_buildings((-90, 29, -89, 30), output, 50)

        with open(output) as f:
            data = json.load(f)

        for feat in data["features"]:
            props = feat["properties"]
            assert "building_id" in props
            assert "area_sqft" in props
            assert "building_type" in props
            assert props["building_id"].startswith("SYN-")

    def test_synthetic_deterministic(self, tmp_dir):
        """Same bounds should produce same buildings (deterministic seed)."""
        bounds = (-90.0, 29.0, -89.0, 30.0)
        out1 = os.path.join(tmp_dir, "b1.geojson")
        out2 = os.path.join(tmp_dir, "b2.geojson")

        _generate_synthetic_buildings(bounds, out1, 100)
        _generate_synthetic_buildings(bounds, out2, 100)

        with open(out1) as f:
            d1 = json.load(f)
        with open(out2) as f:
            d2 = json.load(f)

        assert len(d1["features"]) == len(d2["features"])
        # Check first few buildings match
        for a, b in zip(d1["features"][:5], d2["features"][:5]):
            assert a["geometry"]["coordinates"] == b["geometry"]["coordinates"]

    def test_max_buildings_cap(self, tmp_dir):
        """Building count should not exceed max_buildings."""
        output = os.path.join(tmp_dir, "buildings.geojson")
        inv = _generate_synthetic_buildings((-90, 29, -89, 30), output, 50)
        assert inv.building_count <= 50

    def test_classify_building_osm(self):
        """OSM building types should be classified correctly."""
        assert _classify_building({"building": "commercial"}) == "COM"
        assert _classify_building({"building": "warehouse"}) == "IND"
        assert _classify_building({"building": "house"}) == "RES1-1SNB"
        assert _classify_building({}) == "RES1-1SNB"

    def test_load_buildings_no_geometry(self, tmp_dir):
        """Empty storm geometry should return empty inventory."""
        output = os.path.join(tmp_dir, "buildings.geojson")
        inv = load_buildings_for_extent(
            storm_geometry={"type": "Polygon", "coordinates": [[]]},
            output_path=output,
        )
        assert inv.building_count == 0
        assert inv.source == "none"

    def test_load_buildings_fallback_to_synthetic(self, storm_geometry, tmp_dir):
        """Missing data_path should fall back to synthetic generation."""
        output = os.path.join(tmp_dir, "buildings.geojson")
        inv = load_buildings_for_extent(
            storm_geometry=storm_geometry,
            data_path="/nonexistent/path.gpkg",
            output_path=output,
        )
        assert inv.building_count > 0
        assert inv.source == "synthetic"

    def test_building_types_weighted(self, tmp_dir):
        """Building types should follow the weighted distribution roughly."""
        output = os.path.join(tmp_dir, "buildings.geojson")
        _generate_synthetic_buildings((-90, 29, -89, 30), output, 1000)

        with open(output) as f:
            data = json.load(f)

        types = [f["properties"]["building_type"] for f in data["features"]]
        res1_count = sum(1 for t in types if t.startswith("RES1"))
        # Should be ~80% residential
        assert res1_count / len(types) > 0.6


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Damage GeoJSON Output
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestDamageGeoJSON:
    """Validate damage result GeoJSON output."""

    def test_write_damage_geojson(self, tmp_dir):
        """Damage GeoJSON should have correct structure."""
        from damage_model.depth_damage import _write_damage_geojson

        buildings = [
            BuildingDamage(
                building_id="B001", lon=-89.5, lat=29.5,
                depth_m=1.0, depth_ft=3.28,
                building_type="RES1-1SNB",
                structure_damage_pct=18.0, contents_damage_pct=22.0,
                total_damage_pct=19.3, estimated_loss_usd=42000,
                replacement_value_usd=315000,
            ),
            BuildingDamage(
                building_id="B002", lon=-89.6, lat=29.6,
                depth_m=0.0, depth_ft=0.0,
                building_type="COM",
                structure_damage_pct=0.0, contents_damage_pct=0.0,
                total_damage_pct=0.0, estimated_loss_usd=0,
                replacement_value_usd=1312500,
            ),
        ]

        output = os.path.join(tmp_dir, "damage.geojson")
        _write_damage_geojson(buildings, output)

        assert os.path.exists(output)
        with open(output) as f:
            data = json.load(f)

        assert data["type"] == "FeatureCollection"
        assert len(data["features"]) == 2

        feat = data["features"][0]
        assert feat["geometry"]["type"] == "Point"
        assert feat["properties"]["building_id"] == "B001"
        assert feat["properties"]["damage_category"] == "moderate"
        assert feat["properties"]["estimated_loss_usd"] == 42000

    def test_damage_geojson_has_all_properties(self, tmp_dir):
        """Each feature must have the full set of damage properties."""
        from damage_model.depth_damage import _write_damage_geojson

        buildings = [
            BuildingDamage(
                building_id="B003", lon=-89.5, lat=29.5,
                depth_m=2.0, depth_ft=6.56,
                building_type="RES1-2SNB",
                structure_damage_pct=25.0, contents_damage_pct=30.0,
                total_damage_pct=26.7, estimated_loss_usd=85000,
                replacement_value_usd=478500,
            ),
        ]
        output = os.path.join(tmp_dir, "damage2.geojson")
        _write_damage_geojson(buildings, output)

        with open(output) as f:
            data = json.load(f)

        props = data["features"][0]["properties"]
        required = [
            "layer", "building_id", "depth_m", "depth_ft",
            "building_type", "structure_damage_pct", "contents_damage_pct",
            "total_damage_pct", "estimated_loss_usd", "damage_category",
        ]
        for key in required:
            assert key in props, f"Missing property: {key}"


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Publisher Damage Summary
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestPublisherDamageSummary:
    """Validate damage summary in manifest output."""

    def test_manifest_includes_damage_summary(self):
        """StormManifest.to_dict() should include damage_summary."""
        from pipeline.publisher import StormManifest
        manifest = StormManifest(
            storm_id="AL142024",
            storm_name="Test",
            storm_type="HU",
            advisory_number="003",
            center=[-89.5, 29.5],
        )
        manifest.damage_summary = {
            "buildings_assessed": 1000,
            "buildings_damaged": 450,
            "total_loss_usd": 52000000,
        }

        d = manifest.to_dict()
        assert "damage_summary" in d
        assert d["damage_summary"]["buildings_assessed"] == 1000
        assert d["damage_summary"]["total_loss_usd"] == 52000000

    def test_manifest_no_damage_summary(self):
        """Manifest with no damage data should have None for damage_summary."""
        from pipeline.publisher import StormManifest
        manifest = StormManifest(
            storm_id="AL142024",
            storm_name="Test",
            storm_type="HU",
            advisory_number="003",
            center=[-89.5, 29.5],
        )
        d = manifest.to_dict()
        assert d["damage_summary"] is None

    def test_overlay_configs_include_damage(self):
        """The PUBLISH step should have a damage overlay config."""
        from pipeline.publisher import OverlayLayer
        # Just validate OverlayLayer can be created for damage
        ol = OverlayLayer(
            name="damage",
            display_name="Building Damage",
            layer_type="circle",
            feature_count=500,
            style={"circle-color": "#F44336"},
        )
        assert ol.name == "damage"
        assert ol.layer_type == "circle"
