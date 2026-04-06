"""
Tests for Vector Overlay Tile Builder

Validates GeoJSON generation for storm cone, track line,
NHDPlus reaches, and the combined overlay builder.
"""

import json
import math
import os
import sys
import tempfile

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Fixtures
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


@pytest.fixture
def storm_geometry():
    """A simple polygon representing a storm cone."""
    return {
        "type": "Polygon",
        "coordinates": [[
            [-90.0, 28.0],
            [-88.0, 28.0],
            [-87.0, 30.0],
            [-89.0, 31.0],
            [-91.0, 30.0],
            [-90.0, 28.0],
        ]]
    }


@pytest.fixture
def output_dir():
    """Temporary output directory."""
    with tempfile.TemporaryDirectory() as d:
        yield d


@pytest.fixture
def fake_shapefile_dir(output_dir):
    """
    Create a fake shapefile directory with mock .shp files.

    We can't create real shapefiles without fiona, but we can test
    the fallback behavior when fiona is not available or files don't
    match expected patterns.
    """
    shp_dir = os.path.join(output_dir, "shapefiles")
    os.makedirs(shp_dir, exist_ok=True)
    # Create empty files that look like NHC products
    for name in ["al142024_5day_pgn003.shp", "al142024_5day_lin003.shp",
                 "al142024_5day_pts003.shp", "al142024_5day_pgn003.dbf"]:
        open(os.path.join(shp_dir, name), "w").close()
    return shp_dir


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TestConeGeoJSON
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestConeGeoJSON:
    """Tests for storm cone GeoJSON generation."""

    def test_basic_cone(self, storm_geometry, output_dir):
        from tile_gen.vector_overlays import build_cone_geojson

        out = os.path.join(output_dir, "cone", "cone.geojson")
        result = build_cone_geojson(
            storm_geometry, "AL142024", "003", "Milton", out
        )

        assert os.path.exists(result)
        with open(result) as f:
            data = json.load(f)

        assert data["type"] == "FeatureCollection"
        assert len(data["features"]) == 1
        feat = data["features"][0]
        assert feat["properties"]["storm_id"] == "AL142024"
        assert feat["properties"]["type"] == "forecast_cone"
        assert feat["geometry"]["type"] == "Polygon"

    def test_empty_geometry(self, output_dir):
        from tile_gen.vector_overlays import build_cone_geojson

        out = os.path.join(output_dir, "cone", "empty.geojson")
        result = build_cone_geojson({}, "AL012025", "001", "", out)

        with open(result) as f:
            data = json.load(f)
        assert len(data["features"]) == 0

    def test_null_coordinates(self, output_dir):
        from tile_gen.vector_overlays import build_cone_geojson

        out = os.path.join(output_dir, "cone", "null.geojson")
        geom = {"type": "Polygon", "coordinates": []}
        result = build_cone_geojson(geom, "AL012025", "001", "", out)

        with open(result) as f:
            data = json.load(f)
        assert len(data["features"]) == 0

    def test_properties_populated(self, storm_geometry, output_dir):
        from tile_gen.vector_overlays import build_cone_geojson

        out = os.path.join(output_dir, "cone", "props.geojson")
        build_cone_geojson(storm_geometry, "AL142024", "003", "Milton", out)

        with open(out) as f:
            data = json.load(f)
        props = data["features"][0]["properties"]
        assert props["layer"] == "cone"
        assert props["advisory"] == "003"
        assert props["name"] == "Milton"


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TestWindCategory
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestWindCategory:
    """Tests for wind speed to Saffir-Simpson category conversion."""

    def test_categories(self):
        from tile_gen.vector_overlays import _wind_to_category

        assert _wind_to_category(25) == "TD"
        assert _wind_to_category(34) == "TS"
        assert _wind_to_category(64) == "CAT1"
        assert _wind_to_category(83) == "CAT2"
        assert _wind_to_category(96) == "CAT3"
        assert _wind_to_category(113) == "CAT4"
        assert _wind_to_category(137) == "CAT5"
        assert _wind_to_category(165) == "CAT5"

    def test_boundary_values(self):
        from tile_gen.vector_overlays import _wind_to_category

        assert _wind_to_category(33) == "TD"
        assert _wind_to_category(63) == "TS"
        assert _wind_to_category(82) == "CAT1"
        assert _wind_to_category(95) == "CAT2"
        assert _wind_to_category(112) == "CAT3"
        assert _wind_to_category(136) == "CAT4"

    def test_zero_wind(self):
        from tile_gen.vector_overlays import _wind_to_category

        assert _wind_to_category(0) == "TD"


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TestSyntheticReaches
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestSyntheticReaches:
    """Tests for synthetic river reach generation."""

    def test_generates_reaches(self, storm_geometry, output_dir):
        from tile_gen.vector_overlays import build_reaches_geojson

        out = os.path.join(output_dir, "reaches", "reaches.geojson")
        result = build_reaches_geojson(
            storm_geometry, "", out, reach_ids=None
        )

        assert os.path.exists(result)
        with open(result) as f:
            data = json.load(f)

        assert data["type"] == "FeatureCollection"
        assert len(data["features"]) > 0

    def test_reach_properties(self, storm_geometry, output_dir):
        from tile_gen.vector_overlays import build_reaches_geojson

        out = os.path.join(output_dir, "reaches", "props.geojson")
        build_reaches_geojson(storm_geometry, "", out)

        with open(out) as f:
            data = json.load(f)

        feat = data["features"][0]
        assert feat["properties"]["layer"] == "reach"
        assert "comid" in feat["properties"]
        assert "stream_order" in feat["properties"]
        assert feat["properties"]["synthetic"] is True

    def test_linestring_geometry(self, storm_geometry, output_dir):
        from tile_gen.vector_overlays import build_reaches_geojson

        out = os.path.join(output_dir, "reaches", "geom.geojson")
        build_reaches_geojson(storm_geometry, "", out)

        with open(out) as f:
            data = json.load(f)

        for feat in data["features"]:
            assert feat["geometry"]["type"] == "LineString"
            coords = feat["geometry"]["coordinates"]
            assert len(coords) >= 2

    def test_has_ns_and_ew_reaches(self, storm_geometry, output_dir):
        """Should generate both N-S (main) and E-W (tributary) channels."""
        from tile_gen.vector_overlays import build_reaches_geojson

        out = os.path.join(output_dir, "reaches", "grid.geojson")
        build_reaches_geojson(storm_geometry, "", out)

        with open(out) as f:
            data = json.load(f)

        orders = set(f["properties"]["stream_order"] for f in data["features"])
        assert 3 in orders  # N-S main channels
        assert 1 in orders  # E-W tributaries

    def test_empty_geometry_returns_empty(self, output_dir):
        from tile_gen.vector_overlays import build_reaches_geojson

        geom = {"type": "Polygon", "coordinates": []}
        out = os.path.join(output_dir, "reaches", "empty.geojson")
        build_reaches_geojson(geom, "", out)

        with open(out) as f:
            data = json.load(f)
        assert len(data["features"]) == 0


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TestTrackLine
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestTrackLine:
    """Tests for storm track line extraction and GeoJSON output."""

    def test_no_shapefile_dir(self, output_dir):
        from tile_gen.vector_overlays import extract_track_line

        result = extract_track_line("")
        assert result is None

    def test_nonexistent_dir(self, output_dir):
        from tile_gen.vector_overlays import extract_track_line

        result = extract_track_line("/nonexistent/path")
        assert result is None

    def test_build_track_empty_dir(self, output_dir):
        """Empty shapefile dir should produce empty FeatureCollection."""
        from tile_gen.vector_overlays import build_track_geojson

        empty_dir = os.path.join(output_dir, "empty_shp")
        os.makedirs(empty_dir)

        out = os.path.join(output_dir, "track", "track.geojson")
        build_track_geojson(empty_dir, "AL142024", "003", "Milton", out)

        with open(out) as f:
            data = json.load(f)
        assert data["type"] == "FeatureCollection"
        # No real shapefiles to read, so features may be empty
        assert isinstance(data["features"], list)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TestCombinedOverlays
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestCombinedOverlays:
    """Tests for the combined vector overlay builder."""

    def test_builds_cone_and_reaches(self, storm_geometry, output_dir):
        from tile_gen.vector_overlays import build_vector_overlays

        result = build_vector_overlays(
            output_dir=output_dir,
            storm_id="AL142024",
            advisory_num="003",
            storm_name="Milton",
            storm_geometry=storm_geometry,
        )

        assert "storm_cone" in result.layers
        assert "reaches" in result.layers
        assert result.layers["storm_cone"].feature_count == 1
        assert result.layers["reaches"].feature_count > 0

    def test_cone_geojson_file_exists(self, storm_geometry, output_dir):
        from tile_gen.vector_overlays import build_vector_overlays

        result = build_vector_overlays(
            output_dir=output_dir,
            storm_id="AL142024",
            advisory_num="003",
            storm_geometry=storm_geometry,
        )

        cone = result.layers["storm_cone"]
        assert os.path.exists(cone.geojson_path)

    def test_reaches_geojson_file_exists(self, storm_geometry, output_dir):
        from tile_gen.vector_overlays import build_vector_overlays

        result = build_vector_overlays(
            output_dir=output_dir,
            storm_id="AL142024",
            advisory_num="003",
            storm_geometry=storm_geometry,
        )

        reaches = result.layers["reaches"]
        assert os.path.exists(reaches.geojson_path)

    def test_no_geometry_returns_empty(self, output_dir):
        from tile_gen.vector_overlays import build_vector_overlays

        result = build_vector_overlays(
            output_dir=output_dir,
            storm_id="AL012025",
            advisory_num="001",
        )

        assert len(result.layers) == 0

    def test_layer_names_property(self, storm_geometry, output_dir):
        from tile_gen.vector_overlays import build_vector_overlays

        result = build_vector_overlays(
            output_dir=output_dir,
            storm_id="AL142024",
            advisory_num="003",
            storm_geometry=storm_geometry,
        )

        names = result.layer_names
        assert "storm_cone" in names
        assert "reaches" in names

    def test_overlay_result_dataclass(self):
        from tile_gen.vector_overlays import OverlayResult

        r = OverlayResult(
            layer_name="test",
            geojson_path="/tmp/test.geojson",
            pmtiles_path="/tmp/test.pmtiles",
            feature_count=42,
            size_bytes=1024,
        )
        assert r.layer_name == "test"
        assert r.feature_count == 42

    def test_vector_overlays_result_dataclass(self):
        from tile_gen.vector_overlays import VectorOverlaysResult, OverlayResult

        result = VectorOverlaysResult()
        assert len(result.layers) == 0
        assert result.layer_names == []

        result.layers["test"] = OverlayResult(
            layer_name="test",
            geojson_path="/tmp/test.geojson",
        )
        assert result.layer_names == ["test"]


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TestOverlayManifest
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestOverlayManifest:
    """Tests that overlay layers are correctly represented in the manifest."""

    def test_overlay_layer_dataclass(self):
        from pipeline.publisher import OverlayLayer

        layer = OverlayLayer(
            name="storm_cone",
            display_name="Forecast Cone",
            layer_type="fill",
            tiles_url="storms/AL142024/advisory_003/overlays/storm_cone.pmtiles",
            feature_count=1,
            style={"fill-color": "rgba(255,165,0,0.15)"},
        )
        assert layer.name == "storm_cone"
        assert layer.layer_type == "fill"

    def test_manifest_includes_overlays(self):
        from pipeline.publisher import StormManifest, OverlayLayer

        manifest = StormManifest(
            storm_id="AL142024",
            storm_name="Milton",
            storm_type="Hurricane",
            advisory_number="003",
            center=[-85.0, 26.0],
        )

        manifest.overlays.append(OverlayLayer(
            name="storm_cone",
            display_name="Forecast Cone",
            layer_type="fill",
            feature_count=1,
        ))
        manifest.overlays.append(OverlayLayer(
            name="reaches",
            display_name="River Reaches",
            layer_type="line",
            feature_count=42,
        ))

        d = manifest.to_dict()
        assert "overlays" in d
        assert len(d["overlays"]) == 2
        assert d["overlays"][0]["name"] == "storm_cone"
        assert d["overlays"][1]["feature_count"] == 42

    def test_manifest_empty_overlays(self):
        from pipeline.publisher import StormManifest

        manifest = StormManifest(
            storm_id="AL012025",
            storm_name="Test",
            storm_type="TD",
            advisory_number="001",
            center=[0, 0],
        )

        d = manifest.to_dict()
        assert d["overlays"] == []

    def test_overlay_style_serialized(self):
        from pipeline.publisher import OverlayLayer, StormManifest

        manifest = StormManifest(
            storm_id="AL142024",
            storm_name="Milton",
            storm_type="Hurricane",
            advisory_number="003",
            center=[-85.0, 26.0],
        )

        manifest.overlays.append(OverlayLayer(
            name="storm_track",
            display_name="Forecast Track",
            layer_type="line",
            style={"line-color": "#FF4500", "line-width": 3},
        ))

        d = manifest.to_dict()
        style = d["overlays"][0]["style"]
        assert style["line-color"] == "#FF4500"
        assert style["line-width"] == 3
