"""
Tests for NOAA CO-OPS Tide Gauge Integration

Validates station registry, data parsing, tide bias calculation,
GeoJSON export, and pipeline integration.
"""

import json
import os
import sys
import tempfile
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from data_ingest.tide_gauge import (
    TideStation,
    TideReading,
    TideGaugeData,
    TideGaugeResult,
    TideGaugeFetcher,
    GULF_COAST_STATIONS,
)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Fixtures
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


@pytest.fixture
def gulf_storm_geometry():
    """Storm cone covering Louisiana/Mississippi coast."""
    return {
        "type": "Polygon",
        "coordinates": [[
            [-91.0, 28.5],
            [-88.0, 28.5],
            [-87.5, 31.0],
            [-91.5, 31.0],
            [-91.0, 28.5],
        ]]
    }


@pytest.fixture
def texas_storm_geometry():
    """Storm cone covering Texas coast."""
    return {
        "type": "Polygon",
        "coordinates": [[
            [-97.5, 26.0],
            [-94.5, 26.0],
            [-94.0, 30.0],
            [-98.0, 30.0],
            [-97.5, 26.0],
        ]]
    }


@pytest.fixture
def sample_readings():
    """Sample water level observations."""
    return [
        TideReading(time="2026-04-02 12:00", water_level_m=0.35, sigma=0.01),
        TideReading(time="2026-04-02 12:06", water_level_m=0.38, sigma=0.01),
        TideReading(time="2026-04-02 12:12", water_level_m=0.42, sigma=0.02),
    ]


@pytest.fixture
def sample_predictions():
    """Sample tide predictions."""
    return [
        TideReading(time="2026-04-02 13:00", water_level_m=0.50),
        TideReading(time="2026-04-02 14:00", water_level_m=0.65),
        TideReading(time="2026-04-02 15:00", water_level_m=0.45),
    ]


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TestStationRegistry
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestStationRegistry:
    """Tests for the pre-built Gulf Coast station list."""

    def test_registry_has_stations(self):
        assert len(GULF_COAST_STATIONS) >= 20

    def test_station_ids_are_7_chars(self):
        for s in GULF_COAST_STATIONS:
            assert len(s.station_id) == 7, f"{s.name} has ID {s.station_id}"

    def test_stations_have_coordinates(self):
        for s in GULF_COAST_STATIONS:
            assert -100 < s.lon < -79, f"{s.name} lon={s.lon}"
            assert 24 < s.lat < 31, f"{s.name} lat={s.lat}"

    def test_station_states_are_valid(self):
        valid_states = {"TX", "LA", "MS", "AL", "FL"}
        for s in GULF_COAST_STATIONS:
            assert s.state in valid_states, f"{s.name} state={s.state}"

    def test_coordinates_property(self):
        station = GULF_COAST_STATIONS[0]
        coords = station.coordinates
        assert coords == (station.lon, station.lat)

    def test_texas_stations_exist(self):
        tx = [s for s in GULF_COAST_STATIONS if s.state == "TX"]
        assert len(tx) >= 5

    def test_florida_stations_exist(self):
        fl = [s for s in GULF_COAST_STATIONS if s.state == "FL"]
        assert len(fl) >= 5

    def test_louisiana_stations_exist(self):
        la = [s for s in GULF_COAST_STATIONS if s.state == "LA"]
        assert len(la) >= 3


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TestTideDataClasses
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestTideDataClasses:
    """Tests for TideGaugeData and TideGaugeResult data classes."""

    def test_tide_gauge_data_latest_level(self, sample_readings):
        station = TideStation("8761724", "Grand Isle", 29.26, -89.96, "LA")
        gauge = TideGaugeData(station=station, datum="NAVD", readings=sample_readings)
        assert gauge.latest_level_m == 0.42

    def test_tide_gauge_data_no_readings(self):
        station = TideStation("8761724", "Grand Isle", 29.26, -89.96, "LA")
        gauge = TideGaugeData(station=station, datum="NAVD")
        assert gauge.latest_level_m is None

    def test_tide_bias_uses_observation(self, sample_readings, sample_predictions):
        station = TideStation("8761724", "Grand Isle", 29.26, -89.96, "LA")
        gauge = TideGaugeData(
            station=station, datum="NAVD",
            readings=sample_readings, predictions=sample_predictions,
        )
        # Should use observation (0.42), not prediction
        assert gauge.tide_bias_m == 0.42

    def test_tide_bias_falls_back_to_prediction(self, sample_predictions):
        station = TideStation("8761724", "Grand Isle", 29.26, -89.96, "LA")
        gauge = TideGaugeData(
            station=station, datum="NAVD",
            predictions=sample_predictions,
        )
        assert gauge.tide_bias_m == 0.45  # Last prediction

    def test_tide_bias_zero_when_no_data(self):
        station = TideStation("8761724", "Grand Isle", 29.26, -89.96, "LA")
        gauge = TideGaugeData(station=station, datum="NAVD")
        assert gauge.tide_bias_m == 0.0

    def test_max_predicted(self, sample_predictions):
        station = TideStation("8761724", "Grand Isle", 29.26, -89.96, "LA")
        gauge = TideGaugeData(
            station=station, datum="NAVD",
            predictions=sample_predictions,
        )
        assert gauge.max_predicted_m == 0.65

    def test_result_mean_bias(self, sample_readings):
        stations = []
        for i, level in enumerate([0.3, 0.5, 0.4]):
            s = TideStation(f"800000{i}", f"Station {i}", 29.0 + i * 0.1, -89.0, "LA")
            readings = [TideReading(time="2026-04-02 12:00", water_level_m=level)]
            stations.append(TideGaugeData(station=s, datum="NAVD", readings=readings))

        result = TideGaugeResult(stations=stations)
        assert result.station_count == 3
        assert abs(result.mean_tide_bias_m - 0.4) < 0.001

    def test_result_max_bias(self, sample_readings):
        stations = []
        for level in [0.3, 0.8, 0.5]:
            s = TideStation("8000000", "Test", 29.0, -89.0, "LA")
            readings = [TideReading(time="t", water_level_m=level)]
            stations.append(TideGaugeData(station=s, datum="NAVD", readings=readings))

        result = TideGaugeResult(stations=stations)
        assert result.max_tide_bias_m == 0.8

    def test_result_empty(self):
        result = TideGaugeResult()
        assert result.station_count == 0
        assert result.mean_tide_bias_m == 0.0
        assert result.max_tide_bias_m == 0.0


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TestStationFinder
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestStationFinder:
    """Tests for geographic station lookup."""

    def test_finds_gulf_stations(self, gulf_storm_geometry):
        fetcher = TideGaugeFetcher()
        stations = fetcher._find_stations_in_extent(gulf_storm_geometry, 0.5)
        assert len(stations) > 0
        # Should find LA/MS/AL stations
        states = {s.state for s in stations}
        assert "LA" in states or "MS" in states or "AL" in states

    def test_finds_texas_stations(self, texas_storm_geometry):
        fetcher = TideGaugeFetcher()
        stations = fetcher._find_stations_in_extent(texas_storm_geometry, 0.5)
        assert len(stations) > 0
        tx = [s for s in stations if s.state == "TX"]
        assert len(tx) >= 3

    def test_empty_geometry_returns_empty(self):
        fetcher = TideGaugeFetcher()
        geom = {"type": "Polygon", "coordinates": []}
        stations = fetcher._find_stations_in_extent(geom, 0.5)
        assert len(stations) == 0

    def test_far_away_returns_empty(self):
        """Storm in mid-South-Atlantic shouldn't match any stations."""
        fetcher = TideGaugeFetcher()
        # Middle of the South Atlantic — far from any NOAA coastal station
        geom = {
            "type": "Polygon",
            "coordinates": [[
                [-20, -30], [-18, -30], [-18, -28], [-20, -28], [-20, -30]
            ]]
        }
        with patch.object(fetcher, "_query_metadata_api", return_value=[]):
            stations = fetcher._find_stations_in_extent(geom, 0.5)
        assert len(stations) == 0


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TestAPIParsing
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestAPIParsing:
    """Tests for CO-OPS API response parsing."""

    def test_parse_water_level_response(self):
        data = {
            "data": [
                {"t": "2026-04-02 12:00", "v": "0.350", "s": "0.010", "f": "0,0,0,0", "q": "v"},
                {"t": "2026-04-02 12:06", "v": "0.385", "s": "0.012", "f": "0,0,0,0", "q": "v"},
                {"t": "2026-04-02 12:12", "v": "0.420", "s": "0.015", "f": "0,0,0,0", "q": "v"},
            ]
        }
        readings = TideGaugeFetcher._parse_readings(data)
        assert len(readings) == 3
        assert readings[0].water_level_m == 0.35
        assert readings[2].water_level_m == 0.42
        assert readings[0].flags == "0,0,0,0"

    def test_parse_predictions_response(self):
        data = {
            "predictions": [
                {"t": "2026-04-02 13:00", "v": "0.500"},
                {"t": "2026-04-02 14:00", "v": "0.650"},
                {"t": "2026-04-02 15:00", "v": "0.450"},
            ]
        }
        predictions = TideGaugeFetcher._parse_predictions(data)
        assert len(predictions) == 3
        assert predictions[1].water_level_m == 0.65

    def test_parse_empty_response(self):
        assert TideGaugeFetcher._parse_readings({}) == []
        assert TideGaugeFetcher._parse_predictions({}) == []

    def test_parse_invalid_values_skipped(self):
        data = {
            "data": [
                {"t": "2026-04-02 12:00", "v": "0.350"},
                {"t": "2026-04-02 12:06", "v": "bad_value"},
                {"t": "2026-04-02 12:12", "v": "0.420"},
            ]
        }
        readings = TideGaugeFetcher._parse_readings(data)
        assert len(readings) == 2  # Bad value skipped


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TestGeoJSONExport
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestGeoJSONExport:
    """Tests for tide gauge GeoJSON generation."""

    def test_to_geojson_structure(self, sample_readings, sample_predictions):
        station = TideStation("8761724", "Grand Isle", 29.2633, -89.9567, "LA")
        gauge = TideGaugeData(
            station=station, datum="NAVD",
            readings=sample_readings, predictions=sample_predictions,
        )
        result = TideGaugeResult(stations=[gauge])

        geojson = result.to_geojson()
        assert geojson["type"] == "FeatureCollection"
        assert len(geojson["features"]) == 1

    def test_feature_properties(self, sample_readings, sample_predictions):
        station = TideStation("8761724", "Grand Isle", 29.2633, -89.9567, "LA")
        gauge = TideGaugeData(
            station=station, datum="NAVD",
            readings=sample_readings, predictions=sample_predictions,
        )
        result = TideGaugeResult(stations=[gauge])
        geojson = result.to_geojson()

        props = geojson["features"][0]["properties"]
        assert props["station_id"] == "8761724"
        assert props["name"] == "Grand Isle"
        assert props["state"] == "LA"
        assert props["datum"] == "NAVD"
        assert props["has_observation"] is True
        assert props["water_level_m"] == 0.42
        assert props["max_predicted_m"] == 0.65

    def test_feature_geometry(self, sample_readings):
        station = TideStation("8761724", "Grand Isle", 29.2633, -89.9567, "LA")
        gauge = TideGaugeData(
            station=station, datum="NAVD", readings=sample_readings,
        )
        result = TideGaugeResult(stations=[gauge])
        geojson = result.to_geojson()

        geom = geojson["features"][0]["geometry"]
        assert geom["type"] == "Point"
        assert geom["coordinates"] == [-89.9567, 29.2633]

    def test_write_geojson_file(self, sample_readings):
        station = TideStation("8761724", "Grand Isle", 29.2633, -89.9567, "LA")
        gauge = TideGaugeData(
            station=station, datum="NAVD", readings=sample_readings,
        )
        result = TideGaugeResult(stations=[gauge])

        with tempfile.TemporaryDirectory() as d:
            out_path = os.path.join(d, "gauges", "tide.geojson")
            fetcher = TideGaugeFetcher()
            fetcher.write_geojson(result, out_path)

            assert os.path.exists(out_path)
            with open(out_path) as f:
                data = json.load(f)
            assert len(data["features"]) == 1

    def test_empty_result_exports_empty(self):
        result = TideGaugeResult()
        geojson = result.to_geojson()
        assert len(geojson["features"]) == 0

    def test_multiple_stations(self):
        stations = []
        for i, (sid, name, lat, lon) in enumerate([
            ("8761724", "Grand Isle", 29.26, -89.96),
            ("8729108", "Panama City", 30.15, -85.67),
            ("8771450", "Galveston", 29.31, -94.79),
        ]):
            s = TideStation(sid, name, lat, lon, "LA")
            readings = [TideReading(time="t", water_level_m=0.3 + i * 0.1)]
            stations.append(TideGaugeData(station=s, datum="NAVD", readings=readings))

        result = TideGaugeResult(stations=stations)
        geojson = result.to_geojson()
        assert len(geojson["features"]) == 3
        names = [f["properties"]["name"] for f in geojson["features"]]
        assert "Grand Isle" in names
        assert "Panama City" in names
