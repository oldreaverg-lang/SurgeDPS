"""
Tests for the NHC RSS Feed Parser and Storm Detector.

Uses synthetic XML that mirrors the real NHC feed structure.
Run with: pytest tests/ -v
"""

import json
import os
import sys
import textwrap
from unittest.mock import MagicMock, patch

import pytest

# Add src to path so imports work
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from storm_detector.config import StormDetectorConfig
from storm_detector.nhc_feed import (
    AdvisoryInfo,
    CycloneInfo,
    NHCFeedParser,
)
from storm_detector.state import AdvisoryStateTracker
from storm_detector.handler import StormDetector, PipelineTrigger
from storm_detector.gis_downloader import NHCGISURLBuilder


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Fixtures
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


SAMPLE_NHC_RSS = textwrap.dedent("""\
    <?xml version="1.0" encoding="UTF-8"?>
    <rss version="2.0"
         xmlns:nhc="https://www.nhc.noaa.gov">
      <channel>
        <title>National Hurricane Center (Tropical Cyclones)</title>
        <item>
          <title>Hurricane Milton Advisory #12</title>
          <link>https://www.nhc.noaa.gov/text/MIATCPAT4.shtml</link>
          <guid>advisory-al142024-012-202410081500</guid>
          <pubDate>Tue, 08 Oct 2024 15:00:00 GMT</pubDate>
          <description>
            ...HURRICANE WARNING ISSUED FOR PORTIONS OF THE FLORIDA COAST...
            A Storm Surge Warning is in effect for Tampa Bay to Englewood.
          </description>
          <nhc:Cyclone>
            <nhc:center>25.4, -86.3</nhc:center>
            <nhc:type>Hurricane</nhc:type>
            <nhc:name>Milton</nhc:name>
            <nhc:wallet>AT4</nhc:wallet>
            <nhc:atcf>AL142024</nhc:atcf>
            <nhc:datetime>1500 UTC Tue Oct 08</nhc:datetime>
            <nhc:movement>ENE at 12 mph</nhc:movement>
            <nhc:pressure>940 mb</nhc:pressure>
            <nhc:wind>150 mph</nhc:wind>
            <nhc:headline>Milton rapidly intensifies into a major hurricane</nhc:headline>
          </nhc:Cyclone>
        </item>
        <item>
          <title>Tropical Depression Fifteen Advisory #2</title>
          <link>https://www.nhc.noaa.gov/text/MIATCPAT5.shtml</link>
          <guid>advisory-al152024-002-202410091200</guid>
          <pubDate>Wed, 09 Oct 2024 12:00:00 GMT</pubDate>
          <description>
            ...TROPICAL DEPRESSION FORMS IN THE CENTRAL ATLANTIC...
          </description>
          <nhc:Cyclone>
            <nhc:center>14.2, -42.8</nhc:center>
            <nhc:type>Tropical Depression</nhc:type>
            <nhc:name>Fifteen</nhc:name>
            <nhc:wallet>AT5</nhc:wallet>
            <nhc:atcf>AL152024</nhc:atcf>
            <nhc:datetime>1200 UTC Wed Oct 09</nhc:datetime>
            <nhc:movement>W at 15 mph</nhc:movement>
            <nhc:pressure>1006 mb</nhc:pressure>
            <nhc:wind>35 mph</nhc:wind>
            <nhc:headline></nhc:headline>
          </nhc:Cyclone>
        </item>
        <item>
          <title>Tropical Weather Outlook</title>
          <link>https://www.nhc.noaa.gov/gtwo.php</link>
          <guid>gtwo-at-202410091200</guid>
          <pubDate>Wed, 09 Oct 2024 12:00:00 GMT</pubDate>
          <description>Active tropical weather in the Atlantic basin.</description>
        </item>
      </channel>
    </rss>
""")


@pytest.fixture
def config():
    """Test configuration (dry-run mode)."""
    os.environ["DRY_RUN"] = "true"
    return StormDetectorConfig()


@pytest.fixture
def parser(config):
    return NHCFeedParser(config)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Config Tests
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestConfig:
    def test_default_basins(self, config):
        assert "at" in config.active_basins

    def test_namespace_dict(self, config):
        ns = config.nhc_namespace
        assert ns == {"nhc": "https://www.nhc.noaa.gov"}

    def test_feed_urls_atlantic(self, config):
        urls = config.feed_urls_for_basin("at")
        assert "index-at.xml" in urls["cyclone_rss"]
        assert "gis-at.xml" in urls["gis_rss"]

    def test_feed_urls_invalid_basin(self, config):
        with pytest.raises(ValueError, match="Unknown basin"):
            config.feed_urls_for_basin("xx")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Feed Parser Tests
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestNHCFeedParser:
    def test_parse_finds_two_cyclones(self, parser):
        """Should find Milton (Hurricane) and Fifteen (TD)."""
        with patch.object(parser.session, "get") as mock_get:
            mock_get.return_value = MagicMock(
                status_code=200, text=SAMPLE_NHC_RSS
            )
            mock_get.return_value.raise_for_status = MagicMock()

            result = parser.fetch_and_parse("at")

        assert result.error is None
        assert len(result.cyclones) == 2
        assert "AL142024" in result.cyclones
        assert "AL152024" in result.cyclones

    def test_parse_cyclone_fields(self, parser):
        """Verify all fields on Hurricane Milton are parsed correctly."""
        with patch.object(parser.session, "get") as mock_get:
            mock_get.return_value = MagicMock(
                status_code=200, text=SAMPLE_NHC_RSS
            )
            mock_get.return_value.raise_for_status = MagicMock()

            result = parser.fetch_and_parse("at")

        milton = result.cyclones["AL142024"]
        assert milton.name == "Milton"
        assert milton.storm_type == "Hurricane"
        assert milton.wallet == "AT4"
        assert milton.atcf_id == "AL142024"
        assert milton.center_lat == pytest.approx(25.4)
        assert milton.center_lon == pytest.approx(-86.3)
        assert milton.pressure_mb == 940
        assert milton.wind_mph == 150
        assert milton.movement == "ENE at 12 mph"

    def test_parse_advisories_count(self, parser):
        """Should find 3 items total (2 cyclone advisories + 1 outlook)."""
        with patch.object(parser.session, "get") as mock_get:
            mock_get.return_value = MagicMock(
                status_code=200, text=SAMPLE_NHC_RSS
            )
            mock_get.return_value.raise_for_status = MagicMock()

            result = parser.fetch_and_parse("at")

        assert len(result.advisories) == 3

    def test_advisory_watch_warning_detection(self, parser):
        """Milton's advisory mentions Hurricane Warning and Storm Surge Warning."""
        with patch.object(parser.session, "get") as mock_get:
            mock_get.return_value = MagicMock(
                status_code=200, text=SAMPLE_NHC_RSS
            )
            mock_get.return_value.raise_for_status = MagicMock()

            result = parser.fetch_and_parse("at")

        milton_advisories = [
            a for a in result.advisories
            if a.cyclone and a.cyclone.atcf_id == "AL142024"
        ]
        assert len(milton_advisories) == 1
        assert milton_advisories[0].has_watch_or_warning is True

    def test_td_has_no_watch_warning(self, parser):
        """Tropical Depression Fifteen has no watch/warning."""
        with patch.object(parser.session, "get") as mock_get:
            mock_get.return_value = MagicMock(
                status_code=200, text=SAMPLE_NHC_RSS
            )
            mock_get.return_value.raise_for_status = MagicMock()

            result = parser.fetch_and_parse("at")

        td_advisories = [
            a for a in result.advisories
            if a.cyclone and a.cyclone.atcf_id == "AL152024"
        ]
        assert len(td_advisories) == 1
        assert td_advisories[0].has_watch_or_warning is False

    def test_outlook_has_no_cyclone(self, parser):
        """The Tropical Weather Outlook item has no nhc:Cyclone element."""
        with patch.object(parser.session, "get") as mock_get:
            mock_get.return_value = MagicMock(
                status_code=200, text=SAMPLE_NHC_RSS
            )
            mock_get.return_value.raise_for_status = MagicMock()

            result = parser.fetch_and_parse("at")

        outlook = [
            a for a in result.advisories
            if "Outlook" in a.title
        ]
        assert len(outlook) == 1
        assert outlook[0].cyclone is None

    def test_http_error_returns_error_result(self, parser):
        """Network failure should return FeedResult with error, not raise."""
        import requests as req

        with patch.object(parser.session, "get") as mock_get:
            mock_get.side_effect = req.RequestException("Connection refused")

            result = parser.fetch_and_parse("at")

        assert result.error is not None
        assert "Connection refused" in result.error
        assert len(result.cyclones) == 0

    def test_storm_id_property(self):
        c = CycloneInfo(
            atcf_id="al142024",
            name="Milton",
            storm_type="Hurricane",
            wallet="AT4",
            basin="at",
        )
        assert c.storm_id == "AL142024"


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Coordinate Parsing Tests
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestCoordinateParsing:
    def test_comma_separated(self, parser):
        lat, lon = parser._parse_center("25.4, -86.3")
        assert lat == pytest.approx(25.4)
        assert lon == pytest.approx(-86.3)

    def test_nsew_format(self, parser):
        lat, lon = parser._parse_center("25.4N 86.3W")
        assert lat == pytest.approx(25.4)
        assert lon == pytest.approx(-86.3)

    def test_south_east(self, parser):
        lat, lon = parser._parse_center("15.2S 45.0E")
        assert lat == pytest.approx(-15.2)
        assert lon == pytest.approx(45.0)

    def test_invalid_returns_none(self, parser):
        lat, lon = parser._parse_center("somewhere in the ocean")
        assert lat is None
        assert lon is None


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# GIS URL Builder Tests
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestGISURLBuilder:
    def test_forecast_cone_url(self):
        url = NHCGISURLBuilder.forecast_cone_url("AL142024", "012")
        assert url == (
            "https://www.nhc.noaa.gov/gis/forecast/archive/"
            "al142024_5day_012.zip"
        )

    def test_watches_warnings_url(self):
        url = NHCGISURLBuilder.watches_warnings_url("AL092025", "003A")
        assert "al092025_ww_wwlin003A.zip" in url

    def test_all_products(self):
        products = NHCGISURLBuilder.all_product_urls("AL142024", "012")
        assert "forecast_cone" in products
        assert "watches_warnings" in products
        assert "wind_field" in products
        assert "surge_watch_warning" in products
        assert all(url.endswith(".zip") for url in products.values())


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# State Tracker Tests
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestAdvisoryStateTracker:
    def test_new_advisory_is_detected(self, tmp_path):
        tracker = AdvisoryStateTracker(
            table_name="test",
            dry_run=True,
            local_state_path=str(tmp_path / "state.json"),
        )
        advisory = AdvisoryInfo(
            title="Test Advisory",
            link="https://example.com",
            guid="test-guid-001",
            pub_date="2024-10-08T15:00:00Z",
        )

        assert tracker.is_new_advisory("AL142024", advisory) is True

    def test_processed_advisory_not_new(self, tmp_path):
        tracker = AdvisoryStateTracker(
            table_name="test",
            dry_run=True,
            local_state_path=str(tmp_path / "state.json"),
        )
        advisory = AdvisoryInfo(
            title="Test Advisory",
            link="https://example.com",
            guid="test-guid-001",
            pub_date="2024-10-08T15:00:00Z",
        )

        tracker.mark_processed("AL142024", advisory)
        assert tracker.is_new_advisory("AL142024", advisory) is False


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Advisory Number Extraction Tests
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestAdvisoryNumberExtraction:
    def _make_advisory(self, title="", guid="", description=""):
        return AdvisoryInfo(
            title=title,
            link="https://example.com",
            guid=guid,
            pub_date="2024-10-08T15:00:00Z",
            description=description,
        )

    def test_from_title_hash(self):
        adv = self._make_advisory(title="Hurricane Milton Advisory #12")
        num = StormDetector._extract_advisory_number(adv)
        assert num == "012"

    def test_from_title_intermediate(self):
        adv = self._make_advisory(title="Hurricane Milton Advisory #12A")
        num = StormDetector._extract_advisory_number(adv)
        assert num == "12A"

    def test_from_guid(self):
        adv = self._make_advisory(
            title="Some title",
            guid="advisory_012_al142024_202410081500",
        )
        num = StormDetector._extract_advisory_number(adv)
        assert num == "012"

    def test_no_number_returns_none(self):
        adv = self._make_advisory(
            title="Tropical Weather Outlook",
            guid="gtwo-at-202410091200",
        )
        num = StormDetector._extract_advisory_number(adv)
        assert num is None
